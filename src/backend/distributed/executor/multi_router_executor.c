/*
 * multi_router_executor.c
 *
 * Routines for executing remote tasks as part of a distributed execution plan
 * with synchronous connections. The routines utilize the connection cache.
 * Therefore, only a single connection is opened for each worker. Also, router
 * executor does not require a master table and a master query. In other words,
 * the results that are fetched from a single worker is sent to the output console
 * directly. Lastly, router executor can only execute a single task.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 */

#include "postgres.h" /* IWYU pragma: keep */
#include "c.h"
#include "fmgr.h" /* IWYU pragma: keep */
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include <string.h>

#include "access/htup.h"
#include "access/sdir.h"
#include "access/transam.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "distributed/citus_clauses.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commit_protocol.h"
#include "distributed/connection_cache.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/relay_utility.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "executor/execdesc.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "executor/tuptable.h"
#include "lib/stringinfo.h"
#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "storage/ipc.h"
#include "storage/lock.h"
#include "tcop/dest.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/hsearch.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/tuplestore.h"


/* controls use of locks to enforce safe commutativity */
bool AllModificationsCommutative = false;


/*
 * The following static variables are necessary to track the progression of
 * multi-statement transactions managed by the router executor. After the first
 * modification within a transaction, the executor populates a hash with the
 * transaction's initial participants (nodes hit by that initial modification).
 *
 * To keep track of the reverse mapping (from shards to nodes), we have a list
 * of XactShardConnSets, which map a shard identifier to a set of connection
 * hash entries. This list is walked by MarkRemainingInactivePlacements to
 * ensure we mark placements as failed if they reject a COMMIT.
 *
 * Beyond that, there's a backend hook to register xact callbacks and a flag to
 * track when a user tries to roll back to a savepoint (not allowed).
 */
static HTAB *xactParticipantHash = NULL;
static List *xactShardConnSetList = NIL;
static bool subXactAbortAttempted = false;

/* functions needed during start phase */
static void InitTransactionStateForTask(Task *task);
static HTAB * CreateXactParticipantHash(void);

/* functions needed during run phase */
static void ReacquireMetadataLocks(List *taskList);
static bool ExecuteSingleTask(QueryDesc *queryDesc, Task *task,
							  bool isModificationQuery, bool expectResults);
static void ExecuteMultipleTasks(QueryDesc *queryDesc, List *taskList,
								 bool isModificationQuery, bool expectResults);
static int64 ExecuteModifyTasks(List *taskList, bool expectResults,
								ParamListInfo paramListInfo,
								MaterialState *routerState,
								TupleDesc tupleDescriptor);
static List * TaskShardIntervalList(List *taskList);
static void AcquireExecutorShardLock(Task *task, CmdType commandType);
static void AcquireExecutorMultiShardLocks(List *taskList);
static bool RequiresConsistentSnapshot(Task *task);
static uint64 ReturnRowsFromTuplestore(uint64 tupleCount, TupleDesc tupleDescriptor,
									   DestReceiver *destination,
									   Tuplestorestate *tupleStore);
static PGconn * GetConnectionForPlacement(ShardPlacement *placement,
										  bool isModificationQuery);
static void PurgeConnectionForPlacement(PGconn *connection, ShardPlacement *placement);
static void RemoveXactConnection(PGconn *connection);
static void ExtractParametersFromParamListInfo(ParamListInfo paramListInfo,
											   Oid **parameterTypes,
											   const char ***parameterValues);
static bool SendQueryInSingleRowMode(PGconn *connection, char *query,
									 ParamListInfo paramListInfo);
static bool StoreQueryResult(MaterialState *routerState, PGconn *connection,
							 TupleDesc tupleDescriptor, bool failOnError, int64 *rows);
static bool ConsumeQueryResult(PGconn *connection, bool failOnError, int64 *rows);
static void RecordShardIdParticipant(uint64 affectedShardId,
									 NodeConnectionEntry *participantEntry);

/* functions needed by callbacks and hooks */
static void RouterTransactionCallback(XactEvent event, void *arg);
static void RouterSubtransactionCallback(SubXactEvent event, SubTransactionId subId,
										 SubTransactionId parentSubid, void *arg);
static void ExecuteTransactionEnd(bool commit);
static void MarkRemainingInactivePlacements(void);


/*
 * RouterExecutorStart sets up the executor state and queryDesc for router
 * execution.
 */
void
RouterExecutorStart(QueryDesc *queryDesc, int eflags, List *taskList)
{
	EState *executorState = NULL;
	CmdType commandType = queryDesc->operation;

	/*
	 * If we are executing a prepared statement, then we may not yet have obtained
	 * the metadata locks in this transaction. To prevent a concurrent shard copy,
	 * we re-obtain them here or error out if a shard copy has already started.
	 *
	 * If a shard copy finishes in between fetching a plan from cache and
	 * re-acquiring the locks, then we might still run a stale plan, which could
	 * cause shard placements to diverge. To minimize this window, we take the
	 * locks as early as possible.
	 */
	ReacquireMetadataLocks(taskList);

	/* disallow triggers during distributed modify commands */
	if (commandType != CMD_SELECT)
	{
		eflags |= EXEC_FLAG_SKIP_TRIGGERS;
	}

	/* signal that it is a router execution */
	eflags |= EXEC_FLAG_CITUS_ROUTER_EXECUTOR;

	/* build empty executor state to obtain per-query memory context */
	executorState = CreateExecutorState();
	executorState->es_top_eflags = eflags;
	executorState->es_instrument = queryDesc->instrument_options;

	queryDesc->estate = executorState;

	/*
	 * As it's similar to what we're doing, use a MaterialState node to store
	 * our state. This is used to store our tuplestore, so cursors etc. can
	 * work.
	 */
	queryDesc->planstate = (PlanState *) makeNode(MaterialState);
}


/*
 * ReacquireMetadataLocks re-acquires the metadata locks that are normally
 * acquired during planning.
 *
 * If we are executing a prepared statement, then planning might have
 * happened in a separate transaction and advisory locks are no longer
 * held. If a shard is currently being repaired/copied/moved, then
 * obtaining the locks will fail and this function throws an error to
 * prevent executing a stale plan.
 *
 * If we are executing a non-prepared statement or planning happened in
 * the same transaction, then we already have the locks and obtain them
 * again here. Since we always release these locks at the end of the
 * transaction, this is effectively a no-op.
 */
static void
ReacquireMetadataLocks(List *taskList)
{
	ListCell *taskCell = NULL;

	/*
	 * Note: to avoid the overhead of additional sorting, we assume tasks
	 * to be already sorted by shard ID such that deadlocks are avoided.
	 * This is true for INSERT/SELECT, which is the only multi-shard
	 * command right now.
	 */

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);

		/*
		 * Only obtain metadata locks for modifications to allow reads to
		 * proceed during shard copy.
		 */
		if (task->taskType == MODIFY_TASK &&
			!TryLockShardDistributionMetadata(task->anchorShardId, ShareLock))
		{
			/*
			 * We could error out immediately to give quick feedback to the
			 * client, but this might complicate flow control and our default
			 * behaviour during shard copy is to block.
			 *
			 * Block until the lock becomes available such that the next command
			 * will likely succeed and use the serialization failure error code
			 * to signal to the client that it should retry the current command.
			 */
			LockShardDistributionMetadata(task->anchorShardId, ShareLock);

			ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
							errmsg("prepared modifications cannot be executed on "
								   "a shard while it is being copied")));
		}
	}
}


/*
 * InitTransactionStateForTask is called during executor start with the first
 * modifying (INSERT/UPDATE/DELETE) task during a transaction. It creates the
 * transaction participant hash, opens connections to this task's nodes, and
 * populates the hash with those connections after sending BEGIN commands to
 * each. If a node fails to respond, its connection is set to NULL to prevent
 * further interaction with it during the transaction.
 */
static void
InitTransactionStateForTask(Task *task)
{
	ListCell *placementCell = NULL;

	xactParticipantHash = CreateXactParticipantHash();

	foreach(placementCell, task->taskPlacementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		NodeConnectionKey participantKey;
		NodeConnectionEntry *participantEntry = NULL;
		bool entryFound = false;

		PGconn *connection = NULL;

		MemSet(&participantKey, 0, sizeof(participantKey));
		strlcpy(participantKey.nodeName, placement->nodeName,
				MAX_NODE_LENGTH + 1);
		participantKey.nodePort = placement->nodePort;

		participantEntry = hash_search(xactParticipantHash, &participantKey,
									   HASH_ENTER, &entryFound);
		Assert(!entryFound);

		connection = GetOrEstablishConnection(placement->nodeName,
											  placement->nodePort);
		if (connection != NULL)
		{
			PGresult *result = PQexec(connection, "BEGIN");
			if (PQresultStatus(result) != PGRES_COMMAND_OK)
			{
				WarnRemoteError(connection, result);
				CloseConnectionByPGconn(connection);

				connection = NULL;
			}

			PQclear(result);
		}

		participantEntry->connection = connection;
	}

	XactModificationLevel = XACT_MODIFICATION_DATA;
}


/*
 * AcquireExecutorShardLock acquires a lock on the shard for the given task and
 * command type if necessary to avoid divergence between multiple replicas of
 * the same shard. No lock is obtained when there is only one replica.
 *
 * The function determines the appropriate lock mode based on the commutativity
 * rule of the command. In each case, it uses a lock mode that enforces the
 * commutativity rule.
 *
 * The mapping is overridden when all_modifications_commutative is set to true.
 * In that case, all modifications are treated as commutative, which can be used
 * to communicate that the application is only generating commutative
 * UPDATE/DELETE/UPSERT commands and exclusive locks are unnecessary.
 */
static void
AcquireExecutorShardLock(Task *task, CmdType commandType)
{
	LOCKMODE lockMode = NoLock;
	int64 shardId = task->anchorShardId;

	if (commandType == CMD_SELECT || list_length(task->taskPlacementList) == 1)
	{
		/*
		 * The executor shard lock is used to maintain consistency between
		 * replicas and therefore no lock is required for read-only queries
		 * or in general when there is only one replica.
		 */

		lockMode = NoLock;
	}
	else if (AllModificationsCommutative)
	{
		/*
		 * Bypass commutativity checks when citus.all_modifications_commutative
		 * is enabled.
		 *
		 * A RowExclusiveLock does not conflict with itself and therefore allows
		 * multiple commutative commands to proceed concurrently. It does
		 * conflict with ExclusiveLock, which may still be obtained by another
		 * session that executes an UPDATE/DELETE/UPSERT command with
		 * citus.all_modifications_commutative disabled.
		 */

		lockMode = RowExclusiveLock;
	}
	else if (task->upsertQuery || commandType == CMD_UPDATE || commandType == CMD_DELETE)
	{
		/*
		 * UPDATE/DELETE/UPSERT commands do not commute with other modifications
		 * since the rows modified by one command may be affected by the outcome
		 * of another command.
		 *
		 * We need to handle upsert before INSERT, because PostgreSQL models
		 * upsert commands as INSERT with an ON CONFLICT section.
		 *
		 * ExclusiveLock conflicts with all lock types used by modifications
		 * and therefore prevents other modifications from running
		 * concurrently.
		 */

		lockMode = ExclusiveLock;
	}
	else if (commandType == CMD_INSERT)
	{
		/*
		 * An INSERT commutes with other INSERT commands, since performing them
		 * out-of-order only affects the table order on disk, but not the
		 * contents.
		 *
		 * When a unique constraint exists, INSERTs are not strictly commutative,
		 * but whichever INSERT comes last will error out and thus has no effect.
		 * INSERT is not commutative with UPDATE/DELETE/UPSERT, since the
		 * UPDATE/DELETE/UPSERT may consider the INSERT, depending on execution
		 * order.
		 *
		 * A RowExclusiveLock does not conflict with itself and therefore allows
		 * multiple INSERT commands to proceed concurrently. It conflicts with
		 * ExclusiveLock obtained by UPDATE/DELETE/UPSERT, ensuring those do
		 * not run concurrently with INSERT.
		 */

		lockMode = RowExclusiveLock;
	}
	else
	{
		ereport(ERROR, (errmsg("unrecognized operation code: %d", (int) commandType)));
	}

	if (shardId != INVALID_SHARD_ID && lockMode != NoLock)
	{
		LockShardResource(shardId, lockMode);
	}

	/*
	 * If the task has a subselect, then we may need to lock the shards from which
	 * the query selects as well to prevent the subselects from seeing different
	 * results on different replicas. In particular this prevents INSERT.. SELECT
	 * commands from having a different effect on different placements.
	 */
	if (RequiresConsistentSnapshot(task))
	{
		/*
		 * ExclusiveLock conflicts with all lock types used by modifications
		 * and therefore prevents other modifications from running
		 * concurrently.
		 */

		LockShardListResources(task->selectShardList, ExclusiveLock);
	}
}


/*
 * AcquireExecutorMultiShardLocks acquires shard locks needed for execution
 * of writes on multiple shards. In addition to honouring commutativity
 * rules, we currently only allow a single multi-shard command on a shard at
 * a time. Otherwise, concurrent multi-shard commands may take row-level
 * locks on the shard placements in a different order and create a distributed
 * deadlock. This applies even when writes are commutative and/or there is
 * no replication.
 *
 * 1. If citus.all_modifications_commutative is set to true, then all locks
 * are acquired as ShareUpdateExclusiveLock.
 *
 * 2. If citus.all_modifications_commutative is false, then only the shards
 * with 2 or more replicas are locked with ExclusiveLock. Otherwise, the
 * lock is acquired with ShareUpdateExclusiveLock.
 *
 * ShareUpdateExclusiveLock conflicts with itself such that only one
 * multi-shard modification at a time is allowed on a shard. It also conflicts
 * with ExclusiveLock, which ensures that updates/deletes/upserts are applied
 * in the same order on all placements. It does not conflict with
 * RowExclusiveLock, which is normally obtained by single-shard, commutative
 * writes.
 */
static void
AcquireExecutorMultiShardLocks(List *taskList)
{
	ListCell *taskCell = NULL;

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		LOCKMODE lockMode = NoLock;

		if (AllModificationsCommutative || list_length(task->taskPlacementList) == 1)
		{
			/*
			 * When all writes are commutative then we only need to prevent multi-shard
			 * commands from running concurrently with each other and with commands
			 * that are explicitly non-commutative. When there is no replication then
			 * we only need to prevent concurrent multi-shard commands.
			 *
			 * In either case, ShareUpdateExclusive has the desired effect, since
			 * it conflicts with itself and ExclusiveLock (taken by non-commutative
			 * writes).
			 */

			lockMode = ShareUpdateExclusiveLock;
		}
		else
		{
			/*
			 * When there is replication, prevent all concurrent writes to the same
			 * shards to ensure the writes are ordered.
			 */

			lockMode = ExclusiveLock;
		}

		LockShardResource(task->anchorShardId, lockMode);

		/*
		 * If the task has a subselect, then we may need to lock the shards from which
		 * the query selects as well to prevent the subselects from seeing different
		 * results on different replicas. In particular this prevents INSERT..SELECT
		 * commands from having different effects on different placements.
		 */

		if (RequiresConsistentSnapshot(task))
		{
			/*
			 * ExclusiveLock conflicts with all lock types used by modifications
			 * and therefore prevents other modifications from running
			 * concurrently.
			 */

			LockShardListResources(task->selectShardList, ExclusiveLock);
		}
	}
}


/*
 * RequiresConsistentSnapshot returns true if the given task need to take
 * the necessary locks to ensure that a subquery in the INSERT ... SELECT
 * query returns the same output for all task placements.
 */
static bool
RequiresConsistentSnapshot(Task *task)
{
	bool requiresIsolation = false;

	if (!task->insertSelectQuery)
	{
		/*
		 * Only INSERT/SELECT commands currently require SELECT isolation.
		 * Other commands do not read from other shards.
		 */

		requiresIsolation = false;
	}
	else if (list_length(task->taskPlacementList) == 1)
	{
		/*
		 * If there is only one replica then we fully rely on PostgreSQL to
		 * provide SELECT isolation. In this case, we do not provide isolation
		 * across the shards, but that was never our intention.
		 */

		requiresIsolation = false;
	}
	else if (AllModificationsCommutative)
	{
		/*
		 * An INSERT/SELECT is commutative with other writes if it excludes
		 * any ongoing writes based on the filter conditions. Without knowing
		 * whether this is true, we assume the user took this into account
		 * when enabling citus.all_modifications_commutative. This option
		 * gives users an escape from aggressive locking during INSERT/SELECT.
		 */

		requiresIsolation = false;
	}
	else
	{
		/*
		 * If this is a non-commutative write, then we need to block ongoing
		 * writes to make sure that the subselect returns the same result
		 * on all placements.
		 */

		requiresIsolation = true;
	}

	return requiresIsolation;
}


/*
 * CreateXactParticipantHash initializes the map used to store the connections
 * needed to process distributed transactions. Unlike the connection cache, we
 * permit NULL connections here to signify that a participant has seen an error
 * and is no longer receiving commands during a transaction. This hash should
 * be walked at transaction end to send final COMMIT or ABORT commands.
 */
static HTAB *
CreateXactParticipantHash(void)
{
	HTAB *xactParticipantHash = NULL;
	HASHCTL info;
	int hashFlags = 0;

	MemSet(&info, 0, sizeof(info));
	info.keysize = sizeof(NodeConnectionKey);
	info.entrysize = sizeof(NodeConnectionEntry);
	info.hcxt = TopTransactionContext;
	hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_BLOBS);

	xactParticipantHash = hash_create("citus xact participant hash", 32, &info,
									  hashFlags);

	return xactParticipantHash;
}


/*
 * RouterExecutorRun actually executes a single task on a worker.
 */
void
RouterExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count)
{
	PlannedStmt *planStatement = queryDesc->plannedstmt;
	MultiPlan *multiPlan = GetMultiPlan(planStatement);
	Job *workerJob = multiPlan->workerJob;
	List *taskList = workerJob->taskList;
	EState *estate = queryDesc->estate;
	CmdType operation = queryDesc->operation;
	MemoryContext oldcontext = NULL;
	DestReceiver *destination = queryDesc->dest;
	MaterialState *routerState = (MaterialState *) queryDesc->planstate;
	bool sendTuples = operation == CMD_SELECT || queryDesc->plannedstmt->hasReturning;

	Assert(estate != NULL);
	Assert(!(estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	if (queryDesc->totaltime != NULL)
	{
		InstrStartNode(queryDesc->totaltime);
	}

	estate->es_processed = 0;

	/* startup the tuple receiver */
	if (sendTuples)
	{
		(*destination->rStartup)(destination, operation, queryDesc->tupDesc);
	}

	/* we only support returning nothing or scanning forward */
	if (ScanDirectionIsNoMovement(direction))
	{
		/* comments in PortalRunSelect() explain the reason for this case */
		goto out;
	}
	else if (!ScanDirectionIsForward(direction))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("scan directions other than forward scans "
							   "are unsupported")));
	}

	/*
	 * If query has not yet been executed, do so now. The main reason why the
	 * query might already have been executed is cursors.
	 */
	if (!routerState->eof_underlying)
	{
		bool isModificationQuery = false;
		bool requiresMasterEvaluation = workerJob->requiresMasterEvaluation;

		if (operation == CMD_INSERT || operation == CMD_UPDATE ||
			operation == CMD_DELETE)
		{
			isModificationQuery = true;
		}
		else if (operation != CMD_SELECT)
		{
			ereport(ERROR, (errmsg("unrecognized operation code: %d",
								   (int) operation)));
		}

		if (requiresMasterEvaluation)
		{
			ListCell *taskCell = NULL;
			Query *query = workerJob->jobQuery;
			Oid relationId = ((RangeTblEntry *) linitial(query->rtable))->relid;

			ExecuteMasterEvaluableFunctions(query);

			foreach(taskCell, taskList)
			{
				Task *task = (Task *) lfirst(taskCell);
				StringInfo newQueryString = makeStringInfo();

				deparse_shard_query(query, relationId, task->anchorShardId,
									newQueryString);

				ereport(DEBUG4, (errmsg("query before master evaluation: %s",
										task->queryString)));
				ereport(DEBUG4, (errmsg("query after master evaluation:  %s",
										newQueryString->data)));

				task->queryString = newQueryString->data;
			}
		}

		if (list_length(taskList) == 1)
		{
			Task *task = (Task *) linitial(taskList);
			bool resultsOK = false;

			resultsOK = ExecuteSingleTask(queryDesc, task, isModificationQuery,
										  sendTuples);
			if (!resultsOK)
			{
				ereport(ERROR, (errmsg("could not receive query results")));
			}
		}
		else
		{
			ExecuteMultipleTasks(queryDesc, taskList, isModificationQuery,
								 sendTuples);
		}

		/* mark underlying query as having executed */
		routerState->eof_underlying = true;
	}

	/* if the underlying query produced output, return it */
	if (routerState->tuplestorestate != NULL)
	{
		TupleDesc resultTupleDescriptor = queryDesc->tupDesc;
		int64 returnedRows = 0;

		/* return rows from the tuplestore */
		returnedRows = ReturnRowsFromTuplestore(count, resultTupleDescriptor,
												destination,
												routerState->tuplestorestate);

		/*
		 * Count tuples processed, if this is a SELECT.  (For modifications
		 * it'll already have been increased, as we want the number of
		 * modified tuples, not the number of RETURNed tuples.)
		 */
		if (operation == CMD_SELECT)
		{
			estate->es_processed += returnedRows;
		}
	}

out:

	/* shutdown tuple receiver, if we started it */
	if (sendTuples)
	{
		(*destination->rShutdown)(destination);
	}

	if (queryDesc->totaltime != NULL)
	{
		InstrStopNode(queryDesc->totaltime, estate->es_processed);
	}

	MemoryContextSwitchTo(oldcontext);
}


/*
 * ExecuteSingleTask executes the task on the remote node, retrieves the
 * results and stores them, if SELECT or RETURNING is used, in a tuple
 * store.
 *
 * If the task fails on one of the placements, the function retries it on
 * other placements (SELECT), reraises the remote error (constraint violation
 * in DML), marks the affected placement as invalid (DML on some placement
 * failed), or errors out (DML failed on all placements).
 */
static bool
ExecuteSingleTask(QueryDesc *queryDesc, Task *task,
				  bool isModificationQuery,
				  bool expectResults)
{
	CmdType operation = queryDesc->operation;
	TupleDesc tupleDescriptor = queryDesc->tupDesc;
	EState *executorState = queryDesc->estate;
	MaterialState *routerState = (MaterialState *) queryDesc->planstate;
	ParamListInfo paramListInfo = queryDesc->params;
	bool resultsOK = false;
	List *taskPlacementList = task->taskPlacementList;
	ListCell *taskPlacementCell = NULL;
	List *failedPlacementList = NIL;
	int64 affectedTupleCount = -1;
	bool gotResults = false;
	char *queryString = task->queryString;

	if (XactModificationLevel == XACT_MODIFICATION_MULTI_SHARD)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("single-shard DML commands must not appear in "
							   "transaction blocks which contain multi-shard data "
							   "modifications")));
	}

	/*
	 * We could naturally handle function-based transactions (i.e. those
	 * using PL/pgSQL or similar) by checking the type of queryDesc->dest,
	 * but some customers already use functions that touch multiple shards
	 * from within a function, so we'll ignore functions for now.
	 */
	if (operation != CMD_SELECT && xactParticipantHash == NULL && IsTransactionBlock())
	{
		InitTransactionStateForTask(task);
	}

	/* prevent replicas of the same shard from diverging */
	AcquireExecutorShardLock(task, operation);

	/*
	 * Try to run the query to completion on one placement. If the query fails
	 * attempt the query on the next placement.
	 */
	foreach(taskPlacementCell, taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		bool queryOK = false;
		bool failOnError = false;
		int64 currentAffectedTupleCount = 0;
		PGconn *connection = GetConnectionForPlacement(taskPlacement,
													   isModificationQuery);

		if (connection == NULL)
		{
			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			continue;
		}

		queryOK = SendQueryInSingleRowMode(connection, queryString, paramListInfo);
		if (!queryOK)
		{
			PurgeConnectionForPlacement(connection, taskPlacement);
			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			continue;
		}

		/*
		 * If caller is interested, store query results the first time
		 * through. The output of the query's execution on other shards is
		 * discarded if we run there (because it's a modification query).
		 */
		if (!gotResults && expectResults)
		{
			queryOK = StoreQueryResult(routerState, connection, tupleDescriptor,
									   failOnError, &currentAffectedTupleCount);
		}
		else
		{
			queryOK = ConsumeQueryResult(connection, failOnError,
										 &currentAffectedTupleCount);
		}

		if (queryOK)
		{
			if ((affectedTupleCount == -1) ||
				(affectedTupleCount == currentAffectedTupleCount))
			{
				affectedTupleCount = currentAffectedTupleCount;
			}
			else
			{
				ereport(WARNING,
						(errmsg("modified "INT64_FORMAT " tuples, but expected "
														"to modify "INT64_FORMAT,
								currentAffectedTupleCount, affectedTupleCount),
						 errdetail("modified placement on %s:%d",
								   taskPlacement->nodeName, taskPlacement->nodePort)));
			}

#if (PG_VERSION_NUM < 90600)

			/* before 9.6, PostgreSQL used a uint32 for this field, so check */
			Assert(currentAffectedTupleCount <= 0xFFFFFFFF);
#endif

			resultsOK = true;
			gotResults = true;

			/*
			 * Modifications have to be executed on all placements, but for
			 * read queries we can stop here.
			 */
			if (!isModificationQuery)
			{
				break;
			}
		}
		else
		{
			PurgeConnectionForPlacement(connection, taskPlacement);

			failedPlacementList = lappend(failedPlacementList, taskPlacement);

			continue;
		}
	}

	if (isModificationQuery)
	{
		ListCell *failedPlacementCell = NULL;

		/* if all placements failed, error out */
		if (list_length(failedPlacementList) == list_length(task->taskPlacementList))
		{
			ereport(ERROR, (errmsg("could not modify any active placements")));
		}

		/* otherwise, mark failed placements as inactive: they're stale */
		foreach(failedPlacementCell, failedPlacementList)
		{
			ShardPlacement *failedPlacement =
				(ShardPlacement *) lfirst(failedPlacementCell);

			UpdateShardPlacementState(failedPlacement->placementId, FILE_INACTIVE);
		}

		executorState->es_processed = affectedTupleCount;
	}

	return resultsOK;
}


/*
 * ExecuteMultipleTasks executes a list of tasks on remote nodes, retrieves
 * the results and, if RETURNING is used, stores them in a tuple store.
 *
 * If a task fails on one of the placements, the transaction rolls back.
 * Otherwise, the changes are committed using 2PC when the local transaction
 * commits.
 */
static void
ExecuteMultipleTasks(QueryDesc *queryDesc, List *taskList,
					 bool isModificationQuery, bool expectResults)
{
	TupleDesc tupleDescriptor = queryDesc->tupDesc;
	EState *executorState = queryDesc->estate;
	MaterialState *routerState = (MaterialState *) queryDesc->planstate;
	ParamListInfo paramListInfo = queryDesc->params;
	int64 affectedTupleCount = -1;

	/* can only support modifications right now */
	Assert(isModificationQuery);

	affectedTupleCount = ExecuteModifyTasks(taskList, expectResults, paramListInfo,
											routerState, tupleDescriptor);

	executorState->es_processed = affectedTupleCount;
}


/*
 * ExecuteModifyTasksWithoutResults provides a wrapper around ExecuteModifyTasks
 * for calls that do not require results. In this case, the expectResults flag
 * is set to false and arguments related to result sets and query parameters are
 * NULL. This function is primarily intended to allow DDL and
 * master_modify_multiple_shards to use the router executor infrastructure.
 */
int64
ExecuteModifyTasksWithoutResults(List *taskList)
{
	return ExecuteModifyTasks(taskList, false, NULL, NULL, NULL);
}


/*
 * ExecuteModifyTasks executes a list of tasks on remote nodes, and
 * optionally retrieves the results and stores them in a tuple store.
 *
 * If a task fails on one of the placements, the transaction rolls back.
 * Otherwise, the changes are committed using 2PC when the local transaction
 * commits.
 */
static int64
ExecuteModifyTasks(List *taskList, bool expectResults, ParamListInfo paramListInfo,
				   MaterialState *routerState, TupleDesc tupleDescriptor)
{
	int64 totalAffectedTupleCount = 0;
	ListCell *taskCell = NULL;
	char *userName = CurrentUserName();
	List *shardIntervalList = NIL;
	List *affectedTupleCountList = NIL;
	bool tasksPending = true;
	int placementIndex = 0;

	if (XactModificationLevel == XACT_MODIFICATION_DATA)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("multi-shard data modifications must not appear in "
							   "transaction blocks which contain single-shard DML "
							   "commands")));
	}

	shardIntervalList = TaskShardIntervalList(taskList);

	/* ensure that there are no concurrent modifications on the same shards */
	AcquireExecutorMultiShardLocks(taskList);

	/* open connection to all relevant placements, if not already open */
	OpenTransactionsToAllShardPlacements(shardIntervalList, userName);

	XactModificationLevel = XACT_MODIFICATION_MULTI_SHARD;

	/* iterate over placements in rounds, to ensure in-order execution */
	while (tasksPending)
	{
		int taskIndex = 0;

		tasksPending = false;

		/* send command to all shard placements with the current index in parallel */
		foreach(taskCell, taskList)
		{
			Task *task = (Task *) lfirst(taskCell);
			int64 shardId = task->anchorShardId;
			char *queryString = task->queryString;
			bool shardConnectionsFound = false;
			ShardConnections *shardConnections = NULL;
			List *connectionList = NIL;
			TransactionConnection *transactionConnection = NULL;
			PGconn *connection = NULL;
			bool queryOK = false;

			shardConnections = GetShardConnections(shardId, &shardConnectionsFound);
			connectionList = shardConnections->connectionList;

			if (placementIndex >= list_length(connectionList))
			{
				/* no more active placements for this task */
				continue;
			}

			transactionConnection =
				(TransactionConnection *) list_nth(connectionList, placementIndex);
			connection = transactionConnection->connection;

			queryOK = SendQueryInSingleRowMode(connection, queryString, paramListInfo);
			if (!queryOK)
			{
				ReraiseRemoteError(connection, NULL);
			}
		}

		/* collects results from all relevant shard placements */
		foreach(taskCell, taskList)
		{
			Task *task = (Task *) lfirst(taskCell);
			int64 shardId = task->anchorShardId;
			bool shardConnectionsFound = false;
			ShardConnections *shardConnections = NULL;
			List *connectionList = NIL;
			TransactionConnection *transactionConnection = NULL;
			PGconn *connection = NULL;
			int64 currentAffectedTupleCount = 0;
			bool failOnError = true;
			bool queryOK PG_USED_FOR_ASSERTS_ONLY = false;

			/* abort in case of cancellation */
			CHECK_FOR_INTERRUPTS();

			shardConnections = GetShardConnections(shardId, &shardConnectionsFound);
			connectionList = shardConnections->connectionList;

			if (placementIndex >= list_length(connectionList))
			{
				/* no more active placements for this task */
				taskIndex++;
				continue;
			}

			transactionConnection =
				(TransactionConnection *) list_nth(connectionList, placementIndex);
			connection = transactionConnection->connection;

			/*
			 * If caller is interested, store query results the first time
			 * through. The output of the query's execution on other shards is
			 * discarded if we run there (because it's a modification query).
			 */
			if (placementIndex == 0 && expectResults)
			{
				Assert(routerState != NULL && tupleDescriptor != NULL);

				queryOK = StoreQueryResult(routerState, connection, tupleDescriptor,
										   failOnError, &currentAffectedTupleCount);
			}
			else
			{
				queryOK = ConsumeQueryResult(connection, failOnError,
											 &currentAffectedTupleCount);
			}

			/* should have rolled back on error */
			Assert(queryOK);

			if (placementIndex == 0)
			{
				totalAffectedTupleCount += currentAffectedTupleCount;

				/* keep track of the initial affected tuple count */
				affectedTupleCountList = lappend_int(affectedTupleCountList,
													 currentAffectedTupleCount);
			}
			else
			{
				/* warn the user if shard placements have diverged */
				int64 previousAffectedTupleCount = list_nth_int(affectedTupleCountList,
																taskIndex);

				if (currentAffectedTupleCount != previousAffectedTupleCount)
				{
					char *nodeName = ConnectionGetOptionValue(connection, "host");
					char *nodePort = ConnectionGetOptionValue(connection, "port");

					ereport(WARNING,
							(errmsg("modified "INT64_FORMAT " tuples of shard "
									UINT64_FORMAT ", but expected to modify "INT64_FORMAT,
									currentAffectedTupleCount, shardId,
									previousAffectedTupleCount),
							 errdetail("modified placement on %s:%s", nodeName,
									   nodePort)));
				}
			}

			if (!tasksPending && placementIndex + 1 < list_length(connectionList))
			{
				/* more tasks to be done after thise one */
				tasksPending = true;
			}

			taskIndex++;
		}

		placementIndex++;
	}

	CHECK_FOR_INTERRUPTS();

	return totalAffectedTupleCount;
}


/*
 * TaskShardIntervalList returns a list of shard intervals for a given list of
 * tasks.
 */
static List *
TaskShardIntervalList(List *taskList)
{
	ListCell *taskCell = NULL;
	List *shardIntervalList = NIL;

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);
		int64 shardId = task->anchorShardId;
		ShardInterval *shardInterval = LoadShardInterval(shardId);

		shardIntervalList = lappend(shardIntervalList, shardInterval);
	}

	return shardIntervalList;
}


/*
 * ReturnRowsFromTuplestore moves rows from a given tuplestore into a
 * receiver. It performs the necessary limiting to support cursors.
 */
static uint64
ReturnRowsFromTuplestore(uint64 tupleCount, TupleDesc tupleDescriptor,
						 DestReceiver *destination, Tuplestorestate *tupleStore)
{
	TupleTableSlot *tupleTableSlot = NULL;
	uint64 currentTupleCount = 0;

	tupleTableSlot = MakeSingleTupleTableSlot(tupleDescriptor);

	/* iterate over tuples in tuple store, and send them to destination */
	for (;;)
	{
		bool nextTuple = tuplestore_gettupleslot(tupleStore, true, false, tupleTableSlot);
		if (!nextTuple)
		{
			break;
		}

		(*destination->receiveSlot)(tupleTableSlot, destination);

		ExecClearTuple(tupleTableSlot);

		currentTupleCount++;

		/*
		 * If numberTuples is zero fetch all tuples, otherwise stop after
		 * count tuples.
		 */
		if (tupleCount > 0 && tupleCount == currentTupleCount)
		{
			break;
		}
	}

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	return currentTupleCount;
}


/*
 * GetConnectionForPlacement is the main entry point for acquiring a connection
 * within the router executor. By using placements (rather than node names and
 * ports) to identify connections, the router executor can keep track of shards
 * used by multi-statement transactions and error out if a transaction tries
 * to reach a new node altogether). In the single-statement commands context,
 * GetConnectionForPlacement simply falls through to  GetOrEstablishConnection.
 */
static PGconn *
GetConnectionForPlacement(ShardPlacement *placement, bool isModificationQuery)
{
	NodeConnectionKey participantKey;
	NodeConnectionEntry *participantEntry = NULL;
	bool entryFound = false;

	/* if not in a transaction, fall through to connection cache */
	if (xactParticipantHash == NULL)
	{
		PGconn *connection = GetOrEstablishConnection(placement->nodeName,
													  placement->nodePort);

		return connection;
	}

	Assert(IsTransactionBlock());

	MemSet(&participantKey, 0, sizeof(participantKey));
	strlcpy(participantKey.nodeName, placement->nodeName, MAX_NODE_LENGTH + 1);
	participantKey.nodePort = placement->nodePort;

	participantEntry = hash_search(xactParticipantHash, &participantKey, HASH_FIND,
								   &entryFound);

	if (entryFound)
	{
		if (isModificationQuery)
		{
			RecordShardIdParticipant(placement->shardId, participantEntry);
		}

		return participantEntry->connection;
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_DOES_NOT_EXIST),
						errmsg("no transaction participant matches %s:%d",
							   placement->nodeName, placement->nodePort),
						errdetail("Transactions which modify distributed tables may only "
								  "target nodes affected by the modification command "
								  "which began the transaction.")));
	}
}


/*
 * PurgeConnectionForPlacement provides a way to purge an invalid connection
 * from all relevant connection hashes using the placement involved in the
 * query at the time of the error. If a transaction is ongoing, this function
 * ensures the right node's connection is set to NULL in the participant map
 * for the transaction in addition to purging the connection cache's entry.
 */
static void
PurgeConnectionForPlacement(PGconn *connection, ShardPlacement *placement)
{
	CloseConnectionByPGconn(connection);

	/*
	 * The following is logically identical to RemoveXactConnection, but since
	 * we have a ShardPlacement to help build a NodeConnectionKey, we avoid
	 * any penalty incurred by calling BuildKeyForConnection, which must ex-
	 * tract host, port, and user from the connection options list.
	 */
	if (xactParticipantHash != NULL)
	{
		NodeConnectionEntry *participantEntry = NULL;
		bool entryFound = false;
		NodeConnectionKey nodeKey;
		char *currentUser = CurrentUserName();

		MemSet(&nodeKey, 0, sizeof(NodeConnectionKey));
		strlcpy(nodeKey.nodeName, placement->nodeName, MAX_NODE_LENGTH + 1);
		nodeKey.nodePort = placement->nodePort;
		strlcpy(nodeKey.nodeUser, currentUser, NAMEDATALEN);
		Assert(IsTransactionBlock());

		/* the participant hash doesn't use the user field */
		MemSet(&nodeKey.nodeUser, 0, sizeof(nodeKey.nodeUser));
		participantEntry = hash_search(xactParticipantHash, &nodeKey, HASH_FIND,
									   &entryFound);

		Assert(entryFound);

		participantEntry->connection = NULL;
	}
}


/*
 * Removes a given connection from the transaction participant hash, based on
 * the host and port of the provided connection. If the hash is not NULL, it
 * MUST contain the provided connection, or a FATAL error is raised.
 */
static void
RemoveXactConnection(PGconn *connection)
{
	NodeConnectionKey nodeKey;
	NodeConnectionEntry *participantEntry = NULL;
	bool entryFound = false;

	if (xactParticipantHash == NULL)
	{
		return;
	}

	BuildKeyForConnection(connection, &nodeKey);

	/* the participant hash doesn't use the user field */
	MemSet(&nodeKey.nodeUser, 0, sizeof(nodeKey.nodeUser));
	participantEntry = hash_search(xactParticipantHash, &nodeKey, HASH_FIND,
								   &entryFound);

	if (!entryFound)
	{
		ereport(FATAL, (errmsg("could not find specified transaction connection")));
	}

	participantEntry->connection = NULL;
}


/*
 * SendQueryInSingleRowMode sends the given query on the connection in an
 * asynchronous way. The function also sets the single-row mode on the
 * connection so that we receive results a row at a time.
 */
static bool
SendQueryInSingleRowMode(PGconn *connection, char *query, ParamListInfo paramListInfo)
{
	int querySent = 0;
	int singleRowMode = 0;

	if (paramListInfo != NULL)
	{
		int parameterCount = paramListInfo->numParams;
		Oid *parameterTypes = NULL;
		const char **parameterValues = NULL;

		ExtractParametersFromParamListInfo(paramListInfo, &parameterTypes,
										   &parameterValues);

		querySent = PQsendQueryParams(connection, query, parameterCount, parameterTypes,
									  parameterValues, NULL, NULL, 0);
	}
	else
	{
		querySent = PQsendQuery(connection, query);
	}

	if (querySent == 0)
	{
		WarnRemoteError(connection, NULL);
		return false;
	}

	singleRowMode = PQsetSingleRowMode(connection);
	if (singleRowMode == 0)
	{
		WarnRemoteError(connection, NULL);
		return false;
	}

	return true;
}


/*
 * ExtractParametersFromParamListInfo extracts parameter types and values from
 * the given ParamListInfo structure, and fills parameter type and value arrays.
 */
static void
ExtractParametersFromParamListInfo(ParamListInfo paramListInfo, Oid **parameterTypes,
								   const char ***parameterValues)
{
	int parameterIndex = 0;
	int parameterCount = paramListInfo->numParams;

	*parameterTypes = (Oid *) palloc0(parameterCount * sizeof(Oid));
	*parameterValues = (const char **) palloc0(parameterCount * sizeof(char *));

	/* get parameter types and values */
	for (parameterIndex = 0; parameterIndex < parameterCount; parameterIndex++)
	{
		ParamExternData *parameterData = &paramListInfo->params[parameterIndex];
		Oid typeOutputFunctionId = InvalidOid;
		bool variableLengthType = false;

		/*
		 * Use 0 for data types where the oid values can be different on
		 * the master and worker nodes. Therefore, the worker nodes can
		 * infer the correct oid.
		 */
		if (parameterData->ptype >= FirstNormalObjectId)
		{
			(*parameterTypes)[parameterIndex] = 0;
		}
		else
		{
			(*parameterTypes)[parameterIndex] = parameterData->ptype;
		}

		/*
		 * If the parameter is not referenced / used (ptype == 0) and
		 * would otherwise have errored out inside standard_planner()),
		 * don't pass a value to the remote side, and pass text oid to prevent
		 * undetermined data type errors on workers.
		 */
		if (parameterData->ptype == 0)
		{
			(*parameterValues)[parameterIndex] = NULL;
			(*parameterTypes)[parameterIndex] = TEXTOID;

			continue;
		}

		/*
		 * If the parameter is NULL then we preserve its type, but
		 * don't need to evaluate its value.
		 */
		if (parameterData->isnull)
		{
			(*parameterValues)[parameterIndex] = NULL;

			continue;
		}

		getTypeOutputInfo(parameterData->ptype, &typeOutputFunctionId,
						  &variableLengthType);

		(*parameterValues)[parameterIndex] = OidOutputFunctionCall(typeOutputFunctionId,
																   parameterData->value);
	}
}


/*
 * StoreQueryResult gets the query results from the given connection, builds
 * tuples from the results, and stores them in the a newly created
 * tuple-store. If the function can't receive query results, it returns
 * false. Note that this function assumes the query has already been sent on
 * the connection.
 */
static bool
StoreQueryResult(MaterialState *routerState, PGconn *connection,
				 TupleDesc tupleDescriptor, bool failOnError, int64 *rows)
{
	AttInMetadata *attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
	Tuplestorestate *tupleStore = NULL;
	uint32 expectedColumnCount = tupleDescriptor->natts;
	char **columnArray = (char **) palloc0(expectedColumnCount * sizeof(char *));
	bool commandFailed = false;
	MemoryContext ioContext = AllocSetContextCreate(CurrentMemoryContext,
													"StoreQueryResult",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);
	*rows = 0;

	if (routerState->tuplestorestate == NULL)
	{
		routerState->tuplestorestate = tuplestore_begin_heap(false, false, work_mem);
	}
	else if (!failOnError)
	{
		/* might have failed query execution on another placement before */
		tuplestore_clear(routerState->tuplestorestate);
	}

	tupleStore = routerState->tuplestorestate;

	for (;;)
	{
		uint32 rowIndex = 0;
		uint32 columnIndex = 0;
		uint32 rowCount = 0;
		uint32 columnCount = 0;
		ExecStatusType resultStatus = 0;

		PGresult *result = PQgetResult(connection);
		if (result == NULL)
		{
			break;
		}

		resultStatus = PQresultStatus(result);
		if ((resultStatus != PGRES_SINGLE_TUPLE) && (resultStatus != PGRES_TUPLES_OK))
		{
			char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
			int category = 0;
			bool isConstraintViolation = false;

			/*
			 * If the error code is in constraint violation class, we want to
			 * fail fast because we must get the same error from all shard
			 * placements.
			 */
			category = ERRCODE_TO_CATEGORY(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION);
			isConstraintViolation = SqlStateMatchesCategory(sqlStateString, category);

			if (isConstraintViolation || failOnError)
			{
				RemoveXactConnection(connection);
				ReraiseRemoteError(connection, result);
			}
			else
			{
				WarnRemoteError(connection, result);
			}

			PQclear(result);

			commandFailed = true;

			/* continue, there could be other lingering results due to row mode */
			continue;
		}

		rowCount = PQntuples(result);
		columnCount = PQnfields(result);
		Assert(columnCount == expectedColumnCount);

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			HeapTuple heapTuple = NULL;
			MemoryContext oldContext = NULL;
			memset(columnArray, 0, columnCount * sizeof(char *));

			for (columnIndex = 0; columnIndex < columnCount; columnIndex++)
			{
				if (PQgetisnull(result, rowIndex, columnIndex))
				{
					columnArray[columnIndex] = NULL;
				}
				else
				{
					columnArray[columnIndex] = PQgetvalue(result, rowIndex, columnIndex);
				}
			}

			/*
			 * Switch to a temporary memory context that we reset after each tuple. This
			 * protects us from any memory leaks that might be present in I/O functions
			 * called by BuildTupleFromCStrings.
			 */
			oldContext = MemoryContextSwitchTo(ioContext);

			heapTuple = BuildTupleFromCStrings(attributeInputMetadata, columnArray);

			MemoryContextSwitchTo(oldContext);

			tuplestore_puttuple(tupleStore, heapTuple);
			MemoryContextReset(ioContext);
			(*rows)++;
		}

		PQclear(result);
	}

	pfree(columnArray);

	return !commandFailed;
}


/*
 * ConsumeQueryResult gets a query result from a connection, counting the rows
 * and checking for errors, but otherwise discarding potentially returned
 * rows.  Returns true if a non-error result has been returned, false if there
 * has been an error.
 */
static bool
ConsumeQueryResult(PGconn *connection, bool failOnError, int64 *rows)
{
	bool commandFailed = false;
	bool gotResponse = false;

	*rows = 0;

	/*
	 * Due to single row mode we have to do multiple PQgetResult() to finish
	 * processing of this query, even without RETURNING. For single-row mode
	 * we have to loop until all rows are consumed.
	 */
	while (true)
	{
		PGresult *result = PQgetResult(connection);
		ExecStatusType status = PGRES_COMMAND_OK;

		if (result == NULL)
		{
			break;
		}

		status = PQresultStatus(result);

		if (status != PGRES_COMMAND_OK &&
			status != PGRES_SINGLE_TUPLE &&
			status != PGRES_TUPLES_OK)
		{
			char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
			int category = 0;
			bool isConstraintViolation = false;

			/*
			 * If the error code is in constraint violation class, we want to
			 * fail fast because we must get the same error from all shard
			 * placements.
			 */
			category = ERRCODE_TO_CATEGORY(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION);
			isConstraintViolation = SqlStateMatchesCategory(sqlStateString, category);

			if (isConstraintViolation || failOnError)
			{
				RemoveXactConnection(connection);
				ReraiseRemoteError(connection, result);
			}
			else
			{
				WarnRemoteError(connection, result);
			}
			PQclear(result);

			commandFailed = true;

			/* continue, there could be other lingering results due to row mode */
			continue;
		}

		if (status == PGRES_COMMAND_OK)
		{
			char *currentAffectedTupleString = PQcmdTuples(result);
			int64 currentAffectedTupleCount = 0;

			if (*currentAffectedTupleString != '\0')
			{
				scanint8(currentAffectedTupleString, false, &currentAffectedTupleCount);
				Assert(currentAffectedTupleCount >= 0);
			}

#if (PG_VERSION_NUM < 90600)

			/* before 9.6, PostgreSQL used a uint32 for this field, so check */
			Assert(currentAffectedTupleCount <= 0xFFFFFFFF);
#endif
			*rows += currentAffectedTupleCount;
		}
		else
		{
			*rows += PQntuples(result);
		}

		PQclear(result);
		gotResponse = true;
	}

	return gotResponse && !commandFailed;
}


/*
 * RecordShardIdParticipant registers a connection as being involved with a
 * particular shard during a multi-statement transaction.
 */
static void
RecordShardIdParticipant(uint64 affectedShardId, NodeConnectionEntry *participantEntry)
{
	XactShardConnSet *shardConnSetMatch = NULL;
	ListCell *listCell = NULL;
	MemoryContext oldContext = NULL;
	List *connectionEntryList = NIL;

	/* check whether an entry already exists for this shard */
	foreach(listCell, xactShardConnSetList)
	{
		XactShardConnSet *shardConnSet = (XactShardConnSet *) lfirst(listCell);

		if (shardConnSet->shardId == affectedShardId)
		{
			shardConnSetMatch = shardConnSet;
		}
	}

	/* entries must last through the whole top-level transaction */
	oldContext = MemoryContextSwitchTo(TopTransactionContext);

	/* if no entry found, make one */
	if (shardConnSetMatch == NULL)
	{
		shardConnSetMatch = (XactShardConnSet *) palloc0(sizeof(XactShardConnSet));
		shardConnSetMatch->shardId = affectedShardId;

		xactShardConnSetList = lappend(xactShardConnSetList, shardConnSetMatch);
	}

	/* add connection, avoiding duplicates */
	connectionEntryList = shardConnSetMatch->connectionEntryList;
	shardConnSetMatch->connectionEntryList = list_append_unique_ptr(connectionEntryList,
																	participantEntry);

	MemoryContextSwitchTo(oldContext);
}


/*
 * RouterExecutorFinish cleans up after a distributed execution.
 */
void
RouterExecutorFinish(QueryDesc *queryDesc)
{
	EState *estate = queryDesc->estate;
	Assert(estate != NULL);

	estate->es_finished = true;
}


/*
 * RouterExecutorEnd cleans up the executor state after a distributed
 * execution.
 */
void
RouterExecutorEnd(QueryDesc *queryDesc)
{
	EState *estate = queryDesc->estate;
	MaterialState *routerState = (MaterialState *) queryDesc->planstate;

	if (routerState->tuplestorestate)
	{
		tuplestore_end(routerState->tuplestorestate);
	}

	Assert(estate != NULL);

	FreeExecutorState(estate);
	queryDesc->estate = NULL;
	queryDesc->totaltime = NULL;
}


/*
 * RegisterRouterExecutorXactCallbacks registers this executor's callbacks.
 */
void
RegisterRouterExecutorXactCallbacks(void)
{
	RegisterXactCallback(RouterTransactionCallback, NULL);
	RegisterSubXactCallback(RouterSubtransactionCallback, NULL);
}


/*
 * RouterTransactionCallback handles committing or aborting remote transactions
 * after the local one has committed or aborted. It only sends COMMIT or ABORT
 * commands to still-healthy remotes; the failed ones are marked as inactive if
 * after a successful COMMIT (no need to mark on ABORTs).
 */
static void
RouterTransactionCallback(XactEvent event, void *arg)
{
	if (XactModificationLevel != XACT_MODIFICATION_DATA)
	{
		return;
	}

	switch (event)
	{
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_COMMIT:
		{
			break;
		}

		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_ABORT:
		{
			bool commit = false;

			ExecuteTransactionEnd(commit);

			break;
		}

		/* no support for prepare with multi-statement transactions */
		case XACT_EVENT_PREPARE:
		case XACT_EVENT_PRE_PREPARE:
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot prepare a transaction that modified "
								   "distributed tables")));

			break;
		}

		case XACT_EVENT_PARALLEL_PRE_COMMIT:
		case XACT_EVENT_PRE_COMMIT:
		{
			bool commit = true;

			if (subXactAbortAttempted)
			{
				subXactAbortAttempted = false;

				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot ROLLBACK TO SAVEPOINT in transactions "
									   "which modify distributed tables")));
			}

			ExecuteTransactionEnd(commit);
			MarkRemainingInactivePlacements();

			/* leave early to avoid resetting transaction state */
			return;
		}
	}

	/* reset transaction state */
	xactParticipantHash = NULL;
	xactShardConnSetList = NIL;
	subXactAbortAttempted = false;
}


/*
 * RouterSubtransactionCallback silently keeps track of any attempt to ROLLBACK
 * TO SAVEPOINT, which is not permitted by this executor. At transaction end,
 * the executor checks whether such a rollback was attempted and, if so, errors
 * out entirely (with an appropriate message).
 *
 * This implementation permits savepoints so long as no rollbacks occur.
 */
static void
RouterSubtransactionCallback(SubXactEvent event, SubTransactionId subId,
							 SubTransactionId parentSubid, void *arg)
{
	if ((xactParticipantHash != NULL) && (event == SUBXACT_EVENT_ABORT_SUB))
	{
		subXactAbortAttempted = true;
	}
}


/*
 * ExecuteTransactionEnd ends any remote transactions still taking place on
 * remote nodes. It uses xactParticipantHash to know which nodes need any
 * final COMMIT or ABORT commands. Nodes which fail a final COMMIT will have
 * their connection field set to NULL to permit placement invalidation.
 */
static void
ExecuteTransactionEnd(bool commit)
{
	const char *sqlCommand = commit ? "COMMIT TRANSACTION" : "ABORT TRANSACTION";
	HASH_SEQ_STATUS scan;
	NodeConnectionEntry *participant;
	bool completed = !commit; /* aborts are assumed completed */

	if (xactParticipantHash == NULL)
	{
		return;
	}

	hash_seq_init(&scan, xactParticipantHash);
	while ((participant = (NodeConnectionEntry *) hash_seq_search(&scan)))
	{
		PGconn *connection = participant->connection;
		PGresult *result = NULL;

		if (PQstatus(connection) != CONNECTION_OK)
		{
			continue;
		}

		result = PQexec(connection, sqlCommand);
		if (PQresultStatus(result) == PGRES_COMMAND_OK)
		{
			completed = true;
		}
		else
		{
			WarnRemoteError(connection, result);
			CloseConnectionByPGconn(participant->connection);

			participant->connection = NULL;
		}

		PQclear(result);
	}

	if (!completed)
	{
		ereport(ERROR, (errmsg("could not commit transaction on any active nodes")));
	}
}


/*
 * MarkRemainingInactivePlacements takes care of marking placements of a shard
 * inactive after some of the placements rejected the final COMMIT phase of a
 * transaction. This step is skipped if all placements reject the COMMIT, since
 * in that case no modifications to the placement have persisted.
 *
 * Failures are detected by checking the connection field of the entries in the
 * connection set for each shard: it is always set to NULL after errors.
 */
static void
MarkRemainingInactivePlacements(void)
{
	ListCell *shardConnSetCell = NULL;

	foreach(shardConnSetCell, xactShardConnSetList)
	{
		XactShardConnSet *shardConnSet = (XactShardConnSet *) lfirst(shardConnSetCell);
		List *participantList = shardConnSet->connectionEntryList;
		ListCell *participantCell = NULL;
		int successes = list_length(participantList); /* assume full success */

		/* determine how many actual successes there were: subtract failures */
		foreach(participantCell, participantList)
		{
			NodeConnectionEntry *participant = NULL;
			participant = (NodeConnectionEntry *) lfirst(participantCell);

			/* other codes sets connection to NULL after errors */
			if (participant->connection == NULL)
			{
				successes--;
			}
		}

		/* if no nodes succeeded for this shard, don't do anything */
		if (successes == 0)
		{
			continue;
		}

		/* otherwise, ensure failed placements are marked inactive */
		foreach(participantCell, participantList)
		{
			NodeConnectionEntry *participant = NULL;
			participant = (NodeConnectionEntry *) lfirst(participantCell);

			if (participant->connection == NULL)
			{
				uint64 shardId = shardConnSet->shardId;
				NodeConnectionKey *nodeKey = &participant->cacheKey;
				uint64 shardLength = 0;
				uint64 placementId = INVALID_PLACEMENT_ID;

				placementId = DeleteShardPlacementRow(shardId, nodeKey->nodeName,
													  nodeKey->nodePort);
				InsertShardPlacementRow(shardId, placementId, FILE_INACTIVE, shardLength,
										nodeKey->nodeName, nodeKey->nodePort);
			}
		}
	}
}
