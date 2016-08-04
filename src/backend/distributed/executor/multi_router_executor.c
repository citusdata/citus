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
#include "distributed/citus_clauses.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/connection_cache.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/relay_utility.h"
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
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static bool subXactAbortAttempted = false;

/* functions needed during start phase */
static void InitTransactionStateForTask(Task *task);
static LOCKMODE CommutativityRuleToLockMode(CmdType commandType, bool upsertQuery);
static void AcquireExecutorShardLock(Task *task, LOCKMODE lockMode);
static HTAB * CreateXactParticipantHash(void);

/* functions needed during run phase */
static bool ExecuteTaskAndStoreResults(QueryDesc *queryDesc,
									   Task *task,
									   bool isModificationQuery,
									   bool expectResults);
static uint64 ReturnRowsFromTuplestore(uint64 tupleCount, TupleDesc tupleDescriptor,
									   DestReceiver *destination,
									   Tuplestorestate *tupleStore);
static PGconn * GetConnectionForPlacement(ShardPlacement *placement,
										  bool isModificationQuery);
static void PurgeConnectionForPlacement(ShardPlacement *placement);
static void ExtractParametersFromParamListInfo(ParamListInfo paramListInfo,
											   Oid **parameterTypes,
											   const char ***parameterValues);
static bool SendQueryInSingleRowMode(PGconn *connection, char *query,
									 ParamListInfo paramListInfo);
static bool StoreQueryResult(MaterialState *routerState, PGconn *connection,
							 TupleDesc tupleDescriptor, int64 *rows);
static bool ConsumeQueryResult(PGconn *connection, int64 *rows);
static void RecordShardIdParticipant(uint64 affectedShardId,
									 NodeConnectionEntry *participantEntry);

/* functions needed by callbacks and hooks */
static void RegisterRouterExecutorXactCallbacks(void);
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
RouterExecutorStart(QueryDesc *queryDesc, int eflags, Task *task)
{
	LOCKMODE lockMode = NoLock;
	EState *executorState = NULL;
	CmdType commandType = queryDesc->operation;

	/* ensure that the task is not NULL */
	Assert(task != NULL);

	/* disallow triggers during distributed modify commands */
	if (commandType != CMD_SELECT)
	{
		eflags |= EXEC_FLAG_SKIP_TRIGGERS;

		if (XactModificationLevel == XACT_MODIFICATION_SCHEMA)
		{
			ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
							errmsg("distributed data modifications must not appear in "
								   "transaction blocks which contain distributed DDL "
								   "commands")));
		}

		/*
		 * We could naturally handle function-based transactions (i.e. those
		 * using PL/pgSQL or similar) by checking the type of queryDesc->dest,
		 * but some customers already use functions that touch multiple shards
		 * from within a function, so we'll ignore functions for now.
		 */
		if (IsTransactionBlock() && xactParticipantHash == NULL)
		{
			InitTransactionStateForTask(task);
		}
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

	lockMode = CommutativityRuleToLockMode(commandType, task->upsertQuery);

	if (lockMode != NoLock)
	{
		AcquireExecutorShardLock(task, lockMode);
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
				PurgeConnection(connection);

				connection = NULL;
			}

			PQclear(result);
		}

		participantEntry->connection = connection;
	}

	XactModificationLevel = XACT_MODIFICATION_DATA;
}


/*
 * CommutativityRuleToLockMode determines the commutativity rule for the given
 * command and returns the appropriate lock mode to enforce that rule. The
 * function assumes a SELECT doesn't modify state and therefore is commutative
 * with all other commands. The function also assumes that an INSERT commutes
 * with another INSERT, but not with an UPDATE/DELETE/UPSERT; and an
 * UPDATE/DELETE/UPSERT doesn't commute with an INSERT, UPDATE, DELETE or UPSERT.
 *
 * Note that the above comment defines INSERT INTO ... ON CONFLICT type of queries
 * as an UPSERT. Since UPSERT is not defined as a separate command type in postgres,
 * we have to pass it as a second parameter to the function.
 *
 * The above mapping is overridden entirely when all_modifications_commutative
 * is set to true. In that case, all commands just claim a shared lock. This
 * allows the shard repair logic to lock out modifications while permitting all
 * commands to otherwise commute.
 */
static LOCKMODE
CommutativityRuleToLockMode(CmdType commandType, bool upsertQuery)
{
	LOCKMODE lockMode = NoLock;

	/* bypass commutativity checks when flag enabled */
	if (AllModificationsCommutative)
	{
		return ShareLock;
	}

	if (commandType == CMD_SELECT)
	{
		lockMode = NoLock;
	}
	else if (upsertQuery)
	{
		lockMode = ExclusiveLock;
	}
	else if (commandType == CMD_INSERT)
	{
		lockMode = ShareLock;
	}
	else if (commandType == CMD_UPDATE || commandType == CMD_DELETE)
	{
		lockMode = ExclusiveLock;
	}
	else
	{
		ereport(ERROR, (errmsg("unrecognized operation code: %d", (int) commandType)));
	}

	return lockMode;
}


/*
 * AcquireExecutorShardLock: acquire shard lock needed for execution of
 * a single task within a distributed plan.
 */
static void
AcquireExecutorShardLock(Task *task, LOCKMODE lockMode)
{
	int64 shardId = task->anchorShardId;

	if (shardId != INVALID_SHARD_ID)
	{
		LockShardResource(shardId, lockMode);
	}
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
	List *taskList = multiPlan->workerJob->taskList;
	Task *task = NULL;
	EState *estate = queryDesc->estate;
	CmdType operation = queryDesc->operation;
	MemoryContext oldcontext = NULL;
	DestReceiver *destination = queryDesc->dest;
	MaterialState *routerState = (MaterialState *) queryDesc->planstate;
	bool sendTuples = operation == CMD_SELECT || queryDesc->plannedstmt->hasReturning;

	/* router executor can only execute distributed plans with a single task */
	Assert(list_length(taskList) == 1);
	task = (Task *) linitial(taskList);

	Assert(estate != NULL);
	Assert(!(estate->es_top_eflags & EXEC_FLAG_EXPLAIN_ONLY));
	Assert(task != NULL);

	/* we only support default scan direction and row fetch count */
	if (!ScanDirectionIsForward(direction))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("scan directions other than forward scans "
							   "are unsupported")));
	}

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

	/*
	 * If query has not yet been executed, do so now. The main reason why the
	 * query might already have been executed is cursors.
	 */
	if (!routerState->eof_underlying)
	{
		bool resultsOK = false;
		bool isModificationQuery = false;

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

		resultsOK = ExecuteTaskAndStoreResults(queryDesc, task,
											   isModificationQuery,
											   sendTuples);
		if (!resultsOK)
		{
			ereport(ERROR, (errmsg("could not receive query results")));
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
 * ExecuteTaskAndStoreResults executes the task on the remote node, retrieves
 * the results and stores them, if SELECT or RETURNING is used, in a tuple
 * store.
 *
 * If the task fails on one of the placements, the function retries it on
 * other placements (SELECT), reraises the remote error (constraint violation
 * in DML), marks the affected placement as invalid (DML on some placement
 * failed), or errors out (DML failed on all placements).
 */
static bool
ExecuteTaskAndStoreResults(QueryDesc *queryDesc, Task *task,
						   bool isModificationQuery,
						   bool expectResults)
{
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

	if (isModificationQuery && task->requiresMasterEvaluation)
	{
		PlannedStmt *planStatement = queryDesc->plannedstmt;
		MultiPlan *multiPlan = GetMultiPlan(planStatement);
		Query *query = multiPlan->workerJob->jobQuery;
		Oid relid = ((RangeTblEntry *) linitial(query->rtable))->relid;
		StringInfo queryStringInfo = makeStringInfo();

		ExecuteMasterEvaluableFunctions(query);
		deparse_shard_query(query, relid, task->anchorShardId, queryStringInfo);
		queryString = queryStringInfo->data;

		elog(DEBUG4, "query before master evaluation: %s", task->queryString);
		elog(DEBUG4, "query after master evaluation:  %s", queryString);
	}

	/*
	 * Try to run the query to completion on one placement. If the query fails
	 * attempt the query on the next placement.
	 */
	foreach(taskPlacementCell, taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		bool queryOK = false;
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
			PurgeConnectionForPlacement(taskPlacement);
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
									   &currentAffectedTupleCount);
		}
		else
		{
			queryOK = ConsumeQueryResult(connection, &currentAffectedTupleCount);
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
			PurgeConnectionForPlacement(taskPlacement);

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
			uint64 shardLength = 0;

			DeleteShardPlacementRow(failedPlacement->shardId, failedPlacement->nodeName,
									failedPlacement->nodePort);
			InsertShardPlacementRow(failedPlacement->shardId, FILE_INACTIVE, shardLength,
									failedPlacement->nodeName, failedPlacement->nodePort);
		}

		executorState->es_processed = affectedTupleCount;
	}

	return resultsOK;
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
PurgeConnectionForPlacement(ShardPlacement *placement)
{
	NodeConnectionKey nodeKey;
	char *currentUser = CurrentUserName();

	MemSet(&nodeKey, 0, sizeof(NodeConnectionKey));
	strlcpy(nodeKey.nodeName, placement->nodeName, MAX_NODE_LENGTH + 1);
	nodeKey.nodePort = placement->nodePort;
	strlcpy(nodeKey.nodeUser, currentUser, NAMEDATALEN);

	PurgeConnectionByKey(&nodeKey);

	if (xactParticipantHash != NULL)
	{
		NodeConnectionEntry *participantEntry = NULL;
		bool entryFound = false;

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
		 * If the parameter is NULL, or is not referenced / used (ptype == 0
		 * would otherwise have errored out inside standard_planner()),
		 * don't pass a value to the remote side.
		 */
		if (parameterData->isnull || parameterData->ptype == 0)
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
				 TupleDesc tupleDescriptor, int64 *rows)
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
	else
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
			bool raiseError = false;

			/*
			 * If the error code is in constraint violation class, we want to
			 * fail fast because we must get the same error from all shard
			 * placements.
			 */
			category = ERRCODE_TO_CATEGORY(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION);
			raiseError = SqlStateMatchesCategory(sqlStateString, category);

			if (raiseError)
			{
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
ConsumeQueryResult(PGconn *connection, int64 *rows)
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
			bool raiseError = false;

			/*
			 * If the error code is in constraint violation class, we want to
			 * fail fast because we must get the same error from all shard
			 * placements.
			 */
			category = ERRCODE_TO_CATEGORY(ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION);
			raiseError = SqlStateMatchesCategory(sqlStateString, category);

			if (raiseError)
			{
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

			scanint8(currentAffectedTupleString, false, &currentAffectedTupleCount);
			Assert(currentAffectedTupleCount >= 0);

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
 * InstallRouterExecutorShmemHook simply installs a hook (intended to be called
 * once during backend startup), which will itself register all the transaction
 * callbacks needed by this executor.
 */
void
InstallRouterExecutorShmemHook(void)
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = RegisterRouterExecutorXactCallbacks;
}


/*
 * RegisterRouterExecutorXactCallbacks registers (sub-)transaction callbacks
 * needed by this executor before calling any previous shmem startup hooks.
 */
static void
RegisterRouterExecutorXactCallbacks(void)
{
	RegisterXactCallback(RouterTransactionCallback, NULL);
	RegisterSubXactCallback(RouterSubtransactionCallback, NULL);

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
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
	XactModificationLevel = XACT_MODIFICATION_NONE;
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
			PurgeConnection(participant->connection);

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

				DeleteShardPlacementRow(shardId, nodeKey->nodeName, nodeKey->nodePort);
				InsertShardPlacementRow(shardId, FILE_INACTIVE, shardLength,
										nodeKey->nodeName, nodeKey->nodePort);
			}
		}
	}
}
