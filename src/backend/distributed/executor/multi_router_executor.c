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

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "distributed/connection_cache.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/resource_lock.h"
#include "executor/executor.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"

#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/memutils.h"
#include "utils/palloc.h"


/* controls use of locks to enforce safe commutativity */
bool AllModificationsCommutative = false;


static LOCKMODE CommutativityRuleToLockMode(CmdType commandType, bool upsertQuery);
static void AcquireExecutorShardLock(Task *task, LOCKMODE lockMode);
static int32 ExecuteDistributedModify(Task *task);
static void ExecuteSingleShardSelect(Task *task, EState *executorState,
									 TupleDesc tupleDescriptor,
									 DestReceiver *destination);
static bool SendQueryInSingleRowMode(PGconn *connection, char *query);
static bool StoreQueryResult(PGconn *connection, TupleDesc tupleDescriptor,
							 Tuplestorestate *tupleStore);


/*
 * RouterExecutorStart sets up the executor state and queryDesc for router
 * execution.
 */
void
RouterExecutorStart(QueryDesc *queryDesc, int eflags, Task *task)
{
	bool topLevel = true;
	LOCKMODE lockMode = NoLock;
	EState *executorState = NULL;
	CmdType commandType = queryDesc->operation;

	/* ensure that the task is not NULL */
	Assert(task != NULL);

	/* disallow transactions and triggers during distributed commands */
	PreventTransactionChain(topLevel, "distributed commands");
	eflags |= EXEC_FLAG_SKIP_TRIGGERS;

	/* signal that it is a router execution */
	eflags |= EXEC_FLAG_CITUS_ROUTER_EXECUTOR;

	/* build empty executor state to obtain per-query memory context */
	executorState = CreateExecutorState();
	executorState->es_top_eflags = eflags;
	executorState->es_instrument = queryDesc->instrument_options;

	queryDesc->estate = executorState;

#if (PG_VERSION_NUM < 90500)

	/* make sure that upsertQuery is false for versions that UPSERT is not available */
	Assert(task->upsertQuery == false);
#endif

	lockMode = CommutativityRuleToLockMode(commandType, task->upsertQuery);

	if (lockMode != NoLock)
	{
		AcquireExecutorShardLock(task, lockMode);
	}
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

	LockShardResource(shardId, lockMode);
}


/*
 * RouterExecutorRun actually executes a single task on a worker.
 */
void
RouterExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count, Task *task)
{
	EState *estate = queryDesc->estate;
	CmdType operation = queryDesc->operation;
	MemoryContext oldcontext = NULL;

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
	if (count != 0)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("fetching rows from a query using a cursor "
							   "is unsupported")));
	}

	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	if (queryDesc->totaltime != NULL)
	{
		InstrStartNode(queryDesc->totaltime);
	}

	if (operation == CMD_INSERT || operation == CMD_UPDATE ||
		operation == CMD_DELETE)
	{
		int32 affectedRowCount = ExecuteDistributedModify(task);
		estate->es_processed = affectedRowCount;
	}
	else if (operation == CMD_SELECT)
	{
		DestReceiver *destination = queryDesc->dest;
		TupleDesc resultTupleDescriptor = queryDesc->tupDesc;

		ExecuteSingleShardSelect(task, estate, resultTupleDescriptor, destination);
	}
	else
	{
		ereport(ERROR, (errmsg("unrecognized operation code: %d",
							   (int) operation)));
	}

	if (queryDesc->totaltime != NULL)
	{
		InstrStopNode(queryDesc->totaltime, estate->es_processed);
	}

	MemoryContextSwitchTo(oldcontext);
}


/*
 * ExecuteDistributedModify is the main entry point for modifying distributed
 * tables. A distributed modification is successful if any placement of the
 * distributed table is successful. ExecuteDistributedModify returns the number
 * of modified rows in that case and errors in all others. This function will
 * also generate warnings for individual placement failures.
 */
static int32
ExecuteDistributedModify(Task *task)
{
	int32 affectedTupleCount = -1;
	ListCell *taskPlacementCell = NULL;
	List *failedPlacementList = NIL;
	ListCell *failedPlacementCell = NULL;

	foreach(taskPlacementCell, task->taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		char *nodeName = taskPlacement->nodeName;
		int32 nodePort = taskPlacement->nodePort;

		PGconn *connection = NULL;
		PGresult *result = NULL;
		char *currentAffectedTupleString = NULL;
		int32 currentAffectedTupleCount = -1;

		Assert(taskPlacement->shardState == FILE_FINALIZED);

		connection = GetOrEstablishConnection(nodeName, nodePort);
		if (connection == NULL)
		{
			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			continue;
		}

		result = PQexec(connection, task->queryString);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			ReportRemoteError(connection, result);
			PQclear(result);

			failedPlacementList = lappend(failedPlacementList, taskPlacement);
			continue;
		}

		currentAffectedTupleString = PQcmdTuples(result);
		currentAffectedTupleCount = pg_atoi(currentAffectedTupleString, sizeof(int32), 0);

		if ((affectedTupleCount == -1) ||
			(affectedTupleCount == currentAffectedTupleCount))
		{
			affectedTupleCount = currentAffectedTupleCount;
		}
		else
		{
			ereport(WARNING, (errmsg("modified %d tuples, but expected to modify %d",
									 currentAffectedTupleCount, affectedTupleCount),
							  errdetail("modified placement on %s:%d",
										nodeName, nodePort)));
		}

		PQclear(result);
	}

	/* if all placements failed, error out */
	if (list_length(failedPlacementList) == list_length(task->taskPlacementList))
	{
		ereport(ERROR, (errmsg("could not modify any active placements")));
	}

	/* otherwise, mark failed placements as inactive: they're stale */
	foreach(failedPlacementCell, failedPlacementList)
	{
		ShardPlacement *failedPlacement = (ShardPlacement *) lfirst(failedPlacementCell);
		uint64 shardLength = 0;

		DeleteShardPlacementRow(failedPlacement->shardId, failedPlacement->nodeName,
								failedPlacement->nodePort);
		InsertShardPlacementRow(failedPlacement->shardId, FILE_INACTIVE, shardLength,
								failedPlacement->nodeName, failedPlacement->nodePort);
	}

	return affectedTupleCount;
}


/*
 * ExecuteSingleShardSelect executes the remote select query and sends the
 * resultant tuples to the given destination receiver. If the query fails on a
 * given placement, the function attempts it on its replica.
 */
static void
ExecuteSingleShardSelect(Task *task, EState *executorState,
						 TupleDesc tupleDescriptor, DestReceiver *destination)
{
	Tuplestorestate *tupleStore = NULL;
	bool resultsOK = false;
	TupleTableSlot *tupleTableSlot = NULL;

	tupleStore = tuplestore_begin_heap(false, false, work_mem);

	resultsOK = ExecuteTaskAndStoreResults(task, tupleDescriptor, tupleStore);
	if (!resultsOK)
	{
		ereport(ERROR, (errmsg("could not receive query results")));
	}

	tupleTableSlot = MakeSingleTupleTableSlot(tupleDescriptor);

	/* startup the tuple receiver */
	(*destination->rStartup)(destination, CMD_SELECT, tupleDescriptor);

	/* iterate over tuples in tuple store, and send them to destination */
	for (;;)
	{
		bool nextTuple = tuplestore_gettupleslot(tupleStore, true, false, tupleTableSlot);
		if (!nextTuple)
		{
			break;
		}

		(*destination->receiveSlot)(tupleTableSlot, destination);
		executorState->es_processed++;

		ExecClearTuple(tupleTableSlot);
	}

	/* shutdown the tuple receiver */
	(*destination->rShutdown)(destination);

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	tuplestore_end(tupleStore);
}


/*
 * ExecuteTaskAndStoreResults executes the task on the remote node, retrieves
 * the results and stores them in the given tuple store. If the task fails on
 * one of the placements, the function retries it on other placements.
 */
bool
ExecuteTaskAndStoreResults(Task *task, TupleDesc tupleDescriptor,
						   Tuplestorestate *tupleStore)
{
	bool resultsOK = false;
	List *taskPlacementList = task->taskPlacementList;
	ListCell *taskPlacementCell = NULL;

	/*
	 * Try to run the query to completion on one placement. If the query fails
	 * attempt the query on the next placement.
	 */
	foreach(taskPlacementCell, taskPlacementList)
	{
		ShardPlacement *taskPlacement = (ShardPlacement *) lfirst(taskPlacementCell);
		char *nodeName = taskPlacement->nodeName;
		int32 nodePort = taskPlacement->nodePort;
		bool queryOK = false;
		bool storedOK = false;

		PGconn *connection = GetOrEstablishConnection(nodeName, nodePort);
		if (connection == NULL)
		{
			continue;
		}

		queryOK = SendQueryInSingleRowMode(connection, task->queryString);
		if (!queryOK)
		{
			PurgeConnection(connection);
			continue;
		}

		storedOK = StoreQueryResult(connection, tupleDescriptor, tupleStore);
		if (storedOK)
		{
			resultsOK = true;
			break;
		}
		else
		{
			tuplestore_clear(tupleStore);
			PurgeConnection(connection);
		}
	}

	return resultsOK;
}


/*
 * SendQueryInSingleRowMode sends the given query on the connection in an
 * asynchronous way. The function also sets the single-row mode on the
 * connection so that we receive results a row at a time.
 */
static bool
SendQueryInSingleRowMode(PGconn *connection, char *query)
{
	int querySent = 0;
	int singleRowMode = 0;

	querySent = PQsendQuery(connection, query);
	if (querySent == 0)
	{
		ReportRemoteError(connection, NULL);
		return false;
	}

	singleRowMode = PQsetSingleRowMode(connection);
	if (singleRowMode == 0)
	{
		ReportRemoteError(connection, NULL);
		return false;
	}

	return true;
}


/*
 * StoreQueryResult gets the query results from the given connection, builds
 * tuples from the results and stores them in the given tuple-store. If the
 * function can't receive query results, it returns false. Note that this
 * function assumes the query has already been sent on the connection and the
 * tuplestore has earlier been initialized.
 */
static bool
StoreQueryResult(PGconn *connection, TupleDesc tupleDescriptor,
				 Tuplestorestate *tupleStore)
{
	AttInMetadata *attributeInputMetadata = TupleDescGetAttInMetadata(tupleDescriptor);
	uint32 expectedColumnCount = tupleDescriptor->natts;
	char **columnArray = (char **) palloc0(expectedColumnCount * sizeof(char *));
	MemoryContext ioContext = AllocSetContextCreate(CurrentMemoryContext,
													"StoreQueryResult",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);

	Assert(tupleStore != NULL);

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
			ReportRemoteError(connection, result);
			PQclear(result);

			return false;
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
		}

		PQclear(result);
	}

	pfree(columnArray);

	return true;
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

	Assert(estate != NULL);
	Assert(estate->es_finished);

	FreeExecutorState(estate);
	queryDesc->estate = NULL;
	queryDesc->totaltime = NULL;
}
