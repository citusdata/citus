/*-------------------------------------------------------------------------
 *
 * multi_executor.c
 *
 * Entrypoint into distributed query execution.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "pgstat.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/pg_class.h"
#include "catalog/namespace.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_master_planner.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_resowner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/transmit.h"
#include "distributed/worker_shard_visibility.h"
#include "distributed/worker_protocol.h"
#include "executor/execdebug.h"
#include "commands/copy.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "storage/lmgr.h"
#include "tcop/dest.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"


/*
 * Controls the connection type for multi shard modifications, DDLs
 * TRUNCATE and multi-shard SELECT queries.
 */
int MultiShardConnectionType = PARALLEL_CONNECTION;
bool WritableStandbyCoordinator = false;


/* sort the returning to get consistent outputs, used only for testing */
bool SortReturning = false;

/*
 * How many nested executors have we started? This can happen for SQL
 * UDF calls. The outer query starts an executor, then postgres opens
 * another executor to run the SQL UDF.
 */
int ExecutorLevel = 0;


/* local function forward declarations */
static bool AlterTableConstraintCheck(QueryDesc *queryDesc);
static bool IsLocalReferenceTableJoinPlan(PlannedStmt *plan);

/*
 * CitusExecutorStart is the ExecutorStart_hook that gets called when
 * Postgres prepares for execution or EXPLAIN.
 */
void
CitusExecutorStart(QueryDesc *queryDesc, int eflags)
{
	PlannedStmt *plannedStmt = queryDesc->plannedstmt;

	/*
	 * We cannot modify XactReadOnly on Windows because it is not
	 * declared with PGDLLIMPORT.
	 */
#ifndef WIN32
	if (RecoveryInProgress() && WritableStandbyCoordinator &&
		IsCitusPlan(plannedStmt->planTree))
	{
		PG_TRY();
		{
			/*
			 * To enable writes from a hot standby we cheat our way through
			 * the checks in standard_ExecutorStart by temporarily setting
			 * XactReadOnly to false.
			 */
			XactReadOnly = false;
			standard_ExecutorStart(queryDesc, eflags);
			XactReadOnly = true;
		}
		PG_CATCH();
		{
			XactReadOnly = true;
			PG_RE_THROW();
		}
		PG_END_TRY();
	}
	else
#endif
	{
		standard_ExecutorStart(queryDesc, eflags);
	}
}


/*
 * CitusExecutorRun is the ExecutorRun_hook that gets called when postgres
 * executes a query.
 */
void
CitusExecutorRun(QueryDesc *queryDesc,
				 ScanDirection direction, uint64 count, bool execute_once)
{
	DestReceiver *dest = queryDesc->dest;

	PG_TRY();
	{
		ExecutorLevel++;

		if (CitusHasBeenLoaded())
		{
			if (IsLocalReferenceTableJoinPlan(queryDesc->plannedstmt) &&
				IsMultiStatementTransaction())
			{
				/*
				 * Currently we don't support this to avoid problems with tuple
				 * visibility, locking, etc. For example, change to the reference
				 * table can go through a MultiConnection, which won't be visible
				 * to the locally planned queries.
				 */
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("cannot join local tables and reference tables in "
									   "a transaction block, udf block, or distributed "
									   "CTE subquery")));
			}
		}

		/*
		 * Disable execution of ALTER TABLE constraint validation queries. These
		 * constraints will be validated in worker nodes, so running these queries
		 * from the coordinator would be redundant.
		 *
		 * For example, ALTER TABLE ... ATTACH PARTITION checks that the new
		 * partition doesn't violate constraints of the parent table, which
		 * might involve running some SELECT queries.
		 *
		 * Ideally we'd completely skip these checks in the coordinator, but we don't
		 * have any means to tell postgres to skip the checks. So the best we can do is
		 * to not execute the queries and return an empty result set, as if this table has
		 * no rows, so no constraints will be violated.
		 */
		if (AlterTableConstraintCheck(queryDesc))
		{
			EState *estate = queryDesc->estate;

			estate->es_processed = 0;
#if PG_VERSION_NUM < 120000
			estate->es_lastoid = InvalidOid;
#endif

			/* start and shutdown tuple receiver to simulate empty result */
			dest->rStartup(queryDesc->dest, CMD_SELECT, queryDesc->tupDesc);
			dest->rShutdown(dest);
		}
		else
		{
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
		}

		ExecutorLevel--;
	}
	PG_CATCH();
	{
		ExecutorLevel--;

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * ReturnTupleFromTuplestore reads the next tuple from the tuple store of the
 * given Citus scan node and returns it. It returns null if all tuples are read
 * from the tuple store.
 */
TupleTableSlot *
ReturnTupleFromTuplestore(CitusScanState *scanState)
{
	Tuplestorestate *tupleStore = scanState->tuplestorestate;
	bool forwardScanDirection = true;

	if (tupleStore == NULL)
	{
		return NULL;
	}

	EState *executorState = ScanStateGetExecutorState(scanState);
	ScanDirection scanDirection = executorState->es_direction;
	Assert(ScanDirectionIsValid(scanDirection));

	if (ScanDirectionIsBackward(scanDirection))
	{
		forwardScanDirection = false;
	}

	TupleTableSlot *resultSlot = scanState->customScanState.ss.ps.ps_ResultTupleSlot;
	tuplestore_gettupleslot(tupleStore, forwardScanDirection, false, resultSlot);

	return resultSlot;
}


/*
 * Load data collected by task-tracker executor into the tuplestore
 * of CitusScanState. For that, we first create a tuple store, and then copy the
 * files one-by-one into the tuple store.
 *
 * Note that in the long term it'd be a lot better if Multi*Execute() directly
 * filled the tuplestores, but that's a fair bit of work.
 */
void
LoadTuplesIntoTupleStore(CitusScanState *citusScanState, Job *workerJob)
{
	List *workerTaskList = workerJob->taskList;
	ListCell *workerTaskCell = NULL;
	bool randomAccess = true;
	bool interTransactions = false;
	char *copyFormat = "text";

	TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(citusScanState);

	Assert(citusScanState->tuplestorestate == NULL);
	citusScanState->tuplestorestate =
		tuplestore_begin_heap(randomAccess, interTransactions, work_mem);

	if (BinaryMasterCopyFormat)
	{
		copyFormat = "binary";
	}

	foreach(workerTaskCell, workerTaskList)
	{
		Task *workerTask = (Task *) lfirst(workerTaskCell);

		StringInfo jobDirectoryName = MasterJobDirectoryName(workerTask->jobId);
		StringInfo taskFilename = TaskFilename(jobDirectoryName, workerTask->taskId);

		ReadFileIntoTupleStore(taskFilename->data, copyFormat, tupleDescriptor,
							   citusScanState->tuplestorestate);
	}

	tuplestore_donestoring(citusScanState->tuplestorestate);
}


/*
 * ReadFileIntoTupleStore parses the records in a COPY-formatted file according
 * according to the given tuple descriptor and stores the records in a tuple
 * store.
 */
void
ReadFileIntoTupleStore(char *fileName, char *copyFormat, TupleDesc tupleDescriptor,
					   Tuplestorestate *tupstore)
{
	EState *executorState = CreateExecutorState();
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);

	int columnCount = tupleDescriptor->natts;
	Datum *columnValues = palloc0(columnCount * sizeof(Datum));
	bool *columnNulls = palloc0(columnCount * sizeof(bool));

	const int fileFlags = (O_RDONLY | PG_BINARY);
	const int fileMode = 0;
	StringInfo buffer = makeStringInfo();

	/* we currently do not check if the caller has permissions for this file */
	File fileDesc = FileOpenForTransmit(fileName, fileFlags, fileMode);
	FileCompat fileCompat = FileCompatFromFileStart(fileDesc);

	int tupleSize = 0;

	while (FileReadCompat(&fileCompat, (char *) &tupleSize, sizeof(tupleSize),
						  PG_WAIT_IO) == sizeof(tupleSize))
	{
		ResetPerTupleExprContext(executorState);
		MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

		enlargeStringInfo(buffer, tupleSize);
		FileReadCompat(&fileCompat, buffer->data, tupleSize, PG_WAIT_IO);
		uint32 currentDatumDataOffset = 0;

		for (int columnIndex = 0; columnIndex < tupleDescriptor->natts;)
		{
			unsigned char bitarray = buffer->data[currentDatumDataOffset++];
			for (int bitIndex = 0; bitIndex < 8 && columnIndex < tupleDescriptor->natts;
				 bitIndex++, columnIndex++)
			{
				if (bitarray & (1 << bitIndex))
				{
					columnNulls[columnIndex] = true;
				}
				else
				{
					columnNulls[columnIndex] = false;
				}
			}
		}

		for (int columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
		{
			if (columnNulls[columnIndex])
			{
				continue;
			}

			Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, columnIndex);
			char *currentDatumDataPointer = buffer->data + currentDatumDataOffset;

			columnValues[columnIndex] = fetch_att(currentDatumDataPointer,
												  attributeForm->attbyval,
												  attributeForm->attlen);

			currentDatumDataOffset = att_addlength_datum(currentDatumDataOffset,
														 attributeForm->attlen,
														 currentDatumDataPointer);
			currentDatumDataOffset = att_align_nominal(currentDatumDataOffset,
													   attributeForm->attalign);
		}

		tuplestore_putvalues(tupstore, tupleDescriptor, columnValues, columnNulls);
		MemoryContextSwitchTo(oldContext);
	}

	pfree(columnValues);
	pfree(columnNulls);
}


/*
 * SortTupleStore gets a CitusScanState and sorts the tuplestore by all the
 * entries in the target entry list, starting from the first one and
 * ending with the last entry.
 *
 * The sorting is done in ASC order.
 */
void
SortTupleStore(CitusScanState *scanState)
{
	TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
	Tuplestorestate *tupleStore = scanState->tuplestorestate;

	List *targetList = scanState->customScanState.ss.ps.plan->targetlist;
	uint32 expectedColumnCount = list_length(targetList);

	/* Convert list-ish representation to arrays wanted by executor */
	int numberOfSortKeys = expectedColumnCount;
	AttrNumber *sortColIdx = (AttrNumber *) palloc(numberOfSortKeys * sizeof(AttrNumber));
	Oid *sortOperators = (Oid *) palloc(numberOfSortKeys * sizeof(Oid));
	Oid *collations = (Oid *) palloc(numberOfSortKeys * sizeof(Oid));
	bool *nullsFirst = (bool *) palloc(numberOfSortKeys * sizeof(bool));

	ListCell *targetCell = NULL;
	int sortKeyIndex = 0;


	/*
	 * Iterate on the returning target list and generate the necessary information
	 * for sorting the tuples.
	 */
	foreach(targetCell, targetList)
	{
		TargetEntry *returningEntry = (TargetEntry *) lfirst(targetCell);
		Oid sortop = InvalidOid;

		/* determine the sortop, we don't need anything else */
		get_sort_group_operators(exprType((Node *) returningEntry->expr),
								 true, false, false,
								 &sortop, NULL, NULL,
								 NULL);

		sortColIdx[sortKeyIndex] = sortKeyIndex + 1;
		sortOperators[sortKeyIndex] = sortop;
		collations[sortKeyIndex] = exprCollation((Node *) returningEntry->expr);
		nullsFirst[sortKeyIndex] = false;

		sortKeyIndex++;
	}

	Tuplesortstate *tuplesortstate =
		tuplesort_begin_heap(tupleDescriptor, numberOfSortKeys, sortColIdx, sortOperators,
							 collations, nullsFirst, work_mem, NULL, false);

	while (true)
	{
		TupleTableSlot *slot = ReturnTupleFromTuplestore(scanState);

		if (TupIsNull(slot))
		{
			break;
		}

		/* tuplesort_puttupleslot copies the slot into sort context */
		tuplesort_puttupleslot(tuplesortstate, slot);
	}

	/* perform the actual sort operation */
	tuplesort_performsort(tuplesortstate);

	/*
	 * Truncate the existing tupleStore, because we'll fill it back
	 * from the sorted tuplestore.
	 */
	tuplestore_clear(tupleStore);

	/* iterate over all the sorted tuples, add them to original tuplestore */
	while (true)
	{
		TupleTableSlot *newSlot = MakeSingleTupleTableSlotCompat(tupleDescriptor,
																 &TTSOpsMinimalTuple);
		bool found = tuplesort_gettupleslot(tuplesortstate, true, false, newSlot, NULL);

		if (!found)
		{
			break;
		}

		/* tuplesort_puttupleslot copies the slot into the tupleStore context */
		tuplestore_puttupleslot(tupleStore, newSlot);
	}

	tuplestore_rescan(scanState->tuplestorestate);

	/* terminate the sort, clear unnecessary resources */
	tuplesort_end(tuplesortstate);
}


/*
 * ExecuteQueryStringIntoDestReceiver plans and executes a query and sends results
 * to the given DestReceiver.
 */
void
ExecuteQueryStringIntoDestReceiver(const char *queryString, ParamListInfo params,
								   DestReceiver *dest)
{
	Query *query = ParseQueryString(queryString, NULL, 0);

	ExecuteQueryIntoDestReceiver(query, params, dest);
}


/*
 * ParseQuery parses query string and returns a Query struct.
 */
Query *
ParseQueryString(const char *queryString, Oid *paramOids, int numParams)
{
	RawStmt *rawStmt = (RawStmt *) ParseTreeRawStmt(queryString);
	List *queryTreeList =
		pg_analyze_and_rewrite(rawStmt, queryString, paramOids, numParams, NULL);

	if (list_length(queryTreeList) != 1)
	{
		ereport(ERROR, (errmsg("can only execute a single query")));
	}

	Query *query = (Query *) linitial(queryTreeList);

	return query;
}


/*
 * ExecuteQueryIntoDestReceiver plans and executes a query and sends results to the given
 * DestReceiver.
 */
void
ExecuteQueryIntoDestReceiver(Query *query, ParamListInfo params, DestReceiver *dest)
{
	int cursorOptions = CURSOR_OPT_PARALLEL_OK;

	if (query->commandType == CMD_UTILITY)
	{
		/* can only execute DML/SELECT via this path */
		ereport(ERROR, (errmsg("cannot execute utility commands")));
	}

	/* plan the subquery, this may be another distributed query */
	PlannedStmt *queryPlan = pg_plan_query(query, cursorOptions, params);

	ExecutePlanIntoDestReceiver(queryPlan, params, dest);
}


/*
 * ExecutePlanIntoDestReceiver executes a query plan and sends results to the given
 * DestReceiver.
 */
void
ExecutePlanIntoDestReceiver(PlannedStmt *queryPlan, ParamListInfo params,
							DestReceiver *dest)
{
	int eflags = 0;
	long count = FETCH_ALL;

	/* create a new portal for executing the query */
	Portal portal = CreateNewPortal();

	/* don't display the portal in pg_cursors, it is for internal use only */
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  "",
					  "SELECT",
					  list_make1(queryPlan),
					  NULL);

	PortalStart(portal, params, eflags, GetActiveSnapshot());
	PortalRun(portal, count, false, true, dest, dest, NULL);
	PortalDrop(portal, false);
}


/*
 * SetLocalMultiShardModifyModeToSequential simply a C interface for
 * setting the following:
 *      SET LOCAL citus.multi_shard_modify_mode = 'sequential';
 */
void
SetLocalMultiShardModifyModeToSequential()
{
	set_config_option("citus.multi_shard_modify_mode", "sequential",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
}


/*
 * AlterTableConstraintCheck returns if the given query is an ALTER TABLE
 * constraint check query.
 *
 * Postgres uses SPI to execute these queries. To see examples of how these
 * constraint check queries look like, see RI_Initial_Check() and RI_Fkey_check().
 */
static bool
AlterTableConstraintCheck(QueryDesc *queryDesc)
{
	if (!AlterTableInProgress())
	{
		return false;
	}

	/*
	 * These queries are one or more SELECT queries, where postgres checks
	 * their results either for NULL values or existence of a row at all.
	 */
	if (queryDesc->plannedstmt->commandType != CMD_SELECT)
	{
		return false;
	}

	/*
	 * While an ALTER TABLE is in progress, we might do SELECTs on some
	 * catalog tables too. For example, when dropping a column, citus_drop_trigger()
	 * runs some SELECTs on catalog tables. These are not constraint check queries.
	 */
	if (!IsCitusPlan(queryDesc->plannedstmt->planTree))
	{
		return false;
	}

	return true;
}


/*
 * IsLocalReferenceTableJoinPlan returns true if the given plan joins local tables
 * with reference table shards.
 *
 * This should be consistent with IsLocalReferenceTableJoin() in distributed_planner.c.
 */
static bool
IsLocalReferenceTableJoinPlan(PlannedStmt *plan)
{
	bool hasReferenceTable = false;
	bool hasLocalTable = false;
	ListCell *rangeTableCell = NULL;
	bool hasReferenceTableReplica = false;

	/*
	 * We only allow join between reference tables and local tables in the
	 * coordinator.
	 */
	if (!IsCoordinator())
	{
		return false;
	}

	/*
	 * All groups that have pg_dist_node entries, also have reference
	 * table replicas.
	 */
	PrimaryNodeForGroup(GetLocalGroupId(), &hasReferenceTableReplica);

	/*
	 * If reference table doesn't have replicas on the coordinator, we don't
	 * allow joins with local tables.
	 */
	if (!hasReferenceTableReplica)
	{
		return false;
	}

	/*
	 * No need to check FOR UPDATE/SHARE or modifying subqueries, those have
	 * already errored out in distributed_planner.c if they contain mix of
	 * local and distributed tables.
	 */
	if (plan->commandType != CMD_SELECT)
	{
		return false;
	}

	/*
	 * plan->rtable contains the flattened RTE lists of the plan tree, which
	 * includes rtes in subqueries, CTEs, ...
	 *
	 * It doesn't contain optimized away table accesses (due to join optimization),
	 * which is fine for our purpose.
	 */
	foreach(rangeTableCell, plan->rtable)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		bool onlySearchPath = false;

		/*
		 * Planner's IsLocalReferenceTableJoin() doesn't allow planning functions
		 * in FROM clause locally. Early exit. We cannot use Assert() here since
		 * all non-Citus plans might pass through these checks.
		 */
		if (rangeTableEntry->rtekind == RTE_FUNCTION)
		{
			return false;
		}

		if (rangeTableEntry->rtekind != RTE_RELATION)
		{
			continue;
		}

		/*
		 * Planner's IsLocalReferenceTableJoin() doesn't allow planning reference
		 * table and view join locally. Early exit. We cannot use Assert() here
		 * since all non-Citus plans might pass through these checks.
		 */
		if (rangeTableEntry->relkind == RELKIND_VIEW)
		{
			return false;
		}

		if (RelationIsAKnownShard(rangeTableEntry->relid, onlySearchPath))
		{
			/*
			 * We don't allow joining non-reference distributed tables, so we
			 * can skip checking that this is a reference table shard or not.
			 */
			hasReferenceTable = true;
		}
		else
		{
			hasLocalTable = true;
		}

		if (hasReferenceTable && hasLocalTable)
		{
			return true;
		}
	}

	return false;
}
