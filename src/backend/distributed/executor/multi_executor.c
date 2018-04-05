/*-------------------------------------------------------------------------
 *
 * multi_executor.c
 *
 * Entrypoint into distributed query execution.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/namespace.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_master_planner.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_resowner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/multi_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_protocol.h"
#include "executor/execdebug.h"
#include "commands/copy.h"
#include "nodes/makefuncs.h"
#include "parser/parsetree.h"
#include "storage/lmgr.h"
#include "tcop/pquery.h"
#include "tcop/utility.h"
#include "utils/snapmgr.h"
#include "utils/memutils.h"


/* controls the connection type for multi shard update/delete queries */
int MultiShardConnectionType = PARALLEL_CONNECTION;


/* ocal function forward declarations */
static Relation StubRelation(TupleDesc tupleDescriptor);


/*
 * ReturnTupleFromTuplestore reads the next tuple from the tuple store of the
 * given Citus scan node and returns it. It returns null if all tuples are read
 * from the tuple store.
 */
TupleTableSlot *
ReturnTupleFromTuplestore(CitusScanState *scanState)
{
	Tuplestorestate *tupleStore = scanState->tuplestorestate;
	TupleTableSlot *resultSlot = NULL;
	ScanDirection scanDirection = NoMovementScanDirection;
	bool forwardScanDirection = true;

	if (tupleStore == NULL)
	{
		return NULL;
	}

	scanDirection = scanState->customScanState.ss.ps.state->es_direction;
	Assert(ScanDirectionIsValid(scanDirection));

	if (ScanDirectionIsBackward(scanDirection))
	{
		forwardScanDirection = false;
	}

	resultSlot = scanState->customScanState.ss.ps.ps_ResultTupleSlot;
	tuplestore_gettupleslot(tupleStore, forwardScanDirection, false, resultSlot);

	return resultSlot;
}


/*
 * Load data collected by real-time or task-tracker executors into the tuplestore
 * of CitusScanState. For that, we first create a tuple store, and then copy the
 * files one-by-one into the tuple store.
 *
 * Note that in the long term it'd be a lot better if Multi*Execute() directly
 * filled the tuplestores, but that's a fair bit of work.
 */
void
LoadTuplesIntoTupleStore(CitusScanState *citusScanState, Job *workerJob)
{
	CustomScanState customScanState = citusScanState->customScanState;
	List *workerTaskList = workerJob->taskList;
	TupleDesc tupleDescriptor = NULL;
	ListCell *workerTaskCell = NULL;
	bool randomAccess = true;
	bool interTransactions = false;
	char *copyFormat = "text";

	tupleDescriptor = customScanState.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;

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
		StringInfo jobDirectoryName = NULL;
		StringInfo taskFilename = NULL;

		jobDirectoryName = MasterJobDirectoryName(workerTask->jobId);
		taskFilename = TaskFilename(jobDirectoryName, workerTask->taskId);

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
	CopyState copyState = NULL;

	/*
	 * Trick BeginCopyFrom into using our tuple descriptor by pretending it belongs
	 * to a relation.
	 */
	Relation stubRelation = StubRelation(tupleDescriptor);

	EState *executorState = CreateExecutorState();
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	ExprContext *executorExpressionContext = GetPerTupleExprContext(executorState);
	MemoryContext oldContext = NULL;

	int columnCount = tupleDescriptor->natts;
	Datum *columnValues = palloc0(columnCount * sizeof(Datum));
	bool *columnNulls = palloc0(columnCount * sizeof(bool));

	DefElem *copyOption = NULL;
	List *copyOptions = NIL;

#if (PG_VERSION_NUM >= 100000)
	int location = -1; /* "unknown" token location */
	copyOption = makeDefElem("format", (Node *) makeString(copyFormat), location);
#else
	copyOption = makeDefElem("format", (Node *) makeString(copyFormat));
#endif
	copyOptions = lappend(copyOptions, copyOption);

	PG_TRY();
	{
#if (PG_VERSION_NUM >= 100000)
		copyState = BeginCopyFrom(NULL, stubRelation, fileName, false, NULL,
								  NULL, copyOptions);
#else
		copyState = BeginCopyFrom(stubRelation, fileName, false, NULL,
								  copyOptions);
#endif

		while (true)
		{
			bool nextRowFound = false;

			ResetPerTupleExprContext(executorState);
			oldContext = MemoryContextSwitchTo(executorTupleContext);

			nextRowFound = NextCopyFrom(copyState, executorExpressionContext,
										columnValues, columnNulls, NULL);
			if (!nextRowFound)
			{
				MemoryContextSwitchTo(oldContext);
				break;
			}

			tuplestore_putvalues(tupstore, tupleDescriptor, columnValues, columnNulls);
			MemoryContextSwitchTo(oldContext);
		}
	}
	PG_CATCH();
	{
		if (oldContext != NULL)
		{
			MemoryContextSwitchTo(oldContext);
		}

		/*
		 * This is only necessary on windows, in the abort handler we might try to remove
		 * the file being COPY'd (if it was an intermediate result), but on Windows that's
		 * not possible unless we first close our handle to the file.
		 *
		 * This was already going to be called during abort, but it was going to be called
		 * after we try to delete the file, we need it to be called before.
		 */
		AtEOXact_Files();

		PG_RE_THROW();
	}
	PG_END_TRY();

	EndCopyFrom(copyState);
	pfree(columnValues);
	pfree(columnNulls);
}


/*
 * StubRelation creates a stub Relation from the given tuple descriptor.
 * To be able to use copy.c, we need a Relation descriptor. As there is no
 * relation corresponding to the data loaded from workers, we need to fake one.
 * We just need the bare minimal set of fields accessed by BeginCopyFrom().
 */
static Relation
StubRelation(TupleDesc tupleDescriptor)
{
	Relation stubRelation = palloc0(sizeof(RelationData));
	stubRelation->rd_att = tupleDescriptor;
	stubRelation->rd_rel = palloc0(sizeof(FormData_pg_class));
	stubRelation->rd_rel->relkind = RELKIND_RELATION;

	return stubRelation;
}


/*
 * ExecuteQueryStringIntoDestReceiver plans and executes a query and sends results
 * to the given DestReceiver.
 */
void
ExecuteQueryStringIntoDestReceiver(const char *queryString, ParamListInfo params,
								   DestReceiver *dest)
{
	Query *query = NULL;

#if (PG_VERSION_NUM >= 100000)
	RawStmt *rawStmt = (RawStmt *) ParseTreeRawStmt(queryString);
	List *queryTreeList = pg_analyze_and_rewrite(rawStmt, queryString, NULL, 0, NULL);
#else
	Node *queryTreeNode = ParseTreeNode(queryString);
	List *queryTreeList = pg_analyze_and_rewrite(queryTreeNode, queryString, NULL, 0);
#endif

	if (list_length(queryTreeList) != 1)
	{
		ereport(ERROR, (errmsg("can only execute a single query")));
	}

	query = (Query *) linitial(queryTreeList);

	ExecuteQueryIntoDestReceiver(query, params, dest);
}


/*
 * ExecuteQueryIntoDestReceiver plans and executes a query and sends results to the given
 * DestReceiver.
 */
void
ExecuteQueryIntoDestReceiver(Query *query, ParamListInfo params, DestReceiver *dest)
{
	PlannedStmt *queryPlan = NULL;
	int cursorOptions = 0;

	cursorOptions = CURSOR_OPT_PARALLEL_OK;

	/* plan the subquery, this may be another distributed query */
	queryPlan = pg_plan_query(query, cursorOptions, params);

	ExecutePlanIntoDestReceiver(queryPlan, params, dest);
}


/*
 * ExecuteIntoDestReceiver plans and executes a query and sends results to the given
 * DestReceiver.
 */
void
ExecutePlanIntoDestReceiver(PlannedStmt *queryPlan, ParamListInfo params,
							DestReceiver *dest)
{
	Portal portal = NULL;
	int eflags = 0;
	long count = FETCH_ALL;

	/* create a new portal for executing the query */
	portal = CreateNewPortal();

	/* don't display the portal in pg_cursors, it is for internal use only */
	portal->visible = false;

	PortalDefineQuery(portal,
					  NULL,
					  "",
					  "SELECT",
					  list_make1(queryPlan),
					  NULL);

	PortalStart(portal, params, eflags, GetActiveSnapshot());
#if (PG_VERSION_NUM >= 100000)
	PortalRun(portal, count, false, true, dest, dest, NULL);
#else
	PortalRun(portal, count, false, dest, dest, NULL);
#endif
	PortalDrop(portal, false);
}
