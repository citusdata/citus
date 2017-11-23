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
	List *copyOptions = NIL;
	EState *executorState = NULL;
	MemoryContext executorTupleContext = NULL;
	ExprContext *executorExpressionContext = NULL;
	TupleDesc tupleDescriptor = NULL;
	Relation stubRelation = NULL;
	ListCell *workerTaskCell = NULL;
	uint32 columnCount = 0;
	Datum *columnValues = NULL;
	bool *columnNulls = NULL;
	bool randomAccess = true;
	bool interTransactions = false;

	executorState = citusScanState->customScanState.ss.ps.state;
	executorTupleContext = GetPerTupleMemoryContext(executorState);
	executorExpressionContext = GetPerTupleExprContext(executorState);

	tupleDescriptor = customScanState.ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor;
	stubRelation = StubRelation(tupleDescriptor);

	columnCount = tupleDescriptor->natts;
	columnValues = palloc0(columnCount * sizeof(Datum));
	columnNulls = palloc0(columnCount * sizeof(bool));

	Assert(citusScanState->tuplestorestate == NULL);
	citusScanState->tuplestorestate =
		tuplestore_begin_heap(randomAccess, interTransactions, work_mem);

	if (BinaryMasterCopyFormat)
	{
		DefElem *copyOption = NULL;

#if (PG_VERSION_NUM >= 100000)
		int location = -1; /* "unknown" token location */
		copyOption = makeDefElem("format", (Node *) makeString("binary"), location);
#else
		copyOption = makeDefElem("format", (Node *) makeString("binary"));
#endif

		copyOptions = lappend(copyOptions, copyOption);
	}

	foreach(workerTaskCell, workerTaskList)
	{
		Task *workerTask = (Task *) lfirst(workerTaskCell);
		StringInfo jobDirectoryName = NULL;
		StringInfo taskFilename = NULL;
		CopyState copyState = NULL;

		jobDirectoryName = MasterJobDirectoryName(workerTask->jobId);
		taskFilename = TaskFilename(jobDirectoryName, workerTask->taskId);

#if (PG_VERSION_NUM >= 100000)
		copyState = BeginCopyFrom(NULL, stubRelation, taskFilename->data, false, NULL,
								  NULL, copyOptions);
#else
		copyState = BeginCopyFrom(stubRelation, taskFilename->data, false, NULL,
								  copyOptions);
#endif

		while (true)
		{
			MemoryContext oldContext = NULL;
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

			tuplestore_putvalues(citusScanState->tuplestorestate, tupleDescriptor,
								 columnValues, columnNulls);
			MemoryContextSwitchTo(oldContext);
		}

		EndCopyFrom(copyState);
	}
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
