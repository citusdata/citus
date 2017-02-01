/*-------------------------------------------------------------------------
 *
 * multi_executor.h
 *	  Executor support for Citus.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_EXECUTOR_H
#define MULTI_EXECUTOR_H

#include "executor/execdesc.h"
#include "nodes/parsenodes.h"
#include "nodes/execnodes.h"

#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"

/* signal currently executed statement is a master select statement or router execution */
#define EXEC_FLAG_CITUS_MASTER_SELECT 0x100
#define EXEC_FLAG_CITUS_ROUTER_EXECUTOR 0x200

#if (PG_VERSION_NUM >= 90600)
#define tuplecount_t uint64
#else
#define tuplecount_t long
#endif


typedef struct CitusScanState
{
	CustomScanState customScanState;
	MultiPlan *multiPlan;
	MultiExecutorType executorType;

	/* state for router */
	bool finishedUnderlyingScan;
	Tuplestorestate *tuplestorestate;
} CitusScanState;

Node * CitusCreateScan(CustomScan *scan);
extern void CitusBeginScan(CustomScanState *node,
						   EState *estate,
						   int eflags);
extern TupleTableSlot * CitusExecScan(CustomScanState *node);
extern void CitusEndScan(CustomScanState *node);
extern void CitusReScan(CustomScanState *node);
extern void CitusExplainScan(CustomScanState *node, List *ancestors,
							 struct ExplainState *es);

#endif /* MULTI_EXECUTOR_H */
