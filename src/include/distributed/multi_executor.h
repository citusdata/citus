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

#include "distributed/citus_custom_scan.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"


/* managed via guc.c */
typedef enum
{
	PARALLEL_CONNECTION = 0,
	SEQUENTIAL_CONNECTION = 1
} MultiShardConnectionTypes;
extern int MultiShardConnectionType;


extern bool WritableStandbyCoordinator;


extern void CitusExecutorStart(QueryDesc *queryDesc, int eflags);
extern void
#if (PG_VERSION_NUM >= 100000)
CitusExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count,
				 bool execute_once);
#else
CitusExecutorRun(QueryDesc * queryDesc, ScanDirection direction, uint64 count);
#endif
extern TupleTableSlot * ReturnTupleFromTuplestore(CitusScanState *scanState);
extern void LoadTuplesIntoTupleStore(CitusScanState *citusScanState, Job *workerJob);
extern void ReadFileIntoTupleStore(char *fileName, char *copyFormat, TupleDesc
								   tupleDescriptor, Tuplestorestate *tupstore);
extern Query * ParseQueryString(const char *queryString);
extern void ExecuteQueryStringIntoDestReceiver(const char *queryString, ParamListInfo
											   params,
											   DestReceiver *dest);
extern void ExecuteQueryIntoDestReceiver(Query *query, ParamListInfo params,
										 DestReceiver *dest);
extern void ExecutePlanIntoDestReceiver(PlannedStmt *queryPlan, ParamListInfo params,
										DestReceiver *dest);
extern void SetLocalMultiShardModifyModeToSequential(void);


#endif /* MULTI_EXECUTOR_H */
