/*-------------------------------------------------------------------------
 *
 * local_executor.h
 *	Functions and global variables to control local query execution.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCAL_EXECUTION_H
#define LOCAL_EXECUTION_H

#include "distributed/citus_custom_scan.h"

/* enabled with GUCs*/
extern bool EnableLocalExecution;
extern bool LogLocalCommands;

extern bool TransactionAccessedLocalPlacement;
extern bool TransactionConnectedToLocalGroup;

/* extern function declarations */
extern uint64 ExtractAndExecuteLocalAndRemoteTasks(CitusScanState *scanState,
												   List *taskList, RowModifyLevel
												   rowModifyLevel, bool hasReturning);
extern uint64 ExecuteLocalTaskList(List *taskList,
								   Tuplestorestate *tupleStoreState);
extern uint64 ExecuteLocalTaskListExtended(List *taskList, ParamListInfo
										   orig_paramListInfo,
										   DistributedPlan *distributedPlan,
										   Tuplestorestate *tupleStoreState);
extern void ExecuteLocalUtilityTaskList(List *localTaskList);
extern void ExtractLocalAndRemoteTasks(bool readOnlyPlan, List *taskList,
									   List **localTaskList, List **remoteTaskList);
extern bool ShouldExecuteTasksLocally(List *taskList);
extern bool AnyTaskAccessesLocalNode(List *taskList);
extern bool TaskAccessesLocalNode(Task *task);
extern void ErrorIfTransactionAccessedPlacementsLocally(void);
extern void DisableLocalExecution(void);

#endif /* LOCAL_EXECUTION_H */
