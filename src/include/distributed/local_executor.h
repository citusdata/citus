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
#include "distributed/tuple_destination.h"


/* enabled with GUCs*/
extern bool EnableLocalExecution;
extern bool LogLocalCommands;

extern int LocalExecutorLevel;

typedef enum LocalExecutionStatus
{
	LOCAL_EXECUTION_REQUIRED,
	LOCAL_EXECUTION_OPTIONAL,
	LOCAL_EXECUTION_DISABLED
} LocalExecutionStatus;

/* extern function declarations */
extern LocalExecutionStatus GetCurrentLocalExecutionStatus(void);
extern uint64 ExecuteLocalTaskList(List *taskList, TupleDestination *defaultTupleDest);
extern uint64 ExecuteLocalUtilityTaskList(List *utilityTaskList);
extern uint64 ExecuteLocalTaskListExtended(List *taskList, ParamListInfo
										   orig_paramListInfo,
										   DistributedPlan *distributedPlan,
										   TupleDestination *defaultTupleDest,
										   bool isUtilityCommand);
extern void ExtractLocalAndRemoteTasks(bool readOnlyPlan, List *taskList,
									   List **localTaskList, List **remoteTaskList);
extern void ExecuteUtilityCommand(const char *utilityCommand);
extern bool ShouldExecuteTasksLocally(List *taskList);
extern bool AnyTaskAccessesLocalNode(List *taskList);
extern bool TaskAccessesLocalNode(Task *task);
extern void ErrorIfTransactionAccessedPlacementsLocally(void);
extern void DisableLocalExecution(void);
extern void SetLocalExecutionStatus(LocalExecutionStatus newStatus);
extern void ExtractParametersForLocalExecution(ParamListInfo paramListInfo,
											   Oid **parameterTypes,
											   const char ***parameterValues);

#endif /* LOCAL_EXECUTION_H */
