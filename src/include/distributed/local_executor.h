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

extern HTAB *LocalExecutionStateHash;

/* extern function declarations */
extern uint64 ExecuteLocalTaskList(CitusScanState *scanState, List *taskList,
								   HTAB *localExecutionStateHash);
extern void ExtractLocalAndRemoteTasks(bool readOnlyPlan, List *taskList,
									   List **localTaskList, List **remoteTaskList);
extern bool ShouldExecuteTasksLocally(List *taskList);
extern bool AnyTaskAccessesLocalNode(List *taskList);
extern bool TaskAccessesLocalNode(Task *task);
extern void ErrorIfTransactionAccessedPlacementsLocally(void);
extern void DisableLocalExecution(void);
extern HTAB * CreateLocalExecutionStateHash(void);
#endif /* LOCAL_EXECUTION_H */
