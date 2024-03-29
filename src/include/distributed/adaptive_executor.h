#ifndef ADAPTIVE_EXECUTOR_H
#define ADAPTIVE_EXECUTOR_H

#include "distributed/multi_physical_planner.h"

/* GUC, determining whether Citus opens 1 connection per task */
extern bool ForceMaxQueryParallelization;
extern int MaxAdaptiveExecutorPoolSize;
extern bool EnableBinaryProtocol;


/* GUC, number of ms to wait between opening connections to the same worker */
extern int ExecutorSlowStartInterval;
extern bool EnableCostBasedConnectionEstablishment;
extern bool PreventIncompleteConnectionEstablishment;

extern uint64 ExecuteTaskList(RowModifyLevel modLevel, List *taskList);
extern uint64 ExecuteUtilityTaskList(List *utilityTaskList, bool localExecutionSupported);
extern uint64 ExecuteUtilityTaskListExtended(List *utilityTaskList, int poolSize,
											 bool localExecutionSupported);
extern uint64 ExecuteTaskListOutsideTransaction(RowModifyLevel modLevel, List *taskList,
												int targetPoolSize, List *jobIdList);


#endif /* ADAPTIVE_EXECUTOR_H */
