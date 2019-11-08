#ifndef ADAPTIVE_EXECUTOR_H
#define ADAPTIVE_EXECUTOR_H

#include "distributed/multi_physical_planner.h"

/* GUC, determining whether Citus opens 1 connection per task */
extern bool ForceMaxQueryParallelization;
extern int MaxAdaptiveExecutorPoolSize;

/* GUC, number of ms to wait between opening connections to the same worker */
extern int ExecutorSlowStartInterval;

extern uint64 ExecuteTaskList(RowModifyLevel modLevel, List *taskList, int
							  targetPoolSize);

#endif /* ADAPTIVE_EXECUTOR_H */
