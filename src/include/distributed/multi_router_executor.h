/*
 * multi_router_executor.h
 *
 * Function declarations used in executing distributed execution
 * plan.
 *
 */

#ifndef MULTI_ROUTER_EXECUTOR_H_
#define MULTI_ROUTER_EXECUTOR_H_

#include "distributed/multi_physical_planner.h"
#include "executor/execdesc.h"

extern bool AllModificationsCommutative;


extern void RouterExecutorStart(QueryDesc *queryDesc, int eflags, Task *task);
extern void RouterExecutorRun(QueryDesc *queryDesc, ScanDirection direction, long count);
extern void RouterExecutorFinish(QueryDesc *queryDesc);
extern void RouterExecutorEnd(QueryDesc *queryDesc);

#endif /* MULTI_ROUTER_EXECUTOR_H_ */
