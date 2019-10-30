/*
 * multi_router_executor.h
 *
 * Function declarations used in executing distributed execution
 * plan.
 *
 */

#ifndef MULTI_ROUTER_EXECUTOR_H_
#define MULTI_ROUTER_EXECUTOR_H_

#include "c.h"

#include "access/sdir.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/placement_connection.h"
#include "executor/execdesc.h"
#include "executor/tuptable.h"
#include "nodes/pg_list.h"


/* Config variables managed via guc.c */
extern bool AllModificationsCommutative;
extern bool EnableDeadlockPrevention;
extern bool SortReturning;


extern void CitusModifyBeginScan(CustomScanState *node, EState *estate, int eflags);
extern ShardPlacementAccess * CreatePlacementAccess(ShardPlacement *placement,
													ShardPlacementAccessType accessType);

/* helper functions */
extern void AcquireExecutorShardLocks(Task *task, RowModifyLevel modLevel);
extern void AcquireExecutorMultiShardLocks(List *taskList);
extern ShardPlacementAccess * CreatePlacementAccess(ShardPlacement *placement,
													ShardPlacementAccessType accessType);
extern bool TaskListRequires2PC(List *taskList);
extern bool ReadOnlyTask(TaskType taskType);
extern List * BuildPlacementSelectList(int32 groupId, List *relationShardList);
extern List * BuildPlacementDDLList(int32 groupId, List *relationShardList);
extern void ExtractParametersForRemoteExecution(ParamListInfo paramListInfo,
												Oid **parameterTypes,
												const char ***parameterValues);
extern void ExtractParametersFromParamList(ParamListInfo paramListInfo,
										   Oid **parameterTypes,
										   const char ***parameterValues, bool
										   useOriginalCustomTypeOids);

#endif /* MULTI_ROUTER_EXECUTOR_H_ */
