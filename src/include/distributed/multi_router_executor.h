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


/*
 * XactShardConnSet keeps track of the mapping from shard to the set of nodes
 * involved in multi-statement transaction-wrapped modifications of that shard.
 * This information is used to mark placements inactive at transaction close.
 */
typedef struct XactShardConnSet
{
	uint64 shardId;            /* identifier of the shard that was modified */
	List *connectionEntryList; /* NodeConnectionEntry pointers to participating nodes */
} XactShardConnSet;


/* Config variables managed via guc.c */
extern bool AllModificationsCommutative;
extern bool EnableDeadlockPrevention;
extern bool SortReturning;


extern void CitusModifyBeginScan(CustomScanState *node, EState *estate, int eflags);
extern TupleTableSlot * RouterSelectExecScan(CustomScanState *node);
extern TupleTableSlot * RouterModifyExecScan(CustomScanState *node);

extern void ExecuteMultipleTasks(CitusScanState *scanState, List *taskList,
								 bool isModificationQuery, bool expectResults);

int64 ExecuteModifyTasksSequentially(CitusScanState *scanState, List *taskList,
									 RowModifyLevel modLevel, bool hasReturning);
extern int64 ExecuteModifyTasksWithoutResults(List *taskList);
extern int64 ExecuteModifyTasksSequentiallyWithoutResults(List *taskList,
														  RowModifyLevel modLevel);
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
