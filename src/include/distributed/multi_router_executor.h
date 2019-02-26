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

/* number of nested stored procedure call levels we are currently in */
extern int StoredProcedureLevel;


extern void CitusModifyBeginScan(CustomScanState *node, EState *estate, int eflags);
extern TupleTableSlot * RouterSelectExecScan(CustomScanState *node);
extern TupleTableSlot * RouterModifyExecScan(CustomScanState *node);

extern void ExecuteMultipleTasks(CitusScanState *scanState, List *taskList,
								 bool isModificationQuery, bool expectResults);

extern int64 ExecuteModifyTasksWithoutResults(List *taskList);
extern int64 ExecuteModifyTasksSequentiallyWithoutResults(List *taskList,
														  CmdType operation);

/* helper functions */
extern bool TaskListRequires2PC(List *taskList);
extern List * BuildPlacementSelectList(int32 groupId, List *relationShardList);
extern List * BuildPlacementDDLList(int32 groupId, List *relationShardList);

#endif /* MULTI_ROUTER_EXECUTOR_H_ */
