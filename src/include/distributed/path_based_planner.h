//
// Created by Nils Dijk on 17/01/2020.
//

#ifndef CITUS_PATH_BASED_PLANNER_H
#define CITUS_PATH_BASED_PLANNER_H

#include "nodes/parsenodes.h"
#include "nodes/pathnodes.h"
#include "optimizer/paths.h"

typedef List *(*optimizeFn)(PlannerInfo *root, Path *originalPath);

typedef struct OptimizationEntry
{
	const char *name;
	bool enabled;
	optimizeFn fn;
} OptimizationEntry;

extern bool EnableBroadcastJoin;

extern Cost CollectStartupCost;
extern Cost CollectPerRowCost;
extern Cost CollectPerMBCost;

extern Cost RepartitionStartupCost;
extern Cost RepartitionPerRowCost;
extern Cost RepartitionPerMBCost;

extern OptimizationEntry joinOptimizations[];
extern OptimizationEntry groupOptimizations[];

extern void PathBasedPlannerRelationHook(PlannerInfo *root,
										 RelOptInfo *relOptInfo,
										 Index restrictionIndex,
										 RangeTblEntry *rte);
extern void PathBasedPlannerJoinHook(PlannerInfo *root,
									 RelOptInfo *joinrel,
									 RelOptInfo *outerrel,
									 RelOptInfo *innerrel,
									 JoinType jointype,
									 JoinPathExtraData *extra);
extern void PathBasedPlannedUpperPathHook(PlannerInfo *root,
										  UpperRelationKind stage,
										  RelOptInfo *input_rel,
										  RelOptInfo *output_rel,
										  void *extra);

extern PathComparison PathBasedPlannerComparePath(Path *new_path, Path *old_path);

#endif //CITUS_PATH_BASED_PLANNER_H
