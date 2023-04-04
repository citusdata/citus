/*-------------------------------------------------------------------------
 *
 * merge_planner.h
 *
 * Declarations for public functions and types related to router planning.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MERGE_PLANNER_H
#define MERGE_PLANNER_H

#include "c.h"

#include "nodes/parsenodes.h"
#include "distributed/distributed_planner.h"
#include "distributed/errormessage.h"
#include "distributed/multi_physical_planner.h"

extern bool IsMergeAllowedOnRelation(Query *parse, RangeTblEntry *rte);
extern DeferredErrorMessage * MergeQuerySupported(Query *originalQuery,
												  bool multiShardQuery,
												  PlannerRestrictionContext *
												  plannerRestrictionContext);
extern DistributedPlan * CreateMergePlan(Query *originalQuery, Query *query,
										 PlannerRestrictionContext *
										 plannerRestrictionContext);

#endif /* MERGE_PLANNER_H */
