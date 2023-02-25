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

extern bool IsMergeAllowedOnRelation(Query *parse, RangeTblEntry *rte);
extern DeferredErrorMessage * MergeQuerySupported(Query *originalQuery, bool multiShardQuery,
												  PlannerRestrictionContext *
												  plannerRestrictionContext);
#endif /* MERGE_PLANNER_H */
