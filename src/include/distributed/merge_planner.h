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

extern DistributedPlan * CreateMergePlan(uint64 planId, Query *originalQuery,
										 Query *query,
										 PlannerRestrictionContext *
										 plannerRestrictionContext,
										 ParamListInfo boundParams);
extern bool IsLocalTableModification(Oid targetRelationId, Query *query,
									 uint64 shardId,
									 RTEListProperties *rteProperties);
extern void NonPushableMergeCommandExplainScan(CustomScanState *node, List *ancestors,
											   struct ExplainState *es);
extern Var * FetchAndValidateInsertVarIfExists(Oid targetRelationId, Query *query);
extern RangeTblEntry * ExtractMergeSourceRangeTableEntry(Query *query, bool joinSourceOk);
extern FromExpr * GetMergeJoinTree(Query *mergeQuery);


#endif /* MERGE_PLANNER_H */
