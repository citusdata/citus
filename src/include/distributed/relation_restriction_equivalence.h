/*
 * relation_restriction_equivalence.h
 *
 * This file contains functions helper functions for planning
 * queries with colocated tables and subqueries.
 *
 * Copyright (c) 2017-2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef RELATION_RESTRICTION_EQUIVALENCE_H
#define RELATION_RESTRICTION_EQUIVALENCE_H

#include "distributed/distributed_planner.h"


extern bool ContainsUnionSubquery(Query *queryTree);
extern bool RestrictionEquivalenceForPartitionKeys(PlannerRestrictionContext *
												   plannerRestrictionContext);
extern List * GenerateAllAttributeEquivalences(PlannerRestrictionContext *
											   plannerRestrictionContext);
extern uint32 ReferenceRelationCount(RelationRestrictionContext *restrictionContext);
extern bool SafeToPushdownUnionSubquery(
	PlannerRestrictionContext *plannerRestrictionContext);
extern List * RelationIdList(Query *query);
extern PlannerRestrictionContext * FilterPlannerRestrictionForQuery(
	PlannerRestrictionContext *plannerRestrictionContext,
	Query *query);
extern JoinRestrictionContext * RemoveDuplicateJoinRestrictions(JoinRestrictionContext *
																joinRestrictionContext);

#endif /* RELATION_RESTRICTION_EQUIVALENCE_H */
