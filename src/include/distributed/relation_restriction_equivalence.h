/*
 * relation_restriction_equivalence.h
 *
 * This file contains functions helper functions for planning
 * queries with colocated tables and subqueries.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef RELATION_RESTRICTION_EQUIVALENCE_H
#define RELATION_RESTRICTION_EQUIVALENCE_H

#include "distributed/distributed_planner.h"


extern bool AllDistributionKeysInQueryAreEqual(Query *originalQuery,
											   PlannerRestrictionContext *
											   plannerRestrictionContext);
extern bool SafeToPushdownUnionSubquery(PlannerRestrictionContext *
										plannerRestrictionContext);
extern bool ContainsUnionSubquery(Query *queryTree);
extern bool RestrictionEquivalenceForPartitionKeys(PlannerRestrictionContext *
												   plannerRestrictionContext);
bool RestrictionEquivalenceForPartitionKeysViaEquivalences(PlannerRestrictionContext *
														   plannerRestrictionContext,
														   List *
														   allAttributeEquivalenceList);
extern List * GenerateAllAttributeEquivalences(PlannerRestrictionContext *
											   plannerRestrictionContext);
extern uint32 ReferenceRelationCount(RelationRestrictionContext *restrictionContext);

extern List * DistributedRelationIdList(Query *query);
extern PlannerRestrictionContext * FilterPlannerRestrictionForQuery(
	PlannerRestrictionContext *plannerRestrictionContext,
	Query *query);
extern List * GetRestrictInfoListForRelation(RangeTblEntry *rangeTblEntry,
											 PlannerRestrictionContext *
											 plannerRestrictionContext);
extern JoinRestrictionContext * RemoveDuplicateJoinRestrictions(JoinRestrictionContext *
																joinRestrictionContext);

extern bool EquivalenceListContainsRelationsEquality(List *attributeEquivalenceList,
													 RelationRestrictionContext *
													 restrictionContext);

#endif /* RELATION_RESTRICTION_EQUIVALENCE_H */
