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
#include "distributed/metadata_cache.h"

#define SINGLE_RTE_INDEX 1

extern bool AllDistributionKeysInQueryAreEqual(Query *originalQuery,
											   PlannerRestrictionContext *
											   plannerRestrictionContext);
extern bool SafeToPushdownUnionSubquery(Query *originalQuery, PlannerRestrictionContext *
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
extern uint32 UniqueRelationCount(RelationRestrictionContext *restrictionContext,
								  CitusTableType tableType);
extern List * DistributedRelationIdList(Query *query);
extern PlannerRestrictionContext * FilterPlannerRestrictionForQuery(
	PlannerRestrictionContext *plannerRestrictionContext,
	Query *query);
extern List * GetRestrictInfoListForRelation(RangeTblEntry *rangeTblEntry,
											 PlannerRestrictionContext *
											 plannerRestrictionContext);
extern RelationRestriction * RelationRestrictionForRelation(
	RangeTblEntry *rangeTableEntry,
	PlannerRestrictionContext *
	plannerRestrictionContext);
extern JoinRestrictionContext * RemoveDuplicateJoinRestrictions(JoinRestrictionContext *
																joinRestrictionContext);

extern bool EquivalenceListContainsRelationsEquality(List *attributeEquivalenceList,
													 RelationRestrictionContext *
													 restrictionContext);
extern RelationRestrictionContext * FilterRelationRestrictionContext(
	RelationRestrictionContext *relationRestrictionContext,
	Relids
	queryRteIdentities);
#endif /* RELATION_RESTRICTION_EQUIVALENCE_H */
