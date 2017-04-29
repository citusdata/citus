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

#include "distributed/multi_planner.h"


extern bool ContainsUnionSubquery(Query *queryTree);
extern bool RestrictionEquivalenceForPartitionKeys(PlannerRestrictionContext *
												   plannerRestrictionContext);
extern bool SafeToPushdownUnionSubquery(RelationRestrictionContext *restrictionContext);
extern List * RelationIdList(Query *query);


#endif /* RELATION_RESTRICTION_EQUIVALENCE_H */
