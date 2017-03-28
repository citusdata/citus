/*
 * multi_colocated_subquery_planner.h
 *
 * This file contains functions helper functions for planning
 * queries with colocated tables and subqueries.
 *
 * Copyright (c) 2017-2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_COLOCATED_SUBQUERY_PLANNER_H
#define MULTI_COLOCATED_SUBQUERY_PLANNER_H

#include "distributed/multi_planner.h"


extern bool AllRelationsJoinedOnPartitionKey(RelationRestrictionContext *
											 restrictionContext,
											 JoinRestrictionContext *
											 joinRestrictionContext);


#endif /* MULTI_COLOCATED_SUBQUERY_PLANNER_H */
