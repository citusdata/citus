/*
 * multi_planner_utils.h
 *
 * This file contains functions helper functions for planning
 * queries with colocated tables and subqueries.
 *
 * Copyright (c) 2017-2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_PLANNER_UTILS_H
#define MULTI_PLANNER_UTILS_H

#include "distributed/multi_planner.h"


extern bool AllRelationsJoinedOnPartitionKey(PlannerRestrictionContext *
											 plannerRestrictionContext);


#endif /* MULTI_PLANNER_UTILS_H */
