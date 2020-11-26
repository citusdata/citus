
/*-------------------------------------------------------------------------
 *
 * listutils.h
 *
 * Declarations for public utility functions related to lists.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCAL_DISTRIBUTED_JOIN_PLANNER_H
#define LOCAL_DISTRIBUTED_JOIN_PLANNER_H

#include "postgres.h"
#include "distributed/recursive_planning.h"


extern void ConvertUnplannableTableJoinsToSubqueries(Query *query,
													 RecursivePlanningContext *context);

#endif /* LOCAL_DISTRIBUTED_JOIN_PLANNER_H */
