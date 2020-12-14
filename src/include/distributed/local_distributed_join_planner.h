
/*-------------------------------------------------------------------------
 *
 * local_distributed_join_planner.h
 *
 * Declarations for functions to handle local-distributed table joins.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCAL_DISTRIBUTED_JOIN_PLANNER_H
#define LOCAL_DISTRIBUTED_JOIN_PLANNER_H

#include "postgres.h"
#include "distributed/recursive_planning.h"

/* managed via guc.c */
typedef enum
{
	LOCAL_JOIN_POLICY_NEVER = 0,
	LOCAL_JOIN_POLICY_PREFER_LOCAL = 1,
	LOCAL_JOIN_POLICY_PREFER_DISTRIBUTED = 2,
	LOCAL_JOIN_POLICY_AUTO = 3,
} LocalJoinPolicy;

extern int LocalTableJoinPolicy;

extern bool ShouldConvertLocalTableJoinsToSubqueries(List *rangeTableList);
extern void RecursivelyPlanLocalTableJoins(Query *query,
										   RecursivePlanningContext *context);

#endif /* LOCAL_DISTRIBUTED_JOIN_PLANNER_H */
