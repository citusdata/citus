/*-------------------------------------------------------------------------
 *
 * multi_router_planner.h
 *
 * Declarations for public functions and types related to router planning.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_ROUTER_PLANNER_H
#define MULTI_ROUTER_PLANNER_H

#include "c.h"

#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_server_executor.h"
#include "nodes/parsenodes.h"


/* values used by jobs and tasks which do not require identifiers */
#define INVALID_JOB_ID 0
#define INVALID_TASK_ID 0

/* reserved alias name for UPSERTs */
#define UPSERT_ALIAS "citus_table_alias"


extern MultiPlan * MultiRouterPlanCreate(Query *originalQuery, Query *query,
										 MultiExecutorType taskExecutorType,
										 RelationRestrictionContext *restrictionContext);
extern void ErrorIfModifyQueryNotSupported(Query *queryTree);

#endif /* MULTI_ROUTER_PLANNER_H */
