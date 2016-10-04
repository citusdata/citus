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


/* reserved parameted id */
#define HIDDEN_PARAMETER_ID 0xdeadbeef

/* reserved alias name for UPSERTs */
#define UPSERT_ALIAS "citus_table_alias"


extern MultiPlan * MultiRouterPlanCreate(Query *originalQuery, Query *query,
										 MultiExecutorType taskExecutorType,
										 RelationRestrictionContext *restrictionContext);
extern void AddHiddenPartitionColumnParameter(Query *originalQuery);
extern void ErrorIfModifyQueryNotSupported(Query *queryTree);
extern Query * ReorderInsertSelectTargetListsIfExists(Query *originalQuery);
extern bool InsertSelectQuery(Query *query);

#endif /* MULTI_ROUTER_PLANNER_H */
