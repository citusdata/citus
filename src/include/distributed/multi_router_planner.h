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


/* reserved parameted id, we chose a negative number since it is not assigned by postgres */
#define UNINSTANTIATED_PARAMETER_ID INT_MIN

/* reserved alias name for UPSERTs */
#define CITUS_TABLE_ALIAS "citus_table_alias"


extern MultiPlan * MultiRouterPlanCreate(Query *originalQuery, Query *query,
										 MultiExecutorType taskExecutorType,
										 RelationRestrictionContext *restrictionContext);
extern void AddUninstantiatedPartitionRestriction(Query *originalQuery);
extern void ErrorIfModifyQueryNotSupported(Query *queryTree);
extern Query * ReorderInsertSelectTargetLists(Query *originalQuery,
											  RangeTblEntry *insertRte,
											  RangeTblEntry *subqueryRte);
extern bool InsertSelectQuery(Query *query);

#endif /* MULTI_ROUTER_PLANNER_H */
