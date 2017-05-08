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

#include "distributed/errormessage.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_planner.h"
#include "nodes/parsenodes.h"


/* reserved alias name for UPSERTs */
#define CITUS_TABLE_ALIAS "citus_table_alias"

extern bool EnableRouterExecution;

extern MultiPlan * CreateRouterPlan(Query *originalQuery, Query *query,
									RelationRestrictionContext *restrictionContext);
extern MultiPlan * CreateModifyPlan(Query *originalQuery, Query *query,
									PlannerRestrictionContext *
									plannerRestrictionContext);
extern bool RouterSelectQuery(Query *originalQuery,
							  RelationRestrictionContext *restrictionContext,
							  List **placementList, uint64 *anchorShardId,
							  List **relationShardList, bool replacePrunedQueryWithDummy);
extern MultiPlan * CreateDistributedInsertSelectPlan(Query *originalQuery,
													 PlannerRestrictionContext *
													 plannerRestrictionContext);
extern DeferredErrorMessage * ModifyQuerySupported(Query *queryTree);
extern Query * ReorderInsertSelectTargetLists(Query *originalQuery,
											  RangeTblEntry *insertRte,
											  RangeTblEntry *subqueryRte);
extern List * ShardIntervalOpExpressions(ShardInterval *shardInterval, Index rteIndex);
extern RelationRestrictionContext * CopyRelationRestrictionContext(
	RelationRestrictionContext *oldContext);

extern bool InsertSelectQuery(Query *query);
extern Oid ExtractFirstDistributedTableId(Query *query);
extern RangeTblEntry * ExtractSelectRangeTableEntry(Query *query);
extern RangeTblEntry * ExtractInsertRangeTableEntry(Query *query);
extern void AddShardIntervalRestrictionToSelect(Query *subqery,
												ShardInterval *shardInterval);
extern ShardInterval * FindShardForInsert(Query *query,
										  DeferredErrorMessage **planningError);


#endif /* MULTI_ROUTER_PLANNER_H */
