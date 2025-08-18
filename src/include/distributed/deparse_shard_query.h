/*-------------------------------------------------------------------------
 *
 * deparse_shard_query.h
 *
 * Declarations for public functions and types related to deparsing shard
 * queries.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DEPARSE_SHARD_QUERY_H
#define DEPARSE_SHARD_QUERY_H

#include "c.h"

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"

#include "distributed/citus_custom_scan.h"
#include "distributed/query_utils.h"


extern void RebuildQueryStrings(Job *workerJob);
extern bool UpdateRelationToShardNames(Node *node, List *relationShardList);
extern void UpdateWhereClauseToPushdownRecurringOuterJoin(Query *query,
														  List *relationShardList);
extern bool UpdateWhereClauseToPushdownRecurringOuterJoinWalker(Node *node,
																List *relationShardList);
Node * CreateQualsForShardInterval(RelationShard *relationShard, int attnum,
								   int outerRtIndex);
extern void SetTaskQueryIfShouldLazyDeparse(Task *task, Query *query);
extern void SetTaskQueryString(Task *task, char *queryString);
extern void SetTaskQueryStringList(Task *task, List *queryStringList);
extern void SetTaskQueryPlan(Task *task, Query *query, PlannedStmt *localPlan);
extern char * TaskQueryString(Task *task);
extern PlannedStmt * TaskQueryLocalPlan(Task *task);
extern char * TaskQueryStringAtIndex(Task *task, int index);
extern int GetTaskQueryType(Task *task);
extern void AddInsertAliasIfNeeded(Query *query);


#endif /* DEPARSE_SHARD_QUERY_H */
