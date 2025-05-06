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


bool ExtractIndexesForConstaints(List *fromList, List *rtable, int *outerRtIndex, RangeTblEntry **distRte);
extern void RebuildQueryStrings(Job *workerJob);
extern bool UpdateRelationToShardNames(Node *node, List *relationShardList);
extern bool UpdateWhereClauseForOuterJoin(Node *node, List *relationShardList);
extern void SetTaskQueryIfShouldLazyDeparse(Task *task, Query *query);
extern void SetTaskQueryString(Task *task, char *queryString);
extern void SetTaskQueryStringList(Task *task, List *queryStringList);
extern char * TaskQueryString(Task *task);
extern char * TaskQueryStringAtIndex(Task *task, int index);
extern int GetTaskQueryType(Task *task);
extern void AddInsertAliasIfNeeded(Query *query);


#endif /* DEPARSE_SHARD_QUERY_H */
