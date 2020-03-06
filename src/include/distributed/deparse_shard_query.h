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


extern void RebuildQueryStrings(Job *workerJob);
extern bool UpdateRelationToShardNames(Node *node, List *relationShardList);
extern void SetTaskQuery(Task *task, Query *query);
extern void SetTaskQueryString(Task *task, char *queryString);
extern char * TaskQueryString(Task *task);
extern bool UpdateRelationsToLocalShardTables(Node *node, List *relationShardList);

#endif /* DEPARSE_SHARD_QUERY_H */
