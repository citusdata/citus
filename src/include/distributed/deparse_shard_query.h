/*-------------------------------------------------------------------------
 *
 * deparse_shard_query.h
 *
 * Declarations for public functions and types related to deparsing shard
 * queries.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef DEPARSE_SHARD_QUERY_H
#define DEPARSE_SHARD_QUERY_H

#include "c.h"

#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"


extern void RebuildQueryStrings(Query *originalQuery, List *taskList);
extern bool UpdateRelationToShardNames(Node *node, List *relationShardList);


#endif /* DEPARSE_SHARD_QUERY_H */
