/*-------------------------------------------------------------------------
 *
 * relay_utility.h
 *
 * Header and type declarations that extend relation, index and constraint names
 * with the appropriate shard identifiers.
 *
 * Copyright (c) 2012, Citus Data, Inc.
 *
 * $Id:$
 *
 *-------------------------------------------------------------------------
 */

#ifndef RELAY_UTILITY_H
#define RELAY_UTILITY_H

#include "lib/stringinfo.h"
#include "nodes/nodes.h"


/* Shard name and identifier related defines */
#define SHARD_NAME_SEPARATOR '_'
#define INVALID_SHARD_ID 0


/*
 * RelayFileState represents last known states of shards on a given node. We
 * currently only have shards in finalized or cached state; and set this state
 * after shards are sucessfully staged or cached.
 */
typedef enum
{
	FILE_INVALID_FIRST = 0,
	FILE_FINALIZED = 1,
	FILE_CACHED = 2,
	FILE_INACTIVE = 3,
	FILE_TO_DELETE = 4
} RelayFileState;


/* Function declarations to extend names in DDL commands */
extern void RelayEventExtendNames(Node *parseTree, uint64 shardId);
extern void AppendShardIdToName(char **name, uint64 shardId);
extern void AppendShardIdToStringInfo(StringInfo name, uint64 shardId);


#endif   /* RELAY_UTILITY_H */
