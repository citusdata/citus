/*-------------------------------------------------------------------------
 *
 * relay_utility.h
 *
 * Header and type declarations that extend relation, index and constraint names
 * with the appropriate shard identifiers.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id:$
 *
 *-------------------------------------------------------------------------
 */

#ifndef RELAY_UTILITY_H
#define RELAY_UTILITY_H

#include "fmgr.h"

#include "lib/stringinfo.h"
#include "nodes/nodes.h"


/* Shard name and identifier related defines */
#define SHARD_NAME_SEPARATOR '_'
#define INVALID_SHARD_ID 0
#define INVALID_PLACEMENT_ID 0


/* Function declarations to extend names in DDL commands */
extern void RelayEventExtendNames(Node *parseTree, char *schemaName, uint64 shardId);
extern void RelayEventExtendNamesForInterShardCommands(Node *parseTree,
													   uint64 leftShardId,
													   char *leftShardSchemaName,
													   uint64 rightShardId,
													   char *rightShardSchemaName);
extern void AppendShardIdToName(char **name, uint64 shardId);

extern void SetSchemaNameIfNotExist(char **schemaName, const char *newSchemaName);

#endif   /* RELAY_UTILITY_H */
