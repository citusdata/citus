/*
 * database_sharding.h
 *
 * Data structure definition for managing backend data and related function
 *
 * Copyright (c) Microsoft, Inc.
 */

#ifndef DATABASE_SHARDING_H
#define DATABASE_SHARDING_H


/* attributes of citus_catalog.database_shard */
#define Natts_database_shard 3
#define Anum_database_shard_database_id 1
#define Anum_database_shard_node_group_id 2
#define Anum_database_shard_is_available 3


typedef struct DatabaseShard
{
	/* database oid */
	Oid databaseOid;

	/* node group on which the database shard is placed */
	int nodeGroupId;

	/* whether the database shard is available */
	bool isAvailable;
} DatabaseShard;

/* citus.enable_database_sharding setting */
extern bool EnableDatabaseSharding;

void HandleDDLInDatabaseShard(Node *parseTree, bool *runPreviousUtilityHook);
bool DatabaseShardingEnabled(void);
void AssignDatabaseToShard(Oid databaseOid);
void InsertDatabaseShardAssignmentLocally(Oid databaseOid, int nodeGroupId);
void DeleteDatabaseShardByDatabaseIdLocally(Oid databaseOid);
List * ListDatabaseShards(void);


#endif
