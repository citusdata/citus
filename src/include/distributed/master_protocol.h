/*-------------------------------------------------------------------------
 *
 * master_protocol.h
 *	  Header for shared declarations for access to master node data. These data
 *	  are used to create new shards or update existing ones.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MASTER_PROTOCOL_H
#define MASTER_PROTOCOL_H

#include "postgres.h"
#include "c.h"
#include "fmgr.h"

#include "nodes/pg_list.h"


/*
 * In our distributed database, we need a mechanism to make remote procedure
 * calls between clients, the master node, and worker nodes. These remote calls
 * require serializing and deserializing values and function signatures between
 * nodes; and for these, we currently use PostgreSQL's built-in type and
 * function definition system. This approach is by no means ideal however; and
 * our implementation: (i) cannot perform compile-time type checks, (ii)
 * requires additional effort when upgrading to new function signatures, and
 * (iii) hides argument and return value names and types behind complicated
 * pg_proc.h definitions.
 *
 * An ideal implementation should overcome these problems, and make it much
 * easier to pass values back and forth between nodes. One such implementation
 * that comes close to ideal is Google's Protocol Buffers. Nonetheless, we do
 * not use it in here as its inclusion requires changes to PostgreSQL's make
 * system, and a native C version is currently unavailable.
 */


/* Number of tuple fields that master node functions return */
#define TABLE_METADATA_FIELDS 7
#define CANDIDATE_NODE_FIELDS 2
#define WORKER_NODE_FIELDS 2

/* Name of columnar foreign data wrapper */
#define CSTORE_FDW_NAME "cstore_fdw"

#define SHARDID_SEQUENCE_NAME "pg_dist_shardid_seq"
#define PLACEMENTID_SEQUENCE_NAME "pg_dist_shard_placement_placementid_seq"

/* Remote call definitions to help with data staging and deletion */
#define WORKER_APPLY_SHARD_DDL_COMMAND \
	"SELECT worker_apply_shard_ddl_command (" UINT64_FORMAT ", %s, %s)"
#define WORKER_APPLY_SHARD_DDL_COMMAND_WITHOUT_SCHEMA \
	"SELECT worker_apply_shard_ddl_command (" UINT64_FORMAT ", %s)"
#define WORKER_APPEND_TABLE_TO_SHARD \
	"SELECT worker_append_table_to_shard (%s, %s, %s, %u)"
#define WORKER_APPLY_INTER_SHARD_DDL_COMMAND \
	"SELECT worker_apply_inter_shard_ddl_command (" UINT64_FORMAT ", %s, " UINT64_FORMAT \
	", %s, %s)"
#define SHARD_RANGE_QUERY "SELECT min(%s), max(%s) FROM %s"
#define SHARD_TABLE_SIZE_QUERY "SELECT pg_table_size(%s)"
#define SHARD_CSTORE_TABLE_SIZE_QUERY "SELECT cstore_table_size(%s)"
#define DROP_REGULAR_TABLE_COMMAND "DROP TABLE IF EXISTS %s CASCADE"
#define DROP_FOREIGN_TABLE_COMMAND "DROP FOREIGN TABLE IF EXISTS %s CASCADE"
#define CREATE_SCHEMA_COMMAND "CREATE SCHEMA IF NOT EXISTS %s AUTHORIZATION %s"
#define CREATE_EMPTY_SHARD_QUERY "SELECT master_create_empty_shard('%s')"
#define FINALIZED_SHARD_PLACEMENTS_QUERY \
	"SELECT nodename, nodeport FROM pg_dist_shard_placement WHERE shardstate = 1 AND shardid = %ld"
#define UPDATE_SHARD_STATISTICS_QUERY \
	"SELECT master_update_shard_statistics(%ld)"
#define PARTITION_METHOD_QUERY "SELECT part_method FROM master_get_table_metadata('%s');"

/* Enumeration that defines the shard placement policy to use while staging */
typedef enum
{
	SHARD_PLACEMENT_INVALID_FIRST = 0,
	SHARD_PLACEMENT_LOCAL_NODE_FIRST = 1,
	SHARD_PLACEMENT_ROUND_ROBIN = 2,
	SHARD_PLACEMENT_RANDOM = 3
} ShardPlacementPolicyType;


/* Config variables managed via guc.c */
extern int ShardCount;
extern int ShardReplicationFactor;
extern int ShardMaxSize;
extern int ShardPlacementPolicy;


/* Function declarations local to the distributed module */
extern bool CStoreTable(Oid relationId);
extern uint64 GetNextShardId(void);
extern Oid ResolveRelationId(text *relationName);
extern List * GetTableDDLEvents(Oid relationId);
extern List * GetTableForeignConstraintCommands(Oid relationId);
extern char ShardStorageType(Oid relationId);
extern void CheckDistributedTable(Oid relationId);
extern void CreateShardPlacements(Oid relationId, int64 shardId, List *ddlEventList,
								  char *newPlacementOwner, List *workerNodeList,
								  int workerStartIndex, int replicationFactor);
extern uint64 UpdateShardStatistics(int64 shardId);
extern void CreateShardsWithRoundRobinPolicy(Oid distributedTableId, int32 shardCount,
											 int32 replicationFactor);
extern void CreateColocatedShards(Oid targetRelationId, Oid sourceRelationId);
extern bool WorkerCreateShard(Oid relationId, char *nodeName, uint32 nodePort,
							  int shardIndex, uint64 shardId, char *newShardOwner,
							  List *ddlCommandList, List *foreignConstraintCommadList);
extern Oid ForeignConstraintGetReferencedTableId(char *queryString);
extern void CheckHashPartitionedTable(Oid distributedTableId);

/* Function declarations for generating metadata for shard and placement creation */
extern Datum master_get_table_metadata(PG_FUNCTION_ARGS);
extern Datum master_get_table_ddl_events(PG_FUNCTION_ARGS);
extern Datum master_get_new_shardid(PG_FUNCTION_ARGS);
extern Datum master_get_new_placementid(PG_FUNCTION_ARGS);
extern Datum master_get_local_first_candidate_nodes(PG_FUNCTION_ARGS);
extern Datum master_get_round_robin_candidate_nodes(PG_FUNCTION_ARGS);
extern Datum master_get_active_worker_nodes(PG_FUNCTION_ARGS);

/* Function declarations to help with data staging and deletion */
extern Datum master_create_empty_shard(PG_FUNCTION_ARGS);
extern Datum master_append_table_to_shard(PG_FUNCTION_ARGS);
extern Datum master_update_shard_statistics(PG_FUNCTION_ARGS);
extern Datum master_apply_delete_command(PG_FUNCTION_ARGS);
extern Datum master_drop_sequences(PG_FUNCTION_ARGS);
extern Datum master_modify_multiple_shards(PG_FUNCTION_ARGS);
extern Datum master_drop_all_shards(PG_FUNCTION_ARGS);

/* function declarations for shard creation functionality */
extern Datum master_create_worker_shards(PG_FUNCTION_ARGS);

/* function declarations for shard repair functionality */
extern Datum master_copy_shard_placement(PG_FUNCTION_ARGS);


#endif   /* MASTER_PROTOCOL_H */
