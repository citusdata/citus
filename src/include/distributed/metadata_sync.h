/*-------------------------------------------------------------------------
 *
 * metadata_sync.h
 *	  Type and function declarations used to sync metadata across all
 *	  workers.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef METADATA_SYNC_H
#define METADATA_SYNC_H


#include "distributed/metadata_cache.h"
#include "nodes/pg_list.h"

/* config variables */
extern int MetadataSyncInterval;
extern int MetadataSyncRetryInterval;

typedef enum
{
	METADATA_SYNC_SUCCESS = 0,
	METADATA_SYNC_FAILED_LOCK = 1,
	METADATA_SYNC_FAILED_SYNC = 2
} MetadataSyncResult;

/* Functions declarations for metadata syncing */
extern void StartMetadataSyncToNode(const char *nodeNameString, int32 nodePort);
extern bool ClusterHasKnownMetadataWorkers(void);
extern bool ShouldSyncTableMetadata(Oid relationId);
extern List * MetadataCreateCommands(void);
extern List * GetDistributedTableDDLEvents(Oid relationId);
extern List * MetadataDropCommands(void);
extern char * DistributionCreateCommand(CitusTableCacheEntry *cacheEntry);
extern char * DistributionDeleteCommand(const char *schemaName,
										const char *tableName);
extern char * TableOwnerResetCommand(Oid distributedRelationId);
extern char * NodeListInsertCommand(List *workerNodeList);
extern List * ShardListInsertCommand(List *shardIntervalList);
extern List * ShardDeleteCommandList(ShardInterval *shardInterval);
extern char * NodeDeleteCommand(uint32 nodeId);
extern char * NodeStateUpdateCommand(uint32 nodeId, bool isActive);
extern char * ShouldHaveShardsUpdateCommand(uint32 nodeId, bool shouldHaveShards);
extern char * ColocationIdUpdateCommand(Oid relationId, uint32 colocationId);
extern char * CreateSchemaDDLCommand(Oid schemaId);
extern List * GrantOnSchemaDDLCommands(Oid schemaId);
extern char * PlacementUpsertCommand(uint64 shardId, uint64 placementId, int shardState,
									 uint64 shardLength, int32 groupId);
extern void CreateTableMetadataOnWorkers(Oid relationId);
extern void MarkNodeHasMetadata(const char *nodeName, int32 nodePort, bool hasMetadata);
extern void MarkNodeMetadataSynced(const char *nodeName, int32 nodePort, bool synced);
extern MetadataSyncResult SyncMetadataToNodes(void);
extern bool SendOptionalCommandListToWorkerInTransaction(const char *nodeName, int32
														 nodePort,
														 const char *nodeUser,
														 List *commandList);

#define DELETE_ALL_NODES "TRUNCATE pg_dist_node CASCADE"
#define REMOVE_ALL_CLUSTERED_TABLES_COMMAND \
	"SELECT worker_drop_distributed_table(logicalrelid::regclass::text) FROM pg_dist_partition"
#define DISABLE_DDL_PROPAGATION "SET citus.enable_ddl_propagation TO 'off'"
#define ENABLE_DDL_PROPAGATION "SET citus.enable_ddl_propagation TO 'on'"
#define WORKER_APPLY_SEQUENCE_COMMAND "SELECT worker_apply_sequence_command (%s,%s)"
#define UPSERT_PLACEMENT \
	"INSERT INTO pg_dist_placement " \
	"(shardid, shardstate, shardlength, " \
	"groupid, placementid) " \
	"VALUES (" UINT64_FORMAT ", %d, " UINT64_FORMAT \
	", %d, " UINT64_FORMAT \
	") " \
	"ON CONFLICT (placementid) DO UPDATE SET " \
	"shardid = EXCLUDED.shardid, " \
	"shardstate = EXCLUDED.shardstate, " \
	"shardlength = EXCLUDED.shardlength, " \
	"groupid = EXCLUDED.groupid"
#define METADATA_SYNC_CHANNEL "metadata_sync"

#endif /* METADATA_SYNC_H */
