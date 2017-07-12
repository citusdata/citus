/*-------------------------------------------------------------------------
 *
 * metadata_sync.h
 *	  Type and function declarations used to sync metadata across all
 *	  workers.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef METADATA_SYNC_H
#define METADATA_SYNC_H


#include "distributed/metadata_cache.h"
#include "nodes/pg_list.h"


/* Functions declarations for metadata syncing */
extern bool ShouldSyncTableMetadata(Oid relationId);
extern List * MetadataCreateCommands(void);
extern List * GetDistributedTableDDLEvents(Oid relationId);
extern List * MetadataDropCommands(void);
extern char * DistributionCreateCommand(DistTableCacheEntry *cacheEntry);
extern char * DistributionDeleteCommand(char *schemaName,
										char *tableName);
extern char * TableOwnerResetCommand(Oid distributedRelationId);
extern char * NodeListInsertCommand(List *workerNodeList);
extern List * ShardListInsertCommand(List *shardIntervalList);
extern List * ShardDeleteCommandList(ShardInterval *shardInterval);
extern char * NodeDeleteCommand(uint32 nodeId);
extern char * NodeStateUpdateCommand(uint32 nodeId, bool isActive);
extern char * ColocationIdUpdateCommand(Oid relationId, uint32 colocationId);
extern char * CreateSchemaDDLCommand(Oid schemaId);
extern char * PlacementUpsertCommand(uint64 shardId, uint64 placementId, int shardState,
									 uint64 shardLength, uint32 groupId);
extern void CreateTableMetadataOnWorkers(Oid relationId);


#define DELETE_ALL_NODES "TRUNCATE pg_dist_node"
#define REMOVE_ALL_CLUSTERED_TABLES_COMMAND \
	"SELECT worker_drop_distributed_table(logicalrelid) FROM pg_dist_partition"
#define DISABLE_DDL_PROPAGATION "SET citus.enable_ddl_propagation TO 'off'"
#define WORKER_APPLY_SEQUENCE_COMMAND "SELECT worker_apply_sequence_command (%s)"
#define UPSERT_PLACEMENT "INSERT INTO pg_dist_placement " \
						 "(shardid, shardstate, shardlength, " \
						 "groupid, placementid) " \
						 "VALUES (%lu, %d, %lu, %d, %lu) " \
						 "ON CONFLICT (placementid) DO UPDATE SET " \
						 "shardid = EXCLUDED.shardid, " \
						 "shardstate = EXCLUDED.shardstate, " \
						 "shardlength = EXCLUDED.shardlength, " \
						 "groupid = EXCLUDED.groupid"


#endif /* METADATA_SYNC_H */
