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
extern char * NodeDeleteCommand(uint32 nodeId);
extern char * ColocationIdUpdateCommand(Oid relationId, uint32 colocationId);
extern char * CreateSchemaDDLCommand(Oid schemaId);


#define DELETE_ALL_NODES "TRUNCATE pg_dist_node"
#define REMOVE_ALL_CLUSTERED_TABLES_COMMAND \
	"SELECT worker_drop_distributed_table(logicalrelid) FROM pg_dist_partition"
#define DISABLE_DDL_PROPAGATION "SET citus.enable_ddl_propagation TO 'off'"
#define WORKER_APPLY_SEQUENCE_COMMAND "SELECT worker_apply_sequence_command (%s)"


#endif /* METADATA_SYNC_H */
