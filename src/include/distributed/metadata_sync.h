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
extern List * MetadataDropCommands(void);
extern char * DistributionCreateCommand(DistTableCacheEntry *cacheEntry);
extern char * DistributionDeleteCommand(char *schemaName,
										char *tableName);
extern char * TableOwnerResetCommand(Oid distributedRelationId);
extern char * NodeListInsertCommand(List *workerNodeList);
extern List * ShardListInsertCommand(List *shardIntervalList);
extern char * NodeDeleteCommand(uint32 nodeId);


#define DELETE_ALL_NODES "TRUNCATE pg_dist_node"
#define REMOVE_ALL_CLUSTERED_TABLES_COMMAND \
	"SELECT worker_drop_distributed_table(logicalrelid) FROM pg_dist_partition"


#endif /* METADATA_SYNC_H */
