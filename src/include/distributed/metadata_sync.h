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
extern void SyncNodeMetadataToNode(const char *nodeNameString, int32 nodePort);
extern bool ClusterHasKnownMetadataWorkers(void);
extern char * LocalGroupIdUpdateCommand(int32 groupId);
extern bool ShouldSyncTableMetadata(Oid relationId);
extern bool ShouldSyncTableMetadataViaCatalog(Oid relationId);
extern List * NodeMetadataCreateCommands(void);
extern List * DistributedObjectMetadataSyncCommandList(void);
extern List * NodeMetadataDropCommands(void);
extern char * MarkObjectsDistributedCreateCommand(List *addresses,
												  List *distributionArgumentIndexes,
												  List *colocationIds);
extern char * DistributionCreateCommand(CitusTableCacheEntry *cacheEntry);
extern char * DistributionDeleteCommand(const char *schemaName,
										const char *tableName);
extern char * TableOwnerResetCommand(Oid distributedRelationId);
extern char * NodeListInsertCommand(List *workerNodeList);
extern List * ShardListInsertCommand(List *shardIntervalList);
extern char * NodeDeleteCommand(uint32 nodeId);
extern char * NodeStateUpdateCommand(uint32 nodeId, bool isActive);
extern char * ShouldHaveShardsUpdateCommand(uint32 nodeId, bool shouldHaveShards);
extern char * ColocationIdUpdateCommand(Oid relationId, uint32 colocationId);
extern char * CreateSchemaDDLCommand(Oid schemaId);
extern List * GrantOnSchemaDDLCommands(Oid schemaId);
extern char * PlacementUpsertCommand(uint64 shardId, uint64 placementId, int shardState,
									 uint64 shardLength, int32 groupId);
extern char * TruncateTriggerCreateCommand(Oid relationId);
extern void CreateInterTableRelationshipOfRelationOnWorkers(Oid relationId);
extern void CreateShellTableOnWorkers(Oid relationId);
extern void CreateTableMetadataOnWorkers(Oid relationId);
extern List * DetachPartitionCommandList(void);
extern BackgroundWorkerHandle * SpawnSyncNodeMetadataToNodes(Oid database, Oid owner);
extern void SyncNodeMetadataToNodesMain(Datum main_arg);
extern void SignalMetadataSyncDaemon(Oid database, int sig);
extern bool ShouldInitiateMetadataSync(bool *lockFailure);
extern List * SequenceDependencyCommandList(Oid relationId);

extern List * DDLCommandsForSequence(Oid sequenceOid, char *ownerName);
extern List * SequenceDDLCommandsForTable(Oid relationId);
extern List * GetSequencesFromAttrDef(Oid attrdefOid);
extern void GetDependentSequencesWithRelation(Oid relationId, List **attnumList,
											  List **dependentSequenceList, AttrNumber
											  attnum);
extern Oid GetAttributeTypeOid(Oid relationId, AttrNumber attnum);

#define DELETE_ALL_NODES "TRUNCATE pg_dist_node CASCADE"
#define DELETE_ALL_PLACEMENTS "DELETE FROM pg_dist_placement CASCADE"
#define DELETE_ALL_SHARDS "DELETE FROM pg_dist_shard CASCADE"
#define DELETE_ALL_DISTRIBUTED_OBJECTS "TRUNCATE citus.pg_dist_object"
#define DELETE_ALL_PARTITIONS "DELETE FROM pg_dist_partition CASCADE"
#define REMOVE_ALL_CLUSTERED_TABLES_ONLY_COMMAND \
	"SELECT worker_drop_distributed_table_only(logicalrelid::regclass::text) FROM pg_dist_partition"
#define REMOVE_ALL_CITUS_TABLES_COMMAND \
	"SELECT worker_drop_distributed_table(logicalrelid::regclass::text) FROM pg_dist_partition"
#define BREAK_CITUS_TABLE_SEQUENCE_DEPENDENCY_COMMAND \
	"SELECT pg_catalog.worker_drop_sequence_dependency(logicalrelid::regclass::text) FROM pg_dist_partition"

#define DISABLE_DDL_PROPAGATION "SET citus.enable_ddl_propagation TO 'off'"
#define ENABLE_DDL_PROPAGATION "SET citus.enable_ddl_propagation TO 'on'"
#define DISABLE_OBJECT_PROPAGATION "SET citus.enable_object_propagation TO 'off'"
#define ENABLE_OBJECT_PROPAGATION "SET citus.enable_object_propagation TO 'on'"
#define WORKER_APPLY_SEQUENCE_COMMAND "SELECT worker_apply_sequence_command (%s,%s)"
#define UPSERT_PLACEMENT \
	"INSERT INTO pg_dist_placement " \
	"(shardid, shardstate, shardlength, " \
	"groupid, placementid) " \
	"VALUES (" UINT64_FORMAT ", %d, " UINT64_FORMAT \
	", %d, " UINT64_FORMAT \
	") " \
	"ON CONFLICT (shardid, groupid) DO UPDATE SET " \
	"shardstate = EXCLUDED.shardstate, " \
	"shardlength = EXCLUDED.shardlength, " \
	"placementid = EXCLUDED.placementid"
#define METADATA_SYNC_CHANNEL "metadata_sync"


/* controlled via GUC */
extern char *EnableManualMetadataChangesForUser;
extern bool EnableMetadataSyncByDefault;

#endif /* METADATA_SYNC_H */
