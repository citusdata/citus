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


#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "nodes/pg_list.h"

/* managed via guc.c */
typedef enum
{
	METADATA_SYNC_TRANSACTIONAL = 0,
	METADATA_SYNC_NON_TRANSACTIONAL = 1
} MetadataSyncTransactionMode;

/* config variables */
extern int MetadataSyncInterval;
extern int MetadataSyncRetryInterval;
extern int MetadataSyncTransMode;

/*
 * MetadataSyncContext is used throughout metadata sync.
 */
typedef struct MetadataSyncContext
{
	List *activatedWorkerNodeList; /* activated worker nodes */
	List *activatedWorkerBareConnections; /* bare connections to activated nodes */
	MemoryContext context; /* memory context for all allocations */
	MetadataSyncTransactionMode transactionMode; /* transaction mode for the sync */
	bool collectCommands; /* if we collect commands instead of sending and resetting */
	List *collectedCommands; /* collected commands. (NIL if collectCommands == false) */
	bool nodesAddedInSameTransaction; /* if the nodes are added just before activation */
} MetadataSyncContext;

typedef enum
{
	NODE_METADATA_SYNC_SUCCESS = 0,
	NODE_METADATA_SYNC_FAILED_LOCK = 1,
	NODE_METADATA_SYNC_FAILED_SYNC = 2
} NodeMetadataSyncResult;

/*
 * Information about dependent sequences. We do not have the
 * dependent relationId as no caller needs. But, could be added
 * here if needed.
 */
typedef struct SequenceInfo
{
	Oid sequenceOid;
	int attributeNumber;

	/*
	 * true for nexval(seq) -- which also includes serials
	 * false when only OWNED BY col
	 */
	bool isNextValDefault;
} SequenceInfo;


/* Functions declarations for metadata syncing */
extern void citus_internal_add_placement_metadata_internal(int64 shardId,
														   int64 shardLength,
														   int32 groupId,
														   int64 placementId);
extern void SyncCitusTableMetadata(Oid relationId);
extern void EnsureSequentialModeMetadataOperations(void);
extern bool ClusterHasKnownMetadataWorkers(void);
extern char * LocalGroupIdUpdateCommand(int32 groupId);
extern bool ShouldSyncUserCommandForObject(ObjectAddress objectAddress);
extern bool ShouldSyncTableMetadata(Oid relationId);
extern bool ShouldSyncTableMetadataViaCatalog(Oid relationId);
extern Oid FetchRelationIdFromPgPartitionHeapTuple(HeapTuple heapTuple,
												   TupleDesc tupleDesc);
extern bool ShouldSyncSequenceMetadata(Oid relationId);
extern List * NodeMetadataCreateCommands(void);
extern List * CitusTableMetadataCreateCommandList(Oid relationId);
extern List * NodeMetadataDropCommands(void);
extern char * MarkObjectsDistributedCreateCommand(List *addresses,
												  List *distributionArgumentIndexes,
												  List *colocationIds,
												  List *forceDelegations);
extern char * DistributionCreateCommand(CitusTableCacheEntry *cacheEntry);
extern char * DistributionDeleteCommand(const char *schemaName,
										const char *tableName);
extern char * DistributionDeleteMetadataCommand(Oid relationId);
extern char * TableOwnerResetCommand(Oid distributedRelationId);
extern char * NodeListInsertCommand(List *workerNodeList);
char * NodeListIdempotentInsertCommand(List *workerNodeList);
extern List * ShardListInsertCommand(List *shardIntervalList);
extern List * ShardDeleteCommandList(ShardInterval *shardInterval);
extern char * NodeDeleteCommand(uint32 nodeId);
extern char * NodeStateUpdateCommand(uint32 nodeId, bool isActive);
extern char * ShouldHaveShardsUpdateCommand(uint32 nodeId, bool shouldHaveShards);
extern char * ColocationIdUpdateCommand(Oid relationId, uint32 colocationId);
extern char * CreateSchemaDDLCommand(Oid schemaId);
extern List * GrantOnSchemaDDLCommands(Oid schemaId);
extern List * GrantOnFunctionDDLCommands(Oid functionOid);
extern List * GrantOnForeignServerDDLCommands(Oid serverId);
extern List * GenerateGrantOnForeignServerQueriesFromAclItem(Oid serverId,
															 AclItem *aclItem);
extern List * GenerateGrantOnFDWQueriesFromAclItem(Oid serverId, AclItem *aclItem);
extern char * PlacementUpsertCommand(uint64 shardId, uint64 placementId,
									 uint64 shardLength, int32 groupId);
extern TableDDLCommand * TruncateTriggerCreateCommand(Oid relationId);
extern void CreateInterTableRelationshipOfRelationOnWorkers(Oid relationId);
extern List * InterTableRelationshipOfRelationCommandList(Oid relationId);
extern List * DetachPartitionCommandList(void);
extern void SyncNodeMetadataToNodes(void);
extern BackgroundWorkerHandle * SpawnSyncNodeMetadataToNodes(Oid database, Oid owner);
extern void SyncNodeMetadataToNodesMain(Datum main_arg);
extern void SignalMetadataSyncDaemon(Oid database, int sig);
extern bool ShouldInitiateMetadataSync(bool *lockFailure);
extern List * SequenceDependencyCommandList(Oid relationId);
extern List * IdentitySequenceDependencyCommandList(Oid targetRelationId);

extern List * DDLCommandsForSequence(Oid sequenceOid, char *ownerName);
extern List * GetSequencesFromAttrDef(Oid attrdefOid);
extern void GetDependentSequencesWithRelation(Oid relationId, List **seqInfoList,
											  AttrNumber attnum, char depType);
extern List * GetDependentFunctionsWithRelation(Oid relationId);
extern Oid GetAttributeTypeOid(Oid relationId, AttrNumber attnum);
extern void SetLocalEnableMetadataSync(bool state);
extern void SyncNewColocationGroupToNodes(uint32 colocationId, int shardCount,
										  int replicationFactor,
										  Oid distributionColumType,
										  Oid distributionColumnCollation);
extern void SyncDeleteColocationGroupToNodes(uint32 colocationId);

extern MetadataSyncContext * CreateMetadataSyncContext(List *nodeList,
													   bool collectCommands,
													   bool nodesAddedInSameTransaction);
extern void EstablishAndSetMetadataSyncBareConnections(MetadataSyncContext *context);
extern void SetMetadataSyncNodesFromNodeList(MetadataSyncContext *context,
											 List *nodeList);
extern void ResetMetadataSyncMemoryContext(MetadataSyncContext *context);
extern bool MetadataSyncCollectsCommands(MetadataSyncContext *context);
extern void SendOrCollectCommandListToActivatedNodes(MetadataSyncContext *context,
													 List *commands);
extern void SendOrCollectCommandListToMetadataNodes(MetadataSyncContext *context,
													List *commands);
extern void SendOrCollectCommandListToSingleNode(MetadataSyncContext *context,
												 List *commands, int nodeIdx);

extern void ActivateNodeList(MetadataSyncContext *context);

extern char * WorkerDropAllShellTablesCommand(bool singleTransaction);

extern void SyncDistributedObjects(MetadataSyncContext *context);
extern void SendNodeWideObjectsSyncCommands(MetadataSyncContext *context);
extern void SendShellTableDeletionCommands(MetadataSyncContext *context);
extern void SendMetadataDeletionCommands(MetadataSyncContext *context);
extern void SendColocationMetadataCommands(MetadataSyncContext *context);
extern void SendDependencyCreationCommands(MetadataSyncContext *context);
extern void SendDistTableMetadataCommands(MetadataSyncContext *context);
extern void SendDistObjectCommands(MetadataSyncContext *context);
extern void SendInterTableRelationshipCommands(MetadataSyncContext *context);

#define DELETE_ALL_NODES "DELETE FROM pg_dist_node"
#define DELETE_ALL_PLACEMENTS "DELETE FROM pg_dist_placement"
#define DELETE_ALL_SHARDS "DELETE FROM pg_dist_shard"
#define DELETE_ALL_DISTRIBUTED_OBJECTS "DELETE FROM pg_catalog.pg_dist_object"
#define DELETE_ALL_PARTITIONS "DELETE FROM pg_dist_partition"
#define DELETE_ALL_COLOCATION "DELETE FROM pg_catalog.pg_dist_colocation"
#define WORKER_DROP_ALL_SHELL_TABLES \
	"CALL pg_catalog.worker_drop_all_shell_tables(%s)"
#define CITUS_INTERNAL_MARK_NODE_NOT_SYNCED \
	"SELECT citus_internal_mark_node_not_synced(%d, %d)"

#define REMOVE_ALL_CITUS_TABLES_COMMAND \
	"SELECT worker_drop_distributed_table(logicalrelid::regclass::text) FROM pg_dist_partition"
#define BREAK_CITUS_TABLE_SEQUENCE_DEPENDENCY_COMMAND \
	"SELECT pg_catalog.worker_drop_sequence_dependency(logicalrelid::regclass::text) FROM pg_dist_partition"

#define DISABLE_DDL_PROPAGATION "SET citus.enable_ddl_propagation TO 'off'"
#define ENABLE_DDL_PROPAGATION "SET citus.enable_ddl_propagation TO 'on'"
#define DISABLE_METADATA_SYNC "SET citus.enable_metadata_sync TO 'off'"
#define ENABLE_METADATA_SYNC "SET citus.enable_metadata_sync TO 'on'"
#define WORKER_APPLY_SEQUENCE_COMMAND "SELECT worker_apply_sequence_command (%s,%s)"
#define UPSERT_PLACEMENT \
	"INSERT INTO pg_dist_placement " \
	"(shardid, shardstate, shardlength, " \
	"groupid, placementid) " \
	"VALUES (" UINT64_FORMAT ", 1, " UINT64_FORMAT \
	", %d, " UINT64_FORMAT \
	") " \
	"ON CONFLICT (shardid, groupid) DO UPDATE SET " \
	"shardstate = EXCLUDED.shardstate, " \
	"shardlength = EXCLUDED.shardlength, " \
	"placementid = EXCLUDED.placementid"
#define METADATA_SYNC_CHANNEL "metadata_sync"

#define WORKER_ADJUST_IDENTITY_COLUMN_SEQ_RANGES \
	"SELECT pg_catalog.worker_adjust_identity_column_seq_ranges(%s)"

/* controlled via GUC */
extern char *EnableManualMetadataChangesForUser;
extern bool EnableMetadataSync;

#endif /* METADATA_SYNC_H */
