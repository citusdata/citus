/*-------------------------------------------------------------------------
 *
 * master_metadata_utility.h
 *	  Type and function declarations used for reading and modifying master
 *    node's metadata.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MASTER_METADATA_UTILITY_H
#define MASTER_METADATA_UTILITY_H

#include "access/heapam.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "catalog/indexing.h"
#include "distributed/citus_nodes.h"
#include "distributed/relay_utility.h"
#include "utils/acl.h"
#include "utils/relcache.h"


/* total number of hash tokens (2^32) */
#define HASH_TOKEN_COUNT INT64CONST(4294967296)
#define SELECT_EXIST_QUERY "SELECT EXISTS (SELECT 1 FROM %s)"
#define PG_TABLE_SIZE_FUNCTION "pg_table_size(%s)"
#define PG_RELATION_SIZE_FUNCTION "pg_relation_size(%s)"
#define PG_TOTAL_RELATION_SIZE_FUNCTION "pg_total_relation_size(%s)"
#define CSTORE_TABLE_SIZE_FUNCTION "cstore_table_size(%s)"

#if (PG_VERSION_NUM < 100000)
static inline void
CatalogTupleUpdate(Relation heapRel, ItemPointer otid, HeapTuple tup)
{
	simple_heap_update(heapRel, otid, tup);
	CatalogUpdateIndexes(heapRel, tup);
}


static inline Oid
CatalogTupleInsert(Relation heapRel, HeapTuple tup)
{
	Oid oid = simple_heap_insert(heapRel, tup);
	CatalogUpdateIndexes(heapRel, tup);

	return oid;
}


#endif

/* In-memory representation of a typed tuple in pg_dist_shard. */
typedef struct ShardInterval
{
	CitusNode type;
	Oid relationId;
	char storageType;
	Oid valueTypeId;    /* min/max value datum's typeId */
	int valueTypeLen;   /* min/max value datum's typelen */
	bool valueByVal;    /* min/max value datum's byval */
	bool minValueExists;
	bool maxValueExists;
	Datum minValue;     /* a shard's typed min value datum */
	Datum maxValue;     /* a shard's typed max value datum */
	uint64 shardId;
	int shardIndex;
} ShardInterval;


/* In-memory representation of a tuple in pg_dist_placement. */
typedef struct GroupShardPlacement
{
	CitusNode type;
	uint64 placementId;     /* sequence that implies this placement creation order */
	uint64 shardId;
	uint64 shardLength;
	RelayFileState shardState;
	int32 groupId;
} GroupShardPlacement;


/* A GroupShardPlacement which has had some extra data resolved */
typedef struct ShardPlacement
{
	/*
	 * careful, the rest of the code assumes this exactly matches GroupShardPlacement
	 */
	CitusNode type;
	uint64 placementId;
	uint64 shardId;
	uint64 shardLength;
	RelayFileState shardState;
	int32 groupId;

	/* the rest of the fields aren't from pg_dist_placement */
	char *nodeName;
	uint32 nodePort;
	int nodeId;
	char partitionMethod;
	uint32 colocationGroupId;
	uint32 representativeValue;
} ShardPlacement;


/* Config variable managed via guc.c */
extern int ReplicationModel;

/* Size functions */
extern Datum citus_table_size(PG_FUNCTION_ARGS);
extern Datum citus_total_relation_size(PG_FUNCTION_ARGS);
extern Datum citus_relation_size(PG_FUNCTION_ARGS);

/* Function declarations to read shard and shard placement data */
extern uint32 TableShardReplicationFactor(Oid relationId);
extern List * LoadShardIntervalList(Oid relationId);
extern int ShardIntervalCount(Oid relationId);
extern List * LoadShardList(Oid relationId);
extern void CopyShardInterval(ShardInterval *srcInterval, ShardInterval *destInterval);
extern void CopyShardPlacement(ShardPlacement *srcPlacement,
							   ShardPlacement *destPlacement);
extern uint64 ShardLength(uint64 shardId);
extern bool NodeGroupHasShardPlacements(int32 groupId,
										bool onlyConsiderActivePlacements);
extern List * FinalizedShardPlacementList(uint64 shardId);
extern ShardPlacement * FinalizedShardPlacement(uint64 shardId, bool missingOk);
extern List * BuildShardPlacementList(ShardInterval *shardInterval);
extern List * AllShardPlacementsOnNodeGroup(int32 groupId);
extern List * GroupShardPlacementsForTableOnGroup(Oid relationId, int32 groupId);

/* Function declarations to modify shard and shard placement data */
extern void InsertShardRow(Oid relationId, uint64 shardId, char storageType,
						   text *shardMinValue, text *shardMaxValue);
extern void DeleteShardRow(uint64 shardId);
extern uint64 InsertShardPlacementRow(uint64 shardId, uint64 placementId,
									  char shardState, uint64 shardLength,
									  int32 groupId);
extern void InsertIntoPgDistPartition(Oid relationId, char distributionMethod,
									  Var *distributionColumn, uint32 colocationId,
									  char replicationModel);
extern void DeletePartitionRow(Oid distributedRelationId);
extern void DeleteShardRow(uint64 shardId);
extern void UpdateShardPlacementState(uint64 placementId, char shardState);
extern void DeleteShardPlacementRow(uint64 placementId);
extern void UpdateColocationGroupReplicationFactor(uint32 colocationId,
												   int replicationFactor);
extern void CreateDistributedTable(Oid relationId, Var *distributionColumn,
								   char distributionMethod, char *colocateWithTableName,
								   bool viaDeprecatedAPI);
extern void CreateTruncateTrigger(Oid relationId);

/* Remaining metadata utility functions  */
extern char * TableOwner(Oid relationId);
extern void EnsureTablePermissions(Oid relationId, AclMode mode);
extern void EnsureTableOwner(Oid relationId);
extern void EnsureSchemaOwner(Oid schemaId);
extern void EnsureSequenceOwner(Oid sequenceOid);
extern void EnsureSuperUser(void);
extern void EnsureReplicationSettings(Oid relationId, char replicationModel);
extern bool RegularTable(Oid relationId);
extern char * ConstructQualifiedShardName(ShardInterval *shardInterval);
extern uint64 GetFirstShardId(Oid relationId);
extern Datum StringToDatum(char *inputString, Oid dataType);
extern char * DatumToString(Datum datum, Oid dataType);


#endif   /* MASTER_METADATA_UTILITY_H */
