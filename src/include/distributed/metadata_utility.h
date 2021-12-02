/*-------------------------------------------------------------------------
 *
 * metadata_utility.h
 *	  Type and function declarations used for reading and modifying
 *    coordinator node's metadata.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef METADATA_UTILITY_H
#define METADATA_UTILITY_H

#include "postgres.h"

#include "access/heapam.h"
#include "access/htup.h"
#include "access/tupdesc.h"
#include "catalog/indexing.h"
#include "catalog/objectaddress.h"
#include "distributed/citus_nodes.h"
#include "distributed/connection_management.h"
#include "distributed/relay_utility.h"
#include "utils/acl.h"
#include "utils/relcache.h"


/* total number of hash tokens (2^32) */
#define HASH_TOKEN_COUNT INT64CONST(4294967296)
#define SELECT_TRUE_QUERY "SELECT TRUE FROM %s LIMIT 1"
#define PG_TABLE_SIZE_FUNCTION "pg_table_size(%s)"
#define PG_RELATION_SIZE_FUNCTION "pg_relation_size(%s)"
#define PG_TOTAL_RELATION_SIZE_FUNCTION "pg_total_relation_size(%s)"
#define WORKER_PARTITIONED_TABLE_SIZE_FUNCTION "worker_partitioned_table_size(%s)"
#define WORKER_PARTITIONED_RELATION_SIZE_FUNCTION "worker_partitioned_relation_size(%s)"
#define WORKER_PARTITIONED_RELATION_TOTAL_SIZE_FUNCTION \
	"worker_partitioned_relation_total_size(%s)"

#define SHARD_SIZES_COLUMN_COUNT (3)

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
	ShardState shardState;
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
	ShardState shardState;
	int32 groupId;

	/* the rest of the fields aren't from pg_dist_placement */
	char *nodeName;
	uint32 nodePort;
	uint32 nodeId;
	char partitionMethod;
	uint32 colocationGroupId;
	uint32 representativeValue;
} ShardPlacement;


typedef enum CascadeToColocatedOption
{
	CASCADE_TO_COLOCATED_UNSPECIFIED,
	CASCADE_TO_COLOCATED_YES,
	CASCADE_TO_COLOCATED_NO,
	CASCADE_TO_COLOCATED_NO_ALREADY_CASCADED
}CascadeToColocatedOption;

/*
 * TableConversionParameters are the parameters that are given to
 * table conversion UDFs: undistribute_table, alter_distributed_table,
 * alter_table_set_access_method.
 *
 * When passing a TableConversionParameters object to one of the table
 * conversion functions some of the parameters needs to be set:
 * UndistributeTable: relationId
 * AlterDistributedTable: relationId, distributionColumn, shardCountIsNull,
 * shardCount, colocateWith, cascadeToColocated
 * AlterTableSetAccessMethod: relationId, accessMethod
 *
 * conversionType parameter will be automatically set by the function.
 *
 * TableConversionState objects can be created using TableConversionParameters
 * objects with CreateTableConversion function.
 */
typedef struct TableConversionParameters
{
	/*
	 * Determines type of conversion: UNDISTRIBUTE_TABLE,
	 * ALTER_DISTRIBUTED_TABLE, ALTER_TABLE_SET_ACCESS_METHOD.
	 */
	char conversionType;

	/* Oid of the table to do conversion on */
	Oid relationId;

	/*
	 * Options to do conversions on the table
	 * distributionColumn is the name of the new distribution column,
	 * shardCountIsNull is if the shardCount variable is not given
	 * shardCount is the new shard count,
	 * colocateWith is the name of the table to colocate with, 'none', or
	 * 'default'
	 * accessMethod is the name of the new accessMethod for the table
	 */
	char *distributionColumn;
	bool shardCountIsNull;
	int shardCount;
	char *colocateWith;
	char *accessMethod;

	/*
	 * cascadeToColocated determines whether the shardCount and
	 * colocateWith will be cascaded to the currently colocated tables
	 */
	CascadeToColocatedOption cascadeToColocated;

	/*
	 * cascadeViaForeignKeys determines if the conversion operation
	 * will be cascaded to the graph connected with foreign keys
	 * to the table
	 */
	bool cascadeViaForeignKeys;

	/*
	 * suppressNoticeMessages determines if we want to suppress NOTICE
	 * messages that we explicitly issue
	 */
	bool suppressNoticeMessages;
} TableConversionParameters;

typedef struct TableConversionReturn
{
	/*
	 * commands to create foreign keys for the table
	 *
	 * When the table conversion is cascaded we can recreate
	 * some of the foreign keys of the cascaded tables. So with this
	 * list we can return it to the initial conversion operation so
	 * foreign keys can be created after every colocated table is
	 * converted.
	 */
	List *foreignKeyCommands;
}TableConversionReturn;


/*
 * Size query types for PG and Citus
 * For difference details, please see:
 * https://www.postgresql.org/docs/13/functions-admin.html#FUNCTIONS-ADMIN-DBSIZE
 */
typedef enum SizeQueryType
{
	RELATION_SIZE, /* pg_relation_size() */
	TOTAL_RELATION_SIZE, /* pg_total_relation_size() */
	TABLE_SIZE /* pg_table_size() */
} SizeQueryType;


/* Size functions */
extern Datum citus_table_size(PG_FUNCTION_ARGS);
extern Datum citus_total_relation_size(PG_FUNCTION_ARGS);
extern Datum citus_relation_size(PG_FUNCTION_ARGS);

/* Function declarations to read shard and shard placement data */
extern uint32 TableShardReplicationFactor(Oid relationId);
extern List * LoadShardIntervalList(Oid relationId);
extern List * LoadUnsortedShardIntervalListViaCatalog(Oid relationId);
extern ShardInterval * LoadShardIntervalWithLongestShardName(Oid relationId);
extern int ShardIntervalCount(Oid relationId);
extern List * LoadShardList(Oid relationId);
extern ShardInterval * CopyShardInterval(ShardInterval *srcInterval);
extern uint64 ShardLength(uint64 shardId);
extern bool NodeGroupHasShardPlacements(int32 groupId,
										bool onlyConsiderActivePlacements);
extern List * ActiveShardPlacementListOnGroup(uint64 shardId, int32 groupId);
extern List * ActiveShardPlacementList(uint64 shardId);
extern List * ShardPlacementListWithoutOrphanedPlacements(uint64 shardId);
extern ShardPlacement * ActiveShardPlacement(uint64 shardId, bool missingOk);
extern List * BuildShardPlacementList(int64 shardId);
extern List * AllShardPlacementsOnNodeGroup(int32 groupId);
extern List * AllShardPlacementsWithShardPlacementState(ShardState shardState);
extern List * GroupShardPlacementsForTableOnGroup(Oid relationId, int32 groupId);
extern StringInfo GenerateSizeQueryOnMultiplePlacements(List *shardIntervalList,
														SizeQueryType sizeQueryType,
														bool optimizePartitionCalculations);
extern List * RemoveCoordinatorPlacementIfNotSingleNode(List *placementList);

/* Function declarations to modify shard and shard placement data */
extern void InsertShardRow(Oid relationId, uint64 shardId, char storageType,
						   text *shardMinValue, text *shardMaxValue);
extern void DeleteShardRow(uint64 shardId);
extern uint64 InsertShardPlacementRow(uint64 shardId, uint64 placementId,
									  char shardState, uint64 shardLength,
									  int32 groupId);
extern void InsertIntoPgDistPartition(Oid relationId, char distributionMethod,
									  Var *distributionColumn, uint32 colocationId,
									  char replicationModel, bool autoConverted);
extern void UpdatePgDistPartitionAutoConverted(Oid citusTableId, bool autoConverted);
extern void DeletePartitionRow(Oid distributedRelationId);
extern void DeleteShardRow(uint64 shardId);
extern void UpdateShardPlacementState(uint64 placementId, char shardState);
extern void UpdatePlacementGroupId(uint64 placementId, int groupId);
extern void DeleteShardPlacementRow(uint64 placementId);
extern void CreateDistributedTable(Oid relationId, Var *distributionColumn,
								   char distributionMethod, int shardCount,
								   bool shardCountIsStrict, char *colocateWithTableName,
								   bool viaDeprecatedAPI);
extern void CreateTruncateTrigger(Oid relationId);
extern TableConversionReturn * UndistributeTable(TableConversionParameters *params);

extern void EnsureDependenciesExistOnAllNodes(const ObjectAddress *target);
extern List * GetDistributableDependenciesForObject(const ObjectAddress *target);
extern bool ShouldPropagate(void);
extern bool ShouldPropagateObject(const ObjectAddress *address);
extern void ReplicateAllDependenciesToNode(const char *nodeName, int nodePort);

/* Remaining metadata utility functions  */
extern char * TableOwner(Oid relationId);
extern void EnsureTablePermissions(Oid relationId, AclMode mode);
extern void EnsureTableOwner(Oid relationId);
extern void EnsureSchemaOwner(Oid schemaId);
extern void EnsureHashDistributedTable(Oid relationId);
extern void EnsureFunctionOwner(Oid functionId);
extern void EnsureSuperUser(void);
extern void ErrorIfTableIsACatalogTable(Relation relation);
extern void EnsureTableNotDistributed(Oid relationId);
extern void EnsureRelationExists(Oid relationId);
extern bool RegularTable(Oid relationId);
extern bool TableEmpty(Oid tableId);
extern bool RelationUsesIdentityColumns(TupleDesc relationDesc);
extern char * ConstructQualifiedShardName(ShardInterval *shardInterval);
extern uint64 GetFirstShardId(Oid relationId);
extern Datum StringToDatum(char *inputString, Oid dataType);
extern char * DatumToString(Datum datum, Oid dataType);
extern int CompareShardPlacementsByWorker(const void *leftElement,
										  const void *rightElement);
extern int CompareShardPlacementsByGroupId(const void *leftElement,
										   const void *rightElement);
extern ShardInterval * DeformedDistShardTupleToShardInterval(Datum *datumArray,
															 bool *isNullArray,
															 Oid intervalTypeId,
															 int32 intervalTypeMod);
extern void GetIntervalTypeInfo(char partitionMethod, Var *partitionColumn,
								Oid *intervalTypeId, int32 *intervalTypeMod);
extern List * SendShardStatisticsQueriesInParallel(List *citusTableIds,
												   bool useDistributedTransaction);
extern bool GetNodeDiskSpaceStatsForConnection(MultiConnection *connection,
											   uint64 *availableBytes,
											   uint64 *totalBytes);
extern void ExecuteQueryViaSPI(char *query, int SPIOK);
extern void EnsureSequenceTypeSupported(Oid seqOid, Oid seqTypId);
extern void AlterSequenceType(Oid seqOid, Oid typeOid);
extern void MarkSequenceListDistributedAndPropagateDependencies(List *sequenceList);
extern void MarkSequenceDistributedAndPropagateDependencies(Oid sequenceOid);
extern void EnsureDistributedSequencesHaveOneType(Oid relationId,
												  List *dependentSequenceList,
												  List *attnumList);
#endif   /* METADATA_UTILITY_H */
