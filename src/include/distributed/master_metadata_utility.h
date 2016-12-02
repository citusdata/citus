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

#include "access/htup.h"
#include "access/tupdesc.h"
#include "distributed/citus_nodes.h"
#include "distributed/relay_utility.h"
#include "utils/acl.h"
#include "utils/relcache.h"


/* total number of hash tokens (2^32) */
#define HASH_TOKEN_COUNT INT64CONST(4294967296)
#define SELECT_EXIST_QUERY "SELECT EXISTS (SELECT 1 FROM %s)"

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
} ShardInterval;


/* In-memory representation of a tuple in pg_dist_shard_placement. */
typedef struct ShardPlacement
{
	CitusNode type;
	uint64 placementId;       /* sequence that implies this placement creation order */
	uint64 shardId;
	uint64 shardLength;
	RelayFileState shardState;
	char *nodeName;
	uint32 nodePort;
} ShardPlacement;


/* Function declarations to read shard and shard placement data */
extern uint32 TableShardReplicationFactor(Oid relationId);
extern List * LoadShardIntervalList(Oid relationId);
extern int ShardIntervalCount(Oid relationId);
extern List * LoadShardList(Oid relationId);
extern void CopyShardInterval(ShardInterval *srcInterval, ShardInterval *destInterval);
extern uint64 ShardLength(uint64 shardId);
extern bool NodeHasActiveShardPlacements(char *nodeName, int32 nodePort);
extern List * FinalizedShardPlacementList(uint64 shardId);
extern List * ShardPlacementList(uint64 shardId);
extern ShardPlacement * TupleToShardPlacement(TupleDesc tupleDesc,
											  HeapTuple heapTuple);

/* Function declarations to modify shard and shard placement data */
extern void InsertShardRow(Oid relationId, uint64 shardId, char storageType,
						   text *shardMinValue, text *shardMaxValue);
extern void DeleteShardRow(uint64 shardId);
extern void InsertShardPlacementRow(uint64 shardId, uint64 placementId,
									char shardState, uint64 shardLength,
									char *nodeName, uint32 nodePort);
extern void DeleteShardRow(uint64 shardId);
extern void UpdateShardPlacementState(uint64 placementId, char shardState);
extern uint64 DeleteShardPlacementRow(uint64 shardId, char *workerName, uint32
									  workerPort);
extern void CreateTruncateTrigger(Oid relationId);

/* Remaining metadata utility functions  */
extern char * TableOwner(Oid relationId);
extern void EnsureTablePermissions(Oid relationId, AclMode mode);
extern void EnsureTableOwner(Oid relationId);
extern void EnsureSuperUser(void);

#endif   /* MASTER_METADATA_UTILITY_H */
