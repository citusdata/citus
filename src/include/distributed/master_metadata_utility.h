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
#include "utils/relcache.h"


/* total number of hash tokens (2^32) */
#define HASH_TOKEN_COUNT INT64CONST(4294967296UL)

/* In-memory representation of a typed tuple in pg_dist_shard. */
typedef struct ShardInterval
{
	CitusNodeTag type;
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
	CitusNodeTag type;
	Oid tupleOid;       /* unique oid that implies this row's insertion order */
	uint64 shardId;
	uint64 shardLength;
	RelayFileState shardState;
	char *nodeName;
	uint32 nodePort;
} ShardPlacement;


/* Function declarations to read shard and shard placement data */
extern List * LoadShardIntervalList(Oid relationId);
extern List * LoadShardList(Oid relationId);
extern char * LoadShardAlias(Oid relationId, uint64 shardId);
extern void CopyShardInterval(ShardInterval *srcInterval, ShardInterval *destInterval);
extern uint64 ShardLength(uint64 shardId);
extern List * FinalizedShardPlacementList(uint64 shardId);
extern List * ShardPlacementList(uint64 shardId);
extern ShardPlacement * TupleToShardPlacement(TupleDesc tupleDesc,
											  HeapTuple heapTuple);

/* Function declarations to modify shard and shard placement data */
extern void InsertShardRow(Oid relationId, uint64 shardId, char storageType,
						   text *shardMinValue, text *shardMaxValue);
extern void InsertShardPlacementRow(uint64 shardId, char shardState, uint64 shardLength,
									char *nodeName, uint32 nodePort);
extern void DeleteShardRow(uint64 shardId);
extern void DeleteShardPlacementRow(uint64 shardId, char *workerName, uint32 workerPort);

/* Remaining metadata utility functions  */
extern Node * BuildDistributionKeyFromColumnName(Relation distributedRelation,
												 char *columnName);

#endif   /* MASTER_METADATA_UTILITY_H */
