/*-------------------------------------------------------------------------
 *
 * intermediate_results.h
 *   Functions for writing and reading intermediate results.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef INTERMEDIATE_RESULTS_H
#define INTERMEDIATE_RESULTS_H


#include "fmgr.h"

#include "distributed/commands/multi_copy.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/palloc.h"


/*
 * DistributedResultFragment represents a fragment of a distributed result
 * shard.
 */
typedef struct DistributedResultFragment
{
	/* result's id, which can be used by read_intermediate_results(), ... */
	char *resultId;

	/* location of the result */
	uint32 nodeId;

	/* number of rows in the result file */
	uint64 rowCount;

	/*
	 * The fragment contains the rows which match the partitioning method
	 * and partitioning ranges of targetShardId. The shape of each row matches
	 * the schema of the relation to which targetShardId belongs to.
	 */
	uint64 targetShardId;

	/* what is the index of targetShardId in its relation's sorted shard list? */
	int targetShardIndex;
} DistributedResultFragment;

/*
 * DistributedResultState describes whether a distributed result is planned
 * or already executed.
 */
typedef enum DistributedResultState
{
	DISTRIBUTED_RESULT_PLANNED,
	DISTRIBUTED_RESULT_AVAILABLE
} DistributedResultState;


/*
 * DistributedResultShard represents a shard of a distributed result. A shard
 * can consist of multiple fragments, which are intermediate results that are
 * no the same node.
 */
typedef struct DistributedResultShard
{
	/* what is the index of targetShardId in its relation's sorted shard list? */
	int targetShardIndex;

	/*
	 * The fragment contains the rows which match the partitioning method
	 * and partitioning ranges of targetShardId. The shape of each row matches
	 * the schema of the relation to which targetShardId belongs to.
	 */
	uint64 targetShardId;

	/* result ids of fragments that make up the shard (if result is available) */
	List *fragmentList;

	/* sum of the number of rows in the fragments (if result is available) */
	int64 rowCount;

} DistributedResultShard;


/*
 * DistributedResult describes a distributed intermediate result which can be
 * queried like a distributed table.
 *
 * A distributed intermediate result has a set of distributed result fragment
 * for each shard.
 */
typedef struct DistributedResult
{
	/* state of this distributed result (planner or executed) */
	DistributedResultState state;

	/* co-location ID with which the result is co-located */
	int colocationId;

	/* number of shards in the co-location group */
	int shardCount;

	/* index of the partition column */
	int partitionColumnIndex;

	/* whether the file format is binary */
	bool binaryFormat;

	/* array containing a list of result shards */
	DistributedResultShard *resultShards;

} DistributedResult;


/* intermediate_results.c */
extern DestReceiver * CreateRemoteFileDestReceiver(const char *resultId,
												   EState *executorState,
												   List *initialNodeList, bool
												   writeLocalFile);
extern uint64 RemoteFileDestReceiverBytesSent(DestReceiver *destReceiver);
extern void SendQueryResultViaCopy(const char *resultId);
extern void ReceiveQueryResultViaCopy(const char *resultId);
extern void RemoveIntermediateResultsDirectory(void);
extern int64 IntermediateResultSize(const char *resultId);
extern char * QueryResultFileName(const char *resultId);
extern char * CreateIntermediateResultsDirectory(void);

/* distributed_intermediate_results.c */
extern DistributedResult * RegisterDistributedResult(char *resultIdPrefix, Query *query,
													 int partitionColumnIndex,
													 Oid targetRelationId);
extern DistributedResult * GetNamedDistributedResult(char *resultId);
extern void ClearNamedDistributedResultsHash(void);
extern DistributedResult * RedistributeTaskListResults(const char *resultIdPrefix,
													   List *selectTaskList,
													   int partitionColumnIndex,
													   CitusTableCacheEntry *
													   targetRelation,
													   bool binaryFormat);
extern List * PartitionTasklistResults(const char *resultIdPrefix, List *selectTaskList,
									   int partitionColumnIndex,
									   CitusTableCacheEntry *distributionScheme,
									   bool binaryFormat);

#endif /* INTERMEDIATE_RESULTS_H */
