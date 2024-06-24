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

#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/palloc.h"

#include "distributed/commands/multi_copy.h"


/*
 * DistributedResultFragment represents a fragment of a distributed result.
 */
typedef struct DistributedResultFragment
{
	/* result's id, which can be used by read_intermediate_results(), ... */
	char *resultId;

	/* location of the result */
	uint32 nodeId;

	/* number of rows in the result file */
	int rowCount;

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
 * NodePair contains the source and destination node in a NodeToNodeFragmentsTransfer.
 * It is a separate struct to use it as a key in a hash table.
 */
typedef struct NodePair
{
	uint32 sourceNodeId;
	uint32 targetNodeId;
} NodePair;


/*
 * NodeToNodeFragmentsTransfer contains all fragments that need to be fetched from
 * the source node to the destination node in the NodePair.
 */
typedef struct NodeToNodeFragmentsTransfer
{
	NodePair nodes;
	List *fragmentList;
} NodeToNodeFragmentsTransfer;

/* Forward Declarations */
struct CitusTableCacheEntry;

/* intermediate_results.c */
extern DestReceiver * CreateRemoteFileDestReceiver(const char *resultId,
												   EState *executorState,
												   List *initialNodeList, bool
												   writeLocalFile);
extern DestReceiver * CreatePartitionedResultDestReceiver(int partitionColumnIndex,
														  int partitionCount,
														  CitusTableCacheEntry *
														  shardSearchInfo,
														  DestReceiver **
														  partitionedDestReceivers,
														  bool lazyStartup,
														  bool allowNullPartitionValues);
extern CitusTableCacheEntry * QueryTupleShardSearchInfo(ArrayType *minValuesArray,
														ArrayType *maxValuesArray,
														char partitionMethod,
														Var *partitionColumn);
extern void WriteToLocalFile(StringInfo copyData, FileCompat *fileCompat);
extern uint64 RemoteFileDestReceiverBytesSent(DestReceiver *destReceiver);
extern void SendQueryResultViaCopy(const char *resultId);
extern void ReceiveQueryResultViaCopy(const char *resultId);
extern void RemoveIntermediateResultsDirectories(void);
extern int64 IntermediateResultSize(const char *resultId);
extern char * QueryResultFileName(const char *resultId);
extern char * CreateIntermediateResultsDirectory(void);
extern ArrayType * CreateArrayFromDatums(Datum *datumArray, bool *nullsArray, int
										 datumCount, Oid typeId);


/* distributed_intermediate_results.c */
extern List ** RedistributeTaskListResults(const char *resultIdPrefix,
										   List *selectTaskList,
										   int partitionColumnIndex,
										   CitusTableCacheEntry *targetRelation,
										   bool binaryFormat);
extern List * PartitionTasklistResults(const char *resultIdPrefix, List *selectTaskList,
									   int partitionColumnIndex,
									   CitusTableCacheEntry *distributionScheme,
									   bool binaryFormat);
extern char * QueryStringForFragmentsTransfer(
	NodeToNodeFragmentsTransfer *fragmentsTransfer);
extern void ShardMinMaxValueArrays(ShardInterval **shardIntervalArray, int shardCount,
								   Oid intervalTypeId, ArrayType **minValueArray,
								   ArrayType **maxValueArray);

#endif /* INTERMEDIATE_RESULTS_H */
