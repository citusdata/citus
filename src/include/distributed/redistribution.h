/*-------------------------------------------------------------------------
 *
 * redistribution.h
 *
 * Functions for re-distributing query results.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef REDISTRIBUTION_H
#define REDISTRIBUTION_H


#include "distributed/sharding.h"


/*
 * TargetShardFragmentStats contains statistics on the fragments that came
 * out of predistribution using worker_predistribute_query_result.
 */
typedef struct TargetShardFragmentStats
{
	int sourceNodeId;
	uint64 sourceShardId;
	int targetShardIndex;
	long byteCount;
	long rowCount;
} TargetShardFragmentStats;

/*
 * ReassembledFragmentSet represents a set of fragments that have been fetched
 * to a particular node and when concatenated form one shard of a
 * RedistributedQueryResult.
 */
typedef struct ReassembledFragmentSet
{
	/* node where the fragments are located */
	int nodeId;

	/* list of TargetShardFragmentStats for each fragment in a target partition */
	List *fragments;
} ReassembledFragmentSet;


/*
 * A RedistributedQueryResult represents a temporary distributed table that
 * originated from a redistribuion operation and is therefore stored as a
 * set of fragments per shard, which need to be concatenated at run-time.
 */
typedef struct RedistributedQueryResult
{
	/* prefix of all fragment names */
	char *resultPrefix;

	/* the partitioning between the fragment sets */
	PartitioningScheme *partitioning;

	/*
	 * Array of reassembled fragment sets, keyed by partition index.
	 *
	 * The number of elements in the array is partitioning->partitionCount.
	 */
	ReassembledFragmentSet *reassembledFragmentSets;
} RedistributedQueryResult;



RedistributedQueryResult * RedistributeDistributedPlanResult(char *distResultId,
															 DistributedPlan *distributedPlan,
															 int distributionColumnIndex,
															 DistributionScheme *targetDistribution,
															 bool isForWrites);
Query *ReadReassembledFragmentSetQuery(char *resultPrefix, List *targetList,
									   ReassembledFragmentSet *fragmentSet);

#endif
