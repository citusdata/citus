/*-------------------------------------------------------------------------
 *
 * test/src/distributed_intermediate_results.c
 *
 * This file contains functions to test functions related to
 * src/backend/distributed/executor/distributed_intermediate_results.c.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "catalog/pg_type.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/multi_executor.h"
#include "distributed/remote_commands.h"
#include "distributed/tuplestore.h"
#include "distributed/listutils.h"
#include "tcop/tcopprot.h"

PG_FUNCTION_INFO_V1(partition_task_list_results);
PG_FUNCTION_INFO_V1(redistribute_task_list_results);

/*
 * partition_task_list_results partitions results of each of distributed
 * tasks for the given query with the ranges of the given relation.
 * Partitioned results for a task are stored on the node that the task
 * was targeted for.
 */
Datum
partition_task_list_results(PG_FUNCTION_ARGS)
{
	text *resultIdPrefixText = PG_GETARG_TEXT_P(0);
	char *resultIdPrefix = text_to_cstring(resultIdPrefixText);
	text *queryText = PG_GETARG_TEXT_P(1);
	char *queryString = text_to_cstring(queryText);
	Oid relationId = PG_GETARG_OID(2);
	bool binaryFormat = PG_GETARG_BOOL(3);

	Query *parsedQuery = ParseQueryString(queryString, NULL, 0);
	PlannedStmt *queryPlan = pg_plan_query(parsedQuery,
										   CURSOR_OPT_PARALLEL_OK,
										   NULL);
	if (!IsCitusCustomScan(queryPlan->planTree))
	{
		ereport(ERROR, (errmsg("query must be distributed and shouldn't require "
							   "any merging on the coordinator.")));
	}

	CustomScan *customScan = (CustomScan *) queryPlan->planTree;
	DistributedPlan *distributedPlan = GetDistributedPlan(customScan);

	Job *job = distributedPlan->workerJob;
	List *taskList = job->taskList;

	CitusTableCacheEntryRef *targetRelation = GetCitusTableCacheEntry(relationId);

	/*
	 * Here SELECT query's target list should match column list of target relation,
	 * so their partition column indexes are equal.
	 */
	int partitionColumnIndex =
		targetRelation->cacheEntry->partitionMethod != DISTRIBUTE_BY_NONE ?
		targetRelation->cacheEntry->partitionColumn->varattno - 1 : 0;

	List *fragmentList = PartitionTasklistResults(resultIdPrefix, taskList,
												  partitionColumnIndex,
												  targetRelation->cacheEntry,
												  binaryFormat);

	ReleaseTableCacheEntry(targetRelation);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	DistributedResultFragment *fragment = NULL;
	foreach_ptr(fragment, fragmentList)
	{
		bool columnNulls[5] = { 0 };
		Datum columnValues[5] = {
			CStringGetTextDatum(fragment->resultId),
			UInt32GetDatum(fragment->nodeId),
			Int64GetDatum(fragment->rowCount),
			UInt64GetDatum(fragment->targetShardId),
			Int32GetDatum(fragment->targetShardIndex)
		};

		tuplestore_putvalues(tupleStore, tupleDescriptor, columnValues, columnNulls);
	}

	tuplestore_donestoring(tupleStore);

	PG_RETURN_DATUM(0);
}


/*
 * redistribute_task_list_results exposes RedistributeTaskListResult for testing.
 * It executes a query and repartitions and colocates its results according to
 * a relation.
 */
Datum
redistribute_task_list_results(PG_FUNCTION_ARGS)
{
	text *resultIdPrefixText = PG_GETARG_TEXT_P(0);
	char *resultIdPrefix = text_to_cstring(resultIdPrefixText);
	text *queryText = PG_GETARG_TEXT_P(1);
	char *queryString = text_to_cstring(queryText);
	Oid relationId = PG_GETARG_OID(2);
	bool binaryFormat = PG_GETARG_BOOL(3);

	Query *parsedQuery = ParseQueryString(queryString, NULL, 0);
	PlannedStmt *queryPlan = pg_plan_query(parsedQuery,
										   CURSOR_OPT_PARALLEL_OK,
										   NULL);
	if (!IsCitusCustomScan(queryPlan->planTree))
	{
		ereport(ERROR, (errmsg("query must be distributed and shouldn't require "
							   "any merging on the coordinator.")));
	}

	CustomScan *customScan = (CustomScan *) queryPlan->planTree;
	DistributedPlan *distributedPlan = GetDistributedPlan(customScan);

	Job *job = distributedPlan->workerJob;
	List *taskList = job->taskList;

	CitusTableCacheEntryRef *targetRelation = GetCitusTableCacheEntry(relationId);

	/*
	 * Here SELECT query's target list should match column list of target relation,
	 * so their partition column indexes are equal.
	 */
	int partitionColumnIndex =
		targetRelation->cacheEntry->partitionMethod != DISTRIBUTE_BY_NONE ?
		targetRelation->cacheEntry->partitionColumn->varattno - 1 : 0;

	List **shardResultIds = RedistributeTaskListResults(resultIdPrefix, taskList,
														partitionColumnIndex,
														targetRelation->cacheEntry,
														binaryFormat);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	int shardCount = targetRelation->cacheEntry->shardIntervalArrayLength;

	for (int shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		ShardInterval *shardInterval =
			targetRelation->cacheEntry->sortedShardIntervalArray[shardIndex];
		uint64 shardId = shardInterval->shardId;

		int fragmentCount = list_length(shardResultIds[shardIndex]);
		Datum *resultIdValues = palloc0(fragmentCount * sizeof(Datum));
		List *sortedResultIds = SortList(shardResultIds[shardIndex], pg_qsort_strcmp);

		const char *resultId = NULL;
		int resultIdIndex = 0;
		foreach_ptr(resultId, sortedResultIds)
		{
			resultIdValues[resultIdIndex++] = CStringGetTextDatum(resultId);
		}

		ArrayType *resultIdArray = DatumArrayToArrayType(resultIdValues, fragmentCount,
														 TEXTOID);

		bool columnNulls[2] = { 0 };
		Datum columnValues[2] = {
			Int64GetDatum(shardId),
			PointerGetDatum(resultIdArray)
		};

		tuplestore_putvalues(tupleStore, tupleDescriptor, columnValues, columnNulls);
	}

	tuplestore_donestoring(tupleStore);

	ReleaseTableCacheEntry(targetRelation);

	PG_RETURN_DATUM(0);
}
