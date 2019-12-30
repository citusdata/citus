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

#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/intermediate_results.h"
#include "distributed/multi_executor.h"
#include "distributed/remote_commands.h"
#include "distributed/tuplestore.h"
#include "tcop/tcopprot.h"

PG_FUNCTION_INFO_V1(partition_task_list_results);

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

	DistTableCacheEntry *distTableCacheEntry = DistributedTableCacheEntry(relationId);
	List *fragmentList = PartitionTasklistResults(resultIdPrefix, taskList,
												  distTableCacheEntry, binaryFormat);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	ListCell *fragmentCell = NULL;

	foreach(fragmentCell, fragmentList)
	{
		DistributedResultFragment *fragment = lfirst(fragmentCell);

		bool columnNulls[5] = { 0 };
		Datum columnValues[5] = {
			CStringGetTextDatum(fragment->resultId),
			Int32GetDatum(fragment->nodeId),
			Int64GetDatum(fragment->rowCount),
			Int64GetDatum(fragment->targetShardId),
			Int32GetDatum(fragment->targetShardIndex)
		};

		tuplestore_putvalues(tupleStore, tupleDescriptor, columnValues, columnNulls);
	}

	tuplestore_donestoring(tupleStore);

	PG_RETURN_DATUM(0);
}
