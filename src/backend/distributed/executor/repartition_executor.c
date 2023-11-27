/*-------------------------------------------------------------------
 *
 * repartition_executor.c
 *
 * Definitions for public functions and types related to repartition
 * of select query results.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"

#include "distributed/citus_custom_scan.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/repartition_executor.h"
#include "distributed/resource_lock.h"


/*
 * IsSupportedRedistributionTarget determines whether re-partitioning into the
 * given target relation is supported.
 */
bool
IsSupportedRedistributionTarget(Oid targetRelationId)
{
	CitusTableCacheEntry *tableEntry = GetCitusTableCacheEntry(targetRelationId);

	if (!IsCitusTableTypeCacheEntry(tableEntry, HASH_DISTRIBUTED) &&
		!IsCitusTableTypeCacheEntry(tableEntry, RANGE_DISTRIBUTED))
	{
		return false;
	}

	return true;
}


/*
 * IsRedistributablePlan returns true if the given plan is a distributable plan.
 */
bool
IsRedistributablePlan(Plan *selectPlan)
{
	if (!EnableRepartitionedInsertSelect)
	{
		return false;
	}

	/*
	 * Don't redistribute if query is not distributed or requires
	 * merge on coordinator
	 */
	if (!IsCitusCustomScan(selectPlan))
	{
		return false;
	}

	DistributedPlan *distSelectPlan =
		GetDistributedPlan((CustomScan *) selectPlan);
	Job *distSelectJob = distSelectPlan->workerJob;
	List *distSelectTaskList = distSelectJob->taskList;

	/*
	 * Don't use redistribution if only one task. This is to keep the existing
	 * behaviour for CTEs that the last step is a read_intermediate_result()
	 * call. It doesn't hurt much in other cases too.
	 */
	if (list_length(distSelectTaskList) <= 1)
	{
		return false;
	}

	/* don't use redistribution for repartition joins for now */
	if (distSelectJob->dependentJobList != NIL)
	{
		return false;
	}

	if (distSelectPlan->combineQuery != NULL)
	{
		Query *combineQuery = (Query *) distSelectPlan->combineQuery;

		if (contain_nextval_expression_walker((Node *) combineQuery->targetList, NULL))
		{
			/* nextval needs to be evaluated on the coordinator */
			return false;
		}
	}

	return true;
}


/*
 * GenerateTaskListWithColocatedIntermediateResults generates a list of tasks
 * for a query that inserts into a target relation and selects from a set of
 * co-located intermediate results.
 */
List *
GenerateTaskListWithColocatedIntermediateResults(Oid targetRelationId,
												 Query *
												 modifyQueryViaCoordinatorOrRepartition,
												 char *resultIdPrefix)
{
	List *taskList = NIL;

	/*
	 * Make a copy of the <MODIFY-SQL> ... SELECT. We'll repeatedly replace
	 * the subquery of modifyResultQuery for different intermediate results and
	 * then deparse it.
	 */
	Query *modifyWithResultQuery = copyObject(modifyQueryViaCoordinatorOrRepartition);
	RangeTblEntry *insertRte = ExtractResultRelationRTE(modifyWithResultQuery);
	RangeTblEntry *selectRte = ExtractSourceResultRangeTableEntry(modifyWithResultQuery);

	CitusTableCacheEntry *targetCacheEntry = GetCitusTableCacheEntry(targetRelationId);
	int shardCount = targetCacheEntry->shardIntervalArrayLength;
	uint32 taskIdIndex = 1;
	uint64 jobId = INVALID_JOB_ID;

	for (int shardOffset = 0; shardOffset < shardCount; shardOffset++)
	{
		ShardInterval *targetShardInterval =
			targetCacheEntry->sortedShardIntervalArray[shardOffset];
		uint64 shardId = targetShardInterval->shardId;
		List *columnAliasList = NIL;
		StringInfo queryString = makeStringInfo();
		StringInfo resultId = makeStringInfo();

		/* during COPY, the shard ID is appended to the result name */
		appendStringInfo(resultId, "%s_" UINT64_FORMAT, resultIdPrefix, shardId);

		/*
		 * For MERGE SQL, use the USING clause list, the main query target list
		 * is NULL
		 */
		List *targetList = IsMergeQuery(modifyQueryViaCoordinatorOrRepartition) ?
						   selectRte->subquery->targetList :
						   modifyQueryViaCoordinatorOrRepartition->targetList;

		/* generate the query on the intermediate result */
		Query *resultSelectQuery = BuildSubPlanResultQuery(targetList,
														   columnAliasList,
														   resultId->data);

		/* put the intermediate result query in the INSERT..SELECT */
		selectRte->subquery = resultSelectQuery;

		/* setting an alias simplifies deparsing of RETURNING */
		if (insertRte->alias == NULL)
		{
			Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
			insertRte->alias = alias;
		}

		/*
		 * Generate a query string for the query that inserts into a shard and reads
		 * from an intermediate result.
		 *
		 * Since CTEs have already been converted to intermediate results, they need
		 * to removed from the query. Otherwise, worker queries include both
		 * intermediate results and CTEs in the query.
		 */
		modifyWithResultQuery->cteList = NIL;
		deparse_shard_query(modifyWithResultQuery, targetRelationId, shardId,
							queryString);
		ereport(DEBUG2, (errmsg("distributed statement: %s", queryString->data)));

		LockShardDistributionMetadata(shardId, ShareLock);
		List *insertShardPlacementList = ActiveShardPlacementList(shardId);

		RelationShard *relationShard = CitusMakeNode(RelationShard);
		relationShard->relationId = targetShardInterval->relationId;
		relationShard->shardId = targetShardInterval->shardId;

		Task *modifyTask = CreateBasicTask(jobId, taskIdIndex, MODIFY_TASK,
										   queryString->data);
		modifyTask->dependentTaskList = NIL;
		modifyTask->anchorShardId = shardId;
		modifyTask->taskPlacementList = insertShardPlacementList;
		modifyTask->relationShardList = list_make1(relationShard);
		modifyTask->replicationModel = targetCacheEntry->replicationModel;

		taskList = lappend(taskList, modifyTask);

		taskIdIndex++;
	}

	return taskList;
}


/*
 * GenerateTaskListWithRedistributedResults returns a task list to insert given
 * redistributedResults into the given target relation.
 * redistributedResults[shardIndex] is list of cstrings each of which is
 * a result name which should be inserted into
 * targetRelation->sortedShardIntervalArray[shardIndex].
 */
List *
GenerateTaskListWithRedistributedResults(Query *modifyQueryViaCoordinatorOrRepartition,
										 CitusTableCacheEntry *targetRelation,
										 List **redistributedResults, bool
										 useBinaryFormat)
{
	List *taskList = NIL;

	/*
	 * Make a copy of the <MODIFY-SQL> ... SELECT. We'll repeatedly replace
	 * the subquery of modifyResultQuery for different intermediate results and
	 * then deparse it.
	 */
	Query *modifyResultQuery = copyObject(modifyQueryViaCoordinatorOrRepartition);
	RangeTblEntry *insertRte = ExtractResultRelationRTE(modifyResultQuery);
	Oid targetRelationId = targetRelation->relationId;

	int shardCount = targetRelation->shardIntervalArrayLength;
	int shardOffset = 0;
	uint32 taskIdIndex = 1;
	uint64 jobId = INVALID_JOB_ID;

	RangeTblEntry *selectRte =
		ExtractSourceResultRangeTableEntry(modifyResultQuery);
	List *selectTargetList = selectRte->subquery->targetList;

	for (shardOffset = 0; shardOffset < shardCount; shardOffset++)
	{
		ShardInterval *targetShardInterval =
			targetRelation->sortedShardIntervalArray[shardOffset];
		List *resultIdList = redistributedResults[targetShardInterval->shardIndex];
		uint64 shardId = targetShardInterval->shardId;
		StringInfo queryString = makeStringInfo();

		/* skip empty tasks */
		if (resultIdList == NIL)
		{
			continue;
		}

		/* sort result ids for consistent test output */
		List *sortedResultIds = SortList(resultIdList, pg_qsort_strcmp);

		/* generate the query on the intermediate result */
		Query *fragmentSetQuery = BuildReadIntermediateResultsArrayQuery(selectTargetList,
																		 NIL,
																		 sortedResultIds,
																		 useBinaryFormat);

		/* put the intermediate result query in the INSERT..SELECT */
		selectRte->subquery = fragmentSetQuery;

		/* setting an alias simplifies deparsing of RETURNING */
		if (insertRte->alias == NULL)
		{
			Alias *alias = makeAlias(CITUS_TABLE_ALIAS, NIL);
			insertRte->alias = alias;
		}

		/*
		 * Generate a query string for the query that inserts into a shard and reads
		 * from an intermediate result.
		 *
		 * Since CTEs have already been converted to intermediate results, they need
		 * to removed from the query. Otherwise, worker queries include both
		 * intermediate results and CTEs in the query.
		 */
		modifyResultQuery->cteList = NIL;
		deparse_shard_query(modifyResultQuery, targetRelationId, shardId, queryString);
		ereport(DEBUG2, (errmsg("distributed statement: %s", queryString->data)));

		LockShardDistributionMetadata(shardId, ShareLock);
		List *insertShardPlacementList = ActiveShardPlacementList(shardId);

		RelationShard *relationShard = CitusMakeNode(RelationShard);
		relationShard->relationId = targetShardInterval->relationId;
		relationShard->shardId = targetShardInterval->shardId;

		Task *modifyTask = CreateBasicTask(jobId, taskIdIndex, MODIFY_TASK,
										   queryString->data);
		modifyTask->dependentTaskList = NIL;
		modifyTask->anchorShardId = shardId;
		modifyTask->taskPlacementList = insertShardPlacementList;
		modifyTask->relationShardList = list_make1(relationShard);
		modifyTask->replicationModel = targetRelation->replicationModel;

		taskList = lappend(taskList, modifyTask);

		taskIdIndex++;
	}

	return taskList;
}
