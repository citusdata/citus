/*-------------------------------------------------------------------------
 *
 * insert_select_executor.c
 *
 * Executor logic for INSERT..SELECT.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/adaptive_executor.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/intermediate_results.h"
#include "distributed/local_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_router_planner.h"
#include "distributed/local_executor.h"
#include "distributed/distributed_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/subplan_execution.h"
#include "distributed/transaction_management.h"
#include "distributed/version_compat.h"
#include "executor/executor.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "parser/parse_coerce.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Config variables managed via guc.c */
bool EnableRepartitionedInsertSelect = true;


static List * TwoPhaseInsertSelectTaskList(Oid targetRelationId, Query *insertSelectQuery,
										   char *resultIdPrefix);
static void ExecutePlanIntoRelation(Oid targetRelationId, List *insertTargetList,
									PlannedStmt *selectPlan, EState *executorState);
static HTAB * ExecutePlanIntoColocatedIntermediateResults(Oid targetRelationId,
														  List *insertTargetList,
														  PlannedStmt *selectPlan,
														  EState *executorState,
														  char *intermediateResultIdPrefix);
static List * BuildColumnNameListFromTargetList(Oid targetRelationId,
												List *insertTargetList);
static int PartitionColumnIndexFromColumnList(Oid relationId, List *columnNameList);
static List * RedistributedInsertSelectTaskList(Query *insertSelectQuery,
												CitusTableCacheEntry *targetRelation,
												List **redistributedResults,
												bool useBinaryFormat);
static int PartitionColumnIndex(List *insertTargetList, Var *partitionColumn);
static void WrapTaskListForProjection(List *taskList, List *projectedTargetEntries);


/*
 * NonPushableInsertSelectExecScan executes an INSERT INTO distributed_table
 * SELECT .. query either by routing via coordinator or by repartitioning
 * task results and moving data directly between nodes.
 */
TupleTableSlot *
NonPushableInsertSelectExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;

	if (!scanState->finishedRemoteScan)
	{
		EState *executorState = ScanStateGetExecutorState(scanState);
		DistributedPlan *distributedPlan = scanState->distributedPlan;
		Query *insertSelectQuery = copyObject(distributedPlan->insertSelectQuery);
		List *insertTargetList = insertSelectQuery->targetList;
		RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertSelectQuery);
		RangeTblEntry *insertRte = ExtractResultRelationRTE(insertSelectQuery);
		Oid targetRelationId = insertRte->relid;
		char *intermediateResultIdPrefix = distributedPlan->intermediateResultIdPrefix;
		bool hasReturning = distributedPlan->expectResults;
		HTAB *shardStateHash = NULL;

		Query *selectQuery = selectRte->subquery;
		PlannedStmt *selectPlan = copyObject(distributedPlan->selectPlanForInsertSelect);

		/*
		 * If we are dealing with partitioned table, we also need to lock its
		 * partitions. Here we only lock targetRelation, we acquire necessary
		 * locks on selected tables during execution of those select queries.
		 */
		if (PartitionedTable(targetRelationId))
		{
			LockPartitionRelations(targetRelationId, RowExclusiveLock);
		}

		if (distributedPlan->insertSelectMethod == INSERT_SELECT_REPARTITION)
		{
			ereport(DEBUG1, (errmsg("performing repartitioned INSERT ... SELECT")));

			DistributedPlan *distSelectPlan =
				GetDistributedPlan((CustomScan *) selectPlan->planTree);
			Job *distSelectJob = distSelectPlan->workerJob;
			List *distSelectTaskList = distSelectJob->taskList;
			bool randomAccess = true;
			bool interTransactions = false;
			bool binaryFormat =
				CanUseBinaryCopyFormatForTargetList(selectQuery->targetList);

			ExecuteSubPlans(distSelectPlan);

			/*
			 * We have a separate directory for each transaction, so choosing
			 * the same result prefix won't cause filename conflicts. Results
			 * directory name also includes node id and database id, so we don't
			 * need to include them in the filename. We include job id here for
			 * the case "INSERT/SELECTs" are executed recursively.
			 */
			StringInfo distResultPrefixString = makeStringInfo();
			appendStringInfo(distResultPrefixString,
							 "repartitioned_results_" UINT64_FORMAT,
							 distSelectJob->jobId);
			char *distResultPrefix = distResultPrefixString->data;

			CitusTableCacheEntry *targetRelation =
				GetCitusTableCacheEntry(targetRelationId);

			int partitionColumnIndex =
				PartitionColumnIndex(insertTargetList, targetRelation->partitionColumn);
			if (partitionColumnIndex == -1)
			{
				char *relationName = get_rel_name(targetRelationId);
				Oid schemaOid = get_rel_namespace(targetRelationId);
				char *schemaName = get_namespace_name(schemaOid);

				ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								errmsg(
									"the partition column of table %s should have a value",
									quote_qualified_identifier(schemaName,
															   relationName))));
			}

			TargetEntry *selectPartitionTE = list_nth(selectQuery->targetList,
													  partitionColumnIndex);
			const char *partitionColumnName = selectPartitionTE->resname ?
											  selectPartitionTE->resname : "(none)";

			ereport(DEBUG2, (errmsg(
								 "partitioning SELECT query by column index %d with name %s",
								 partitionColumnIndex, quote_literal_cstr(
									 partitionColumnName))));

			/*
			 * ExpandWorkerTargetEntry() can add additional columns to the worker
			 * query. Modify the task queries to only select columns we need.
			 */
			int requiredColumnCount = list_length(insertTargetList);
			List *jobTargetList = distSelectJob->jobQuery->targetList;
			if (list_length(jobTargetList) > requiredColumnCount)
			{
				List *projectedTargetEntries = ListTake(jobTargetList,
														requiredColumnCount);
				WrapTaskListForProjection(distSelectTaskList, projectedTargetEntries);
			}

			List **redistributedResults = RedistributeTaskListResults(distResultPrefix,
																	  distSelectTaskList,
																	  partitionColumnIndex,
																	  targetRelation,
																	  binaryFormat);

			/*
			 * At this point select query has been executed on workers and results
			 * have been fetched in such a way that they are colocated with corresponding
			 * target shard. Create and execute a list of tasks of form
			 * INSERT INTO ... SELECT * FROM read_intermediate_results(...);
			 */
			List *taskList = RedistributedInsertSelectTaskList(insertSelectQuery,
															   targetRelation,
															   redistributedResults,
															   binaryFormat);

			scanState->tuplestorestate =
				tuplestore_begin_heap(randomAccess, interTransactions, work_mem);
			TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
			TupleDestination *tupleDest = CreateTupleStoreTupleDest(
				scanState->tuplestorestate, tupleDescriptor);
			uint64 rowsInserted = ExecuteTaskListIntoTupleDest(ROW_MODIFY_COMMUTATIVE,
															   taskList, tupleDest,
															   hasReturning);

			executorState->es_processed = rowsInserted;

			if (SortReturning && hasReturning)
			{
				SortTupleStore(scanState);
			}
		}
		else if (insertSelectQuery->onConflict || hasReturning)
		{
			ereport(DEBUG1, (errmsg(
								 "Collecting INSERT ... SELECT results on coordinator")));

			/*
			 * If we also have a workerJob that means there is a second step
			 * to the INSERT...SELECT. This happens when there is a RETURNING
			 * or ON CONFLICT clause which is implemented as a separate
			 * distributed INSERT...SELECT from a set of intermediate results
			 * to the target relation.
			 */
			List *prunedTaskList = NIL;

			shardStateHash = ExecutePlanIntoColocatedIntermediateResults(
				targetRelationId,
				insertTargetList,
				selectPlan,
				executorState,
				intermediateResultIdPrefix);

			/* generate tasks for the INSERT..SELECT phase */
			List *taskList = TwoPhaseInsertSelectTaskList(targetRelationId,
														  insertSelectQuery,
														  intermediateResultIdPrefix);

			/*
			 * We cannot actually execute INSERT...SELECT tasks that read from
			 * intermediate results that weren't created because no rows were
			 * written to them. Prune those tasks out by only including tasks
			 * on shards with connections.
			 */
			Task *task = NULL;
			foreach_ptr(task, taskList)
			{
				uint64 shardId = task->anchorShardId;
				bool shardModified = false;

				hash_search(shardStateHash, &shardId, HASH_FIND, &shardModified);
				if (shardModified)
				{
					prunedTaskList = lappend(prunedTaskList, task);
				}
			}

			if (prunedTaskList != NIL)
			{
				bool randomAccess = true;
				bool interTransactions = false;

				Assert(scanState->tuplestorestate == NULL);
				scanState->tuplestorestate =
					tuplestore_begin_heap(randomAccess, interTransactions, work_mem);

				TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
				TupleDestination *tupleDest = CreateTupleStoreTupleDest(
					scanState->tuplestorestate, tupleDescriptor);

				ExecuteTaskListIntoTupleDest(ROW_MODIFY_COMMUTATIVE, prunedTaskList,
											 tupleDest, hasReturning);

				if (SortReturning && hasReturning)
				{
					SortTupleStore(scanState);
				}
			}
		}
		else
		{
			ereport(DEBUG1, (errmsg(
								 "Collecting INSERT ... SELECT results on coordinator")));

			ExecutePlanIntoRelation(targetRelationId, insertTargetList, selectPlan,
									executorState);
		}

		scanState->finishedRemoteScan = true;
	}

	TupleTableSlot *resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * TwoPhaseInsertSelectTaskList generates a list of tasks for a query that
 * inserts into a target relation and selects from a set of co-located
 * intermediate results.
 */
static List *
TwoPhaseInsertSelectTaskList(Oid targetRelationId, Query *insertSelectQuery,
							 char *resultIdPrefix)
{
	List *taskList = NIL;

	/*
	 * Make a copy of the INSERT ... SELECT. We'll repeatedly replace the
	 * subquery of insertResultQuery for different intermediate results and
	 * then deparse it.
	 */
	Query *insertResultQuery = copyObject(insertSelectQuery);
	RangeTblEntry *insertRte = ExtractResultRelationRTE(insertResultQuery);
	RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertResultQuery);

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

		/* generate the query on the intermediate result */
		Query *resultSelectQuery = BuildSubPlanResultQuery(insertSelectQuery->targetList,
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
		insertResultQuery->cteList = NIL;
		deparse_shard_query(insertResultQuery, targetRelationId, shardId, queryString);
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
 * ExecutePlanIntoColocatedIntermediateResults executes the given PlannedStmt
 * and inserts tuples into a set of intermediate results that are colocated with
 * the target table for further processing of ON CONFLICT or RETURNING. It also
 * returns the hash of shard states that were used to insert tuplesinto the target
 * relation.
 */
static HTAB *
ExecutePlanIntoColocatedIntermediateResults(Oid targetRelationId,
											List *insertTargetList,
											PlannedStmt *selectPlan,
											EState *executorState,
											char *intermediateResultIdPrefix)
{
	ParamListInfo paramListInfo = executorState->es_param_list_info;

	/* Get column name list and partition column index for the target table */
	List *columnNameList = BuildColumnNameListFromTargetList(targetRelationId,
															 insertTargetList);
	int partitionColumnIndex = PartitionColumnIndexFromColumnList(targetRelationId,
																  columnNameList);

	/* set up a DestReceiver that copies into the intermediate table */
	CitusCopyDestReceiver *copyDest = CreateCitusCopyDestReceiver(targetRelationId,
																  columnNameList,
																  partitionColumnIndex,
																  executorState,
																  intermediateResultIdPrefix);

	ExecutePlanIntoDestReceiver(selectPlan, paramListInfo, (DestReceiver *) copyDest);

	executorState->es_processed = copyDest->tuplesSent;

	XactModificationLevel = XACT_MODIFICATION_DATA;

	return copyDest->shardStateHash;
}


/*
 * ExecutePlanIntoRelation executes the given plan and inserts the
 * results into the target relation, which is assumed to be a distributed
 * table.
 */
static void
ExecutePlanIntoRelation(Oid targetRelationId, List *insertTargetList,
						PlannedStmt *selectPlan, EState *executorState)
{
	ParamListInfo paramListInfo = executorState->es_param_list_info;

	/* Get column name list and partition column index for the target table */
	List *columnNameList = BuildColumnNameListFromTargetList(targetRelationId,
															 insertTargetList);
	int partitionColumnIndex = PartitionColumnIndexFromColumnList(targetRelationId,
																  columnNameList);

	/* set up a DestReceiver that copies into the distributed table */
	CitusCopyDestReceiver *copyDest = CreateCitusCopyDestReceiver(targetRelationId,
																  columnNameList,
																  partitionColumnIndex,
																  executorState, NULL);

	ExecutePlanIntoDestReceiver(selectPlan, paramListInfo, (DestReceiver *) copyDest);

	executorState->es_processed = copyDest->tuplesSent;

	XactModificationLevel = XACT_MODIFICATION_DATA;
}


/*
 * BuildColumnNameListForCopyStatement build the column name list given the insert
 * target list.
 */
static List *
BuildColumnNameListFromTargetList(Oid targetRelationId, List *insertTargetList)
{
	List *columnNameList = NIL;

	/* build the list of column names for the COPY statement */
	TargetEntry *insertTargetEntry = NULL;
	foreach_ptr(insertTargetEntry, insertTargetList)
	{
		columnNameList = lappend(columnNameList, insertTargetEntry->resname);
	}

	return columnNameList;
}


/*
 * PartitionColumnIndexFromColumnList returns the index of partition column from given
 * column name list and relation ID. If given list doesn't contain the partition
 * column, it returns -1.
 */
static int
PartitionColumnIndexFromColumnList(Oid relationId, List *columnNameList)
{
	Var *partitionColumn = PartitionColumn(relationId, 0);
	int partitionColumnIndex = 0;

	const char *columnName = NULL;
	foreach_ptr(columnName, columnNameList)
	{
		AttrNumber attrNumber = get_attnum(relationId, columnName);

		/* check whether this is the partition column */
		if (partitionColumn != NULL && attrNumber == partitionColumn->varattno)
		{
			return partitionColumnIndex;
		}

		partitionColumnIndex++;
	}

	return -1;
}


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
 * RedistributedInsertSelectTaskList returns a task list to insert given
 * redistributedResults into the given target relation.
 * redistributedResults[shardIndex] is list of cstrings each of which is
 * a result name which should be inserted into
 * targetRelation->sortedShardIntervalArray[shardIndex].
 */
static List *
RedistributedInsertSelectTaskList(Query *insertSelectQuery,
								  CitusTableCacheEntry *targetRelation,
								  List **redistributedResults,
								  bool useBinaryFormat)
{
	List *taskList = NIL;

	/*
	 * Make a copy of the INSERT ... SELECT. We'll repeatedly replace the
	 * subquery of insertResultQuery for different intermediate results and
	 * then deparse it.
	 */
	Query *insertResultQuery = copyObject(insertSelectQuery);
	RangeTblEntry *insertRte = ExtractResultRelationRTE(insertResultQuery);
	RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertResultQuery);
	List *selectTargetList = selectRte->subquery->targetList;
	Oid targetRelationId = targetRelation->relationId;

	int shardCount = targetRelation->shardIntervalArrayLength;
	int shardOffset = 0;
	uint32 taskIdIndex = 1;
	uint64 jobId = INVALID_JOB_ID;

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
		insertResultQuery->cteList = NIL;
		deparse_shard_query(insertResultQuery, targetRelationId, shardId, queryString);
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


/*
 * PartitionColumnIndex finds the index of given partition column in the
 * given target list.
 */
static int
PartitionColumnIndex(List *insertTargetList, Var *partitionColumn)
{
	TargetEntry *insertTargetEntry = NULL;
	int targetEntryIndex = 0;
	foreach_ptr(insertTargetEntry, insertTargetList)
	{
		if (insertTargetEntry->resno == partitionColumn->varattno)
		{
			return targetEntryIndex;
		}

		targetEntryIndex++;
	}

	return -1;
}


/*
 * IsRedistributablePlan returns true if the given plan is a redistrituable plan.
 */
bool
IsRedistributablePlan(Plan *selectPlan)
{
	if (!EnableRepartitionedInsertSelect)
	{
		return false;
	}

	/* don't redistribute if query is not distributed or requires merge on coordinator */
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
 * WrapTaskListForProjection wraps task query string to only select given
 * projected columns. It modifies the taskList.
 */
static void
WrapTaskListForProjection(List *taskList, List *projectedTargetEntries)
{
	StringInfo projectedColumnsString = makeStringInfo();
	int entryIndex = 0;
	TargetEntry *targetEntry = NULL;
	foreach_ptr(targetEntry, projectedTargetEntries)
	{
		if (entryIndex != 0)
		{
			appendStringInfoChar(projectedColumnsString, ',');
		}

		char *columnName = targetEntry->resname;
		Assert(columnName != NULL);
		appendStringInfoString(projectedColumnsString, quote_identifier(columnName));

		entryIndex++;
	}

	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		StringInfo wrappedQuery = makeStringInfo();
		appendStringInfo(wrappedQuery, "SELECT %s FROM (%s) subquery",
						 projectedColumnsString->data,
						 TaskQueryString(task));
		SetTaskQueryString(task, wrappedQuery->data);
	}
}
