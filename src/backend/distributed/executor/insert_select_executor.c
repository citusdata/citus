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

#include "distributed/adaptive_executor.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/distributed_planner.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/merge_planner.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/repartition_executor.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/stats/stat_counters.h"
#include "distributed/subplan_execution.h"
#include "distributed/transaction_management.h"
#include "distributed/version_compat.h"

/* Config variables managed via guc.c */
bool EnableRepartitionedInsertSelect = true;


static void ExecutePlanIntoRelation(Oid targetRelationId, List *insertTargetList,
									PlannedStmt *selectPlan, EState *executorState);
static HTAB * ExecutePlanIntoColocatedIntermediateResults(Oid targetRelationId,
														  List *insertTargetList,
														  PlannedStmt *selectPlan,
														  EState *executorState,
														  char *intermediateResultIdPrefix);
static int PartitionColumnIndexFromColumnList(Oid relationId, List *columnNameList);
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
		Query *insertSelectQuery =
			copyObject(distributedPlan->modifyQueryViaCoordinatorOrRepartition);
		List *insertTargetList = insertSelectQuery->targetList;
		RangeTblEntry *selectRte = ExtractSelectRangeTableEntry(insertSelectQuery);
		RangeTblEntry *insertRte = ExtractResultRelationRTE(insertSelectQuery);
		Oid targetRelationId = insertRte->relid;
		char *intermediateResultIdPrefix = distributedPlan->intermediateResultIdPrefix;
		bool hasReturning = distributedPlan->expectResults;
		HTAB *shardStateHash = NULL;

		Query *selectQuery = selectRte->subquery;
		PlannedStmt *selectPlan =
			copyObject(distributedPlan->selectPlanForModifyViaCoordinatorOrRepartition);

		/*
		 * If we are dealing with partitioned table, we also need to lock its
		 * partitions. Here we only lock targetRelation, we acquire necessary
		 * locks on selected tables during execution of those select queries.
		 */
		if (PartitionedTable(targetRelationId))
		{
			LockPartitionRelations(targetRelationId, RowExclusiveLock);
		}

		if (distributedPlan->modifyWithSelectMethod == MODIFY_WITH_SELECT_REPARTITION)
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

			ExecuteSubPlans(distSelectPlan, false);

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

			int distributionColumnIndex =
				DistributionColumnIndex(insertTargetList,
										targetRelation->partitionColumn);
			if (distributionColumnIndex == -1)
			{
				ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
								errmsg(
									"the partition column of table %s should have a value",
									generate_qualified_relation_name(targetRelationId))));
			}

			TargetEntry *selectPartitionTE = list_nth(selectQuery->targetList,
													  distributionColumnIndex);
			const char *partitionColumnName = selectPartitionTE->resname ?
											  selectPartitionTE->resname : "(none)";

			ereport(DEBUG2, (errmsg(
								 "partitioning SELECT query by column index %d with name %s",
								 distributionColumnIndex, quote_literal_cstr(
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
																	  distributionColumnIndex,
																	  targetRelation,
																	  binaryFormat);

			if (list_length(distSelectTaskList) <= 1)
			{
				/*
				 * Probably we will never get here for a repartitioned
				 * INSERT..SELECT because when the source is a single shard
				 * table, we should most probably choose to use
				 * MODIFY_WITH_SELECT_VIA_COORDINATOR, but we still keep this
				 * here.
				 */
				IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_SINGLE_SHARD);
			}
			else
			{
				IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_MULTI_SHARD);
			}

			/*
			 * At this point select query has been executed on workers and results
			 * have been fetched in such a way that they are colocated with corresponding
			 * target shard. Create and execute a list of tasks of form
			 * INSERT INTO ... SELECT * FROM read_intermediate_results(...);
			 */
			List *taskList = GenerateTaskListWithRedistributedResults(insertSelectQuery,
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

			if (list_length(taskList) <= 1)
			{
				IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_SINGLE_SHARD);
			}
			else
			{
				IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_MULTI_SHARD);
			}

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
			List *taskList =
				GenerateTaskListWithColocatedIntermediateResults(
					targetRelationId, insertSelectQuery,
					intermediateResultIdPrefix);

			/*
			 * We cannot actually execute INSERT...SELECT tasks that read from
			 * intermediate results that weren't created because no rows were
			 * written to them. Prune those tasks out by only including tasks
			 * on shards with connections.
			 */
			Task *task = NULL;
			foreach_declared_ptr(task, taskList)
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

			if (list_length(prunedTaskList) <= 1)
			{
				IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_SINGLE_SHARD);
			}
			else
			{
				IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_MULTI_SHARD);
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

	/*
	 * We don't track query counters for the COPY commands that are executed to
	 * prepare intermediate results.
	 */
	const bool trackQueryCounters = false;

	/* set up a DestReceiver that copies into the intermediate table */
	const bool publishableData = true;
	CitusCopyDestReceiver *copyDest = CreateCitusCopyDestReceiver(targetRelationId,
																  columnNameList,
																  partitionColumnIndex,
																  executorState,
																  intermediateResultIdPrefix,
																  publishableData,
																  trackQueryCounters);

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

	/*
	 * We want to track query counters for the COPY commands that are executed to
	 * perform the final INSERT for such INSERT..SELECT queries.
	 */
	const bool trackQueryCounters = true;

	/* set up a DestReceiver that copies into the distributed table */
	const bool publishableData = true;
	CitusCopyDestReceiver *copyDest = CreateCitusCopyDestReceiver(targetRelationId,
																  columnNameList,
																  partitionColumnIndex,
																  executorState, NULL,
																  publishableData,
																  trackQueryCounters);

	ExecutePlanIntoDestReceiver(selectPlan, paramListInfo, (DestReceiver *) copyDest);

	executorState->es_processed = copyDest->tuplesSent;

	XactModificationLevel = XACT_MODIFICATION_DATA;
}


/*
 * BuildColumnNameListForCopyStatement build the column name list given the insert
 * target list.
 */
List *
BuildColumnNameListFromTargetList(Oid targetRelationId, List *insertTargetList)
{
	List *columnNameList = NIL;

	/* build the list of column names for the COPY statement */
	TargetEntry *insertTargetEntry = NULL;
	foreach_declared_ptr(insertTargetEntry, insertTargetList)
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
	foreach_declared_ptr(columnName, columnNameList)
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
 * DistributionColumnIndex finds the index of given distribution column in the
 * given target list.
 */
int
DistributionColumnIndex(List *insertTargetList, Var *distributionColumn)
{
	TargetEntry *insertTargetEntry = NULL;
	int targetEntryIndex = 0;
	foreach_declared_ptr(insertTargetEntry, insertTargetList)
	{
		if (insertTargetEntry->resno == distributionColumn->varattno)
		{
			return targetEntryIndex;
		}

		targetEntryIndex++;
	}

	return -1;
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
	foreach_declared_ptr(targetEntry, projectedTargetEntries)
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
	foreach_declared_ptr(task, taskList)
	{
		StringInfo wrappedQuery = makeStringInfo();
		appendStringInfo(wrappedQuery, "SELECT %s FROM (%s) subquery",
						 projectedColumnsString->data,
						 TaskQueryString(task));
		SetTaskQueryString(task, wrappedQuery->data);
	}
}
