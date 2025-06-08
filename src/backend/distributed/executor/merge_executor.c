/*-------------------------------------------------------------------------
 *
 * merge_executor.c
 *
 * Executor logic for MERGE SQL statement.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"

#include "distributed/distributed_execution_locks.h"
#include "distributed/insert_select_executor.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/merge_executor.h"
#include "distributed/merge_planner.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_router_planner.h"
#include "distributed/repartition_executor.h"
#include "distributed/stats/stat_counters.h"
#include "distributed/subplan_execution.h"

static void ExecuteSourceAtWorkerAndRepartition(CitusScanState *scanState);
static void ExecuteSourceAtCoordAndRedistribution(CitusScanState *scanState);
static HTAB * ExecuteMergeSourcePlanIntoColocatedIntermediateResults(Oid targetRelationId,
																	 Query *mergeQuery,
																	 List *
																	 sourceTargetList,
																	 PlannedStmt *
																	 sourcePlan,
																	 EState *executorState,
																	 char *
																	 intermediateResultIdPrefix,
																	 int
																	 partitionColumnIndex);


/*
 * NonPushableMergeCommandExecScan performs an MERGE INTO distributed_table
 * USING (source-query) ... command. This can be done either by aggregating
 * task results at the coordinator and repartitioning the results, or by
 * repartitioning task results and directly transferring data between nodes.
 */
TupleTableSlot *
NonPushableMergeCommandExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *distributedPlan = scanState->distributedPlan;

	if (!scanState->finishedRemoteScan)
	{
		switch (distributedPlan->modifyWithSelectMethod)
		{
			case MODIFY_WITH_SELECT_REPARTITION:
			{
				ExecuteSourceAtWorkerAndRepartition(scanState);
				break;
			}

			case MODIFY_WITH_SELECT_VIA_COORDINATOR:
			{
				ExecuteSourceAtCoordAndRedistribution(scanState);
				break;
			}

			default:
			{
				ereport(ERROR, (errmsg("Unexpected MERGE execution method(%d)",
									   distributedPlan->modifyWithSelectMethod)));
			}
		}

		scanState->finishedRemoteScan = true;
	}

	TupleTableSlot *resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * ExecuteSourceAtWorkerAndRepartition Executes the Citus distributed plan, including any
 * sub-plans, and captures the results in intermediate files. Subsequently, redistributes
 * the result files to ensure colocation with the target, and directs the MERGE SQL
 * operation to the target shards on the worker nodes, utilizing the colocated
 * intermediate files as the data source.
 */
static void
ExecuteSourceAtWorkerAndRepartition(CitusScanState *scanState)
{
	DistributedPlan *distributedPlan = scanState->distributedPlan;
	Query *mergeQuery =
		copyObject(distributedPlan->modifyQueryViaCoordinatorOrRepartition);
	RangeTblEntry *targetRte = ExtractResultRelationRTE(mergeQuery);
	RangeTblEntry *sourceRte = ExtractMergeSourceRangeTableEntry(mergeQuery, false);
	Oid targetRelationId = targetRte->relid;
	bool hasReturning = distributedPlan->expectResults;
	Query *sourceQuery = sourceRte->subquery;
	PlannedStmt *sourcePlan =
		copyObject(distributedPlan->selectPlanForModifyViaCoordinatorOrRepartition);
	EState *executorState = ScanStateGetExecutorState(scanState);

	/*
	 * If we are dealing with partitioned table, we also need to lock its
	 * partitions. Here we only lock targetRelation, we acquire necessary
	 * locks on source tables during execution of those source queries.
	 */
	if (PartitionedTable(targetRelationId))
	{
		LockPartitionRelations(targetRelationId, RowExclusiveLock);
	}

	bool randomAccess = true;
	bool interTransactions = false;
	DistributedPlan *distSourcePlan =
		GetDistributedPlan((CustomScan *) sourcePlan->planTree);
	Job *distSourceJob = distSourcePlan->workerJob;
	List *distSourceTaskList = distSourceJob->taskList;
	bool binaryFormat =
		CanUseBinaryCopyFormatForTargetList(sourceQuery->targetList);

	ereport(DEBUG1, (errmsg("Executing subplans of the source query and "
							"storing the results at the respective node(s)")));

	ExecuteSubPlans(distSourcePlan, false);

	/*
	 * We have a separate directory for each transaction, so choosing
	 * the same result prefix won't cause filename conflicts. Results
	 * directory name also includes node id and database id, so we don't
	 * need to include them in the filename. We include job id here for
	 * the case "MERGE USING <source query>" is executed recursively.
	 */
	StringInfo distResultPrefixString = makeStringInfo();
	appendStringInfo(distResultPrefixString,
					 "repartitioned_results_" UINT64_FORMAT,
					 distSourceJob->jobId);
	char *distResultPrefix = distResultPrefixString->data;
	CitusTableCacheEntry *targetRelation = GetCitusTableCacheEntry(targetRelationId);

	ereport(DEBUG1, (errmsg("Redistributing source result rows across nodes")));

	/*
	 * partitionColumnIndex determines the column in the selectTaskList to
	 * use for (re)partitioning of the source result, which will colocate
	 * the result data with the target.
	 */
	int partitionColumnIndex = distributedPlan->sourceResultRepartitionColumnIndex;

	/*
	 * Below call partitions the results using shard ranges and partition method of
	 * targetRelation, and then colocates the result files with shards. These
	 * transfers are done by calls to fetch_intermediate_results() between nodes.
	 */
	List **redistributedResults =
		RedistributeTaskListResults(distResultPrefix,
									distSourceTaskList, partitionColumnIndex,
									targetRelation, binaryFormat);

	if (list_length(distSourceTaskList) <= 1)
	{
		/*
		 * Probably we will never get here for a repartitioned MERGE
		 * because when the source is a single shard table, we should
		 * most probably choose to use ExecuteSourceAtCoordAndRedistribution(),
		 * but we still keep this here.
		 */
		IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_SINGLE_SHARD);
	}
	else
	{
		IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_MULTI_SHARD);
	}

	ereport(DEBUG1, (errmsg("Executing final MERGE on workers using "
							"intermediate results")));

	/*
	 * At this point source query has been executed on workers and results
	 * have been fetched in such a way that they are colocated with corresponding
	 * target shard(s). Create and execute a list of tasks of form
	 * MERGE  INTO ... USING SELECT * FROM read_intermediate_results(...);
	 */
	List *taskList =
		GenerateTaskListWithRedistributedResults(mergeQuery,
												 targetRelation,
												 redistributedResults,
												 binaryFormat);

	scanState->tuplestorestate =
		tuplestore_begin_heap(randomAccess, interTransactions, work_mem);
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
	TupleDestination *tupleDest =
		CreateTupleStoreTupleDest(scanState->tuplestorestate,
								  tupleDescriptor);
	uint64 rowsMerged =
		ExecuteTaskListIntoTupleDestWithParam(ROW_MODIFY_NONCOMMUTATIVE, taskList,
											  tupleDest,
											  hasReturning,
											  paramListInfo);

	if (list_length(taskList) <= 1)
	{
		IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_SINGLE_SHARD);
	}
	else
	{
		IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_MULTI_SHARD);
	}

	executorState->es_processed = rowsMerged;
}


/*
 * ExecuteSourceAtCoordAndRedistribution Executes the plan that necessitates evaluation
 * at the coordinator and redistributes the resulting rows to intermediate files,
 * ensuring colocation with the target shards. Directs the MERGE SQL operation to the
 * target shards on the worker nodes, utilizing the colocated intermediate files as the
 * data source.
 */
void
ExecuteSourceAtCoordAndRedistribution(CitusScanState *scanState)
{
	EState *executorState = ScanStateGetExecutorState(scanState);
	DistributedPlan *distributedPlan = scanState->distributedPlan;
	Query *mergeQuery =
		copyObject(distributedPlan->modifyQueryViaCoordinatorOrRepartition);
	RangeTblEntry *targetRte = ExtractResultRelationRTE(mergeQuery);
	RangeTblEntry *sourceRte = ExtractMergeSourceRangeTableEntry(mergeQuery, false);
	Query *sourceQuery = sourceRte->subquery;
	Oid targetRelationId = targetRte->relid;
	PlannedStmt *sourcePlan =
		copyObject(distributedPlan->selectPlanForModifyViaCoordinatorOrRepartition);
	char *intermediateResultIdPrefix = distributedPlan->intermediateResultIdPrefix;
	bool hasReturning = distributedPlan->expectResults;
	bool hasNotMatchedBySource = HasMergeNotMatchedBySource(mergeQuery);
	int partitionColumnIndex = distributedPlan->sourceResultRepartitionColumnIndex;

	/*
	 * If we are dealing with partitioned table, we also need to lock its
	 * partitions. Here we only lock targetRelation, we acquire necessary
	 * locks on source tables during execution of those source queries.
	 */
	if (PartitionedTable(targetRelationId))
	{
		LockPartitionRelations(targetRelationId, RowExclusiveLock);
	}

	ereport(DEBUG1, (errmsg("Collect source query results on coordinator")));

	List *prunedTaskList = NIL, *emptySourceTaskList = NIL;
	HTAB *shardStateHash =
		ExecuteMergeSourcePlanIntoColocatedIntermediateResults(
			targetRelationId,
			mergeQuery,
			sourceQuery->targetList,
			sourcePlan,
			executorState,
			intermediateResultIdPrefix,
			partitionColumnIndex);

	ereport(DEBUG1, (errmsg("Create a MERGE task list that needs to be routed")));

	/* generate tasks for the .. phase */
	List *taskList =
		GenerateTaskListWithColocatedIntermediateResults(targetRelationId, mergeQuery,
														 intermediateResultIdPrefix);

	/*
	 * We cannot actually execute MERGE INTO ... tasks that read from
	 * intermediate results that weren't created because no rows were
	 * written to them. Prune those tasks out by only including tasks
	 * on shards with connections; however, if the MERGE INTO includes
	 * a NOT MATCHED BY SOURCE clause we need to include the task.
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
		else if (hasNotMatchedBySource)
		{
			emptySourceTaskList = lappend(emptySourceTaskList, task);
		}
	}

	if (emptySourceTaskList != NIL)
	{
		ereport(DEBUG1, (errmsg("MERGE has NOT MATCHED BY SOURCE clause, "
								"execute MERGE on all shards")));
		AdjustTaskQueryForEmptySource(targetRelationId, mergeQuery, emptySourceTaskList,
									  intermediateResultIdPrefix);
		prunedTaskList = list_concat(prunedTaskList, emptySourceTaskList);
	}

	if (prunedTaskList == NIL)
	{
		/*
		 * No task to execute, but we still increment STAT_QUERY_EXECUTION_SINGLE_SHARD
		 * as per our convention.
		 */
		IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_SINGLE_SHARD);
		return;
	}

	ereport(DEBUG1, (errmsg("Execute MERGE task list")));
	bool randomAccess = true;
	bool interTransactions = false;
	Assert(scanState->tuplestorestate == NULL);
	scanState->tuplestorestate = tuplestore_begin_heap(randomAccess, interTransactions,
													   work_mem);
	TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	TupleDestination *tupleDest =
		CreateTupleStoreTupleDest(scanState->tuplestorestate, tupleDescriptor);
	uint64 rowsMerged =
		ExecuteTaskListIntoTupleDestWithParam(ROW_MODIFY_NONCOMMUTATIVE,
											  prunedTaskList,
											  tupleDest,
											  hasReturning,
											  paramListInfo);

	if (list_length(prunedTaskList) == 1)
	{
		IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_SINGLE_SHARD);
	}
	else
	{
		IncrementStatCounterForMyDb(STAT_QUERY_EXECUTION_MULTI_SHARD);
	}

	executorState->es_processed = rowsMerged;
}


/*
 * ExecuteMergeSourcePlanIntoColocatedIntermediateResults Executes the given PlannedStmt
 * and inserts tuples into a set of intermediate results that are colocated with the
 * target table for further processing MERGE INTO. It also returns the hash of shard
 * states that were used to insert tuplesinto the target relation.
 */
static HTAB *
ExecuteMergeSourcePlanIntoColocatedIntermediateResults(Oid targetRelationId,
													   Query *mergeQuery,
													   List *sourceTargetList,
													   PlannedStmt *sourcePlan,
													   EState *executorState,
													   char *intermediateResultIdPrefix,
													   int partitionColumnIndex)
{
	ParamListInfo paramListInfo = executorState->es_param_list_info;

	/* Get column name list and partition column index for the target table */
	List *columnNameList =
		BuildColumnNameListFromTargetList(targetRelationId, sourceTargetList);

	/*
	 * We don't track query counters for the COPY commands that are executed to
	 * prepare intermediate results.
	 */
	const bool trackQueryCounters = false;

	/* set up a DestReceiver that copies into the intermediate file */
	const bool publishableData = false;
	CitusCopyDestReceiver *copyDest = CreateCitusCopyDestReceiver(targetRelationId,
																  columnNameList,
																  partitionColumnIndex,
																  executorState,
																  intermediateResultIdPrefix,
																  publishableData,
																  trackQueryCounters);

	/* We can skip when writing to intermediate files */
	copyDest->skipCoercions = true;

	ExecutePlanIntoDestReceiver(sourcePlan, paramListInfo, (DestReceiver *) copyDest);

	executorState->es_processed = copyDest->tuplesSent;
	XactModificationLevel = XACT_MODIFICATION_DATA;

	return copyDest->shardStateHash;
}
