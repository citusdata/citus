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

#include "distributed/distributed_execution_locks.h"
#include "distributed/insert_select_executor.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/merge_executor.h"
#include "distributed/merge_planner.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_router_planner.h"
#include "distributed/subplan_execution.h"

#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"


static int sourceResultPartitionColumnIndex(Query *mergeQuery,
											List *sourceTargetList,
											CitusTableCacheEntry *targetRelation);
static bool ExtractEqualOpExprWalker(Node *node, List **equalOpExprList);

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

	if (!scanState->finishedRemoteScan)
	{
		EState *executorState = ScanStateGetExecutorState(scanState);
		DistributedPlan *distributedPlan = scanState->distributedPlan;
		Query *mergeQuery = copyObject(distributedPlan->insertSelectQuery);
		RangeTblEntry *targetRte = ExtractResultRelationRTE(mergeQuery);
		RangeTblEntry *sourceRte = ExtractSourceRangeTableEntry(mergeQuery);
		Oid targetRelationId = targetRte->relid;
		bool hasReturning = distributedPlan->expectResults;
		Query *selectQuery = sourceRte->subquery;
		PlannedStmt *selectPlan = copyObject(distributedPlan->selectPlanForInsertSelect);

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
		DistributedPlan *distSelectPlan =
			GetDistributedPlan((CustomScan *) selectPlan->planTree);
		Job *distSelectJob = distSelectPlan->workerJob;
		List *distSelectTaskList = distSelectJob->taskList;
		bool binaryFormat =
			CanUseBinaryCopyFormatForTargetList(selectQuery->targetList);

		ereport(DEBUG1, (errmsg("Executing subplans of the source")));
		ExecuteSubPlans(distSelectPlan);

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
						 distSelectJob->jobId);
		char *distResultPrefix = distResultPrefixString->data;
		CitusTableCacheEntry *targetRelation = GetCitusTableCacheEntry(targetRelationId);
		ereport(DEBUG1, (errmsg("Executing task list and "
								"redistributing the source rows")));

		/*
		 * partitionColumnIndex determines the column in the selectTaskList to
		 * use for (re)partitioning of the source result, which will colocate
		 * the result data with the target.
		 */
		int partitionColumnIndex =
			sourceResultPartitionColumnIndex(mergeQuery,
											 selectQuery->targetList,
											 targetRelation);

		if (partitionColumnIndex == -1)
		{
			ereport(ERROR, (errmsg("Missing required join condition between "
								   "source and target's distribution column"),
							errhint("Without a join condition on the "
									"target's distribution column, "
									"the source rows cannot be efficiently "
									"redistributed, and the NOT-MATCHED "
									"condition cannot be evaluated "
									"unambiguously. This can result in "
									"incorrect or unexpected results when "
									"attempting to merge tables in a distributed setting")));
		}
		else
		{
			ereport(DEBUG1,
					(errmsg("Using column - index:%d from the source list "
							"to redistribute", partitionColumnIndex)));
		}


		/*
		 * Below call executes the given task list and partitions the results using shard
		 * ranges and partition method of targetRelation, and then colocates the result files
		 * with shards. It also moves the fragments in the cluster so they are colocated
		 * with the shards of target relation. These transfers are done by calls to
		 * fetch_intermediate_results() between nodes.
		 */
		List **redistributedResults =
			RedistributeTaskListResults(distResultPrefix,
										distSelectTaskList, partitionColumnIndex,
										targetRelation, binaryFormat);

		ereport(DEBUG1, (errmsg("Executing final MERGE on workers")));

		/*
		 * At this point source query has been executed on workers and results
		 * have been fetched in such a way that they are colocated with corresponding
		 * target shard(s). Create and execute a list of tasks of form
		 * MERGE  INTO ... USING SELECT * FROM read_intermediate_results(...);
		 */
		List *taskList = RedistributedInsertSelectTaskList(mergeQuery,
														   targetRelation,
														   redistributedResults,
														   binaryFormat);

		scanState->tuplestorestate =
			tuplestore_begin_heap(randomAccess, interTransactions, work_mem);
		TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);
		TupleDestination *tupleDest =
			CreateTupleStoreTupleDest(scanState->tuplestorestate,
									  tupleDescriptor);
		uint64 rowsMerged =
			ExecuteTaskListIntoTupleDest(ROW_MODIFY_COMMUTATIVE, taskList,
										 tupleDest,
										 hasReturning);
		executorState->es_processed = rowsMerged;
		scanState->finishedRemoteScan = true;
	}

	TupleTableSlot *resultSlot = ReturnTupleFromTuplestore(scanState);

	return resultSlot;
}


/*
 * ExtractEqualOpExprWalker traverses the ON clause and gathers
 * all expressions of the following types: ... ON t.id = s.id
 * or t.id = <const>
 */
static bool
ExtractEqualOpExprWalker(Node *node, List **equalOpExprList)
{
	if (node == NULL)
	{
		return false;
	}

	if (NodeIsEqualsOpExpr(node))
	{
		OpExpr *eqExpr = (OpExpr *) node;
		(*equalOpExprList) = lappend(*equalOpExprList, eqExpr);
	}

	bool walkerResult =
		expression_tree_walker(node, ExtractEqualOpExprWalker,
							   equalOpExprList);
	return walkerResult;
}


/*
 * sourceResultPartitionColumnIndex collects all the "=" expressions from the
 * provided quals and verifies if there is a join, either left or right, with
 * the distribution column of the given target. Once a match is found, it returns
 * the index of that match in the source's target list.
 */
static int
sourceResultPartitionColumnIndex(Query *mergeQuery, List *sourceTargetList,
								 CitusTableCacheEntry *targetRelation)
{
	Node *quals = mergeQuery->jointree->quals;

	if (IsA(quals, List))
	{
		quals = (Node *) make_ands_explicit((List *) quals);
	}

	/* Get all the equal(=) expressions from the ON clause */
	List *equalOpExprList = NIL;
	ExtractEqualOpExprWalker((Node *) quals, &equalOpExprList);

	/*
	 * Now that we have gathered all the _equal_ expressions in the list, we proceed
	 * to match the one that joins with the distribution column of the target.
	 */
	Var *targetColumn = targetRelation->partitionColumn;
	Var *sourceJoinColumn = NULL;
	Node *joinClause = NULL;
	foreach_ptr(joinClause, equalOpExprList)
	{
		Node *leftOperand;
		Node *rightOperand;
		if (!BinaryOpExpression((Expr *) joinClause, &leftOperand, &rightOperand))
		{
			continue;
		}

		if (equal(leftOperand, targetColumn) && IsA(rightOperand, Var))
		{
			sourceJoinColumn = (Var *) rightOperand;
		}
		else if (equal(rightOperand, targetColumn) && IsA(leftOperand, Var))
		{
			sourceJoinColumn = (Var *) leftOperand;
		}
	}

	if (!sourceJoinColumn)
	{
		return -1;
	}

	ErrorIfInsertNotMatchTargetDistributionColumn(targetRelation->relationId,
												  mergeQuery,
												  sourceJoinColumn);

	/*
	 * Find the index in the source list that will be utilized
	 * for colocation-redistribution.
	 */
	return PartitionColumnIndex(sourceTargetList, sourceJoinColumn);
}
