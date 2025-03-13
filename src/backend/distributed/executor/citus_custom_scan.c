/*-------------------------------------------------------------------------
 *
 * citus_custom_scan.c
 *
 * Definitions of custom scan methods for all executor types.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "commands/copy.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "pg_version_constants.h"

#include "distributed/backend_data.h"
#include "distributed/citus_clauses.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/function_call_delegation.h"
#include "distributed/insert_select_executor.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/local_plan_cache.h"
#include "distributed/merge_executor.h"
#include "distributed/merge_planner.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/query_stats.h"
#include "distributed/shard_utils.h"
#include "distributed/subplan_execution.h"
#include "distributed/worker_log_messages.h"
#include "distributed/worker_protocol.h"


extern AllowedDistributionColumn AllowedDistributionColumnValue;

/* functions for creating custom scan nodes */
static Node * AdaptiveExecutorCreateScan(CustomScan *scan);
static Node * NonPushableInsertSelectCreateScan(CustomScan *scan);
static Node * DelayedErrorCreateScan(CustomScan *scan);
static Node * NonPushableMergeCommandCreateScan(CustomScan *scan);

/* functions that are common to different scans */
static void CitusBeginScan(CustomScanState *node, EState *estate, int eflags);
static void CitusBeginReadOnlyScan(CustomScanState *node, EState *estate, int eflags);
static void CitusBeginModifyScan(CustomScanState *node, EState *estate, int eflags);
static void CitusPreExecScan(CitusScanState *scanState);
static bool ModifyJobNeedsEvaluation(Job *workerJob);
static void RegenerateTaskForFasthPathQuery(Job *workerJob);
static void RegenerateTaskListForInsert(Job *workerJob);
static DistributedPlan * CopyDistributedPlanWithoutCache(
	DistributedPlan *originalDistributedPlan);
static void CitusEndScan(CustomScanState *node);
static void CitusReScan(CustomScanState *node);
static void EnsureForceDelegationDistributionKey(Job *job);
static void EnsureAnchorShardsInJobExist(Job *job);
static bool AnchorShardsInTaskListExist(List *taskList);
static void TryToRerouteFastPathModifyQuery(Job *job);


/* create custom scan methods for all executors */
CustomScanMethods AdaptiveExecutorCustomScanMethods = {
	"Citus Adaptive",
	AdaptiveExecutorCreateScan
};

CustomScanMethods NonPushableInsertSelectCustomScanMethods = {
	"Citus INSERT ... SELECT",
	NonPushableInsertSelectCreateScan
};

CustomScanMethods DelayedErrorCustomScanMethods = {
	"Citus Delayed Error",
	DelayedErrorCreateScan
};

CustomScanMethods NonPushableMergeCommandCustomScanMethods = {
	"Citus MERGE INTO ...",
	NonPushableMergeCommandCreateScan
};


/*
 * Define executor methods for the different executor types.
 */
static CustomExecMethods AdaptiveExecutorCustomExecMethods = {
	.CustomName = "AdaptiveExecutorScan",
	.BeginCustomScan = CitusBeginScan,
	.ExecCustomScan = CitusExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = CitusExplainScan
};

static CustomExecMethods NonPushableInsertSelectCustomExecMethods = {
	.CustomName = "NonPushableInsertSelectScan",
	.BeginCustomScan = CitusBeginScan,
	.ExecCustomScan = NonPushableInsertSelectExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = NonPushableInsertSelectExplainScan
};


static CustomExecMethods NonPushableMergeCommandCustomExecMethods = {
	.CustomName = "NonPushableMergeCommandScan",
	.BeginCustomScan = CitusBeginScan,
	.ExecCustomScan = NonPushableMergeCommandExecScan,
	.EndCustomScan = CitusEndScan,
	.ReScanCustomScan = CitusReScan,
	.ExplainCustomScan = NonPushableMergeCommandExplainScan
};


/*
 * IsCitusCustomState returns if a given PlanState node is a CitusCustomState node.
 */
bool
IsCitusCustomState(PlanState *planState)
{
	if (!IsA(planState, CustomScanState))
	{
		return false;
	}

	CustomScanState *css = castNode(CustomScanState, planState);
	if (css->methods == &AdaptiveExecutorCustomExecMethods ||
		css->methods == &NonPushableInsertSelectCustomExecMethods ||
		css->methods == &NonPushableMergeCommandCustomExecMethods)
	{
		return true;
	}

	return false;
}


/*
 * Let PostgreSQL know about Citus' custom scan nodes.
 */
void
RegisterCitusCustomScanMethods(void)
{
	RegisterCustomScanMethods(&AdaptiveExecutorCustomScanMethods);
	RegisterCustomScanMethods(&NonPushableInsertSelectCustomScanMethods);
	RegisterCustomScanMethods(&DelayedErrorCustomScanMethods);
	RegisterCustomScanMethods(&NonPushableMergeCommandCustomScanMethods);
}


/*
 * CitusBeginScan sets the coordinator backend initiated by Citus for queries using
 * that function as the BeginCustomScan callback.
 *
 * The function also handles deferred shard pruning along with function evaluations.
 */
static void
CitusBeginScan(CustomScanState *node, EState *estate, int eflags)
{
	CitusScanState *scanState = (CitusScanState *) node;

	/*
	 * Make sure we can see notices during regular queries, which would typically
	 * be the result of a function that raises a notices being called.
	 */
	EnableWorkerMessagePropagation();


	/*
	 * Since we are using a tuplestore we cannot use the virtual tuples postgres had
	 * already setup on the CustomScan. Instead we need to reinitialize the tuples as
	 * minimal.
	 *
	 * During initialization postgres also created the projection information and the
	 * quals, but both are 'compiled' to be executed on virtual tuples. Since we replaced
	 * the tuples with minimal tuples we also compile both the projection and the quals
	 * on to these 'new' tuples.
	 */
	ExecInitResultSlot(&scanState->customScanState.ss.ps, &TTSOpsMinimalTuple);

	ExecInitScanTupleSlot(node->ss.ps.state, &node->ss, node->ss.ps.scandesc,
						  &TTSOpsMinimalTuple);
	ExecAssignScanProjectionInfoWithVarno(&node->ss, INDEX_VAR);

	node->ss.ps.qual = ExecInitQual(node->ss.ps.plan->qual, (PlanState *) node);

	DistributedPlan *distributedPlan = scanState->distributedPlan;
	if (distributedPlan->modifyQueryViaCoordinatorOrRepartition != NULL)
	{
		/*
		 * INSERT..SELECT via coordinator or re-partitioning are special because
		 * the SELECT part is planned separately.
		 */
		return;
	}
	else if (distributedPlan->modLevel == ROW_MODIFY_READONLY)
	{
		CitusBeginReadOnlyScan(node, estate, eflags);
	}
	else
	{
		CitusBeginModifyScan(node, estate, eflags);
	}


	/*
	 * If there is force_delgation functions' distribution argument set,
	 * enforce it
	 */
	if (AllowedDistributionColumnValue.isActive)
	{
		Job *workerJob = scanState->distributedPlan->workerJob;
		EnsureForceDelegationDistributionKey(workerJob);
	}

	/*
	 * In case of a prepared statement, we will see this distributed plan again
	 * on the next execution with a higher usage counter.
	 */
	distributedPlan->numberOfTimesExecuted++;
}


/*
 * CitusPreExecScan is called right before postgres' executor starts pulling tuples.
 */
static void
CitusPreExecScan(CitusScanState *scanState)
{
	AdaptiveExecutorPreExecutorRun(scanState);
}


/*
 * CitusExecScan is called when a tuple is pulled from a custom scan.
 * On the first call, it executes the distributed query and writes the
 * results to a tuple store. The postgres executor calls this function
 * repeatedly to read tuples from the tuple store.
 */
TupleTableSlot *
CitusExecScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;

	if (!scanState->finishedRemoteScan)
	{
		AdaptiveExecutor(scanState);

		scanState->finishedRemoteScan = true;
	}

	return ReturnTupleFromTuplestore(scanState);
}


/*
 * CitusBeginReadOnlyScan handles deferred pruning and plan caching for SELECTs.
 */
static void
CitusBeginReadOnlyScan(CustomScanState *node, EState *estate, int eflags)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *originalDistributedPlan = scanState->distributedPlan;

	Assert(originalDistributedPlan->workerJob->jobQuery->commandType == CMD_SELECT);

	if (!originalDistributedPlan->workerJob->deferredPruning)
	{
		/*
		 * For SELECT queries that have already been pruned we can proceed straight
		 * to execution, since none of the prepared statement logic applies.
		 */
		return;
	}

	/*
	 * Create a copy of the generic plan for the current execution, but make a shallow
	 * copy of the plan cache. That means we'll be able to access the plan cache via
	 * currentPlan->workerJob->localPlannedStatements, but it will be preserved across
	 * executions by the prepared statement logic.
	 */
	DistributedPlan *currentPlan =
		CopyDistributedPlanWithoutCache(originalDistributedPlan);
	scanState->distributedPlan = currentPlan;

	Job *workerJob = currentPlan->workerJob;
	Query *jobQuery = workerJob->jobQuery;
	PlanState *planState = &(scanState->customScanState.ss.ps);

	/*
	 * We only do deferred pruning for fast path queries, which have a single
	 * partition column value.
	 */
	Assert(currentPlan->fastPathRouterPlan || !EnableFastPathRouterPlanner);

	/*
	 * Evaluate parameters, because the parameters are only available on the
	 * coordinator and are required for pruning.
	 *
	 * We don't evaluate functions for read-only queries on the coordinator
	 * at the moment. Most function calls would be in a context where they
	 * should be re-evaluated for every row in case of volatile functions.
	 *
	 * TODO: evaluate stable functions
	 */
	ExecuteCoordinatorEvaluableExpressions(jobQuery, planState);

	/* job query no longer has parameters, so we should not send any */
	workerJob->parametersInJobQueryResolved = true;

	/* parameters are filled in, so we can generate a task for this execution */
	RegenerateTaskForFasthPathQuery(workerJob);

	if (IsLocalPlanCachingSupported(workerJob, originalDistributedPlan))
	{
		Task *task = linitial(workerJob->taskList);

		/*
		 * We are going to execute this task locally. If it's not already in
		 * the cache, create a local plan now and add it to the cache. During
		 * execution, we will get the plan from the cache.
		 *
		 * The plan will be cached across executions when originalDistributedPlan
		 * represents a prepared statement.
		 */
		CacheLocalPlanForShardQuery(task, originalDistributedPlan,
									estate->es_param_list_info);
	}
}


/*
 * CitusBeginModifyScan prepares the scan state for a modification.
 *
 * Modifications are special because:
 * a) we evaluate function calls (e.g. nextval) here and the outcome may
 *    determine which shards are affected by this query.
 * b) we need to take metadata locks to make sure no write is left behind
 *    when finalizing a shard move.
 */
static void
CitusBeginModifyScan(CustomScanState *node, EState *estate, int eflags)
{
	CitusScanState *scanState = (CitusScanState *) node;
	PlanState *planState = &(scanState->customScanState.ss.ps);
	DistributedPlan *originalDistributedPlan = scanState->distributedPlan;

	MemoryContext localContext = AllocSetContextCreate(CurrentMemoryContext,
													   "CitusBeginModifyScan",
													   ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(localContext);

	DistributedPlan *currentPlan =
		CopyDistributedPlanWithoutCache(originalDistributedPlan);
	scanState->distributedPlan = currentPlan;

	Job *workerJob = currentPlan->workerJob;

	Query *jobQuery = workerJob->jobQuery;

	if (ModifyJobNeedsEvaluation(workerJob))
	{
		ExecuteCoordinatorEvaluableExpressions(jobQuery, planState);

		/* job query no longer has parameters, so we should not send any */
		workerJob->parametersInJobQueryResolved = true;
	}

	if (workerJob->deferredPruning)
	{
		/*
		 * At this point, we're about to do the shard pruning for fast-path queries.
		 * Given that pruning is deferred always for INSERTs, we get here
		 * !EnableFastPathRouterPlanner  as well. Given that INSERT statements with
		 * CTEs/sublinks etc are not eligible for fast-path router plan, we get here
		 * jobQuery->commandType == CMD_INSERT as well.
		 */
		Assert(currentPlan->fastPathRouterPlan || !EnableFastPathRouterPlanner ||
			   jobQuery->commandType == CMD_INSERT);

		/*
		 * We can only now decide which shard to use, so we need to build a new task
		 * list.
		 */
		if (jobQuery->commandType == CMD_INSERT)
		{
			RegenerateTaskListForInsert(workerJob);
		}
		else
		{
			RegenerateTaskForFasthPathQuery(workerJob);
		}
	}
	else if (workerJob->requiresCoordinatorEvaluation)
	{
		/*
		 * When there is no deferred pruning, but we did evaluate functions, then
		 * we only rebuild the query strings in the existing tasks.
		 */
		RebuildQueryStrings(workerJob);
	}


	/* We skip shard related things if the job contains only local tables */
	if (!ModifyLocalTableJob(workerJob))
	{
		/*
		 * Now that we know the shard ID(s) we can acquire the necessary shard metadata
		 * locks. Once we have the locks it's safe to load the placement metadata.
		 */

		/* prevent concurrent placement changes */
		AcquireMetadataLocks(workerJob->taskList);

		/*
		 * In case of a split, the shard might no longer be available. In that
		 * case try to reroute. We can only do this for fast path queries.
		 */
		if (currentPlan->fastPathRouterPlan &&
			!AnchorShardsInTaskListExist(workerJob->taskList))
		{
			TryToRerouteFastPathModifyQuery(workerJob);
		}

		/* ensure there is no invalid shard */
		EnsureAnchorShardsInJobExist(workerJob);

		/* modify tasks are always assigned using first-replica policy */
		workerJob->taskList = FirstReplicaAssignTaskList(workerJob->taskList);
	}


	/*
	 * Now that we have populated the task placements we can determine whether
	 * any of them are local to this node and cache a plan if needed.
	 */
	if (IsLocalPlanCachingSupported(workerJob, originalDistributedPlan))
	{
		Task *task = linitial(workerJob->taskList);

		/*
		 * We are going to execute this task locally. If it's not already in
		 * the cache, create a local plan now and add it to the cache. During
		 * execution, we will get the plan from the cache.
		 *
		 * WARNING: In this function we'll use the original plan with the original
		 * query tree, meaning parameters and function calls are back and we'll
		 * redo evaluation in the local (Postgres) executor. The reason we do this
		 * is that we only need to cache one generic plan per shard.
		 *
		 * The plan will be cached across executions when originalDistributedPlan
		 * represents a prepared statement.
		 */
		CacheLocalPlanForShardQuery(task, originalDistributedPlan,
									estate->es_param_list_info);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * TryToRerouteFastPathModifyQuery tries to reroute non-existent shards in given job if it finds any such shard,
 * only for fastpath queries.
 *
 * Should only be called if the job belongs to a fastpath modify query
 */
static void
TryToRerouteFastPathModifyQuery(Job *job)
{
	if (job->jobQuery->commandType == CMD_INSERT)
	{
		RegenerateTaskListForInsert(job);
	}
	else
	{
		RegenerateTaskForFasthPathQuery(job);
		RebuildQueryStrings(job);
	}
}


/*
 * EnsureAnchorShardsInJobExist ensures all shards are valid in job.
 * If it finds a non-existent shard in given job, it throws an error.
 */
static void
EnsureAnchorShardsInJobExist(Job *job)
{
	if (!AnchorShardsInTaskListExist(job->taskList))
	{
		ereport(ERROR, (errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
						errmsg("shard for the given value does not exist"),
						errdetail(
							"A concurrent shard split may have moved the data into a new set of shards."),
						errhint("Retry the query.")));
	}
}


/*
 * AnchorShardsInTaskListExist checks whether all the anchor shards in the task list
 * still exist.
 */
static bool
AnchorShardsInTaskListExist(List *taskList)
{
	Task *task = NULL;
	foreach_declared_ptr(task, taskList)
	{
		if (!ShardExists(task->anchorShardId))
		{
			return false;
		}
	}

	return true;
}


/*
 * ModifyJobNeedsEvaluation checks whether the functions and parameters in the job query
 * need to be evaluated before we can build task query strings.
 */
static bool
ModifyJobNeedsEvaluation(Job *workerJob)
{
	if (workerJob->requiresCoordinatorEvaluation)
	{
		/* query contains functions that need to be evaluated on the coordinator */
		return true;
	}

	if (workerJob->partitionKeyValue != NULL)
	{
		/* the value of the distribution column is already known */
		return false;
	}

	/* pruning was deferred due to a parameter in the partition column */
	return workerJob->deferredPruning;
}


/*
 * CopyDistributedPlanWithoutCache is a helper function which copies the
 * distributedPlan into the current memory context.
 *
 * We must not change the distributed plan since it may be reused across multiple
 * executions of a prepared statement. Instead we create a deep copy that we only
 * use for the current execution.
 *
 * We also exclude localPlannedStatements from the copyObject call for performance
 * reasons, as they are immutable, so no need to have a deep copy.
 */
static DistributedPlan *
CopyDistributedPlanWithoutCache(DistributedPlan *originalDistributedPlan)
{
	List *localPlannedStatements =
		originalDistributedPlan->workerJob->localPlannedStatements;
	originalDistributedPlan->workerJob->localPlannedStatements = NIL;

	DistributedPlan *distributedPlan = copyObject(originalDistributedPlan);

	/* set back the immutable field */
	originalDistributedPlan->workerJob->localPlannedStatements = localPlannedStatements;
	distributedPlan->workerJob->localPlannedStatements = localPlannedStatements;

	return distributedPlan;
}


/*
 * RegenerateTaskListForInsert does the shard pruning for an INSERT query
 * queries and rebuilds the query strings.
 */
static void
RegenerateTaskListForInsert(Job *workerJob)
{
	Query *jobQuery = workerJob->jobQuery;
	bool parametersInJobQueryResolved = workerJob->parametersInJobQueryResolved;
	DeferredErrorMessage *planningError = NULL;

	/* need to perform shard pruning, rebuild the task list from scratch */
	List *taskList = RouterInsertTaskList(jobQuery, parametersInJobQueryResolved,
										  &planningError);
	if (planningError != NULL)
	{
		RaiseDeferredError(planningError, ERROR);
	}

	workerJob->taskList = taskList;

	if (workerJob->partitionKeyValue == NULL)
	{
		/*
		 * If we were not able to determine the partition key value in the planner,
		 * take another shot now. It may still be NULL in case of a multi-row
		 * insert.
		 */
		workerJob->partitionKeyValue = ExtractInsertPartitionKeyValue(jobQuery);
	}

	RebuildQueryStrings(workerJob);
}


/*
 * RegenerateTaskForFasthPathQuery does the shard pruning for
 * UPDATE/DELETE/SELECT fast path router queries and rebuilds the query strings.
 */
static void
RegenerateTaskForFasthPathQuery(Job *workerJob)
{
	bool isMultiShardQuery = false;
	List *shardIntervalList =
		TargetShardIntervalForFastPathQuery(workerJob->jobQuery,
											&isMultiShardQuery, NULL,
											&workerJob->partitionKeyValue);

	/*
	 * A fast-path router query can only yield multiple shards when the parameter
	 * cannot be resolved properly, which can be triggered by SQL function.
	 */
	if (isMultiShardQuery)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning on this "
							   "query because parameterized queries for SQL "
							   "functions referencing distributed tables are "
							   "not supported"),
						errhint("Consider using PL/pgSQL functions instead.")));
	}

	bool shardsPresent = false;
	List *relationShardList =
		RelationShardListForShardIntervalList(shardIntervalList, &shardsPresent);

	UpdateRelationToShardNames((Node *) workerJob->jobQuery, relationShardList);

	/* fast path queries cannot have local tables */
	bool hasLocalRelation = false;

	List *placementList =
		CreateTaskPlacementListForShardIntervals(shardIntervalList, shardsPresent, true,
												 hasLocalRelation);
	uint64 shardId = INVALID_SHARD_ID;

	if (shardsPresent)
	{
		shardId = GetAnchorShardId(shardIntervalList);
	}

	bool isLocalTableModification = false;
	GenerateSingleShardRouterTaskList(workerJob,
									  relationShardList,
									  placementList,
									  shardId,
									  isLocalTableModification);
}


/*
 * AdaptiveExecutorCreateScan creates the scan state for the adaptive executor.
 */
static Node *
AdaptiveExecutorCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_ADAPTIVE;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->distributedPlan = GetDistributedPlan(scan);

	scanState->customScanState.methods = &AdaptiveExecutorCustomExecMethods;
	scanState->PreExecScan = &CitusPreExecScan;

	scanState->finishedPreScan = false;
	scanState->finishedRemoteScan = false;

	return (Node *) scanState;
}


/*
 * NonPushableInsertSelectCrateScan creates the scan state for executing
 * INSERT..SELECT into a distributed table via the coordinator.
 */
static Node *
NonPushableInsertSelectCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_NON_PUSHABLE_INSERT_SELECT;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->distributedPlan = GetDistributedPlan(scan);

	scanState->customScanState.methods =
		&NonPushableInsertSelectCustomExecMethods;

	scanState->finishedPreScan = false;
	scanState->finishedRemoteScan = false;

	return (Node *) scanState;
}


/*
 * DelayedErrorCreateScan is only called if we could not plan for the given
 * query. This is the case when a plan is not ready for execution because
 * CreateDistributedPlan() couldn't find a plan due to unresolved prepared
 * statement parameters, but didn't error out, because we expect custom plans
 * to come to our rescue. But sql (not plpgsql) functions unfortunately don't
 * go through a codepath supporting custom plans. Here, we error out with this
 * delayed error message.
 */
static Node *
DelayedErrorCreateScan(CustomScan *scan)
{
	DistributedPlan *distributedPlan = GetDistributedPlan(scan);

	/* raise the deferred error */
	RaiseDeferredError(distributedPlan->planningError, ERROR);

	return NULL;
}


/*
 * NonPushableMergeCommandCreateScan creates the scan state for executing
 * MERGE INTO ... into a distributed table with repartition of source rows.
 */
static Node *
NonPushableMergeCommandCreateScan(CustomScan *scan)
{
	CitusScanState *scanState = palloc0(sizeof(CitusScanState));

	scanState->executorType = MULTI_EXECUTOR_NON_PUSHABLE_MERGE_QUERY;
	scanState->customScanState.ss.ps.type = T_CustomScanState;
	scanState->distributedPlan = GetDistributedPlan(scan);
	scanState->customScanState.methods = &NonPushableMergeCommandCustomExecMethods;
	scanState->finishedPreScan = false;
	scanState->finishedRemoteScan = false;

	return (Node *) scanState;
}


/*
 * CitusEndScan is used to clean up tuple store of the given custom scan state.
 */
static void
CitusEndScan(CustomScanState *node)
{
	CitusScanState *scanState = (CitusScanState *) node;
	Job *workerJob = scanState->distributedPlan->workerJob;
	uint64 queryId = scanState->distributedPlan->queryId;
	MultiExecutorType executorType = scanState->executorType;
	Const *partitionKeyConst = NULL;
	char *partitionKeyString = NULL;

	/* stop propagating notices */
	DisableWorkerMessagePropagation();

	/*
	 * Check whether we received warnings that should not have been
	 * ignored.
	 */
	ErrorIfWorkerErrorIndicationReceived();

	if (workerJob != NULL)
	{
		partitionKeyConst = workerJob->partitionKeyValue;
	}

	/*
	 * queryId is not set if pg_stat_statements is not installed,
	 * it can be set with as of pg14: set compute_query_id to on;
	 */
	if (queryId != 0)
	{
		if (partitionKeyConst != NULL && executorType == MULTI_EXECUTOR_ADAPTIVE)
		{
			partitionKeyString = DatumToString(partitionKeyConst->constvalue,
											   partitionKeyConst->consttype);
		}

		/* queries without partition key are also recorded */
		CitusQueryStatsExecutorsEntry(queryId, executorType, partitionKeyString);
	}

	if (scanState->tuplestorestate)
	{
		tuplestore_end(scanState->tuplestorestate);
		scanState->tuplestorestate = NULL;
	}
}


/*
 * CitusReScan is not normally called, except in certain cases of
 * DECLARE .. CURSOR WITH HOLD ..
 */
static void
CitusReScan(CustomScanState *node)
{
	if (node->ss.ps.ps_ResultTupleSlot)
	{
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	}
	ExecScanReScan(&node->ss);

	CitusScanState *scanState = (CitusScanState *) node;
	if (scanState->tuplestorestate)
	{
		tuplestore_rescan(scanState->tuplestorestate);
	}
}


/*
 * ScanStateGetTupleDescriptor returns the tuple descriptor for the given
 * scan state.
 */
TupleDesc
ScanStateGetTupleDescriptor(CitusScanState *scanState)
{
	return scanState->customScanState.ss.ss_ScanTupleSlot->tts_tupleDescriptor;
}


/*
 * ScanStateGetExecutorState returns the executor state for the given scan
 * state.
 */
EState *
ScanStateGetExecutorState(CitusScanState *scanState)
{
	return scanState->customScanState.ss.ps.state;
}


/*
 * FetchCitusCustomScanIfExists traverses a given plan and returns a Citus CustomScan
 * if it has any.
 */
CustomScan *
FetchCitusCustomScanIfExists(Plan *plan)
{
	if (plan == NULL)
	{
		return NULL;
	}

	if (IsCitusCustomScan(plan))
	{
		return (CustomScan *) plan;
	}

	CustomScan *customScan = FetchCitusCustomScanIfExists(plan->lefttree);

	if (customScan == NULL)
	{
		customScan = FetchCitusCustomScanIfExists(plan->righttree);
	}

	return customScan;
}


/*
 * IsCitusPlan returns whether a Plan contains a CustomScan generated by Citus
 * by recursively walking through the plan tree.
 */
bool
IsCitusPlan(Plan *plan)
{
	if (plan == NULL)
	{
		return false;
	}

	if (IsCitusCustomScan(plan))
	{
		return true;
	}

	return IsCitusPlan(plan->lefttree) || IsCitusPlan(plan->righttree);
}


/*
 * IsCitusCustomScan returns whether Plan node is a CustomScan generated by Citus.
 */
bool
IsCitusCustomScan(Plan *plan)
{
	if (plan == NULL)
	{
		return false;
	}

	if (!IsA(plan, CustomScan))
	{
		return false;
	}

	CustomScan *customScan = (CustomScan *) plan;
	if (list_length(customScan->custom_private) == 0)
	{
		return false;
	}

	Node *privateNode = (Node *) linitial(customScan->custom_private);
	if (!CitusIsA(privateNode, DistributedPlan))
	{
		return false;
	}

	return true;
}


/*
 * In a Job, given a list of relations, if all them belong to the same
 * colocation group, the Job's colocation ID is set to the group ID, else,
 * it will be set to INVALID_COLOCATION_ID.
 */
void
SetJobColocationId(Job *job)
{
	uint32 jobColocationId = INVALID_COLOCATION_ID;

	List *rangeTableList = ExtractRangeTableEntryList(job->jobQuery);
	ListCell *rangeTableCell = NULL;
	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		Oid relationId = rangeTableEntry->relid;

		if (!IsCitusTable(relationId))
		{
			/* ignore the non distributed table */
			continue;
		}

		uint32 colocationId = TableColocationId(relationId);

		if (jobColocationId == INVALID_COLOCATION_ID)
		{
			/* Initialize the ID */
			jobColocationId = colocationId;
		}
		else if (jobColocationId != colocationId)
		{
			/* Tables' colocationId is not the same */
			jobColocationId = INVALID_COLOCATION_ID;
			break;
		}
	}

	job->colocationId = jobColocationId;
}


/*
 * Any function with force_delegate flag(true) must ensure that the Job's
 * partition key match with the functions' distribution argument.
 */
static void
EnsureForceDelegationDistributionKey(Job *job)
{
	/* If the Job has the subquery, punt the shard-key-check to the subquery */
	if (job->subqueryPushdown)
	{
		return;
	}

	/*
	 * If the query doesn't have shard key, nothing to check, only exception is when
	 * the query doesn't have distributed tables but an RTE with intermediate_results
	 * function (a subquery plan).
	 */
	if (!job->partitionKeyValue)
	{
		bool queryContainsDistributedTable =
			FindNodeMatchingCheckFunction((Node *) job->jobQuery, IsDistributedTableRTE);

		if (!queryContainsDistributedTable)
		{
			return;
		}
	}

	/* We should match both the key and the colocation ID */
	SetJobColocationId(job);

	if (!IsShardKeyValueAllowed(job->partitionKeyValue, job->colocationId))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg(
							"queries must filter by the distribution argument in the same "
							"colocation group when using the forced function pushdown"),
						errhint(
							"consider disabling forced delegation through "
							"create_distributed_table(..., force_delegation := false)")));
	}
}
