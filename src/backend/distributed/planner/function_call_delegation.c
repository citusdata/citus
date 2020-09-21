/*-------------------------------------------------------------------------
 *
 * function_call_delegation.c
 *    Planning logic for delegating a function call to a worker when the
 *    function was distributed with a distribution argument and the worker
 *    has metadata.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/function_call_delegation.h"
#include "distributed/insert_select_planner.h"
#include "distributed/metadata_utility.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_pruning.h"
#include "distributed/recursive_planning.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "parser/parse_coerce.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "parser/parsetree.h"
#endif
#include "miscadmin.h"
#include "tcop/dest.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

struct ParamWalkerContext
{
	bool hasParam;
	ParamKind paramKind;
};

static bool contain_param_walker(Node *node, void *context);


/*
 * contain_param_walker scans node for Param nodes.
 * Ignore the return value, instead check context afterwards.
 *
 * context is a struct ParamWalkerContext*.
 * hasParam is set to true if we find a Param node.
 * paramKind is set to the paramkind of the Param node if any found.
 * paramKind is set to PARAM_EXEC if both PARAM_EXEC & PARAM_EXTERN are found.
 *
 * By time we walk, Param nodes are either PARAM_EXTERN or PARAM_EXEC.
 */
static bool
contain_param_walker(Node *node, void *context)
{
	if (IsA(node, Param))
	{
		Param *paramNode = (Param *) node;
		struct ParamWalkerContext *pwcontext =
			(struct ParamWalkerContext *) context;

		pwcontext->hasParam = true;
		pwcontext->paramKind = paramNode->paramkind;
		if (paramNode->paramkind == PARAM_EXEC)
		{
			return true;
		}
	}

	return false;
}


/*
 * TryToDelegateFunctionCall calls a function on the worker if possible.
 * We only support delegating the SELECT func(...) form for distributed
 * functions colocated by distributed tables, and not more complicated
 * forms involving multiple function calls, FROM clauses, WHERE clauses,
 * ... Those complex forms are handled in the coordinator.
 */
PlannedStmt *
TryToDelegateFunctionCall(DistributedPlanningContext *planContext)
{
	List *targetList = NIL;
	TargetEntry *targetEntry = NULL;
	FuncExpr *funcExpr = NULL;
	DistObjectCacheEntry *procedure = NULL;
	Oid colocatedRelationId = InvalidOid;
	bool colocatedWithReferenceTable = false;
	CitusTableCacheEntry *distTable = NULL;
	Var *partitionColumn = NULL;
	ShardPlacement *placement = NULL;
	WorkerNode *workerNode = NULL;
	Task *task = NULL;
	Job *job = NULL;
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	struct ParamWalkerContext walkerParamContext = { 0 };

	if (!CitusHasBeenLoaded() || !CheckCitusVersion(DEBUG4))
	{
		/* Citus is not ready to determine whether function is distributed */
		return NULL;
	}

	int32 localGroupId = GetLocalGroupId();
	if (localGroupId != COORDINATOR_GROUP_ID || localGroupId == GROUP_ID_UPGRADING)
	{
		/* do not delegate from workers, or while upgrading */
		return NULL;
	}

	if (planContext->query == NULL)
	{
		/* no query (mostly here to be defensive) */
		return NULL;
	}

	if (planContext->query->commandType != CMD_SELECT)
	{
		/* not a SELECT */
		return NULL;
	}

	FromExpr *joinTree = planContext->query->jointree;
	if (joinTree == NULL)
	{
		/* no join tree (mostly here to be defensive) */
		return NULL;
	}

	if (joinTree->quals != NULL)
	{
		/* query has a WHERE section */
		return NULL;
	}

	if (joinTree->fromlist != NIL)
	{
#if PG_VERSION_NUM >= PG_VERSION_12

		/*
		 * In pg12's planning phase empty FROMs are represented with an RTE_RESULT.
		 * When we arrive here, standard_planner has already been called which calls
		 * replace_empty_jointree() which replaces empty fromlist with a list of
		 * single RTE_RESULT RangleTableRef node.
		 */
		if (list_length(joinTree->fromlist) == 1)
		{
			RangeTblRef *reference = linitial(joinTree->fromlist);

			if (IsA(reference, RangeTblRef))
			{
				RangeTblEntry *rtentry = rt_fetch(reference->rtindex,
												  planContext->query->rtable);
				if (rtentry->rtekind != RTE_RESULT)
				{
					/* e.g. SELECT f() FROM rel */
					return NULL;
				}
			}
			else
			{
				/*
				 * e.g. IsA(reference, JoinExpr). This is explicit join expressions
				 * like INNER JOIN, NATURAL JOIN, ...
				 */
				return NULL;
			}
		}
		else
		{
			/* e.g. SELECT ... FROM rel1, rel2. */
			Assert(list_length(joinTree->fromlist) > 1);
			return NULL;
		}
#else

		/* query has a FROM section */
		return NULL;
#endif
	}

	targetList = planContext->query->targetList;
	if (list_length(planContext->query->targetList) != 1)
	{
		/* multiple target list items */
		return NULL;
	}

	targetEntry = (TargetEntry *) linitial(targetList);
	if (!IsA(targetEntry->expr, FuncExpr))
	{
		/* target list item is not a function call */
		return NULL;
	}

	funcExpr = (FuncExpr *) targetEntry->expr;
	procedure = LookupDistObjectCacheEntry(ProcedureRelationId, funcExpr->funcid, 0);
	if (procedure == NULL || !procedure->isDistributed)
	{
		/* not a distributed function call */
		return NULL;
	}
	else
	{
		ereport(DEBUG4, (errmsg("function is distributed")));
	}

	/*
	 * Cannot delegate functions for INSERT ... SELECT func(), since they require
	 * coordinated transactions.
	 */
	if (PlanningInsertSelect())
	{
		ereport(DEBUG1, (errmsg("not pushing down function calls in INSERT ... SELECT")));
		return NULL;
	}

	if (IsMultiStatementTransaction())
	{
		/* cannot delegate function calls in a multi-statement transaction */
		ereport(DEBUG1, (errmsg("not pushing down function calls in "
								"a multi-statement transaction")));
		return NULL;
	}

	if (contain_volatile_functions((Node *) funcExpr->args))
	{
		ereport(DEBUG1, (errmsg("arguments in a distributed function must "
								"be constant expressions")));
		return NULL;
	}

	colocatedRelationId = ColocatedTableId(procedure->colocationId);
	if (colocatedRelationId == InvalidOid)
	{
		ereport(DEBUG1, (errmsg("function does not have co-located tables")));
		return NULL;
	}

	distTable = GetCitusTableCacheEntry(colocatedRelationId);
	partitionColumn = distTable->partitionColumn;
	if (partitionColumn == NULL)
	{
		colocatedWithReferenceTable = true;
	}

	/*
	 * This can be called in queries like SELECT ... WHERE EXISTS(SELECT func()), or other
	 * forms of CTEs or subqueries. We don't push-down in those cases.
	 */
	if (GeneratingSubplans())
	{
		ereport(DEBUG1, (errmsg(
							 "not pushing down function calls in CTEs or Subqueries")));
		return NULL;
	}

	if (colocatedWithReferenceTable)
	{
		placement = ShardPlacementForFunctionColocatedWithReferenceTable(distTable);
	}
	else
	{
		placement = ShardPlacementForFunctionColocatedWithDistTable(procedure, funcExpr,
																	partitionColumn,
																	distTable,
																	planContext->plan);
	}

	/* return if we could not find a placement */
	if (placement == NULL)
	{
		return false;
	}

	workerNode = FindWorkerNode(placement->nodeName, placement->nodePort);

	if (workerNode == NULL || !workerNode->hasMetadata || !workerNode->metadataSynced)
	{
		ereport(DEBUG1, (errmsg("the worker node does not have metadata")));
		return NULL;
	}
	else if (workerNode->groupId == GetLocalGroupId())
	{
		/*
		 * Two reasons for this:
		 *  (a) It would lead to infinite recursion as the node would
		 *      keep pushing down the procedure as it gets
		 *  (b) It doesn't have any value to pushdown as we are already
		 *      on the node itself
		 */
		ereport(DEBUG1, (errmsg("not pushing down function to the same node")));
		return NULL;
	}

	(void) expression_tree_walker((Node *) funcExpr->args, contain_param_walker,
								  &walkerParamContext);
	if (walkerParamContext.hasParam)
	{
		if (walkerParamContext.paramKind == PARAM_EXTERN)
		{
			/* Don't log a message, we should end up here again without a parameter */
			DissuadePlannerFromUsingPlan(planContext->plan);
		}
		else
		{
			ereport(DEBUG1, (errmsg("arguments in a distributed function must "
									"not contain subqueries")));
		}
		return NULL;
	}

	ereport(DEBUG1, (errmsg("pushing down the function call")));

	task = CitusMakeNode(Task);
	task->taskType = READ_TASK;
	task->taskPlacementList = list_make1(placement);
	SetTaskQueryIfShouldLazyDeparse(task, planContext->query);
	task->anchorShardId = placement->shardId;
	task->replicationModel = distTable->replicationModel;

	job = CitusMakeNode(Job);
	job->jobId = UniqueJobId();
	job->jobQuery = planContext->query;
	job->taskList = list_make1(task);

	distributedPlan = CitusMakeNode(DistributedPlan);
	distributedPlan->workerJob = job;
	distributedPlan->combineQuery = NULL;
	distributedPlan->expectResults = true;

	/* worker will take care of any necessary locking, treat query as read-only */
	distributedPlan->modLevel = ROW_MODIFY_READONLY;

	return FinalizePlan(planContext->plan, distributedPlan);
}


/*
 * ShardPlacementForFunctionColocatedWithDistTable decides on a placement
 * for delegating a procedure call that accesses a distributed table.
 */
ShardPlacement *
ShardPlacementForFunctionColocatedWithDistTable(DistObjectCacheEntry *procedure,
												FuncExpr *funcExpr,
												Var *partitionColumn,
												CitusTableCacheEntry *cacheEntry,
												PlannedStmt *plan)
{
	if (procedure->distributionArgIndex < 0 ||
		procedure->distributionArgIndex >= list_length(funcExpr->args))
	{
		ereport(DEBUG1, (errmsg("cannot push down invalid distribution_argument_index")));
		return NULL;
	}

	Node *partitionValueNode = (Node *) list_nth(funcExpr->args,
												 procedure->distributionArgIndex);
	partitionValueNode = strip_implicit_coercions(partitionValueNode);

	if (IsA(partitionValueNode, Param))
	{
		Param *partitionParam = (Param *) partitionValueNode;

		if (partitionParam->paramkind == PARAM_EXTERN)
		{
			/* Don't log a message, we should end up here again without a parameter */
			DissuadePlannerFromUsingPlan(plan);
			return NULL;
		}
	}

	if (!IsA(partitionValueNode, Const))
	{
		ereport(DEBUG1, (errmsg("distribution argument value must be a constant")));
		return NULL;
	}

	Const *partitionValue = (Const *) partitionValueNode;

	if (partitionValue->consttype != partitionColumn->vartype)
	{
		bool missingOk = false;
		partitionValue =
			TransformPartitionRestrictionValue(partitionColumn, partitionValue,
											   missingOk);
	}

	Datum partitionValueDatum = partitionValue->constvalue;
	ShardInterval *shardInterval = FindShardInterval(partitionValueDatum, cacheEntry);
	if (shardInterval == NULL)
	{
		ereport(DEBUG1, (errmsg("cannot push down call, failed to find shard interval")));
		return NULL;
	}

	List *placementList = ActiveShardPlacementList(shardInterval->shardId);
	if (list_length(placementList) != 1)
	{
		/* punt on this for now */
		ereport(DEBUG1, (errmsg(
							 "cannot push down function call for replicated distributed tables")));
		return NULL;
	}

	return linitial(placementList);
}


/*
 * ShardPlacementForFunctionColocatedWithReferenceTable decides on a placement for delegating
 * a function call that reads from a reference table.
 *
 * If citus.task_assignment_policy is set to round-robin, we assign a different placement
 * on consecutive runs. Otherwise the function returns the first placement available.
 */
ShardPlacement *
ShardPlacementForFunctionColocatedWithReferenceTable(CitusTableCacheEntry *cacheEntry)
{
	const ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[0];
	const uint64 referenceTableShardId = shardInterval->shardId;

	/* Get the list of active shard placements ordered by the groupid */
	List *placementList = ActiveShardPlacementList(referenceTableShardId);
	placementList = SortList(placementList, CompareShardPlacementsByGroupId);

	/* do not try to delegate to coordinator even if it is in metadata */
	placementList = RemoveCoordinatorPlacementIfNotSingleNode(placementList);

	if (TaskAssignmentPolicy == TASK_ASSIGNMENT_ROUND_ROBIN)
	{
		/* reorder the placement list */
		placementList = RoundRobinReorder(placementList);
	}

	return (ShardPlacement *) linitial(placementList);
}
