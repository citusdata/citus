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
#include "distributed/insert_select_executor.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
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
	Const *partitionValue = NULL;
	Datum partitionValueDatum = 0;
	ShardInterval *shardInterval = NULL;
	List *placementList = NIL;
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
	 * This can be called while executing INSERT ... SELECT func(). insert_select_executor
	 * doesn't get the planned subquery and gets the actual struct Query, so the planning
	 * for these kinds of queries happens at the execution time.
	 */
	if (ExecutingInsertSelect())
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

	if (procedure->distributionArgIndex < 0 ||
		procedure->distributionArgIndex >= list_length(funcExpr->args))
	{
		ereport(DEBUG1, (errmsg("function call does not have a distribution argument")));
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
		/* This can happen if colocated with a reference table. Punt for now. */
		ereport(DEBUG1, (errmsg(
							 "cannnot push down function call for reference tables")));
		return NULL;
	}

	partitionValue = (Const *) list_nth(funcExpr->args, procedure->distributionArgIndex);

	if (IsA(partitionValue, Param))
	{
		Param *partitionParam = (Param *) partitionValue;

		if (partitionParam->paramkind == PARAM_EXTERN)
		{
			/* Don't log a message, we should end up here again without a parameter */
			DissuadePlannerFromUsingPlan(planContext->plan);
			return NULL;
		}
	}

	if (!IsA(partitionValue, Const))
	{
		ereport(DEBUG1, (errmsg("distribution argument value must be a constant")));
		return NULL;
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

	partitionValueDatum = partitionValue->constvalue;

	if (partitionValue->consttype != partitionColumn->vartype)
	{
		CopyCoercionData coercionData;

		ConversionPathForTypes(partitionValue->consttype, partitionColumn->vartype,
							   &coercionData);

		partitionValueDatum = CoerceColumnValue(partitionValueDatum, &coercionData);
	}

	shardInterval = FindShardInterval(partitionValueDatum, distTable);
	if (shardInterval == NULL)
	{
		ereport(DEBUG1, (errmsg("cannot push down call, failed to find shard interval")));
		return NULL;
	}

	placementList = ActiveShardPlacementList(shardInterval->shardId);
	if (list_length(placementList) != 1)
	{
		/* punt on this for now */
		ereport(DEBUG1, (errmsg(
							 "cannot push down function call for replicated distributed tables")));
		return NULL;
	}

	placement = (ShardPlacement *) linitial(placementList);
	workerNode = FindWorkerNode(placement->nodeName, placement->nodePort);

	if (workerNode == NULL || !workerNode->hasMetadata || !workerNode->metadataSynced)
	{
		ereport(DEBUG1, (errmsg("the worker node does not have metadata")));
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
	task->taskPlacementList = placementList;
	SetTaskQueryIfShouldLazyDeparse(task, planContext->query);
	task->anchorShardId = shardInterval->shardId;
	task->replicationModel = distTable->replicationModel;

	job = CitusMakeNode(Job);
	job->jobId = UniqueJobId();
	job->jobQuery = planContext->query;
	job->taskList = list_make1(task);

	distributedPlan = CitusMakeNode(DistributedPlan);
	distributedPlan->workerJob = job;
	distributedPlan->masterQuery = NULL;
	distributedPlan->routerExecutable = true;
	distributedPlan->expectResults = true;

	/* worker will take care of any necessary locking, treat query as read-only */
	distributedPlan->modLevel = ROW_MODIFY_READONLY;

	return FinalizePlan(planContext->plan, distributedPlan);
}
