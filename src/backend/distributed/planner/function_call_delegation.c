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

#include "miscadmin.h"

#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "parser/parse_coerce.h"
#include "parser/parsetree.h"
#include "tcop/dest.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "pg_version_constants.h"

#include "distributed/backend_data.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/function_call_delegation.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_pruning.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"

struct ParamWalkerContext
{
	bool hasParam;
	ParamKind paramKind;
};

extern AllowedDistributionColumn AllowedDistributionColumnValue;

static bool contain_param_walker(Node *node, void *context);
static void CheckDelegatedFunctionExecution(DistObjectCacheEntry *procedure,
											FuncExpr *funcExpr);
static bool IsQuerySimple(Query *query);
static FuncExpr * FunctionInFromClause(List *fromlist, Query *query);
static void EnableInForceDelegatedFuncExecution(Const *distArgument, uint32 colocationId);


/* global variable keeping track of whether we are in a delegated function call */
bool InTopLevelDelegatedFunctionCall = false;


/* global variable keeping track of whether we are in a delegated function call */
bool InDelegatedFunctionCall = false;


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
	if (node == NULL)
	{
		return false;
	}
	if (IsA(node, Param))
	{
		Param *paramNode = (Param *) node;
		struct ParamWalkerContext *pwcontext =
			(struct ParamWalkerContext *) context;

		pwcontext->hasParam = true;
		pwcontext->paramKind = paramNode->paramkind;
		return paramNode->paramkind == PARAM_EXEC;
	}
	else
	{
		return expression_tree_walker((Node *) node, contain_param_walker, context);
	}
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
	ShardPlacement *placement = NULL;
	struct ParamWalkerContext walkerParamContext = { 0 };
	bool inTransactionBlock = false;

	if (!CitusHasBeenLoaded() || !CheckCitusVersion(DEBUG4))
	{
		/* Citus is not ready to determine whether function is distributed */
		return NULL;
	}

	int32 localGroupId = GetLocalGroupId();
	if (localGroupId == GROUP_ID_UPGRADING)
	{
		/* do not delegate while upgrading */
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

	FuncExpr *fromFuncExpr = NULL;
	if (joinTree->fromlist != NIL)
	{
		if (list_length(joinTree->fromlist) != 1)
		{
			/* e.g. SELECT ... FROM rel1, rel2. */
			Assert(list_length(joinTree->fromlist) > 1);
			return NULL;
		}

		/*
		 * In the planning phase empty FROMs are represented with an RTE_RESULT.
		 * When we arrive here, standard_planner has already been called which calls
		 * replace_empty_jointree() which replaces empty fromlist with a list of
		 * single RTE_RESULT RangleTableRef node.
		 */
		RangeTblRef *reference = linitial(joinTree->fromlist);

		if (IsA(reference, RangeTblRef))
		{
			RangeTblEntry *rtentry = rt_fetch(reference->rtindex,
											  planContext->query->rtable);

			if (rtentry->rtekind == RTE_FUNCTION)
			{
				/*
				 * Look for a function in the FROM clause.
				 */
				fromFuncExpr = FunctionInFromClause(joinTree->fromlist,
													planContext->query);
			}
			else if (rtentry->rtekind != RTE_RESULT)
			{
				/* e.g. SELECT f() FROM rel */
				ereport(DEBUG4, (errmsg("FromList item is not empty")));
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

	FuncExpr *targetFuncExpr = NULL;
	List *targetList = planContext->query->targetList;
	int targetListLen = list_length(targetList);

	if (targetListLen == 1)
	{
		TargetEntry *targetEntry = (TargetEntry *) linitial(targetList);
		if (IsA(targetEntry->expr, FuncExpr))
		{
			/* function from the SELECT clause e.g. SELECT fn() FROM  */
			targetFuncExpr = (FuncExpr *) targetEntry->expr;
		}
	}

	/*
	 * Look for one of:
	 * SELECT fn(...);
	 * SELECT ... FROM fn(...);
	 */
	FuncExpr *funcExpr = NULL;
	if (targetFuncExpr != NULL)
	{
		if (fromFuncExpr != NULL)
		{
			/* query is of the form: SELECT fn() FROM fn() */
			return NULL;
		}

		/* query is of the form: SELECT fn(); */
		funcExpr = targetFuncExpr;
	}
	else if (fromFuncExpr != NULL)
	{
		/* query is of the form: SELECT ... FROM fn(); */
		funcExpr = fromFuncExpr;
	}
	else
	{
		/* query does not have a function call in SELECT or FROM */
		return NULL;
	}

	DistObjectCacheEntry *procedure = LookupDistObjectCacheEntry(ProcedureRelationId,
																 funcExpr->funcid, 0);
	if (procedure == NULL || !procedure->isDistributed)
	{
		/* not a distributed function call */
		return NULL;
	}
	else
	{
		ereport(DEBUG4, (errmsg("function is distributed")));
	}

	if (IsCitusInternalBackend())
	{
		bool isFunctionForceDelegated = procedure->forceDelegation;

		/*
		 * We are planning a call to a distributed function within a Citus backend,
		 * that means that this is the delegated call. If the function is forcefully
		 * delegated, capture the distribution argument.
		 */
		if (isFunctionForceDelegated)
		{
			CheckDelegatedFunctionExecution(procedure, funcExpr);
		}

		/* Are we planning the top function call? */
		if (ExecutorLevel == 0 && PlannerLevel == 1)
		{
			/*
			 * InTopLevelDelegatedFunctionCall flag grants the levy
			 * to do remote tasks from a delegated function.
			 */
			if (!isFunctionForceDelegated)
			{
				/*
				 * we are planning a regular delegated call, we
				 * are allowed to do remote execution.
				 */
				InTopLevelDelegatedFunctionCall = true;
			}
			else if (!IsMultiStatementTransaction())
			{
				/*
				 * we are planning a force-delegated call, we
				 * are allowed to do remote execution if there
				 * is no explicit BEGIN-END transaction.
				 */
				InTopLevelDelegatedFunctionCall = true;
			}
		}

		return NULL;
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

	/* dissuade the planner from trying a generic plan with parameters */
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

	if (IsMultiStatementTransaction())
	{
		if (!procedure->forceDelegation)
		{
			/* cannot delegate function calls in a multi-statement transaction */
			ereport(DEBUG4, (errmsg("not pushing down function calls in "
									"a multi-statement transaction")));
			return NULL;
		}
		else
		{
			Node *partitionValueNode = (Node *) list_nth(funcExpr->args,
														 procedure->distributionArgIndex);

			if (!IsA(partitionValueNode, Const))
			{
				ereport(DEBUG1, (errmsg("distribution argument value must be a "
										"constant when using force_delegation flag")));
				return NULL;
			}

			/*
			 * If the expression is simple, such as, SELECT function() or PEFORM function()
			 * in PL/PgSQL code, PL engine does a simple expression evaluation which can't
			 * interpret the Citus CustomScan Node.
			 * Note: Function from FROM clause is not simple, so it's ok to pushdown.
			 */
			if ((MaybeExecutingUDF() || DoBlockLevel > 0) &&
				IsQuerySimple(planContext->query) &&
				!fromFuncExpr)
			{
				ereport(DEBUG1, (errmsg("Skipping pushdown of function "
										"from a PL/PgSQL simple expression")));
				return NULL;
			}

			/*
			 * When is flag is on, delegate the function call in a multi-statement
			 * transaction but with restrictions.
			 */
			ereport(DEBUG1, (errmsg("pushing down function call in "
									"a multi-statement transaction")));
			inTransactionBlock = true;
		}
	}

	if (contain_volatile_functions((Node *) funcExpr->args))
	{
		ereport(DEBUG1, (errmsg("arguments in a distributed function must "
								"be constant expressions")));
		return NULL;
	}

	Oid colocatedRelationId = ColocatedTableId(procedure->colocationId);
	if (colocatedRelationId == InvalidOid)
	{
		ereport(DEBUG4, (errmsg("function does not have co-located tables")));
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

	CitusTableCacheEntry *distTable = GetCitusTableCacheEntry(colocatedRelationId);
	if (IsCitusTableType(colocatedRelationId, REFERENCE_TABLE))
	{
		placement = ShardPlacementForFunctionColocatedWithReferenceTable(distTable);
	}
	else if (IsCitusTableType(colocatedRelationId, SINGLE_SHARD_DISTRIBUTED))
	{
		placement = ShardPlacementForFunctionColocatedWithSingleShardTable(distTable);
	}
	else
	{
		placement = ShardPlacementForFunctionColocatedWithDistTable(procedure,
																	funcExpr->args,
																	distTable->
																	partitionColumn,
																	distTable,
																	planContext->plan);
	}

	/* return if we could not find a placement */
	if (placement == NULL)
	{
		return NULL;
	}

	WorkerNode *workerNode = FindWorkerNode(placement->nodeName, placement->nodePort);

	if (workerNode == NULL || !workerNode->hasMetadata || !workerNode->metadataSynced)
	{
		ereport(DEBUG1, (errmsg("the worker node does not have metadata")));
		return NULL;
	}
	else if (workerNode->groupId == GetLocalGroupId())
	{
		/* If the force_pushdown flag is set, capture the distribution argument */
		if (procedure->forceDelegation)
		{
			CheckDelegatedFunctionExecution(procedure, funcExpr);
		}

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

	ereport(DEBUG1, (errmsg("pushing down the function call")));

	Task *task = CitusMakeNode(Task);

	/*
	 * In a multi-statement block the function should be part of the sorrounding
	 * transaction, at this time, not knowing the operations in the function, it
	 * is safe to assume that it's a write task.
	 *
	 * TODO: We should compile the function to see the internals of the function
	 * and find if this has read-only tasks, does it involve doing a remote task
	 * or queries involving non-distribution column, etc.
	 */
	if (inTransactionBlock)
	{
		task->taskType = MODIFY_TASK;
	}
	else
	{
		task->taskType = READ_TASK;
	}

	task->taskPlacementList = list_make1(placement);
	SetTaskQueryIfShouldLazyDeparse(task, planContext->query);
	task->anchorShardId = placement->shardId;
	task->replicationModel = distTable->replicationModel;

	Job *job = CitusMakeNode(Job);
	job->jobId = UniqueJobId();
	job->jobQuery = planContext->query;
	job->taskList = list_make1(task);

	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
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
												List *argumentList,
												Var *partitionColumn,
												CitusTableCacheEntry *cacheEntry,
												PlannedStmt *plan)
{
	if (procedure->distributionArgIndex < 0 ||
		procedure->distributionArgIndex >= list_length(argumentList))
	{
		ereport(DEBUG1, (errmsg("cannot push down invalid distribution_argument_index")));
		return NULL;
	}

	Node *partitionValueNode = (Node *) list_nth(argumentList,
												 procedure->distributionArgIndex);
	partitionValueNode = strip_implicit_coercions(partitionValueNode);

	if (IsA(partitionValueNode, Param))
	{
		Param *partitionParam = (Param *) partitionValueNode;

		if (partitionParam->paramkind == PARAM_EXTERN)
		{
			/*
			 * Don't log a message, we should end up here again without a
			 * parameter.
			 * Note that "plan" can be null, for example when a CALL statement
			 * is prepared.
			 */
			if (plan)
			{
				DissuadePlannerFromUsingPlan(plan);
			}
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
 * ShardPlacementForFunctionColocatedWithSingleShardTable decides on a placement
 * for delegating a function call that reads from a single shard table.
 */
ShardPlacement *
ShardPlacementForFunctionColocatedWithSingleShardTable(CitusTableCacheEntry *cacheEntry)
{
	const ShardInterval *shardInterval = cacheEntry->sortedShardIntervalArray[0];

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

	return (ShardPlacement *) linitial(placementList);
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


/*
 * Checks to see if the procedure is being executed on a worker after delegated
 * by the coordinator. If the flag forceDelegation is set, capture the distribution
 * argument value, to be used by the planner to make sure that function uses only
 * the colocated shards of the distribution argument.
 */
void
CheckDelegatedFunctionExecution(DistObjectCacheEntry *procedure, FuncExpr *funcExpr)
{
	Assert(procedure->forceDelegation);

	/*
	 * On the coordinator PartiallyEvaluateExpression() descends into an
	 * expression tree to evaluate expressions that can be resolved to a
	 * constant. Expressions containing a Var are skipped, since the value
	 * of the Var is not known on the coordinator.
	 */
	Node *partitionValueNode = (Node *) list_nth(funcExpr->args,
												 procedure->distributionArgIndex);
	Assert(partitionValueNode);
	partitionValueNode = strip_implicit_coercions(partitionValueNode);

	if (IsA(partitionValueNode, Param))
	{
		Param *partitionParam = (Param *) partitionValueNode;

		if (partitionParam->paramkind == PARAM_EXTERN)
		{
			/* we should end up here again without a parameter */
			return;
		}
	}

	if (IsA(partitionValueNode, Const))
	{
		Const *partitionValueConst = (Const *) partitionValueNode;

		ereport(DEBUG1, (errmsg("Pushdown argument: %s", pretty_format_node_dump(
									nodeToString(partitionValueNode)))));
		EnableInForceDelegatedFuncExecution(partitionValueConst, procedure->colocationId);
	}
}


/*
 * Function returns true if the query is simple enough to skip the full executor
 * It checks only for expressions in the query clauses, and not WHERE and FROM
 * lists.
 */
static bool
IsQuerySimple(Query *query)
{
	if (query->hasAggs ||
		query->hasWindowFuncs ||
		query->hasTargetSRFs ||
		query->hasSubLinks ||
		query->cteList ||
		query->groupClause ||
		query->groupingSets ||
		query->havingQual ||
		query->windowClause ||
		query->distinctClause ||
		query->sortClause ||
		query->limitOffset ||
		query->limitCount ||
		query->setOperations)
	{
		return false;
	}

	return true;
}


/*
 * Look for a function in the FROM clause.
 */
static FuncExpr *
FunctionInFromClause(List *fromlist, Query *query)
{
	if (list_length(fromlist) != 1)
	{
		/* We are looking for a single function */
		return NULL;
	}

	RangeTblRef *reference = linitial(fromlist);
	if (!IsA(reference, RangeTblRef))
	{
		/* Skip if there is no RTE */
		return NULL;
	}

	RangeTblEntry *rtentry = rt_fetch(reference->rtindex, query->rtable);
	if (rtentry->rtekind != RTE_FUNCTION)
	{
		return NULL;
	}

	if (list_length(rtentry->functions) != 1)
	{
		/* Skip if RTE isn't a single FuncExpr */
		return NULL;
	}

	RangeTblFunction *rtfunc = (RangeTblFunction *) linitial(rtentry->functions);
	if (!IsA(rtfunc->funcexpr, FuncExpr))
	{
		/* Skip if RTE isn't a simple FuncExpr */
		return NULL;
	}

	return (FuncExpr *) rtfunc->funcexpr;
}


/*
 * Sets a flag to true indicating that the current node is executing a delegated
 * function call, using forceDelegation, within a distributed transaction issued
 * by the coordinator. Also, saves the distribution argument.
 */
static void
EnableInForceDelegatedFuncExecution(Const *distArgument, uint32 colocationId)
{
	/*
	 * If the distribution key is already set, the key is fixed until
	 * the force-delegation function returns. All nested force-delegation
	 * functions must use the same key.
	 */
	if (AllowedDistributionColumnValue.isActive)
	{
		return;
	}

	/*
	 * The saved distribution argument need to persist through the life
	 * of the query, both during the planning (where we save) and execution
	 * (where we compare)
	 */
	MemoryContext oldcontext = MemoryContextSwitchTo(TopTransactionContext);
	ereport(DEBUG1, errmsg("Saving Distribution Argument: %s:%d",
						   pretty_format_node_dump(nodeToString(distArgument)),
						   colocationId));
	AllowedDistributionColumnValue.distributionColumnValue = copyObject(distArgument);
	AllowedDistributionColumnValue.colocationId = colocationId;
	AllowedDistributionColumnValue.executorLevel = ExecutorLevel;
	AllowedDistributionColumnValue.isActive = true;
	MemoryContextSwitchTo(oldcontext);
}


/*
 * Within a 2PC, when a function is delegated to a remote node, we pin
 * the distribution argument as the shard key for all the SQL in the
 * function's block. The restriction is imposed to not to access other
 * nodes from the current node and violate the transactional integrity of
 * the 2PC. Reset the distribution argument value once the function ends.
 */
void
CheckAndResetAllowedShardKeyValueIfNeeded(void)
{
	/*
	 * If no distribution argument is pinned or the pinned argument was
	 * set by a nested-executor from upper level, nothing to reset.
	 */
	if (!AllowedDistributionColumnValue.isActive ||
		ExecutorLevel > AllowedDistributionColumnValue.executorLevel)
	{
		return;
	}

	Assert(ExecutorLevel == AllowedDistributionColumnValue.executorLevel);
	pfree(AllowedDistributionColumnValue.distributionColumnValue);
	AllowedDistributionColumnValue.isActive = false;
	AllowedDistributionColumnValue.executorLevel = 0;
}


/*
 * Function returns true if the current shard key in the adaptive executor
 * matches the saved distribution argument of a force_delegation function.
 */
bool
IsShardKeyValueAllowed(Const *shardKey, uint32 colocationId)
{
	Assert(AllowedDistributionColumnValue.isActive);
	Assert(ExecutorLevel > AllowedDistributionColumnValue.executorLevel);

	ereport(DEBUG4, errmsg("Comparing saved:%s with Shard key: %s colocationid:%d:%d",
						   pretty_format_node_dump(
							   nodeToString(
								   AllowedDistributionColumnValue.distributionColumnValue)),
						   pretty_format_node_dump(nodeToString(shardKey)),
						   AllowedDistributionColumnValue.colocationId, colocationId));

	return (equal(AllowedDistributionColumnValue.distributionColumnValue, shardKey) &&
			(AllowedDistributionColumnValue.colocationId == colocationId));
}
