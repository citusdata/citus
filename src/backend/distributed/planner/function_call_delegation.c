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

#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/function_call_delegation.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_pruning.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "parser/parse_coerce.h"
#if PG_VERSION_NUM >= 120000
#include "parser/parsetree.h"
#endif
#include "miscadmin.h"
#include "tcop/dest.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static bool contain_param_walker(Node *node, void *context);

/*
 * contain_param_walker scans node for Param nodes.
 * returns whether any such nodes found.
 */
static bool
contain_param_walker(Node *node, void *context)
{
	return IsA(node, Param);
}


/*
 * CallDistributedProcedureRemotely calls a stored procedure on the worker if possible.
 */
DistributedPlan *
TryToDelegateFunctionCall(Query *query)
{
	FromExpr *joinTree = NULL;
	List *targetList = NIL;
	TargetEntry *targetEntry = NULL;
	FuncExpr *funcExpr = NULL;
	DistObjectCacheEntry *procedure = NULL;
	Oid colocatedRelationId = InvalidOid;
	Const *partitionValue = NULL;
	Datum partitionValueDatum = 0;
	ShardInterval *shardInterval = NULL;
	List *placementList = NIL;
	DistTableCacheEntry *distTable = NULL;
	Var *partitionColumn = NULL;
	ShardPlacement *placement = NULL;
	WorkerNode *workerNode = NULL;
	StringInfo queryString = NULL;
	Task *task = NULL;
	Job *job = NULL;
	DistributedPlan *distributedPlan = NULL;

	if (IsMultiStatementTransaction())
	{
		/* cannot delegate function calls in a multi-statement transaction */
		return NULL;
	}

	if (!IsCoordinator())
	{
		/* do not delegate from workers */
		return NULL;
	}

	if (query == NULL)
	{
		/* no query (mostly here to be defensive) */
		return NULL;
	}

	joinTree = query->jointree;
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
		/* query has a FROM section */
#if PG_VERSION_NUM >= 120000

		/* in pg12 empty FROMs are represented with an RTE_RESULT */
		if (list_length(joinTree->fromlist) == 1)
		{
			RangeTblRef *reference = linitial(joinTree->fromlist);
			RangeTblEntry *rtentry = rt_fetch(reference->rtindex, query->rtable);
			if (rtentry->rtekind != RTE_RESULT)
			{
				return NULL;
			}
		}
		else
		{
			return NULL;
		}
#else
		return NULL;
#endif
	}

	targetList = query->targetList;
	if (list_length(query->targetList) != 1)
	{
		/* multiple target list items */
		return NULL;
	}

	targetEntry = (TargetEntry *) linitial(targetList);
	if (!IsA(targetEntry->expr, FuncExpr))
	{
		/* target list item is not a function call */
		ereport(DEBUG4, (errmsg("query is not a function call")));
		return NULL;
	}

	if (!CitusHasBeenLoaded() || !CheckCitusVersion(DEBUG4))
	{
		/* Citus is not ready to determine whether function is distributed */
		return NULL;
	}

	funcExpr = (FuncExpr *) targetEntry->expr;
	procedure = LookupDistObjectCacheEntry(ProcedureRelationId, funcExpr->funcid, 0);
	if (procedure == NULL)
	{
		/* not a distributed function call */
		ereport(DEBUG4, (errmsg("function is not distributed")));
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

	if (expression_tree_walker((Node *) funcExpr->args, contain_param_walker, NULL))
	{
		ereport(DEBUG1, (errmsg("arguments in a distributed function must "
								"not contain subqueries")));
		return NULL;
	}

	colocatedRelationId = ColocatedTableId(procedure->colocationId);
	if (colocatedRelationId == InvalidOid)
	{
		ereport(DEBUG1, (errmsg("function does not have co-located tables")));
		return NULL;
	}

	distTable = DistributedTableCacheEntry(colocatedRelationId);
	partitionColumn = distTable->partitionColumn;
	if (partitionColumn == NULL)
	{
		/* This can happen if colocated with a reference table. Punt for now. */
		ereport(DEBUG1, (errmsg(
							 "cannnot push down function call for reference tables")));
		return NULL;
	}

	partitionValue = (Const *) list_nth(funcExpr->args, procedure->distributionArgIndex);
	if (!IsA(partitionValue, Const))
	{
		ereport(DEBUG1, (errmsg("distribution argument value must be a constant")));
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

	placementList = FinalizedShardPlacementList(shardInterval->shardId);
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

	ereport(DEBUG1, (errmsg("pushing down the function call")));

	queryString = makeStringInfo();
	pg_get_query_def(query, queryString);

	task = CitusMakeNode(Task);
	task->taskType = SQL_TASK;
	task->queryString = queryString->data;
	task->taskPlacementList = placementList;
	task->anchorShardId = shardInterval->shardId;
	task->replicationModel = distTable->replicationModel;

	job = CitusMakeNode(Job);
	job->jobId = UniqueJobId();
	job->jobQuery = query;
	job->taskList = list_make1(task);

	distributedPlan = CitusMakeNode(DistributedPlan);
	distributedPlan->workerJob = job;
	distributedPlan->masterQuery = NULL;
	distributedPlan->routerExecutable = true;
	distributedPlan->hasReturning = false;

	/* worker will take care of any necessary locking, treat query as read-only */
	distributedPlan->modLevel = ROW_MODIFY_READONLY;

	return distributedPlan;
}
