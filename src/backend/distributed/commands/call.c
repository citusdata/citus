/*-------------------------------------------------------------------------
 *
 * call.c
 *    Commands for call remote stored procedures.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if PG_VERSION_NUM >= 110000

#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/connection_management.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_pruning.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "miscadmin.h"
#include "tcop/dest.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

static bool CallFuncExprRemotely(CallStmt *callStmt,
								 DistObjectCacheEntry *procedure,
								 FuncExpr *funcExpr, const char *queryString,
								 DestReceiver *dest);

/*
 * CallDistributedProcedure calls a stored procedure on the worker if possible.
 */
bool
CallDistributedProcedureRemotely(CallStmt *callStmt, const char *queryString,
								 DestReceiver *dest)
{
	DistObjectCacheEntry *procedure = NULL;
	FuncExpr *funcExpr = callStmt->funcexpr;
	Oid functionId = funcExpr->funcid;

	procedure = LookupDistObjectCacheEntry(ProcedureRelationId, functionId, 0);
	if (procedure == NULL)
	{
		return false;
	}

	return CallFuncExprRemotely(callStmt, procedure, funcExpr, queryString, dest);
}


/*
 * CallFuncExprRemotely calls a procedure of function on the worker if possible.
 */
static bool
CallFuncExprRemotely(CallStmt *callStmt, DistObjectCacheEntry *procedure,
					 FuncExpr *funcExpr, const char *queryString, DestReceiver *dest)
{
	Oid colocatedRelationId = InvalidOid;
	Const *partitionValue = NULL;
	ShardInterval *shardInterval = NULL;
	List *placementList = NIL;
	ListCell *argCell = NULL;
	WorkerNode *preferredWorkerNode = NULL;
	DistTableCacheEntry *distTable = NULL;
	ShardPlacement *placement = NULL;
	WorkerNode *workerNode = NULL;

	if (IsMultiStatementTransaction())
	{
		ereport(DEBUG2, (errmsg("cannot push down CALL in multi-statement transaction")));
		return false;
	}

	colocatedRelationId = ColocatedTableId(procedure->colocationId);
	if (colocatedRelationId == InvalidOid)
	{
		ereport(DEBUG2, (errmsg("stored procedure does not have co-located tables")));
		return false;
	}

	if (procedure->distributionArgIndex < 0 ||
		procedure->distributionArgIndex >= list_length(funcExpr->args))
	{
		ereport(DEBUG2, (errmsg("cannot push down invalid distribution_argument_index")));
		return false;
	}

	foreach(argCell, funcExpr->args)
	{
		Node *argNode = (Node *) lfirst(argCell);
		if (!IsA(argNode, Const))
		{
			ereport(DEBUG2, (errmsg("cannot push down non-constant argument value")));
			return false;
		}
	}

	partitionValue = (Const *) list_nth(funcExpr->args, procedure->distributionArgIndex);
	distTable = DistributedTableCacheEntry(colocatedRelationId);
	shardInterval = FindShardInterval(partitionValue->constvalue, distTable);

	if (shardInterval == NULL)
	{
		ereport(DEBUG2, (errmsg("cannot push down call, failed to find shard interval")));
		return false;
	}

	placementList = FinalizedShardPlacementList(shardInterval->shardId);
	if (list_length(placementList) != 1)
	{
		/* punt on reference tables for now */
		ereport(DEBUG2, (errmsg(
							 "cannot push down CALL for reference tables or replicated distributed tables")));
		return false;
	}

	placement = (ShardPlacement *) linitial(placementList);
	workerNode = FindWorkerNode(placement->nodeName, placement->nodePort);

	if (workerNode->hasMetadata)
	{
		/* we can execute this procedure on the worker! */
		preferredWorkerNode = workerNode;
	}

	if (preferredWorkerNode == NULL)
	{
		ereport(DEBUG2, (errmsg("there is no worker node with metadata")));
		return false;
	}

	{
		Tuplestorestate *tupleStore = tuplestore_begin_heap(true, false, work_mem);
		TupleDesc tupleDesc = CallStmtResultDesc(callStmt);
		TupleTableSlot *slot = MakeSingleTupleTableSlotCompat(tupleDesc, &TTSOpsVirtual);

		Task *task = CitusMakeNode(Task);
		task->jobId = INVALID_JOB_ID;
		task->taskId = 0;
		task->taskType = DDL_TASK;
		task->queryString = pstrdup(queryString);
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependedTaskList = NIL;
		task->anchorShardId = placement->shardId;
		task->relationShardList = NIL;
		task->taskPlacementList = placementList;

		ExecuteTaskListExtended(ROW_MODIFY_COMMUTATIVE, list_make1(task),
								tupleDesc, tupleStore, true,
								MaxAdaptiveExecutorPoolSize);

		while (tuplestore_gettupleslot(tupleStore, true, false, slot))
		{
			dest->receiveSlot(slot, dest);
		}
	}

	return true;
}


#endif /* PG_VERSION_NUM >= 110000 */
