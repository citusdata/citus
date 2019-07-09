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

#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/connection_management.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/procedure_metadata.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_pruning.h"
#include "distributed/worker_manager.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "tcop/dest.h"


static bool CallFuncExprRemotely(DistributedProcedureRecord *procedure,
								 FuncExpr *funcExpr, const char *queryString,
								 DestReceiver *dest);


/*
 * CallDistributedProcedure calls a stored procedure on the worker if possible.
 */
bool
CallDistributedProcedureRemotely(CallStmt *callStmt, const char *queryString,
								 DestReceiver *dest)
{
	DistributedProcedureRecord *procedure = NULL;
	FuncExpr *funcExpr = callStmt->funcexpr;
	Oid functionId = funcExpr->funcid;

	procedure = LoadDistributedProcedureRecord(functionId);
	if (procedure == NULL)
	{
		return false;
	}

	return CallFuncExprRemotely(procedure, funcExpr, queryString, dest);
}


/*
 * CallFuncExprRemotely calls a procedure of function on the worker if possible.
 */
static bool
CallFuncExprRemotely(DistributedProcedureRecord *procedure, FuncExpr *funcExpr,
					 const char *queryString, DestReceiver *dest)
{
	Oid colocatedRelationId = InvalidOid;
	Node *partitionValue = NULL;
	List *shardList = NIL;
	ShardInterval *shard = NULL;
	List *placementList = NIL;
	ListCell *placementCell = NULL;
	WorkerNode *preferredWorkerNode = NULL;
	int connectionFlags = 0;
	MultiConnection *connection = NULL;

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

	partitionValue = list_nth(funcExpr->args, procedure->distributionArgumentIndex);

	shardList = ShardsForPartitionColumnValue(colocatedRelationId, partitionValue);
	if (list_length(shardList) != 1)
	{
		ereport(DEBUG2, (errmsg("cannot push down CALL when there are overlapping "
								"shards")));
		return false;
	}

	shard = (ShardInterval *) linitial(shardList);
	placementList = FinalizedShardPlacementList(shard->shardId);
	if (list_length(placementList) != 1)
	{
		/* punt on reference tables for now */
		ereport(DEBUG2, (errmsg("cannot push down CALL without a distribution column")));
		return false;
	}

	foreach(placementCell, placementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
		WorkerNode *workerNode = FindWorkerNode(placement->nodeName, placement->nodePort);

		if (workerNode->hasMetadata)
		{
			/* we can execute this procedure on the worker! */
			preferredWorkerNode = workerNode;
		}
	}

	if (preferredWorkerNode == NULL)
	{
		ereport(DEBUG2, (errmsg("there is no worker node with metadata")));
		return false;
	}

	connection = GetNodeConnection(connectionFlags, preferredWorkerNode->workerName,
								   preferredWorkerNode->workerPort);

	ExecuteCriticalRemoteCommand(connection, queryString);

	return true;
}
