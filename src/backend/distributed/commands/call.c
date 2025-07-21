/*-------------------------------------------------------------------------
 *
 * call.c
 *    Commands for distributing CALL for distributed procedures.
 *
 *    Procedures can be distributed with create_distributed_function.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "tcop/dest.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "pg_version_constants.h"

#include "distributed/adaptive_executor.h"
#include "distributed/backend_data.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/function_call_delegation.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_pruning.h"
#include "distributed/tuple_destination.h"
#include "distributed/version_compat.h"
#include "distributed/worker_log_messages.h"
#include "distributed/worker_manager.h"


/* global variable tracking whether we are in a delegated procedure call */
bool InDelegatedProcedureCall = false;


/*
 * CallDistributedProcedureRemotely calls a stored procedure on the worker if possible.
 */
bool
CallDistributedProcedureRemotely(CallStmt *callStmt, DestReceiver *dest)
{
	FuncExpr *funcExpr = callStmt->funcexpr;
	Oid functionId = funcExpr->funcid;

	DistObjectCacheEntry *procedure = LookupDistObjectCacheEntry(ProcedureRelationId,
																 functionId, 0);
	if (procedure == NULL || !procedure->isDistributed)
	{
		return false;
	}

	if (IsCitusInternalBackend())
	{
		/*
		 * We are in a citus-initiated backend handling a CALL to a distributed
		 * procedure. That means that this is the delegated call.
		 */
		InDelegatedProcedureCall = true;
		return false;
	}

	if (IsMultiStatementTransaction())
	{
		ereport(DEBUG1, (errmsg("cannot push down CALL in multi-statement transaction")));
		return false;
	}

	Oid colocatedRelationId = ColocatedTableId(procedure->colocationId);
	if (colocatedRelationId == InvalidOid)
	{
		ereport(DEBUG1, (errmsg("stored procedure does not have co-located tables")));
		return false;
	}

	if (contain_volatile_functions((Node *) funcExpr->args))
	{
		ereport(DEBUG1, (errmsg("arguments in a distributed stored procedure must "
								"be constant expressions")));
		return false;
	}

	CitusTableCacheEntry *distTable = GetCitusTableCacheEntry(colocatedRelationId);
	Var *partitionColumn = distTable->partitionColumn;
	bool colocatedWithReferenceTable = false;
	if (IsCitusTableTypeCacheEntry(distTable, REFERENCE_TABLE))
	{
		/* This can happen if colocated with a reference table. Punt for now. */
		ereport(DEBUG1, (errmsg(
							 "will push down CALL for reference tables")));
		colocatedWithReferenceTable = true;
	}

	ShardPlacement *placement = NULL;
	if (colocatedWithReferenceTable)
	{
		placement = ShardPlacementForFunctionColocatedWithReferenceTable(distTable);
	}
	else
	{
		List *argumentList = NIL;
		List *namedArgList;
		int numberOfArgs;
		Oid *argumentTypes;

		if (!get_merged_argument_list(callStmt, &namedArgList, &argumentTypes,
									  &argumentList, &numberOfArgs))
		{
			argumentList = funcExpr->args;
		}

		placement =
			ShardPlacementForFunctionColocatedWithDistTable(procedure, argumentList,
															partitionColumn, distTable,
															NULL);
	}

	/* return if we could not find a placement */
	if (placement == NULL)
	{
		return false;
	}

	WorkerNode *workerNode = FindWorkerNode(placement->nodeName, placement->nodePort);
	if (workerNode == NULL || !workerNode->hasMetadata || !workerNode->metadataSynced)
	{
		ereport(DEBUG1, (errmsg("there is no worker node with metadata")));
		return false;
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
		ereport(DEBUG1, (errmsg("not pushing down procedure to the same node")));
		return false;
	}

	ereport(DEBUG1, (errmsg("pushing down the procedure")));

	/* build remote command with fully qualified names */
	StringInfo callCommand = makeStringInfo();

	appendStringInfo(callCommand, "CALL %s", pg_get_rule_expr((Node *) callStmt));
	{
		Tuplestorestate *tupleStore = tuplestore_begin_heap(true, false, work_mem);
		TupleDesc tupleDesc = CallStmtResultDesc(callStmt);
		TupleTableSlot *slot = MakeSingleTupleTableSlot(tupleDesc,
														&TTSOpsMinimalTuple);
		bool expectResults = true;
		Task *task = CitusMakeNode(Task);

		task->jobId = INVALID_JOB_ID;
		task->taskId = INVALID_TASK_ID;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, callCommand->data);
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependentTaskList = NIL;
		task->anchorShardId = placement->shardId;
		task->relationShardList = NIL;
		task->taskPlacementList = list_make1(placement);

		/*
		 * We are delegating the distributed transaction to the worker, so we
		 * should not run the CALL in a transaction block.
		 */
		TransactionProperties xactProperties = {
			.errorOnAnyFailure = true,
			.useRemoteTransactionBlocks = TRANSACTION_BLOCKS_DISALLOWED,
			.requires2PC = false
		};

		EnableWorkerMessagePropagation();

		bool localExecutionSupported = true;
		ExecutionParams *executionParams = CreateBasicExecutionParams(
			ROW_MODIFY_NONE, list_make1(task), MaxAdaptiveExecutorPoolSize,
			localExecutionSupported
			);
		executionParams->tupleDestination = CreateTupleStoreTupleDest(tupleStore,
																	  tupleDesc);
		executionParams->expectResults = expectResults;
		executionParams->xactProperties = xactProperties;
		executionParams->isUtilityCommand = true;
		ExecuteTaskListExtended(executionParams);

		DisableWorkerMessagePropagation();

		while (tuplestore_gettupleslot(tupleStore, true, false, slot))
		{
			if (!dest->receiveSlot(slot, dest))
			{
				break;
			}
		}

		/* Don't call tuplestore_end(tupleStore). It'll be freed soon enough in a top level CALL,
		 * & dest->receiveSlot could conceivably rely on slots being long lived.
		 */
	}

	return true;
}
