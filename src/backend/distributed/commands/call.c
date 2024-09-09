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
#include "miscadmin.h"  // for elog functions


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

    /* Log the function ID being called */
    elog(DEBUG1, "Calling distributed procedure with functionId: %u", functionId);

    /* Log additional details from CallStmt */
    if (callStmt->funcexpr != NULL)
    {
        elog(DEBUG1, "Function expression type: %d", nodeTag(callStmt->funcexpr));
    }

	if (funcExpr->args != NIL)
	{
		ListCell *lc;
		int argIndex = 0;
		foreach(lc, funcExpr->args)
		{
			Node *arg = (Node *) lfirst(lc);
			elog(DEBUG1, "Argument %d: NodeTag: %d", argIndex, nodeTag(arg));
			argIndex++;
		}
	}
	else
	{
		elog(DEBUG1, "No arguments in the function expression");
	}

	ListCell *argCell1;
    int argIndex1 = 0;

    /* Iterate over the arguments and log them */
    foreach(argCell1, callStmt->funcexpr->args)
    {
    	Node *argNode = (Node *) lfirst(argCell1);
    
		// Check if the node is valid
		if (argNode != NULL)
		{
			// Use nodeToString() to convert the node into a string representation for debugging
			char *argStr = nodeToString(argNode);
			
			// Log the argument index and its string representation
			elog(DEBUG1, "Argument %d: %s", argIndex1, argStr);
			
			// Free the string memory after logging (it's a good practice to avoid memory leaks)
			pfree(argStr);
		}
		else
		{
			elog(DEBUG1, "Argument %d: (null)", argIndex1);
		}
        argIndex1++;
    }


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
		elog(DEBUG1, "Generated CALL statement: %s", callCommand->data);
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
		elog(DEBUG1, "Worker message propagation enabled");

		bool localExecutionSupported = true;
		elog(DEBUG1, "Local execution supported: %d", localExecutionSupported);

		/* Log task details */
		elog(DEBUG1, "Creating execution params for task");

		/* Create execution parameters */
		ExecutionParams *executionParams = CreateBasicExecutionParams(
			ROW_MODIFY_NONE, list_make1(task), MaxAdaptiveExecutorPoolSize,
			localExecutionSupported
		);
		
		const char* NodeTagToString(NodeTag tag)
		{
			switch (tag)
			{
				case T_Var: return "Var";
				case T_Const: return "Const";
				case T_Param: return "Param";
				case T_FuncExpr: return "FuncExpr";
				case T_OpExpr: return "OpExpr";
				case T_BoolExpr: return "BoolExpr";
				case T_Aggref: return "Aggref";
				case T_WindowFunc: return "WindowFunc";
				case T_SubLink: return "SubLink";
				case T_CoalesceExpr: return "CoalesceExpr";
				case T_CaseExpr: return "CaseExpr";
				case T_NullTest: return "NullTest";
				case T_CollateExpr: return "CollateExpr";
				case T_FieldSelect: return "FieldSelect";
				case T_FieldStore: return "FieldStore";
				case T_SubPlan: return "SubPlan";
				default: return "Unknown";
			}
		}

		// Create a ParamListInfo structure
		List *argList = funcExpr->args;  // Extract arguments from the function expression
		int paramCount = list_length(argList);  // Get the number of arguments
		ParamListInfo paramListInfo = makeParamList(paramCount);  // Create ParamListInfo structure

		// Loop through the argument list and populate ParamListInfo
		int paramIndex = 0;
		ListCell *argCell;
		foreach(argCell, argList)
		{
			Node *argNode = (Node *) lfirst(argCell);

			// Log the type of the argument
			NodeTag nodeType = nodeTag(argNode);
			elog(DEBUG1, "Processing argument at index %d of type: %s", paramIndex, NodeTagToString(nodeType));


			if (IsA(argNode, Const))
			{
				Const *constArg = (Const *) argNode;
				paramListInfo->params[paramIndex].ptype = constArg->consttype;  // Set parameter type
				paramListInfo->params[paramIndex].value = constArg->constvalue; // Set parameter value
				paramListInfo->params[paramIndex].isnull = constArg->constisnull;  // Set if the parameter is null

				// Log the constant parameter's type, value, and null status
        		elog(DEBUG1, "Populating ParamListInfo with constant parameter: paramIndex: %d, paramType: %d, isNull: %s",
					paramIndex, paramListInfo->params[paramIndex].ptype,
					constArg->constisnull ? "true" : "false");
			}
			else if (IsA(argNode, Param))
			{
				Param *paramArg = (Param *) argNode;

				// Set the parameter type
				paramListInfo->params[paramIndex].ptype = paramArg->paramtype;

				// Fetch the value of the parameter if necessary
				if (paramListInfo->paramFetch != NULL)
				{
					ParamExternData paramData;
					paramListInfo->paramFetch(paramListInfo, paramArg->paramid, true, &paramData);

					// Log the fetched parameter details
					elog(DEBUG1, "paramFetch for paramId: %d returned value: %d, type: %d, isNull: %s", 
						paramArg->paramid, DatumGetInt32(paramData.value), paramData.ptype, 
						paramData.isnull ? "true" : "false");

					paramListInfo->params[paramIndex].value = paramData.value;
					paramListInfo->params[paramIndex].isnull = paramData.isnull;

					// Log fetched value and type
					elog(DEBUG1, "Fetched dynamic parameter: paramIndex: %d, paramType: %d, paramValue: %d", 
						paramIndex, paramListInfo->params[paramIndex].ptype, DatumGetInt32(paramListInfo->params[paramIndex].value));
				}
				else
				{
					// Handle the case where paramFetch is NULL
					elog(DEBUG1, "Could not fetch value for parameter: %d", paramArg->paramid);
				}
			}
			else if (IsA(argNode, FuncExpr))
			{
				// FuncExpr *funcExpr = (FuncExpr *) argNode;

				// Log function expression details
				elog(DEBUG1, "Processing function expression: funcid: %d", funcExpr->funcid);

				// Iterate through the arguments of the function expression
				ListCell *funcArgCell;
				foreach(funcArgCell, funcExpr->args)
				{
					Node *funcArgNode = (Node *) lfirst(funcArgCell);

					// Check if the argument is a Param or Const
					if (IsA(funcArgNode, Param))
					{
						Param *paramArg = (Param *) funcArgNode;

						// Fetch the parameter value (same as your param-fetch logic)
						ParamExternData paramData;
						paramListInfo->paramFetch(paramListInfo, paramArg->paramid, true, &paramData);

						// Populate ParamListInfo with fetched param
						paramListInfo->params[paramIndex].ptype = paramArg->paramtype;
						paramListInfo->params[paramIndex].value = paramData.value;
						paramListInfo->params[paramIndex].isnull = paramData.isnull;

						// Log fetched parameter details
						elog(DEBUG1, "Populating ParamListInfo with fetched parameter: paramIndex: %d, paramType: %d, paramValue: %d",
							paramIndex, paramListInfo->params[paramIndex].ptype, DatumGetInt32(paramListInfo->params[paramIndex].value));
					}
					else if (IsA(funcArgNode, Const))
					{
						Const *constArg = (Const *) funcArgNode;

						// Handle Const values within the function expression
						paramListInfo->params[paramIndex].ptype = constArg->consttype;
						paramListInfo->params[paramIndex].value = constArg->constvalue;
						paramListInfo->params[paramIndex].isnull = constArg->constisnull;

						// Log constant parameter
						elog(DEBUG1, "Populating ParamListInfo with constant parameter inside function expression: paramIndex: %d, paramType: %d",
							paramIndex, paramListInfo->params[paramIndex].ptype);
					}
					else
					{
						elog(DEBUG1, "Unsupported argument type in function expression at paramIndex: %d", paramIndex);
					}
				}
			}
			else
			{
				// Handle other cases if necessary
				elog(DEBUG1, "Unsupported argument type at paramIndex: %d", paramIndex);
			}

			// Log populated parameters
			// elog(DEBUG1, "Populating ParamListInfo, paramIndex: %d, paramType: %d, paramValue: %d", 
			// 	paramIndex, paramListInfo->params[paramIndex].ptype, DatumGetInt32(paramListInfo->params[paramIndex].value));

			paramIndex++;
		}


		
		/* Set tuple destination and execution properties */
		executionParams->tupleDestination = CreateTupleStoreTupleDest(tupleStore, tupleDesc);
		executionParams->expectResults = expectResults;
		executionParams->xactProperties = xactProperties;
		executionParams->isUtilityCommand = true;
		executionParams->paramListInfo = paramListInfo;

		/* Log before executing task list */
		elog(DEBUG1, "Executing task list with ExecuteTaskListExtended");

		/* Execute the task list */
		ExecuteTaskListExtended(executionParams);

		/* Log after task execution */
		elog(DEBUG1, "Task list execution completed");


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
