/*
 * local_executor.c
 *
 * The scope of the local execution is locally executing the queries on the
 * shards. In other words, local execution does not deal with any local tables
 * that are not on shards on the node that the query is being executed. In that
 * sense, the local executor is only triggered if the node has both the metadata
 * and the shards (e.g., only Citus MX worker nodes).
 *
 * The goal of the local execution is to skip the unnecessary network round-trip
 * happening on the node itself. Instead, identify the locally executable tasks
 * and simply call PostgreSQL's planner and executor.
 *
 * The local executor is an extension of the adaptive executor. So, the executor
 * uses adaptive executor's custom scan nodes.
 *
 * One thing to note is that Citus MX is only supported with replication factor
 * to be equal to 1, so keep that in mind while continuing the comments below.
 *
 * On the high level, there are 3 slightly different ways of utilizing local
 * execution:
 *
 * (1) Execution of local single shard queries of a distributed table
 *
 *      This is the simplest case. The executor kicks at the start of the adaptive
 *      executor, and since the query is only a single task the execution finishes
 *      without going to the network at all.
 *
 *      Even if there is a transaction block (or recursively planned CTEs), as
 *      long as the queries hit the shards on the same node, the local execution
 *      will kick in.
 *
 * (2) Execution of local single queries and remote multi-shard queries
 *
 *      The rule is simple. If a transaction block starts with a local query
 *      execution,
 *      all the other queries in the same transaction block that touch any local
 *      shard have to use the local execution. Although this sounds restrictive,
 *      we prefer to implement it in this way, otherwise we'd end-up with as
 *      complex scenarios as we have in the connection managements due to foreign
 *      keys.
 *
 *      See the following example:
 *      BEGIN;
 *          -- assume that the query is executed locally
 *          SELECT count(*) FROM test WHERE key = 1;
 *
 *          -- at this point, all the shards that reside on the
 *          -- node is executed locally one-by-one. After those finishes
 *          -- the remaining tasks are handled by adaptive executor
 *          SELECT count(*) FROM test;
 *
 *
 * (3) Modifications of reference tables
 *
 *		Modifications to reference tables have to be executed on all nodes. So,
 *      after the local execution, the adaptive executor keeps continuing the
 *      execution on the other nodes.
 *
 *		Note that for read-only queries, after the local execution, there is no
 *      need to kick in adaptive executor.
 *
 *  There are also a few limitations/trade-offs that are worth mentioning.
 *  - The local execution on multiple shards might be slow because the execution
 *  has to happen one task at a time (e.g., no parallelism).
 *  - If a transaction block/CTE starts with a multi-shard command, we do not
 *  use local query execution since local execution is sequential. Basically,
 *  we do not want to lose parallelism across local tasks by switching to local
 *  execution.
 *  - The local execution cannot be mixed with the executors other than adaptive,
 *  namely task-tracker executor.
 *  - Related with the previous item, COPY command cannot be mixed with local
 *  execution in a transaction. The implication of that is any part of INSERT..SELECT
 *  via coordinator cannot happen via the local execution.
 */
#include "postgres.h"
#include "miscadmin.h"

#include "distributed/commands/utility_hook.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h" /* to access LogRemoteCommands */
#include "distributed/transaction_management.h"
#include "distributed/worker_protocol.h"
#include "executor/tstoreReceiver.h"
#include "executor/tuptable.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#else
#include "optimizer/planner.h"
#endif
#include "nodes/params.h"
#include "utils/snapmgr.h"


/* controlled via a GUC */
bool EnableLocalExecution = true;
bool LogLocalCommands = false;

bool TransactionAccessedLocalPlacement = false;
bool TransactionConnectedToLocalGroup = false;


static void SplitLocalAndRemotePlacements(List *taskPlacementList,
										  List **localTaskPlacementList,
										  List **remoteTaskPlacementList);
static uint64 ExecuteLocalTaskPlan(CitusScanState *scanState, PlannedStmt *taskPlan,
								   char *queryString);
static void LogLocalCommand(Task *task);
static void ExtractParametersForLocalExecution(ParamListInfo paramListInfo,
											   Oid **parameterTypes,
											   const char ***parameterValues);
static void LocallyExecuteUtilityTask(const char *utilityCommand);
static void LocallyExecuteUdfTaskQuery(Query *localUdfCommandQuery);


/*
 * ExecuteLocalTasks gets a CitusScanState node and list of local tasks.
 *
 * The function goes over the task list and executes them locally.
 * The returning tuples (if any) is stored in the CitusScanState.
 *
 * The function returns totalRowsProcessed.
 */
uint64
ExecuteLocalTaskList(CitusScanState *scanState, List *taskList)
{
	EState *executorState = ScanStateGetExecutorState(scanState);
	DistributedPlan *distributedPlan = scanState->distributedPlan;
	ParamListInfo paramListInfo = copyParamList(executorState->es_param_list_info);
	int numParams = 0;
	Oid *parameterTypes = NULL;
	uint64 totalRowsProcessed = 0;

	if (paramListInfo != NULL)
	{
		/* not used anywhere, so declare here */
		const char **parameterValues = NULL;

		ExtractParametersForLocalExecution(paramListInfo, &parameterTypes,
										   &parameterValues);

		numParams = paramListInfo->numParams;
	}

	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		/*
		 * If we have a valid shard id, a distributed table will be accessed
		 * during execution. Record it to apply the restrictions related to
		 * local execution.
		 */
		if (task->anchorShardId != INVALID_SHARD_ID)
		{
			TransactionAccessedLocalPlacement = true;
		}

		PlannedStmt *localPlan = GetCachedLocalPlan(task, distributedPlan);

		/*
		 * If the plan is already cached, don't need to re-plan, just
		 * acquire necessary locks.
		 */
		if (localPlan != NULL)
		{
			Query *jobQuery = distributedPlan->workerJob->jobQuery;
			LOCKMODE lockMode =
				IsModifyCommand(jobQuery) ? RowExclusiveLock : (jobQuery->hasForUpdate ?
																RowShareLock :
																AccessShareLock);

			Oid relationId = InvalidOid;
			foreach_oid(relationId, localPlan->relationOids)
			{
				LockRelationOid(relationId, lockMode);
			}
		}
		else
		{
			int taskNumParams = numParams;
			Oid *taskParameterTypes = parameterTypes;

			if (task->parametersInQueryStringResolved)
			{
				/*
				 * Parameters were removed from the query string so do not pass them
				 * here. Otherwise, we might see errors when passing custom types,
				 * since their OIDs were set to 0 and their type is normally
				 * inferred from
				 */
				taskNumParams = 0;
				taskParameterTypes = NULL;
			}

			Query *shardQuery = ParseQueryString(TaskQueryString(task),
												 taskParameterTypes,
												 taskNumParams);

			int cursorOptions = 0;

			/*
			 * Altough the shardQuery is local to this node, we prefer planner()
			 * over standard_planner(). The primary reason for that is Citus itself
			 * is not very tolarent standard_planner() calls that doesn't go through
			 * distributed_planner() because of the way that restriction hooks are
			 * implemented. So, let planner to call distributed_planner() which
			 * eventually calls standard_planner().
			 */
			localPlan = planner(shardQuery, cursorOptions, paramListInfo);
		}

		LogLocalCommand(task);

		char *shardQueryString = task->queryStringLazy
								 ? task->queryStringLazy
								 : "<optimized out by local execution>";

		totalRowsProcessed +=
			ExecuteLocalTaskPlan(scanState, localPlan, shardQueryString);
	}

	return totalRowsProcessed;
}


/*
 * ExtractAndExecuteLocalAndRemoteTasks extracts local and remote tasks
 * if local execution can be used and executes them.
 */
uint64
ExtractAndExecuteLocalAndRemoteTasks(CitusScanState *scanState,
									 List *taskList, RowModifyLevel rowModifyLevel, bool
									 hasReturning)
{
	uint64 processedRows = 0;
	List *localTaskList = NIL;
	List *remoteTaskList = NIL;
	TupleDesc tupleDescriptor = ScanStateGetTupleDescriptor(scanState);

	if (ShouldExecuteTasksLocally(taskList))
	{
		bool readOnlyPlan = false;

		/* set local (if any) & remote tasks */
		ExtractLocalAndRemoteTasks(readOnlyPlan, taskList, &localTaskList,
								   &remoteTaskList);
		processedRows += ExecuteLocalTaskList(scanState, localTaskList);
	}
	else
	{
		/* all tasks should be executed via remote connections */
		remoteTaskList = taskList;
	}

	/* execute remote tasks if any */
	if (list_length(remoteTaskList) > 0)
	{
		processedRows += ExecuteTaskListIntoTupleStore(rowModifyLevel, remoteTaskList,
													   tupleDescriptor,
													   scanState->tuplestorestate,
													   hasReturning);
	}

	return processedRows;
}


/*
 * ExtractParametersForLocalExecution extracts parameter types and values
 * from the given ParamListInfo structure, and fills parameter type and
 * value arrays. It does not change the oid of custom types, because the
 * query will be run locally.
 */
static void
ExtractParametersForLocalExecution(ParamListInfo paramListInfo, Oid **parameterTypes,
								   const char ***parameterValues)
{
	ExtractParametersFromParamList(paramListInfo, parameterTypes,
								   parameterValues, true);
}


/*
 * ExecuteLocalUtilityTaskList executes a list of tasks locally. This function
 * also logs local execution notice for each task and sets
 * TransactionAccessedLocalPlacement to true for next set of possible queries
 * & commands within the current transaction block. See the comment in function.
 */
void
ExecuteLocalUtilityTaskList(List *localTaskList)
{
	Task *localTask = NULL;

	foreach_ptr(localTask, localTaskList)
	{
		const char *localTaskQueryCommand = TaskQueryString(localTask);

		/* we do not expect tasks with INVALID_SHARD_ID for utility commands */
		Assert(localTask->anchorShardId != INVALID_SHARD_ID);

		Assert(TaskAccessesLocalNode(localTask));

		/*
		 * We should register the access to local placement to force the local
		 * execution of the following commands withing the current transaction.
		 */
		TransactionAccessedLocalPlacement = true;

		LogLocalCommand(localTask);

		LocallyExecuteUtilityTask(localTaskQueryCommand);
	}
}


/*
 * LocallyExecuteUtilityTask executes the given local task query in the current
 * session.
 */
static void
LocallyExecuteUtilityTask(const char *localTaskQueryCommand)
{
	RawStmt *localTaskRawStmt = (RawStmt *) ParseTreeRawStmt(localTaskQueryCommand);

	Node *localTaskRawParseTree = localTaskRawStmt->stmt;

	/*
	 * Actually, the query passed to this function would mostly be a
	 * utility command to be executed locally. However, some utility
	 * commands do trigger udf calls (e.g worker_apply_shard_ddl_command)
	 * to execute commands in a generic way. But as we support local
	 * execution of utility commands, we should also process those udf
	 * calls locally as well. In that case, we simply execute the query
	 * implying the udf call in below conditional block.
	 */
	if (IsA(localTaskRawParseTree, SelectStmt))
	{
		/* we have no external parameters to rewrite the UDF call RawStmt */
		Query *localUdfTaskQuery =
			RewriteRawQueryStmt(localTaskRawStmt, localTaskQueryCommand, NULL, 0);

		LocallyExecuteUdfTaskQuery(localUdfTaskQuery);
	}
	else
	{
		/*
		 * It is a regular utility command we should execute it locally via
		 * process utility.
		 */
		CitusProcessUtility(localTaskRawParseTree, localTaskQueryCommand,
							PROCESS_UTILITY_TOPLEVEL, NULL, None_Receiver, NULL);
	}
}


/*
 * LocallyExecuteUdfTaskQuery executes the given udf command locally. Local udf
 * command is simply a "SELECT udf_call()" query and so it cannot be executed
 * via process utility.
 */
static void
LocallyExecuteUdfTaskQuery(Query *localUdfTaskQuery)
{
	/* we do not expect any results */
	ExecuteQueryIntoDestReceiver(localUdfTaskQuery, NULL, None_Receiver);
}


/*
 * LogLocalCommand logs commands executed locally on this node. Although we're
 * talking about local execution, the function relies on citus.log_remote_commands
 * GUC. This makes sense because the local execution is still on a shard of a
 * distributed table, meaning it is part of distributed execution.
 */
static void
LogLocalCommand(Task *task)
{
	if (!(LogRemoteCommands || LogLocalCommands))
	{
		return;
	}

	ereport(NOTICE, (errmsg("executing the command locally: %s",
							ApplyLogRedaction(TaskQueryString(task)))));
}


/*
 * ExtractLocalAndRemoteTasks gets a taskList and generates two
 * task lists namely localTaskList and remoteTaskList. The function goes
 * over the input taskList and puts the tasks that are local to the node
 * into localTaskList and the remaining to the remoteTaskList. Either of
 * the lists could be NIL depending on the input taskList.
 *
 * One slightly different case is modifications to replicated tables
 * (e.g., reference tables) where a single task ends in two separate tasks
 * and the local task is added to localTaskList and the remaning ones to
 * the remoteTaskList.
 */
void
ExtractLocalAndRemoteTasks(bool readOnly, List *taskList, List **localTaskList,
						   List **remoteTaskList)
{
	*remoteTaskList = NIL;
	*localTaskList = NIL;

	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		List *localTaskPlacementList = NULL;
		List *remoteTaskPlacementList = NULL;

		SplitLocalAndRemotePlacements(
			task->taskPlacementList, &localTaskPlacementList, &remoteTaskPlacementList);

		/* either the local or the remote should be non-nil */
		Assert(!(localTaskPlacementList == NIL && remoteTaskPlacementList == NIL));

		if (list_length(task->taskPlacementList) == 1)
		{
			/*
			 * At this point, the task has a single placement (e.g,. anchor shard
			 * is distributed table's shard). So, it is either added to local or
			 * remote taskList.
			 */
			if (localTaskPlacementList == NIL)
			{
				*remoteTaskList = lappend(*remoteTaskList, task);
			}
			else
			{
				*localTaskList = lappend(*localTaskList, task);
			}
		}
		else
		{
			/*
			 * At this point, we're dealing with reference tables or intermediate
			 * results where the task has placements on both local and remote
			 * nodes. We always prefer to use local placement, and require remote
			 * placements only for modifications.
			 */
			task->partiallyLocalOrRemote = true;

			Task *localTask = copyObject(task);

			localTask->taskPlacementList = localTaskPlacementList;
			*localTaskList = lappend(*localTaskList, localTask);

			if (readOnly)
			{
				/* read-only tasks should only be executed on the local machine */
			}
			else
			{
				Task *remoteTask = copyObject(task);
				remoteTask->taskPlacementList = remoteTaskPlacementList;

				*remoteTaskList = lappend(*remoteTaskList, remoteTask);
			}
		}
	}
}


/*
 * SplitLocalAndRemotePlacements is a helper function which iterates over the
 * input taskPlacementList and puts the placements into corresponding list of
 * either localTaskPlacementList or remoteTaskPlacementList.
 */
static void
SplitLocalAndRemotePlacements(List *taskPlacementList, List **localTaskPlacementList,
							  List **remoteTaskPlacementList)
{
	int32 localGroupId = GetLocalGroupId();

	*localTaskPlacementList = NIL;
	*remoteTaskPlacementList = NIL;

	ShardPlacement *taskPlacement = NULL;
	foreach_ptr(taskPlacement, taskPlacementList)
	{
		if (taskPlacement->groupId == localGroupId)
		{
			*localTaskPlacementList = lappend(*localTaskPlacementList, taskPlacement);
		}
		else
		{
			*remoteTaskPlacementList = lappend(*remoteTaskPlacementList, taskPlacement);
		}
	}
}


/*
 * ExecuteLocalTaskPlan gets a planned statement which can be executed locally.
 * The function simply follows the steps to have a local execution, sets the
 * tupleStore if necessary. The function returns the
 */
static uint64
ExecuteLocalTaskPlan(CitusScanState *scanState, PlannedStmt *taskPlan, char *queryString)
{
	EState *executorState = ScanStateGetExecutorState(scanState);
	ParamListInfo paramListInfo = executorState->es_param_list_info;
	DestReceiver *tupleStoreDestReceiver = CreateDestReceiver(DestTuplestore);
	ScanDirection scanDirection = ForwardScanDirection;
	QueryEnvironment *queryEnv = create_queryEnv();
	int eflags = 0;
	uint64 totalRowsProcessed = 0;

	/*
	 * Use the tupleStore provided by the scanState because it is shared accross
	 * the other task executions and the adaptive executor.
	 */
	SetTuplestoreDestReceiverParams(tupleStoreDestReceiver,
									scanState->tuplestorestate,
									CurrentMemoryContext, false);

	/* Create a QueryDesc for the query */
	QueryDesc *queryDesc = CreateQueryDesc(taskPlan, queryString,
										   GetActiveSnapshot(), InvalidSnapshot,
										   tupleStoreDestReceiver, paramListInfo,
										   queryEnv, 0);

	ExecutorStart(queryDesc, eflags);
	ExecutorRun(queryDesc, scanDirection, 0L, true);

	/*
	 * We'll set the executorState->es_processed later, for now only remember
	 * the count.
	 */
	if (taskPlan->commandType != CMD_SELECT)
	{
		totalRowsProcessed = queryDesc->estate->es_processed;
	}

	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	return totalRowsProcessed;
}


/*
 *  ShouldExecuteTasksLocally gets a task list and returns true if the
 *  any of the tasks should be executed locally. This function does not
 *  guarantee that any task have to be executed locally.
 */
bool
ShouldExecuteTasksLocally(List *taskList)
{
	if (!EnableLocalExecution)
	{
		return false;
	}

	if (TransactionAccessedLocalPlacement)
	{
		bool isValidLocalExecutionPath PG_USED_FOR_ASSERTS_ONLY = false;

		/*
		 * For various reasons, including the transaction visibility
		 * rules (e.g., read-your-own-writes), we have to use local
		 * execution again if it has already happened within this
		 * transaction block.
		 */
		isValidLocalExecutionPath = IsMultiStatementTransaction() ||
									InCoordinatedTransaction();

		/*
		 * In some cases, such as when a single command leads to a local
		 * command execution followed by remote task (list) execution, we
		 * still expect the remote execution to first try local execution
		 * as TransactionAccessedLocalPlacement is set by the local execution.
		 * The remote execution shouldn't create any local tasks as the local
		 * execution should have executed all the local tasks. And, we are
		 * ensuring it here.
		 */
		isValidLocalExecutionPath |= !AnyTaskAccessesLocalNode(taskList);

		/*
		 * We might error out later in the execution if it is not suitable
		 * to execute the tasks locally.
		 */
		Assert(isValidLocalExecutionPath);

		/*
		 * TODO: A future improvement could be to keep track of which placements
		 * have been locally executed. At this point, only use local execution
		 * for those placements. That'd help to benefit more from parallelism.
		 */

		return true;
	}

	bool singleTask = (list_length(taskList) == 1);

	if (singleTask && TaskAccessesLocalNode((Task *) linitial(taskList)))
	{
		/*
		 * This is the valuable time to use the local execution. We are likely
		 * to avoid any network round-trips by simply executing the command
		 * within this session.
		 *
		 * We cannot avoid network round trips if the task is not a read only
		 * task and accesses multiple placements. For example, modifications to
		 * distributed tables (with replication factor == 1) would avoid network
		 * round-trips. However, modifications to reference tables still needs
		 * to go to over the network to do the modification on the other placements.
		 * Still, we'll be avoding the network round trip for this node.
		 *
		 * Note that we shouldn't use local execution if any distributed execution
		 * has happened because that'd break transaction visibility rules and
		 * many other things.
		 */
		return !TransactionConnectedToLocalGroup;
	}

	if (!singleTask)
	{
		/*
		 * For multi-task executions, switching to local execution would likely
		 * to perform poorly, because we'd lose the parallelism. Note that the
		 * local execution is happening one task at a time (e.g., similar to
		 * sequential distributed execution).
		 */
		Assert(!TransactionAccessedLocalPlacement);

		return false;
	}

	return false;
}


/*
 * AnyTaskAccessesLocalNode returns true if a task within the task list accesses
 * to the local node.
 */
bool
AnyTaskAccessesLocalNode(List *taskList)
{
	Task *task = NULL;

	foreach_ptr(task, taskList)
	{
		if (TaskAccessesLocalNode(task))
		{
			return true;
		}
	}

	return false;
}


/*
 * TaskAccessesLocalNode returns true if any placements of the task reside on
 * the node that we're executing the query.
 */
bool
TaskAccessesLocalNode(Task *task)
{
	int32 localGroupId = GetLocalGroupId();

	ShardPlacement *taskPlacement = NULL;
	foreach_ptr(taskPlacement, task->taskPlacementList)
	{
		if (taskPlacement->groupId == localGroupId)
		{
			return true;
		}
	}

	return false;
}


/*
 * ErrorIfTransactionAccessedPlacementsLocally errors out if a local query
 * on any shard has already been executed in the same transaction.
 *
 * This check is required because Citus currently hasn't implemented local
 * execution infrastructure for all the commands/executors. As we implement
 * local execution for the command/executor that this function call exists,
 * we should simply  remove the check.
 */
void
ErrorIfTransactionAccessedPlacementsLocally(void)
{
	if (TransactionAccessedLocalPlacement)
	{
		ereport(ERROR,
				(errmsg("cannot execute command because a local execution has "
						"accessed a placement in the transaction"),
				 errhint("Try re-running the transaction with "
						 "\"SET LOCAL citus.enable_local_execution TO OFF;\""),
				 errdetail("Some parallel commands cannot be executed if a "
						   "previous command has already been executed locally")));
	}
}


/*
 * DisableLocalExecution simply a C interface for
 * setting the following:
 *      SET LOCAL citus.enable_local_execution TO off;
 */
void
DisableLocalExecution(void)
{
	set_config_option("citus.enable_local_execution", "off",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
}
