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
 * (4) Execution of multi shards local queries and
 *     remote multi-shard queries within a transaction block
 *
 *      We prefer local execution when we are inside a transaction block, because not using
 *      local execution might create some limitations for other commands in the transaction
 *      block. To simplify things, whenever we are inside a transaction block, we prefer local
 *      execution if possible.
 *
 *  There are also a few limitations/trade-offs that are worth mentioning.
 *  - The local execution on multiple shards might be slow because the execution
 *  has to happen one task at a time (e.g., no parallelism).
 *  - Related with the previous item, COPY command cannot be mixed with local
 *  execution in a transaction. The implication of that is any part of INSERT..SELECT
 *  via coordinator cannot happen via the local execution.
 */
#include "postgres.h"
#include "miscadmin.h"

#include "distributed/pg_version_constants.h"

#include "distributed/commands/utility_hook.h"
#include "distributed/citus_custom_scan.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/query_utils.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/local_plan_cache.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_server_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/remote_commands.h" /* to access LogRemoteCommands */
#include "distributed/transaction_management.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "executor/tstoreReceiver.h"
#include "executor/tuptable.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "optimizer/optimizer.h"
#else
#include "optimizer/planner.h"
#endif
#include "nodes/params.h"
#include "utils/snapmgr.h"

/* controlled via a GUC */
bool EnableLocalExecution = true;
bool LogLocalCommands = false;

static LocalExecutionStatus CurrentLocalExecutionStatus = LOCAL_EXECUTION_OPTIONAL;

static void SplitLocalAndRemotePlacements(List *taskPlacementList,
										  List **localTaskPlacementList,
										  List **remoteTaskPlacementList);
static uint64 ExecuteLocalTaskPlan(PlannedStmt *taskPlan, char *queryString,
								   TupleDestination *tupleDest, Task *task,
								   ParamListInfo paramListInfo);
static int ActivePlacementCount(uint64 shardId);
static void RecordNonDistTableAccessesForTask(Task *task);
static void LogLocalCommand(Task *task);
static uint64 LocallyPlanAndExecuteMultipleQueries(List *queryStrings,
												   TupleDestination *tupleDest,
												   Task *task);
static void ExtractParametersForLocalExecution(ParamListInfo paramListInfo,
											   Oid **parameterTypes,
											   const char ***parameterValues);
static void LocallyExecuteUtilityTask(const char *utilityCommand);
static void LocallyExecuteUdfTaskQuery(Query *localUdfCommandQuery);
static void EnsureTransitionPossible(LocalExecutionStatus from,
									 LocalExecutionStatus to);


/*
 * GetCurrentLocalExecutionStatus returns the current local execution status.
 */
LocalExecutionStatus
GetCurrentLocalExecutionStatus(void)
{
	return CurrentLocalExecutionStatus;
}


/*
 * ExecuteLocalTasks executes the given tasks locally.
 *
 * The function goes over the task list and executes them locally.
 * The returning tuples (if any) is stored in the tupleStoreState.
 *
 * The function returns totalRowsProcessed.
 */
uint64
ExecuteLocalTaskList(List *taskList, TupleDestination *defaultTupleDest)
{
	if (list_length(taskList) == 0)
	{
		return 0;
	}
	DistributedPlan *distributedPlan = NULL;
	ParamListInfo paramListInfo = NULL;
	bool isUtilityCommand = false;
	return ExecuteLocalTaskListExtended(taskList, paramListInfo, distributedPlan,
										defaultTupleDest, isUtilityCommand);
}


/*
 * ExecuteLocalUtilityTaskList executes the given tasks locally.
 *
 * The function returns totalRowsProcessed.
 */
uint64
ExecuteLocalUtilityTaskList(List *utilityTaskList)
{
	if (list_length(utilityTaskList) == 0)
	{
		return 0;
	}
	DistributedPlan *distributedPlan = NULL;
	ParamListInfo paramListInfo = NULL;
	TupleDestination *defaultTupleDest = CreateTupleDestNone();
	bool isUtilityCommand = true;
	return ExecuteLocalTaskListExtended(utilityTaskList, paramListInfo, distributedPlan,
										defaultTupleDest, isUtilityCommand);
}


/*
 * ExecuteLocalTaskListExtended executes the given tasks locally.
 *
 * The function goes over the task list and executes them locally.
 * The returning tuples (if any) is stored in the tupleStoreState.
 *
 * It uses a cached plan if distributedPlan is found in cache.
 *
 * The function returns totalRowsProcessed.
 */
uint64
ExecuteLocalTaskListExtended(List *taskList,
							 ParamListInfo orig_paramListInfo,
							 DistributedPlan *distributedPlan,
							 TupleDestination *defaultTupleDest,
							 bool isUtilityCommand)
{
	ParamListInfo paramListInfo = copyParamList(orig_paramListInfo);
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
		TupleDestination *tupleDest = task->tupleDest ?
									  task->tupleDest :
									  defaultTupleDest;

		/*
		 * If we have a valid shard id, a distributed table will be accessed
		 * during execution. Record it to apply the restrictions related to
		 * local execution.
		 */
		if (task->anchorShardId != INVALID_SHARD_ID)
		{
			SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);
		}
		LogLocalCommand(task);

		if (isUtilityCommand)
		{
			LocallyExecuteUtilityTask(TaskQueryString(task));
			continue;
		}

		PlannedStmt *localPlan = GetCachedLocalPlan(task, distributedPlan);

		/*
		 * If the plan is already cached, don't need to re-plan, just
		 * acquire necessary locks.
		 */
		if (localPlan != NULL)
		{
			Query *jobQuery = distributedPlan->workerJob->jobQuery;
			LOCKMODE lockMode = GetQueryLockMode(jobQuery);

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

			/*
			 * for concatenated strings, we set queryStringList so that we can access
			 * each query string.
			 */
			if (GetTaskQueryType(task) == TASK_QUERY_TEXT_LIST)
			{
				List *queryStringList = task->taskQuery.data.queryStringList;
				totalRowsProcessed +=
					LocallyPlanAndExecuteMultipleQueries(queryStringList, tupleDest,
														 task);
				continue;
			}

			Query *shardQuery = ParseQueryString(TaskQueryString(task),
												 taskParameterTypes,
												 taskNumParams);


			int cursorOptions = CURSOR_OPT_PARALLEL_OK;

			/*
			 * Altough the shardQuery is local to this node, we prefer planner()
			 * over standard_planner(). The primary reason for that is Citus itself
			 * is not very tolarent standard_planner() calls that doesn't go through
			 * distributed_planner() because of the way that restriction hooks are
			 * implemented. So, let planner to call distributed_planner() which
			 * eventually calls standard_planner().
			 */
			localPlan = planner_compat(shardQuery, cursorOptions, paramListInfo);
		}

		char *shardQueryString = NULL;
		if (GetTaskQueryType(task) == TASK_QUERY_TEXT)
		{
			shardQueryString = TaskQueryString(task);
		}
		else
		{
			/* avoid the overhead of deparsing when using local execution */
			shardQueryString = "<optimized out by local execution>";
		}

		totalRowsProcessed +=
			ExecuteLocalTaskPlan(localPlan, shardQueryString,
								 tupleDest, task, paramListInfo);
	}

	return totalRowsProcessed;
}


/*
 * LocallyPlanAndExecuteMultipleQueries plans and executes the given query strings
 * one by one.
 */
static uint64
LocallyPlanAndExecuteMultipleQueries(List *queryStrings, TupleDestination *tupleDest,
									 Task *task)
{
	char *queryString = NULL;
	uint64 totalProcessedRows = 0;
	foreach_ptr(queryString, queryStrings)
	{
		Query *shardQuery = ParseQueryString(queryString,
											 NULL,
											 0);
		int cursorOptions = 0;
		ParamListInfo paramListInfo = NULL;
		PlannedStmt *localPlan = planner_compat(shardQuery, cursorOptions,
												paramListInfo);
		totalProcessedRows += ExecuteLocalTaskPlan(localPlan, queryString,
												   tupleDest, task,
												   paramListInfo);
	}
	return totalProcessedRows;
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
 * LocallyExecuteUtilityTask executes the given local task query in the current
 * session.
 */
static void
LocallyExecuteUtilityTask(const char *localTaskQueryCommand)
{
	List *parseTreeList = pg_parse_query(localTaskQueryCommand);
	RawStmt *localTaskRawStmt = NULL;

	foreach_ptr(localTaskRawStmt, parseTreeList)
	{
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

		if (localTaskPlacementList == NIL)
		{
			*remoteTaskList = lappend(*remoteTaskList, task);
		}
		else if (remoteTaskPlacementList == NIL)
		{
			*localTaskList = lappend(*localTaskList, task);
		}
		else
		{
			/*
			 * At this point, we're dealing with a task that has placements on both
			 * local and remote nodes.
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
				/* since shard replication factor > 1, we should have at least 1 remote task */
				Assert(remoteTaskPlacementList != NIL);
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
ExecuteLocalTaskPlan(PlannedStmt *taskPlan, char *queryString,
					 TupleDestination *tupleDest, Task *task,
					 ParamListInfo paramListInfo)
{
	ScanDirection scanDirection = ForwardScanDirection;
	QueryEnvironment *queryEnv = create_queryEnv();
	int eflags = 0;
	uint64 totalRowsProcessed = 0;
	bool storeTuples = true;

	RecordNonDistTableAccessesForTask(task);

	/*
	 * For modifications to reference tables (or any replicated table in
	 * general), remote execution have already stored the tuples and
	 * incremented es_processed, so skip now.
	 */
	if (task->partiallyLocalOrRemote && !ReadOnlyTask(task->taskType) &&
		ActivePlacementCount(task->anchorShardId) > 1)
	{
		storeTuples = false;
	}

	/*
	 * Use the tupleStore provided by the scanState because it is shared accross
	 * the other task executions and the adaptive executor.
	 */
	DestReceiver *destReceiver = tupleDest && storeTuples ?
								 CreateTupleDestDestReceiver(tupleDest, task,
															 LOCAL_PLACEMENT_INDEX) :
								 CreateDestReceiver(DestNone);

	/* Create a QueryDesc for the query */
	QueryDesc *queryDesc = CreateQueryDesc(taskPlan, queryString,
										   GetActiveSnapshot(), InvalidSnapshot,
										   destReceiver, paramListInfo,
										   queryEnv, 0);

	ExecutorStart(queryDesc, eflags);
	ExecutorRun(queryDesc, scanDirection, 0L, true);

	/*
	 * We'll set the executorState->es_processed later, for now only remember
	 * the count.
	 */
	if (taskPlan->commandType != CMD_SELECT && storeTuples)
	{
		totalRowsProcessed = queryDesc->estate->es_processed;
	}

	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	return totalRowsProcessed;
}


/*
 * ActivePlacementCount gets a shardId and returns the number of
 * active placements. If the input shardId is INVALID_SHARD_ID,
 * the function returns 0.
 */
static int
ActivePlacementCount(uint64 shardId)
{
	if (shardId == INVALID_SHARD_ID)
	{
		return 0;
	}

	List *activeShardPlacementList = ActiveShardPlacementList(shardId);

	return list_length(activeShardPlacementList);
}


/*
 * RecordNonDistTableAccessesForTask records relation accesses for the non-distributed
 * relations that given task will access (if any).
 */
static void
RecordNonDistTableAccessesForTask(Task *task)
{
	List *taskPlacementList = task->taskPlacementList;
	if (list_length(taskPlacementList) == 0)
	{
		/*
		 * We need at least one task placement to record relation access.
		 * FIXME: Unfortunately, it is possible due to
		 * https://github.com/citusdata/citus/issues/4104.
		 * We can safely remove this check when above bug is fixed.
		 */
		return;
	}

	/*
	 * We use only the first placement to find the relation accesses. It is
	 * sufficient as PlacementAccessListForTask iterates relationShardList
	 * field of the task and generates accesses per relation in the task.
	 * As we are only interested in relations, not the placements, we can
	 * skip rest of the placements.
	 * Also, here we don't need to iterate relationShardList field of task
	 * to mark each accessed relation because PlacementAccessListForTask
	 * already computes and returns relations that task accesses.
	 */
	ShardPlacement *taskPlacement = linitial(taskPlacementList);
	List *placementAccessList = PlacementAccessListForTask(task, taskPlacement);

	ShardPlacementAccess *placementAccess = NULL;
	foreach_ptr(placementAccess, placementAccessList)
	{
		uint64 placementAccessShardId = placementAccess->placement->shardId;
		if (placementAccessShardId == INVALID_SHARD_ID)
		{
			/*
			 * When a SELECT prunes down to 0 shard, we still may pass through
			 * the local executor. In that case, we don't need to record any
			 * relation access as we don't actually access any shard placement.
			 */
			continue;
		}

		Oid accessedRelationId = RelationIdForShard(placementAccessShardId);
		ShardPlacementAccessType shardPlacementAccessType = placementAccess->accessType;
		RecordRelationAccessIfNonDistTable(accessedRelationId, shardPlacementAccessType);
	}
}


/*
 * SetLocalExecutionStatus sets the local execution status to
 * the given status, it errors if the transition is not possible from the
 * current status.
 */
void
SetLocalExecutionStatus(LocalExecutionStatus newStatus)
{
	EnsureTransitionPossible(GetCurrentLocalExecutionStatus(), newStatus);

	CurrentLocalExecutionStatus = newStatus;
}


/*
 * EnsureTransitionPossible errors if we cannot switch to the 'to' status
 * from the 'from' status.
 */
static void
EnsureTransitionPossible(LocalExecutionStatus from, LocalExecutionStatus
						 to)
{
	if (from == LOCAL_EXECUTION_REQUIRED && to == LOCAL_EXECUTION_DISABLED)
	{
		ereport(ERROR,
				(errmsg(
					 "cannot switch local execution status from local execution required "
					 "to local execution disabled since it can cause "
					 "visibility problems in the current transaction")));
	}
	if (from == LOCAL_EXECUTION_DISABLED && to == LOCAL_EXECUTION_REQUIRED)
	{
		ereport(ERROR,
				(errmsg(
					 "cannot switch local execution status from local execution disabled "
					 "to local execution enabled since it can cause "
					 "visibility problems in the current transaction")));
	}
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

	if (GetCurrentLocalExecutionStatus() == LOCAL_EXECUTION_DISABLED)
	{
		/*
		 * if the current transaction accessed the local node over a connection
		 * then we can't use local execution because of visibility problems.
		 */
		return false;
	}

	if (GetCurrentLocalExecutionStatus() == LOCAL_EXECUTION_REQUIRED)
	{
		/*
		 * If we already used local execution for a previous command
		 * we should stick to it for read-your-writes policy, this can be a
		 * case when we are inside a transaction block. Such as:
		 *
		 * BEGIN;
		 * some-command; -- executed via local execution
		 * another-command; -- this should be executed via local execution for visibility
		 * COMMIT;
		 *
		 * We may need to use local execution even if we are not inside a transaction block,
		 * however the state will go back to LOCAL_EXECUTION_OPTIONAL at the end of transaction.
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
		return true;
	}

	if (!singleTask)
	{
		/*
		 * For multi-task executions, we prefer to use connections for parallelism,
		 * except when in a multi-statement transaction since there could be other
		 * commands that require local execution.
		 */

		return IsMultiStatementTransaction() && AnyTaskAccessesLocalNode(taskList);
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
	if (GetCurrentLocalExecutionStatus() == LOCAL_EXECUTION_REQUIRED)
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
 * DisableLocalExecution is simply a C interface for setting the following:
 *      SET LOCAL citus.enable_local_execution TO off;
 */
void
DisableLocalExecution(void)
{
	set_config_option("citus.enable_local_execution", "off",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
}
