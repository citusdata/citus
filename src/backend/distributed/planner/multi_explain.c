/*-------------------------------------------------------------------------
 *
 * multi_explain.c
 *	  Citus explain support.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/copy.h"
#include "commands/createas.h"
#include "commands/dbcommands.h"
#include "commands/explain.h"
#include "commands/tablecmds.h"
#include "optimizer/cost.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/connection_management.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/insert_select_planner.h"
#include "distributed/insert_select_executor.h"
#include "distributed/listutils.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/remote_commands.h"
#include "distributed/recursive_planning.h"
#include "distributed/placement_connection.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "lib/stringinfo.h"
#include "nodes/plannodes.h"
#include "nodes/primnodes.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "portability/instr_time.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

/* Config variables that enable printing distributed query plans */
bool ExplainDistributedQueries = true;
bool ExplainAllTasks = false;
bool ExplainWorkerQuery = false;

/* struct to save explain flags */
typedef struct
{
	bool verbose;
	bool costs;
	bool buffers;
	bool timing;
	bool summary;
	bool query;
	ExplainFormat format;
} ExplainOptions;

/*
 * When set by worker_save_query_explain_analyze(), EXPLAIN ANALYZE output of
 * worker tasks are saved, and can be fetched by worker_last_saved_explain_analyze().
 */
static bool SaveTaskExplainPlans = false;

/*
 * If enabled, EXPLAIN ANALYZE output of last worker task are saved in
 * SavedExplainPlan.
 */
static char *SavedExplainPlan = NULL;

/*
 * EXPLAIN ANALYZE flags to be used for saving worker query EXPLAIN ANALYZE
 * when SaveTaskExplainPlans is set. These are set by a call to
 * worker_save_query_explain_analyze.
 */
static ExplainOptions WorkerQueryExplainOptions = { 0, 0, 0, 0, 0, EXPLAIN_FORMAT_TEXT };

/* EXPLAIN flags of current distributed explain */
static ExplainOptions CurrentDistributedQueryExplainOptions = {
	0, 0, 0, 0, 0, EXPLAIN_FORMAT_TEXT
};

/* Result for a single remote EXPLAIN command */
typedef struct RemoteExplainPlan
{
	int placementIndex;
	List *explainOutputList;
} RemoteExplainPlan;


/* Explain functions for distributed queries */
static void ExplainSubPlans(DistributedPlan *distributedPlan, ExplainState *es);
static void ExplainJob(Job *job, ExplainState *es);
static void ExplainMapMergeJob(MapMergeJob *mapMergeJob, ExplainState *es);
static void ExplainTaskList(List *taskList, ExplainState *es);
static RemoteExplainPlan * RemoteExplain(Task *task, ExplainState *es);
static void ExplainTask(Task *task, int placementIndex, List *explainOutputList,
						ExplainState *es);
static void ExplainTaskPlacement(ShardPlacement *taskPlacement, List *explainOutputList,
								 ExplainState *es);
static StringInfo BuildRemoteExplainQuery(const char *queryString, ExplainState *es);
static void ExplainAnalyzePreExecutionHook(Task *task, int placementIndex,
										   MultiConnection *connection);
static void ExplainAnalyzePostExecutionHook(Task *task, int placementIndex,
											MultiConnection *connection);
static List * SplitString(StringInfo str, char delimiter);

/* Static Explain functions copied from explain.c */
static void ExplainOneQuery(Query *query, int cursorOptions,
							IntoClause *into, ExplainState *es,
							const char *queryString, ParamListInfo params,
							QueryEnvironment *queryEnv);

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_last_saved_explain_analyze);
PG_FUNCTION_INFO_V1(worker_save_query_explain_analyze);


/*
 * CitusExplainScan is a custom scan explain callback function which is used to
 * print explain information of a Citus plan which includes both master and
 * distributed plan.
 */
void
CitusExplainScan(CustomScanState *node, List *ancestors, struct ExplainState *es)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *distributedPlan = scanState->distributedPlan;

	if (!ExplainDistributedQueries)
	{
		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "explain statements for distributed queries ");
		appendStringInfo(es->str, "are not enabled\n");
		return;
	}

	ExplainOpenGroup("Distributed Query", "Distributed Query", true, es);

	if (distributedPlan->subPlanList != NIL)
	{
		ExplainSubPlans(distributedPlan, es);
	}

	ExplainJob(distributedPlan->workerJob, es);

	ExplainCloseGroup("Distributed Query", "Distributed Query", true, es);
}


/*
 * CoordinatorInsertSelectExplainScan is a custom scan explain callback function
 * which is used to print explain information of a Citus plan for an INSERT INTO
 * distributed_table SELECT ... query that is evaluated on the coordinator.
 */
void
CoordinatorInsertSelectExplainScan(CustomScanState *node, List *ancestors,
								   struct ExplainState *es)
{
	CitusScanState *scanState = (CitusScanState *) node;
	DistributedPlan *distributedPlan = scanState->distributedPlan;
	Query *insertSelectQuery = distributedPlan->insertSelectQuery;
	Query *query = BuildSelectForInsertSelect(insertSelectQuery);
	RangeTblEntry *insertRte = ExtractResultRelationRTE(insertSelectQuery);
	Oid targetRelationId = insertRte->relid;
	IntoClause *into = NULL;
	ParamListInfo params = NULL;
	char *queryString = NULL;
	int cursorOptions = CURSOR_OPT_PARALLEL_OK;

	/*
	 * Make a copy of the query, since pg_plan_query may scribble on it and later
	 * stages of EXPLAIN require it.
	 */
	Query *queryCopy = copyObject(query);
	PlannedStmt *selectPlan = pg_plan_query(queryCopy, cursorOptions, params);
	bool repartition = IsRedistributablePlan(selectPlan->planTree) &&
					   IsSupportedRedistributionTarget(targetRelationId);

	if (es->analyze)
	{
		ereport(ERROR, (errmsg("EXPLAIN ANALYZE is currently not supported for INSERT "
							   "... SELECT commands %s",
							   repartition ? "with repartitioning" : "via coordinator")));
	}

	if (repartition)
	{
		ExplainPropertyText("INSERT/SELECT method", "repartition", es);
	}
	else
	{
		ExplainPropertyText("INSERT/SELECT method", "pull to coordinator", es);
	}

	ExplainOpenGroup("Select Query", "Select Query", false, es);

	/* explain the inner SELECT query */
	ExplainOneQuery(query, 0, into, es, queryString, params, NULL);

	ExplainCloseGroup("Select Query", "Select Query", false, es);
}


/*
 * ExplainSubPlans generates EXPLAIN output for subplans for CTEs
 * and complex subqueries. Because the planning for these queries
 * is done along with the top-level plan, we cannot determine the
 * planning time and set it to 0.
 */
static void
ExplainSubPlans(DistributedPlan *distributedPlan, ExplainState *es)
{
	ListCell *subPlanCell = NULL;
	uint64 planId = distributedPlan->planId;

	ExplainOpenGroup("Subplans", "Subplans", false, es);

	foreach(subPlanCell, distributedPlan->subPlanList)
	{
		DistributedSubPlan *subPlan = (DistributedSubPlan *) lfirst(subPlanCell);
		PlannedStmt *plan = subPlan->plan;
		IntoClause *into = NULL;
		ParamListInfo params = NULL;
		char *queryString = NULL;
		instr_time planduration;

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			char *resultId = GenerateResultId(planId, subPlan->subPlanId);

			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "->  Distributed Subplan %s\n", resultId);
			es->indent += 3;
		}

		INSTR_TIME_SET_ZERO(planduration);

		ExplainOnePlan(plan, into, es, queryString, params, NULL, &planduration);

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			es->indent -= 3;
		}
	}

	ExplainCloseGroup("Subplans", "Subplans", false, es);
}


/*
 * ExplainJob shows the EXPLAIN output for a Job in the physical plan of
 * a distributed query by showing the remote EXPLAIN for the first task,
 * or all tasks if citus.explain_all_tasks is on.
 */
static void
ExplainJob(Job *job, ExplainState *es)
{
	List *dependentJobList = job->dependentJobList;
	int dependentJobCount = list_length(dependentJobList);
	ListCell *dependentJobCell = NULL;
	List *taskList = job->taskList;
	int taskCount = list_length(taskList);

	ExplainOpenGroup("Job", "Job", true, es);

	ExplainPropertyInteger("Task Count", NULL, taskCount, es);

	if (dependentJobCount > 0)
	{
		ExplainPropertyText("Tasks Shown", "None, not supported for re-partition "
										   "queries", es);
	}
	else if (ExplainAllTasks || taskCount <= 1)
	{
		ExplainPropertyText("Tasks Shown", "All", es);
	}
	else
	{
		StringInfo tasksShownText = makeStringInfo();
		appendStringInfo(tasksShownText, "One of %d", taskCount);

		ExplainPropertyText("Tasks Shown", tasksShownText->data, es);
	}

	/*
	 * We cannot fetch EXPLAIN plans for jobs that have dependencies, since the
	 * intermediate tables have not been created.
	 */
	if (dependentJobCount == 0)
	{
		ExplainOpenGroup("Tasks", "Tasks", false, es);

		ExplainTaskList(taskList, es);

		ExplainCloseGroup("Tasks", "Tasks", false, es);
	}
	else
	{
		ExplainOpenGroup("Dependent Jobs", "Dependent Jobs", false, es);

		/* show explain output for dependent jobs, if any */
		foreach(dependentJobCell, dependentJobList)
		{
			Job *dependentJob = (Job *) lfirst(dependentJobCell);

			if (CitusIsA(dependentJob, MapMergeJob))
			{
				ExplainMapMergeJob((MapMergeJob *) dependentJob, es);
			}
		}

		ExplainCloseGroup("Dependent Jobs", "Dependent Jobs", false, es);
	}

	ExplainCloseGroup("Job", "Job", true, es);
}


/*
 * ExplainMapMergeJob shows a very basic EXPLAIN plan for a MapMergeJob. It does
 * not yet show the EXPLAIN plan for the individual tasks, because this requires
 * specific logic for getting the query (which is wrapped in a UDF), and the
 * queries may use intermediate tables that have not been created.
 */
static void
ExplainMapMergeJob(MapMergeJob *mapMergeJob, ExplainState *es)
{
	List *dependentJobList = mapMergeJob->job.dependentJobList;
	int dependentJobCount = list_length(dependentJobList);
	ListCell *dependentJobCell = NULL;
	int mapTaskCount = list_length(mapMergeJob->mapTaskList);
	int mergeTaskCount = list_length(mapMergeJob->mergeTaskList);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "->  MapMergeJob\n");
		es->indent += 3;
	}

	ExplainOpenGroup("MapMergeJob", NULL, true, es);
	ExplainPropertyInteger("Map Task Count", NULL, mapTaskCount, es);
	ExplainPropertyInteger("Merge Task Count", NULL, mergeTaskCount, es);

	if (dependentJobCount > 0)
	{
		ExplainOpenGroup("Dependent Jobs", "Dependent Jobs", false, es);

		foreach(dependentJobCell, dependentJobList)
		{
			Job *dependentJob = (Job *) lfirst(dependentJobCell);

			if (CitusIsA(dependentJob, MapMergeJob))
			{
				ExplainMapMergeJob((MapMergeJob *) dependentJob, es);
			}
		}

		ExplainCloseGroup("Dependent Jobs", "Dependent Jobs", false, es);
	}

	ExplainCloseGroup("MapMergeJob", NULL, true, es);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		es->indent -= 3;
	}
}


/*
 * ExplainTaskList shows the remote EXPLAIN for the first task in taskList,
 * or all tasks if citus.explain_all_tasks is on.
 */
static void
ExplainTaskList(List *taskList, ExplainState *es)
{
	ListCell *taskCell = NULL;
	ListCell *remoteExplainCell = NULL;
	List *remoteExplainList = NIL;

	/* make sure that the output is consistent */
	taskList = SortList(taskList, CompareTasksByTaskId);

	foreach(taskCell, taskList)
	{
		Task *task = (Task *) lfirst(taskCell);

		RemoteExplainPlan *remoteExplain = RemoteExplain(task, es);
		remoteExplainList = lappend(remoteExplainList, remoteExplain);

		if (!ExplainAllTasks)
		{
			break;
		}
	}

	forboth(taskCell, taskList, remoteExplainCell, remoteExplainList)
	{
		Task *task = (Task *) lfirst(taskCell);
		RemoteExplainPlan *remoteExplain =
			(RemoteExplainPlan *) lfirst(remoteExplainCell);

		ExplainTask(task, remoteExplain->placementIndex,
					remoteExplain->explainOutputList, es);
	}
}


/*
 * RemoteExplain fetches the remote EXPLAIN output for a single
 * task. It tries each shard placement until one succeeds or all
 * failed.
 */
static RemoteExplainPlan *
RemoteExplain(Task *task, ExplainState *es)
{
	List *taskPlacementList = task->taskPlacementList;
	int placementCount = list_length(taskPlacementList);

	RemoteExplainPlan *remotePlan = (RemoteExplainPlan *) palloc0(
		sizeof(RemoteExplainPlan));

	if (es->analyze)
	{
		Assert(task->savedPlan);

		/*
		 * Similar to postgres' ExplainQuery(), we split by newline only for
		 * text format.
		 */
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			remotePlan->explainOutputList = SplitString(task->savedPlan, '\n');
		}
		else
		{
			remotePlan->explainOutputList = list_make1(task->savedPlan);
		}

		remotePlan->placementIndex = task->savedPlanPlacementIndex;

		return remotePlan;
	}

	const char *queryText = TaskQueryStringForAllPlacements(task);
	StringInfo explainQuery = BuildRemoteExplainQuery(queryText, es);

	for (int placementIndex = 0; placementIndex < placementCount; placementIndex++)
	{
		ShardPlacement *taskPlacement = list_nth(taskPlacementList, placementIndex);
		PGresult *queryResult = NULL;
		int connectionFlags = 0;

		remotePlan->placementIndex = placementIndex;

		MultiConnection *connection = GetPlacementConnection(connectionFlags,
															 taskPlacement, NULL);

		/* try other placements if we fail to connect this one */
		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			continue;
		}

		/* run explain query */
		int executeResult = ExecuteOptionalRemoteCommand(connection, explainQuery->data,
														 &queryResult);
		if (executeResult != 0)
		{
			PQclear(queryResult);
			ForgetResults(connection);

			continue;
		}

		/* read explain query results */
		remotePlan->explainOutputList = ReadFirstColumnAsText(queryResult);

		/* if requested, add query text to explain output */
		if (ExplainWorkerQuery)
		{
			StringInfo queryTextStr = makeStringInfo();
			appendStringInfo(queryTextStr, "Query Text: %s", queryText);

			remotePlan->explainOutputList = lcons(queryTextStr,
												  remotePlan->explainOutputList);
		}

		PQclear(queryResult);
		ForgetResults(connection);

		break;
	}

	return remotePlan;
}


/*
 * ExplainTask shows the EXPLAIN output for an single task. The output has been
 * fetched from the placement at index placementIndex. If explainOutputList is NIL,
 * then the EXPLAIN output could not be fetched from any placement.
 */
static void
ExplainTask(Task *task, int placementIndex, List *explainOutputList, ExplainState *es)
{
	ExplainOpenGroup("Task", NULL, true, es);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "->  Task\n");
		es->indent += 3;
	}

	if (explainOutputList != NIL)
	{
		List *taskPlacementList = task->taskPlacementList;
		ShardPlacement *taskPlacement = list_nth(taskPlacementList, placementIndex);

		ExplainTaskPlacement(taskPlacement, explainOutputList, es);
	}
	else
	{
		ExplainPropertyText("Error", "Could not get remote plan.", es);
	}

	ExplainCloseGroup("Task", NULL, true, es);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		es->indent -= 3;
	}
}


/*
 * ExplainTaskPlacement shows the EXPLAIN output for an individual task placement.
 * It corrects the indentation of the remote explain output to match the local
 * output.
 */
static void
ExplainTaskPlacement(ShardPlacement *taskPlacement, List *explainOutputList,
					 ExplainState *es)
{
	int savedIndentation = es->indent;
	StringInfo nodeAddress = makeStringInfo();
	char *nodeName = taskPlacement->nodeName;
	uint32 nodePort = taskPlacement->nodePort;
	const char *nodeDatabase = CurrentDatabaseName();
	ListCell *explainOutputCell = NULL;
	int rowIndex = 0;

	appendStringInfo(nodeAddress, "host=%s port=%d dbname=%s", nodeName, nodePort,
					 nodeDatabase);
	ExplainPropertyText("Node", nodeAddress->data, es);

	ExplainOpenGroup("Remote Plan", "Remote Plan", false, es);

	if (es->format == EXPLAIN_FORMAT_JSON || es->format == EXPLAIN_FORMAT_YAML)
	{
		/* prevent appending the remote EXPLAIN on the same line */
		appendStringInfoChar(es->str, '\n');
	}

	foreach(explainOutputCell, explainOutputList)
	{
		StringInfo rowString = (StringInfo) lfirst(explainOutputCell);

		int rowLength = strlen(rowString->data);
		char *lineStart = rowString->data;

		/* parse the lines in the remote EXPLAIN for proper indentation */
		while (lineStart < rowString->data + rowLength)
		{
			/* find the end-of-line */
			char *lineEnd = strchr(lineStart, '\n');

			if (lineEnd == NULL)
			{
				/* no end-of-line, use end of row string instead */
				lineEnd = rowString->data + rowLength;
			}

			/* convert line to a separate string */
			*lineEnd = '\0';

			/* indentation that is applied to all lines */
			appendStringInfoSpaces(es->str, es->indent * 2);

			if (es->format == EXPLAIN_FORMAT_TEXT && rowIndex == 0)
			{
				/* indent the first line of the remote plan with an arrow */
				appendStringInfoString(es->str, "->  ");
				es->indent += 2;
			}

			/* show line in the output */
			appendStringInfo(es->str, "%s\n", lineStart);

			/* continue at the start of the next line */
			lineStart = lineEnd + 1;
		}

		rowIndex++;
	}

	ExplainCloseGroup("Remote Plan", "Remote Plan", false, es);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		es->indent = savedIndentation;
	}
}


/*
 * BuildRemoteExplainQuery returns an EXPLAIN query string
 * to run on a worker node which explicitly contains all
 * the options in the explain state.
 */
static StringInfo
BuildRemoteExplainQuery(const char *queryString, ExplainState *es)
{
	StringInfo explainQuery = makeStringInfo();
	char *formatStr = NULL;

	switch (es->format)
	{
		case EXPLAIN_FORMAT_XML:
		{
			formatStr = "XML";
			break;
		}

		case EXPLAIN_FORMAT_JSON:
		{
			formatStr = "JSON";
			break;
		}

		case EXPLAIN_FORMAT_YAML:
		{
			formatStr = "YAML";
			break;
		}

		default:
		{
			formatStr = "TEXT";
			break;
		}
	}

	appendStringInfo(explainQuery,
					 "EXPLAIN (ANALYZE %s, VERBOSE %s, "
					 "COSTS %s, BUFFERS %s, TIMING %s, SUMMARY %s, "
					 "FORMAT %s) %s",
					 es->analyze ? "TRUE" : "FALSE",
					 es->verbose ? "TRUE" : "FALSE",
					 es->costs ? "TRUE" : "FALSE",
					 es->buffers ? "TRUE" : "FALSE",
					 es->timing ? "TRUE" : "FALSE",
					 es->summary ? "TRUE" : "FALSE",
					 formatStr,
					 queryString);

	return explainQuery;
}


/*
 * ShouldSaveWorkerQueryExplainAnalyze returns true if we should save the EXPLAIN
 * ANALYZE output of given query while executing it.
 */
bool
ShouldSaveWorkerQueryExplainAnalyze(QueryDesc *queryDesc)
{
	PlannedStmt *plannedStmt = queryDesc->plannedstmt;

	return SaveTaskExplainPlans &&
		   ExecutorLevel == 0 &&
		   !IsParallelWorker() &&
		   !IsCitusPlan(plannedStmt->planTree);
}


/*
 * SaveQueryExplain saves the EXPLAIN ANALYZE output of an already executed
 * and instrumented query. This can be retrieved later by calling
 * worker_worker_last_saved_explain_analyze.
 */
void
SaveQueryExplainAnalyze(QueryDesc *queryDesc)
{
	/*
	 * Make sure stats accumulation is done.  (Note: it's okay if several
	 * levels of hook all do this.)
	 */
	InstrEndLoop(queryDesc->totaltime);

	ExplainState *es = NewExplainState();

	es->analyze = true;
	es->verbose = WorkerQueryExplainOptions.verbose;
	es->buffers = WorkerQueryExplainOptions.buffers;
	es->timing = WorkerQueryExplainOptions.timing;
	es->summary = WorkerQueryExplainOptions.summary;
	es->format = WorkerQueryExplainOptions.format;
	es->costs = WorkerQueryExplainOptions.costs;

	ExplainBeginOutput(es);

	if (WorkerQueryExplainOptions.query)
	{
		ExplainQueryText(es, queryDesc);
	}

	ExplainPrintPlan(es, queryDesc);

	if (es->costs)
	{
		ExplainPrintJITSummary(es, queryDesc);
	}

	ExplainEndOutput(es);

	/*
	 * We don't always send BEGIN/END to workers for all queries. Changing
	 * transaction management for EXPLAIN ANALYZE can change their runtime
	 * metrics, so we don't want to change it for more realistic EXPLAIN
	 * ANALYZE.
	 *
	 * Therefore, SavedExplainPlan might be fetched in a different transaction
	 * than it was created at. So we allocate it in TopMemoryContext so it
	 * survives transaction boundaries.
	 */
	MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);

	if (SavedExplainPlan != NULL)
	{
		pfree(SavedExplainPlan);
	}

	SavedExplainPlan = pstrdup(es->str->data);

	MemoryContextSwitchTo(oldContext);

	pfree(es->str->data);
}


/*
 * RequestedForExplainAnalyze returns true if we should get the EXPLAIN ANALYZE
 * output for the given custom scan node.
 */
bool
RequestedForExplainAnalyze(CustomScanState *node)
{
	return (node->ss.ps.state->es_instrument != 0);
}


/*
 * InstallExplainAnalyzeHooks installs hooks on given tasks so EXPLAIN ANALYZE
 * output of them are saved & fetched from workers.
 */
void
InstallExplainAnalyzeHooks(List *taskList)
{
	Task *task = NULL;
	foreach_ptr(task, taskList)
	{
		task->preExecutionHook = ExplainAnalyzePreExecutionHook;
		task->postExecutionHook = ExplainAnalyzePostExecutionHook;
	}
}


/*
 * ExplainAnalyzePreExecutionHook implements task->preExecutionHook and sends
 * EXPLAIN ANALYZE params over the given connection.
 */
static void
ExplainAnalyzePreExecutionHook(Task *task, int placementIndex,
							   MultiConnection *connection)
{
	StringInfo query = makeStringInfo();
	appendStringInfo(query,
					 "SELECT worker_save_query_explain_analyze(true, %s, %s, %s, %s, %s, %d)",
					 CurrentDistributedQueryExplainOptions.verbose ? "true" : "false",
					 CurrentDistributedQueryExplainOptions.costs ? "true" : "false",
					 CurrentDistributedQueryExplainOptions.timing ? "true" : "false",
					 CurrentDistributedQueryExplainOptions.summary ? "true" : "false",
					 CurrentDistributedQueryExplainOptions.query ? "true" : "false",
					 (int) CurrentDistributedQueryExplainOptions.format);

	ExecuteCriticalRemoteCommand(connection, query->data);
}


/*
 * ExplainAnalyzePostExecutionHook implements task->postExecutionHook and fetches the task's
 * EXPLAIN ANALYZE output from the worker.
 */
static void
ExplainAnalyzePostExecutionHook(Task *task, int placementIndex,
								MultiConnection *connection)
{
	PGresult *planResult = NULL;
	const char *fetchQuery = "SELECT worker_last_saved_explain_analyze()";

	int execResult = ExecuteOptionalRemoteCommand(connection, fetchQuery, &planResult);
	if (execResult == RESPONSE_OKAY)
	{
		List *planList = ReadFirstColumnAsText(planResult);
		StringInfo remotePlan = (StringInfo) linitial(planList);

		task->savedPlan = makeStringInfo();
		appendStringInfoString(task->savedPlan, remotePlan->data);

		task->savedPlanPlacementIndex = placementIndex;

		PQclear(planResult);
	}
	else
	{
		ereport(ERROR, (errmsg("failed to fetch remote task's EXPLAIN ANALYZE")));
	}

	ClearResults(connection, false);

	/*
	 * Disable worker EXPLAIN/ANALYZE, so we don't do save EXPLAIN ANALYZE output
	 * in workers for subsequent queries if not asked to.
	 */
	const char *resetQuery = "SELECT worker_save_query_explain_analyze("
							 "false, false, false, false, false, false, 0)";
	ExecuteCriticalRemoteCommand(connection, resetQuery);
}


/*
 * worker_last_saved_explain_analyze returns the last saved EXPLAIN ANALYZE output of
 * a worker task query. It returns NULL if nothing have been saved yet.
 */
Datum
worker_last_saved_explain_analyze(PG_FUNCTION_ARGS)
{
	if (SavedExplainPlan == NULL)
	{
		PG_RETURN_NULL();
	}
	else
	{
		PG_RETURN_TEXT_P(cstring_to_text(SavedExplainPlan));
	}
}


/*
 * worker_save_query_explain_analyze enables/disables saving of EXPLAIN ANALYZE
 * output for worker task queries, and also sets EXPLAIN flags.
 */
Datum
worker_save_query_explain_analyze(PG_FUNCTION_ARGS)
{
	SaveTaskExplainPlans = PG_GETARG_BOOL(0);
	WorkerQueryExplainOptions.verbose = PG_GETARG_BOOL(1);
	WorkerQueryExplainOptions.costs = PG_GETARG_BOOL(2);
	WorkerQueryExplainOptions.timing = PG_GETARG_BOOL(3);
	WorkerQueryExplainOptions.summary = PG_GETARG_BOOL(4);
	WorkerQueryExplainOptions.query = PG_GETARG_BOOL(5);
	WorkerQueryExplainOptions.format = (ExplainFormat) PG_GETARG_INT32(6);

	PG_RETURN_VOID();
}


/*
 * CitusExplainOneQuery is the executor hook that is called when
 * postgres wants to explain a query.
 */
void
CitusExplainOneQuery(Query *query, int cursorOptions, IntoClause *into,
					 ExplainState *es, const char *queryString, ParamListInfo params,
					 QueryEnvironment *queryEnv)
{
	/* save the flags of current EXPLAIN command */
	CurrentDistributedQueryExplainOptions.costs = es->costs;
	CurrentDistributedQueryExplainOptions.buffers = es->buffers;
	CurrentDistributedQueryExplainOptions.verbose = es->verbose;
	CurrentDistributedQueryExplainOptions.summary = es->summary;
	CurrentDistributedQueryExplainOptions.timing = es->timing;
	CurrentDistributedQueryExplainOptions.format = es->format;
	CurrentDistributedQueryExplainOptions.query = ExplainWorkerQuery;

	/* rest is copied from ExplainOneQuery() */
	instr_time planstart,
			   planduration;

	INSTR_TIME_SET_CURRENT(planstart);

	/* plan the query */
	PlannedStmt *plan = pg_plan_query(query, cursorOptions, params);

	INSTR_TIME_SET_CURRENT(planduration);
	INSTR_TIME_SUBTRACT(planduration, planstart);

	/* run it (if needed) and produce output */
	ExplainOnePlan(plan, into, es, queryString, params, queryEnv,
				   &planduration);
}


/*
 * SplitString splits the given string by the given delimiter.
 *
 * Why not use strtok_s()? Its signature and semantics are difficult to understand.
 *
 * Why not use strchr() (similar to do_text_output_multiline)? Although not banned,
 * it isn't safe if by any chance str is not null-terminated.
 */
static List *
SplitString(StringInfo str, char delimiter)
{
	if (str->len == 0)
	{
		return NIL;
	}

	List *tokenList = NIL;
	StringInfo token = makeStringInfo();

	for (size_t index = 0; index < str->len; index++)
	{
		if (str->data[index] == delimiter)
		{
			tokenList = lappend(tokenList, token);
			token = makeStringInfo();
		}
		else
		{
			appendStringInfoChar(token, str->data[index]);
		}
	}

	/* append last token */
	tokenList = lappend(tokenList, token);

	return tokenList;
}


/* below are private functions copied from explain.c */


/* *INDENT-OFF* */
/*
 * ExplainOneQuery -
 *	  print out the execution plan for one Query
 *
 * "into" is NULL unless we are explaining the contents of a CreateTableAsStmt.
 */
static void
ExplainOneQuery(Query *query, int cursorOptions,
				IntoClause *into, ExplainState *es,
				const char *queryString, ParamListInfo params,
				QueryEnvironment *queryEnv)
{
	/* if an advisor plugin is present, let it manage things */
	if (ExplainOneQuery_hook)
	{
		(*ExplainOneQuery_hook) (query, cursorOptions, into, es,
								 queryString, params, queryEnv);
	}
	else
	{
		instr_time	planstart,
					planduration;

		INSTR_TIME_SET_CURRENT(planstart);

		/* plan the query */
		PlannedStmt *plan = pg_plan_query(query, cursorOptions, params);

		INSTR_TIME_SET_CURRENT(planduration);
		INSTR_TIME_SUBTRACT(planduration, planstart);

		/* run it (if needed) and produce output */
		ExplainOnePlan(plan, into, es, queryString, params, queryEnv,
					   &planduration);
	}
}
