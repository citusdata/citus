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
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
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
#include "distributed/tuplestore.h"
#include "distributed/listutils.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "executor/tstoreReceiver.h"
#include "fmgr.h"
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

/*
 * If enabled, EXPLAIN ANALYZE output of last worker task are saved in
 * SavedExplainPlan.
 */
static char *SavedExplainPlan = NULL;


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
static StringInfo BuildRemoteExplainQuery(char *queryString, ExplainState *es);
static void ExplainWorkerPlan(PlannedStmt *plannedStmt, DestReceiver *dest,
							  ExplainState *es,
							  const char *queryString, ParamListInfo params,
							  QueryEnvironment *queryEnv,
							  const instr_time *planduration);
static bool ExtractFieldBoolean(Datum jsonbDoc, const char *fieldName, bool defaultValue);
static ExplainFormat ExtractFieldExplainFormat(Datum jsonbDoc, const char *fieldName,
											   ExplainFormat defaultValue);
static bool ExtractFieldJsonbDatum(Datum jsonbDoc, const char *fieldName, Datum *result);

/* Static Explain functions copied from explain.c */
static void ExplainOneQuery(Query *query, int cursorOptions,
							IntoClause *into, ExplainState *es,
							const char *queryString, ParamListInfo params,
							QueryEnvironment *queryEnv);
static double elapsed_time(instr_time *starttime);


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
	StringInfo explainQuery = BuildRemoteExplainQuery(TaskQueryStringForAllPlacements(
														  task),
													  es);

	/*
	 * Use a coordinated transaction to ensure that we open a transaction block
	 * such that we can set a savepoint.
	 */
	UseCoordinatedTransaction();

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

		RemoteTransactionBeginIfNecessary(connection);

		/*
		 * Start a savepoint for the explain query. After running the explain
		 * query, we will rollback to this savepoint. This saves us from side
		 * effects of EXPLAIN ANALYZE on DML queries.
		 */
		ExecuteCriticalRemoteCommand(connection, "SAVEPOINT citus_explain_savepoint");

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

		PQclear(queryResult);
		ForgetResults(connection);

		/* rollback to the savepoint */
		ExecuteCriticalRemoteCommand(connection,
									 "ROLLBACK TO SAVEPOINT citus_explain_savepoint");

		if (remotePlan->explainOutputList != NIL)
		{
			break;
		}
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
BuildRemoteExplainQuery(char *queryString, ExplainState *es)
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
 * worker_last_saved_explain_analyze returns the last saved EXPLAIN ANALYZE output of
 * a worker task query. It returns NULL if nothing has been saved yet.
 */
Datum
worker_last_saved_explain_analyze(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

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
 * worker_save_query_explain_analyze executes and returns results of query while
 * saving its EXPLAIN ANALYZE to be fetched later.
 */
Datum
worker_save_query_explain_analyze(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *queryText = PG_GETARG_TEXT_P(0);
	char *queryString = text_to_cstring(queryText);

	Datum explainOptions = PG_GETARG_DATUM(1);
	ExplainState *es = NewExplainState();
	es->analyze = true;

	/* use the same defaults as NewExplainState() for following options */
	es->buffers = ExtractFieldBoolean(explainOptions, "buffers", es->buffers);
	es->costs = ExtractFieldBoolean(explainOptions, "costs", es->costs);
	es->summary = ExtractFieldBoolean(explainOptions, "summary", es->summary);
	es->verbose = ExtractFieldBoolean(explainOptions, "verbose", es->verbose);
	es->timing = ExtractFieldBoolean(explainOptions, "timing", es->timing);
	es->format = ExtractFieldExplainFormat(explainOptions, "format", es->format);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	DestReceiver *tupleStoreDest = CreateTuplestoreDestReceiver();
	SetTuplestoreDestReceiverParams(tupleStoreDest, tupleStore,
									CurrentMemoryContext, false);

	List *parseTreeList = pg_parse_query(queryString);
	if (list_length(parseTreeList) != 1)
	{
		ereport(ERROR, (errmsg("cannot EXPLAIN ANALYZE multiple queries")));
	}

	RawStmt *parseTree = linitial(parseTreeList);

	List *queryList = pg_analyze_and_rewrite(parseTree, queryString, NULL, 0, NULL);
	if (list_length(queryList) != 1)
	{
		ereport(ERROR, (errmsg("cannot EXPLAIN ANALYZE a query rewritten "
							   "into multiple queries")));
	}

	Query *query = linitial(queryList);

	ExplainBeginOutput(es);

	/* plan query and record planning stats */
	instr_time planStart;
	instr_time planDuration;

	INSTR_TIME_SET_CURRENT(planStart);

	PlannedStmt *plan = pg_plan_query(query, 0, NULL);

	INSTR_TIME_SET_CURRENT(planDuration);
	INSTR_TIME_SUBTRACT(planDuration, planStart);

	/* do the actual EXPLAIN ANALYZE */
	ExplainWorkerPlan(plan, tupleStoreDest, es, queryString, NULL, NULL, &planDuration);

	ExplainEndOutput(es);

	tuplestore_donestoring(tupleStore);

	/* save EXPLAIN ANALYZE result to be fetched later */
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
	FreeSavedExplainPlan();

	SavedExplainPlan = pstrdup(es->str->data);

	MemoryContextSwitchTo(oldContext);

	PG_RETURN_DATUM(0);
}


/*
 * FreeSavedExplainPlan frees allocated saved explain plan if any.
 */
void
FreeSavedExplainPlan(void)
{
	if (SavedExplainPlan)
	{
		pfree(SavedExplainPlan);
		SavedExplainPlan = NULL;
	}
}


/*
 * ExtractFieldBoolean gets value of fieldName from jsonbDoc, or returns
 * defaultValue if it doesn't exist.
 */
static bool
ExtractFieldBoolean(Datum jsonbDoc, const char *fieldName, bool defaultValue)
{
	Datum jsonbDatum = 0;
	bool found = ExtractFieldJsonbDatum(jsonbDoc, fieldName, &jsonbDatum);
	if (!found)
	{
		return defaultValue;
	}

	Datum boolDatum = DirectFunctionCall1(jsonb_bool, jsonbDatum);
	return DatumGetBool(boolDatum);
}


/*
 * ExtractFieldExplainFormat gets value of fieldName from jsonbDoc, or returns
 * defaultValue if it doesn't exist.
 */
static ExplainFormat
ExtractFieldExplainFormat(Datum jsonbDoc, const char *fieldName, ExplainFormat
						  defaultValue)
{
	Datum jsonbDatum = 0;
	bool found = ExtractFieldJsonbDatum(jsonbDoc, fieldName, &jsonbDatum);
	if (!found)
	{
		return defaultValue;
	}

	const char *formatStr = DatumGetCString(DirectFunctionCall1(jsonb_out, jsonbDatum));
	if (pg_strcasecmp(formatStr, "\"text\"") == 0)
	{
		return EXPLAIN_FORMAT_TEXT;
	}
	else if (pg_strcasecmp(formatStr, "\"xml\"") == 0)
	{
		return EXPLAIN_FORMAT_XML;
	}
	else if (pg_strcasecmp(formatStr, "\"yaml\"") == 0)
	{
		return EXPLAIN_FORMAT_YAML;
	}
	else if (pg_strcasecmp(formatStr, "\"json\"") == 0)
	{
		return EXPLAIN_FORMAT_JSON;
	}

	ereport(ERROR, (errmsg("Invalid explain analyze format: %s", formatStr)));
	return 0;
}


/*
 * ExtractFieldJsonbDatum gets value of fieldName from jsonbDoc and puts it
 * into result. If not found, returns false. Otherwise, returns true.
 */
static bool
ExtractFieldJsonbDatum(Datum jsonbDoc, const char *fieldName, Datum *result)
{
	Datum pathArray[1] = { CStringGetTextDatum(fieldName) };
	bool pathNulls[1] = { false };
	bool typeByValue = false;
	char typeAlignment = 0;
	int16 typeLength = 0;
	int dimensions[1] = { 1 };
	int lowerbounds[1] = { 1 };

	get_typlenbyvalalign(TEXTOID, &typeLength, &typeByValue, &typeAlignment);

	ArrayType *pathArrayObject = construct_md_array(pathArray, pathNulls, 1, dimensions,
													lowerbounds, TEXTOID, typeLength,
													typeByValue, typeAlignment);
	Datum pathDatum = PointerGetDatum(pathArrayObject);

	/*
	 * We need to check whether the result of jsonb_extract_path is NULL or not, so use
	 * FunctionCallInvoke() instead of other function call api.
	 *
	 * We cannot use jsonb_path_exists to ensure not-null since it is not available in
	 * postgres 11.
	 */
	FmgrInfo fmgrInfo;
	fmgr_info(JsonbExtractPathFuncId(), &fmgrInfo);

	LOCAL_FCINFO(functionCallInfo, 2);
	InitFunctionCallInfoData(*functionCallInfo, &fmgrInfo, 2, DEFAULT_COLLATION_OID, NULL,
							 NULL);

	fcSetArg(functionCallInfo, 0, jsonbDoc);
	fcSetArg(functionCallInfo, 1, pathDatum);

	*result = FunctionCallInvoke(functionCallInfo);
	return !functionCallInfo->isnull;
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


/*
 * ExplainAnalyzeWorkerPlan produces explain output into es. If es->analyze, it also executes
 * the given plannedStmt and sends the results to dest.
 *
 * This is based on postgres' ExplainOnePlan(). We couldn't use an IntoClause to store results
 * into tupleStore, so we had to copy the same functionality with some minor changes.
 *
 * Keeping the formatting to make comparing with the ExplainOnePlan() easier.
 *
 * TODO: Send a PR to postgres to change ExplainOnePlan's API to use a more generic result
 * destination.
 */
static void
ExplainWorkerPlan(PlannedStmt *plannedstmt, DestReceiver *dest, ExplainState *es,
				  const char *queryString, ParamListInfo params, QueryEnvironment *queryEnv,
				  const instr_time *planduration)
{
	QueryDesc  *queryDesc;
	instr_time	starttime;
	double		totaltime = 0;
	int			eflags;
	int			instrument_option = 0;

	Assert(plannedstmt->commandType != CMD_UTILITY);

	if (es->analyze && es->timing)
		instrument_option |= INSTRUMENT_TIMER;
	else if (es->analyze)
		instrument_option |= INSTRUMENT_ROWS;

	if (es->buffers)
		instrument_option |= INSTRUMENT_BUFFERS;

	/*
	 * We always collect timing for the entire statement, even when node-level
	 * timing is off, so we don't look at es->timing here.  (We could skip
	 * this if !es->summary, but it's hardly worth the complication.)
	 */
	INSTR_TIME_SET_CURRENT(starttime);

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc for the query */
	queryDesc = CreateQueryDesc(plannedstmt, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, params, queryEnv, instrument_option);

	/* Select execution options */
	if (es->analyze)
		eflags = 0;				/* default run-to-completion flags */
	else
		eflags = EXEC_FLAG_EXPLAIN_ONLY;

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, eflags);

	/* Execute the plan for statistics if asked for */
	if (es->analyze)
	{
		ScanDirection dir = ForwardScanDirection;

		/* run the plan */
		ExecutorRun(queryDesc, dir, 0L, true);

		/* run cleanup too */
		ExecutorFinish(queryDesc);

		/* We can't run ExecutorEnd 'till we're done printing the stats... */
		totaltime += elapsed_time(&starttime);
	}

	ExplainOpenGroup("Query", NULL, true, es);

	/* Create textual dump of plan tree */
	ExplainPrintPlan(es, queryDesc);

	if (es->summary && planduration)
	{
		double		plantime = INSTR_TIME_GET_DOUBLE(*planduration);

		ExplainPropertyFloat("Planning Time", "ms", 1000.0 * plantime, 3, es);
	}

	/* Print info about runtime of triggers */
	if (es->analyze)
		ExplainPrintTriggers(es, queryDesc);

	/*
	 * Print info about JITing. Tied to es->costs because we don't want to
	 * display this in regression tests, as it'd cause output differences
	 * depending on build options.  Might want to separate that out from COSTS
	 * at a later stage.
	 */
	if (es->costs)
		ExplainPrintJITSummary(es, queryDesc);

	/*
	 * Close down the query and free resources.  Include time for this in the
	 * total execution time (although it should be pretty minimal).
	 */
	INSTR_TIME_SET_CURRENT(starttime);

	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	/* We need a CCI just in case query expanded to multiple plans */
	if (es->analyze)
		CommandCounterIncrement();

	totaltime += elapsed_time(&starttime);

	/*
	 * We only report execution time if we actually ran the query (that is,
	 * the user specified ANALYZE), and if summary reporting is enabled (the
	 * user can set SUMMARY OFF to not have the timing information included in
	 * the output).  By default, ANALYZE sets SUMMARY to true.
	 */
	if (es->summary && es->analyze)
		ExplainPropertyFloat("Execution Time", "ms", 1000.0 * totaltime, 3,
							 es);

	ExplainCloseGroup("Query", NULL, true, es);
}


/*
 * Compute elapsed time in seconds since given timestamp.
 *
 * Copied from explain.c.
 */
static double
elapsed_time(instr_time *starttime)
{
	instr_time	endtime;

	INSTR_TIME_SET_CURRENT(endtime);
	INSTR_TIME_SUBTRACT(endtime, *starttime);
	return INSTR_TIME_GET_DOUBLE(endtime);
}
