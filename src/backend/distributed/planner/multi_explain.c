/*-------------------------------------------------------------------------
 *
 * multi_explain.c
 *	  Citus explain support.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
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
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_physical_planner.h"
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


/* OR-able flags for ExplainXMLTag() (explain.c) */
#define X_OPENING 0
#define X_CLOSING 1
#define X_CLOSE_IMMEDIATE 2
#define X_NOWHITESPACE 4


/* Config variables that enable printing distributed query plans */
bool ExplainDistributedQueries = true;
bool ExplainAllTasks = false;


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

/* Static Explain functions copied from explain.c */
static void ExplainOneQuery(Query *query, int cursorOptions,
							IntoClause *into, ExplainState *es,
							const char *queryString, ParamListInfo params,
							QueryEnvironment *queryEnv);
#if (PG_VERSION_NUM < 110000)
static void ExplainOpenGroup(const char *objtype, const char *labelname,
							 bool labeled, ExplainState *es);
static void ExplainCloseGroup(const char *objtype, const char *labelname,
							  bool labeled, ExplainState *es);
static void ExplainXMLTag(const char *tagname, int flags, ExplainState *es);
static void ExplainJSONLineEnding(ExplainState *es);
static void ExplainYAMLLineStarting(ExplainState *es);
#endif


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
	Query *query = distributedPlan->insertSelectSubquery;
	IntoClause *into = NULL;
	ParamListInfo params = NULL;
	char *queryString = NULL;

	if (es->analyze)
	{
		/* avoiding double execution here is tricky, error out for now */
		ereport(ERROR, (errmsg("EXPLAIN ANALYZE is currently not supported for INSERT "
							   "... SELECT commands via the coordinator")));
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

		/* set the planning time to 0 */
		INSTR_TIME_SET_CURRENT(planduration);
		INSTR_TIME_SUBTRACT(planduration, planduration);

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
	List *dependedJobList = job->dependedJobList;
	int dependedJobCount = list_length(dependedJobList);
	ListCell *dependedJobCell = NULL;
	List *taskList = job->taskList;
	int taskCount = list_length(taskList);

	ExplainOpenGroup("Job", "Job", true, es);

	ExplainPropertyIntegerInternal("Task Count", NULL, taskCount, es);

	if (dependedJobCount > 0)
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
	if (dependedJobCount == 0)
	{
		ExplainOpenGroup("Tasks", "Tasks", false, es);

		ExplainTaskList(taskList, es);

		ExplainCloseGroup("Tasks", "Tasks", false, es);
	}
	else
	{
		ExplainOpenGroup("Depended Jobs", "Depended Jobs", false, es);

		/* show explain output for depended jobs, if any */
		foreach(dependedJobCell, dependedJobList)
		{
			Job *dependedJob = (Job *) lfirst(dependedJobCell);

			if (CitusIsA(dependedJob, MapMergeJob))
			{
				ExplainMapMergeJob((MapMergeJob *) dependedJob, es);
			}
		}

		ExplainCloseGroup("Depended Jobs", "Depended Jobs", false, es);
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
	List *dependedJobList = mapMergeJob->job.dependedJobList;
	int dependedJobCount = list_length(dependedJobList);
	ListCell *dependedJobCell = NULL;
	int mapTaskCount = list_length(mapMergeJob->mapTaskList);
	int mergeTaskCount = list_length(mapMergeJob->mergeTaskList);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "->  MapMergeJob\n");
		es->indent += 3;
	}

	ExplainOpenGroup("MapMergeJob", NULL, true, es);
	ExplainPropertyIntegerInternal("Map Task Count", NULL, mapTaskCount, es);
	ExplainPropertyIntegerInternal("Merge Task Count", NULL, mergeTaskCount, es);

	if (dependedJobCount > 0)
	{
		ExplainOpenGroup("Depended Jobs", "Depended Jobs", false, es);

		foreach(dependedJobCell, dependedJobList)
		{
			Job *dependedJob = (Job *) lfirst(dependedJobCell);

			if (CitusIsA(dependedJob, MapMergeJob))
			{
				ExplainMapMergeJob((MapMergeJob *) dependedJob, es);
			}
		}

		ExplainCloseGroup("Depended Jobs", "Depended Jobs", false, es);
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
		RemoteExplainPlan *remoteExplain = NULL;

		remoteExplain = RemoteExplain(task, es);
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
 * RemoteExplain fetches the the remote EXPLAIN output for a single
 * task. It tries each shard placement until one succeeds or all
 * failed.
 */
static RemoteExplainPlan *
RemoteExplain(Task *task, ExplainState *es)
{
	StringInfo explainQuery = NULL;
	List *taskPlacementList = task->taskPlacementList;
	int placementCount = list_length(taskPlacementList);
	int placementIndex = 0;
	RemoteExplainPlan *remotePlan = NULL;

	remotePlan = (RemoteExplainPlan *) palloc0(sizeof(RemoteExplainPlan));
	explainQuery = BuildRemoteExplainQuery(task->queryString, es);

	/*
	 * Use a coordinated transaction to ensure that we open a transaction block
	 * such that we can set a savepoint.
	 */
	BeginOrContinueCoordinatedTransaction();

	for (placementIndex = 0; placementIndex < placementCount; placementIndex++)
	{
		ShardPlacement *taskPlacement = list_nth(taskPlacementList, placementIndex);
		MultiConnection *connection = NULL;
		PGresult *queryResult = NULL;
		int connectionFlags = 0;
		int executeResult = 0;

		remotePlan->placementIndex = placementIndex;

		connection = GetPlacementConnection(connectionFlags, taskPlacement, NULL);

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
		executeResult = ExecuteOptionalRemoteCommand(connection, explainQuery->data,
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
	char *nodeDatabase = CurrentDatabaseName();
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
		int rowLength = 0;
		char *lineStart = NULL;

		rowLength = strlen(rowString->data);
		lineStart = rowString->data;

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
					 "COSTS %s, BUFFERS %s, TIMING %s, "
					 "FORMAT %s) %s",
					 es->analyze ? "TRUE" : "FALSE",
					 es->verbose ? "TRUE" : "FALSE",
					 es->costs ? "TRUE" : "FALSE",
					 es->buffers ? "TRUE" : "FALSE",
					 es->timing ? "TRUE" : "FALSE",
					 formatStr,
					 queryString);

	return explainQuery;
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
#if (PG_VERSION_NUM >= 110000)
		(*ExplainOneQuery_hook) (query, cursorOptions, into, es,
								 queryString, params, queryEnv);
#elif (PG_VERSION_NUM >= 100000)
		(*ExplainOneQuery_hook) (query, cursorOptions, into, es,
								 queryString, params);
#endif
	else
	{
		PlannedStmt *plan;
		instr_time	planstart,
					planduration;

		INSTR_TIME_SET_CURRENT(planstart);

		/* plan the query */
		plan = pg_plan_query(query, cursorOptions, params);

		INSTR_TIME_SET_CURRENT(planduration);
		INSTR_TIME_SUBTRACT(planduration, planstart);

		/* run it (if needed) and produce output */
		ExplainOnePlan(plan, into, es, queryString, params, queryEnv,
					   &planduration);
	}
}

#if (PG_VERSION_NUM < 110000)
/*
 * Open a group of related objects.
 *
 * objtype is the type of the group object, labelname is its label within
 * a containing object (if any).
 *
 * If labeled is true, the group members will be labeled properties,
 * while if it's false, they'll be unlabeled objects.
 */
static void
ExplainOpenGroup(const char *objtype, const char *labelname,
				 bool labeled, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			ExplainXMLTag(objtype, X_OPENING, es);
			es->indent++;
			break;

		case EXPLAIN_FORMAT_JSON:
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, 2 * es->indent);
			if (labelname)
			{
				escape_json(es->str, labelname);
				appendStringInfoString(es->str, ": ");
			}
			appendStringInfoChar(es->str, labeled ? '{' : '[');

			/*
			 * In JSON format, the grouping_stack is an integer list.  0 means
			 * we've emitted nothing at this grouping level, 1 means we've
			 * emitted something (and so the next item needs a comma). See
			 * ExplainJSONLineEnding().
			 */
			es->grouping_stack = lcons_int(0, es->grouping_stack);
			es->indent++;
			break;

		case EXPLAIN_FORMAT_YAML:

			/*
			 * In YAML format, the grouping stack is an integer list.  0 means
			 * we've emitted nothing at this grouping level AND this grouping
			 * level is unlabelled and must be marked with "- ".  See
			 * ExplainYAMLLineStarting().
			 */
			ExplainYAMLLineStarting(es);
			if (labelname)
			{
				appendStringInfo(es->str, "%s: ", labelname);
				es->grouping_stack = lcons_int(1, es->grouping_stack);
			}
			else
			{
				appendStringInfoString(es->str, "- ");
				es->grouping_stack = lcons_int(0, es->grouping_stack);
			}
			es->indent++;
			break;
	}
}


/*
 * Close a group of related objects.
 * Parameters must match the corresponding ExplainOpenGroup call.
 */
static void
ExplainCloseGroup(const char *objtype, const char *labelname,
				  bool labeled, ExplainState *es)
{
	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			/* nothing to do */
			break;

		case EXPLAIN_FORMAT_XML:
			es->indent--;
			ExplainXMLTag(objtype, X_CLOSING, es);
			break;

		case EXPLAIN_FORMAT_JSON:
			es->indent--;
			appendStringInfoChar(es->str, '\n');
			appendStringInfoSpaces(es->str, 2 * es->indent);
			appendStringInfoChar(es->str, labeled ? '}' : ']');
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;

		case EXPLAIN_FORMAT_YAML:
			es->indent--;
			es->grouping_stack = list_delete_first(es->grouping_stack);
			break;
	}
}


/*
 * Emit opening or closing XML tag.
 *
 * "flags" must contain X_OPENING, X_CLOSING, or X_CLOSE_IMMEDIATE.
 * Optionally, OR in X_NOWHITESPACE to suppress the whitespace we'd normally
 * add.
 *
 * XML tag names can't contain white space, so we replace any spaces in
 * "tagname" with dashes.
 */
static void
ExplainXMLTag(const char *tagname, int flags, ExplainState *es)
{
	const char *s;

	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoSpaces(es->str, 2 * es->indent);
	appendStringInfoCharMacro(es->str, '<');
	if ((flags & X_CLOSING) != 0)
		appendStringInfoCharMacro(es->str, '/');
	for (s = tagname; *s; s++)
		appendStringInfoCharMacro(es->str, (*s == ' ') ? '-' : *s);
	if ((flags & X_CLOSE_IMMEDIATE) != 0)
		appendStringInfoString(es->str, " /");
	appendStringInfoCharMacro(es->str, '>');
	if ((flags & X_NOWHITESPACE) == 0)
		appendStringInfoCharMacro(es->str, '\n');
}


/*
 * Emit a JSON line ending.
 *
 * JSON requires a comma after each property but the last.  To facilitate this,
 * in JSON format, the text emitted for each property begins just prior to the
 * preceding line-break (and comma, if applicable).
 */
static void
ExplainJSONLineEnding(ExplainState *es)
{
	Assert(es->format == EXPLAIN_FORMAT_JSON);
	if (linitial_int(es->grouping_stack) != 0)
		appendStringInfoChar(es->str, ',');
	else
		linitial_int(es->grouping_stack) = 1;
	appendStringInfoChar(es->str, '\n');
}


/*
 * Indent a YAML line.
 *
 * YAML lines are ordinarily indented by two spaces per indentation level.
 * The text emitted for each property begins just prior to the preceding
 * line-break, except for the first property in an unlabelled group, for which
 * it begins immediately after the "- " that introduces the group.  The first
 * property of the group appears on the same line as the opening "- ".
 */
static void
ExplainYAMLLineStarting(ExplainState *es)
{
	Assert(es->format == EXPLAIN_FORMAT_YAML);
	if (linitial_int(es->grouping_stack) == 0)
	{
		linitial_int(es->grouping_stack) = 1;
	}
	else
	{
		appendStringInfoChar(es->str, '\n');
		appendStringInfoSpaces(es->str, es->indent * 2);
	}
}
#endif
