/*-------------------------------------------------------------------------
 *
 * multi_explain.c
 *	  Citus explain support.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/copy.h"
#include "commands/createas.h"
#include "commands/dbcommands.h"
#include "commands/explain.h"
#include "commands/tablecmds.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/worker_protocol.h"
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
#include "utils/json.h"
#include "utils/snapmgr.h"


#if (PG_VERSION_NUM >= 90400 && PG_VERSION_NUM < 90500)

/* Crude hack to avoid changing sizeof(ExplainState) in released branches (explain.c) */
#define grouping_stack extra->groupingstack
#endif

/* OR-able flags for ExplainXMLTag() (explain.c) */
#define X_OPENING 0
#define X_CLOSING 1
#define X_CLOSE_IMMEDIATE 2
#define X_NOWHITESPACE 4


/* Config variables that enable printing distributed query plans */
bool ExplainMultiLogicalPlan = false;
bool ExplainMultiPhysicalPlan = false;
bool ExplainDistributedQueries = true;
bool ExplainAllTasks = false;


/* Result for a single remote EXPLAIN command */
typedef struct RemoteExplainPlan
{
	int placementIndex;
	List *explainOutputList;
} RemoteExplainPlan;


/* Explain functions for distributed queries */
static void ExplainMasterPlan(PlannedStmt *masterPlan, IntoClause *into,
							  ExplainState *es, const char *queryString,
							  ParamListInfo params, const instr_time *planDuration);
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
static void ExplainOpenGroup(const char *objtype, const char *labelname,
							 bool labeled, ExplainState *es);
static void ExplainCloseGroup(const char *objtype, const char *labelname,
							  bool labeled, ExplainState *es);
static void ExplainXMLTag(const char *tagname, int flags, ExplainState *es);
static void ExplainJSONLineEnding(ExplainState *es);
static void ExplainYAMLLineStarting(ExplainState *es);


/*
 * MultiExplainOneQuery takes the given query, and checks if the query is local
 * or distributed. If the query is local, the function runs the standard explain
 * logic. If the query is distributed, the function looks up configuration and
 * prints out the distributed logical and physical plans as appropriate.
 */
void
MultiExplainOneQuery(Query *query, IntoClause *into, ExplainState *es,
					 const char *queryString, ParamListInfo params)
{
	MultiPlan *multiPlan = NULL;
	CmdType commandType = CMD_UNKNOWN;
	PlannedStmt *initialPlan = NULL;
	Job *workerJob = NULL;
	bool routerExecutablePlan = false;
	instr_time planStart;
	instr_time planDuration;

	/* if local query, run the standard explain and return */
	bool localQuery = !NeedsDistributedPlanning(query);
	if (localQuery)
	{
		PlannedStmt *plan = NULL;

		INSTR_TIME_SET_CURRENT(planStart);

		/* plan the query */
		plan = pg_plan_query(query, 0, params);

		INSTR_TIME_SET_CURRENT(planDuration);
		INSTR_TIME_SUBTRACT(planDuration, planStart);

		/* run it (if needed) and produce output */
		ExplainOnePlan(plan, into, es, queryString, params, &planDuration);

		return;
	}

	/* measure the full planning time to display in EXPLAIN ANALYZE */
	INSTR_TIME_SET_CURRENT(planStart);

	/* call standard planner to modify the query structure before multi planning */
	initialPlan = standard_planner(query, 0, params);

	commandType = initialPlan->commandType;
	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		if (es->analyze)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("Using ANALYZE for INSERT/UPDATE/DELETE on "
								   "distributed tables is not supported.")));
		}
	}

	multiPlan = CreatePhysicalPlan(query);

	INSTR_TIME_SET_CURRENT(planDuration);
	INSTR_TIME_SUBTRACT(planDuration, planStart);

	if (ExplainMultiLogicalPlan)
	{
		MultiTreeRoot *multiTree = MultiLogicalPlanCreate(query);
		char *logicalPlanString = CitusNodeToString(multiTree);
		char *formattedPlanString = pretty_format_node_dump(logicalPlanString);

		appendStringInfo(es->str, "logical plan:\n");
		appendStringInfo(es->str, "%s\n", formattedPlanString);
	}

	if (ExplainMultiPhysicalPlan)
	{
		char *physicalPlanString = CitusNodeToString(multiPlan);
		char *formattedPlanString = pretty_format_node_dump(physicalPlanString);

		appendStringInfo(es->str, "physical plan:\n");
		appendStringInfo(es->str, "%s\n", formattedPlanString);
	}

	if (!ExplainDistributedQueries)
	{
		appendStringInfo(es->str, "explain statements for distributed queries ");
		appendStringInfo(es->str, "are not enabled\n");
		return;
	}

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		appendStringInfoSpaces(es->str, es->indent * 2);
		appendStringInfo(es->str, "Distributed Query");

		if (multiPlan->masterTableName != NULL)
		{
			appendStringInfo(es->str, " into %s", multiPlan->masterTableName);
		}

		appendStringInfo(es->str, "\n");

		es->indent += 1;
	}
	else if (es->format == EXPLAIN_FORMAT_JSON)
	{
		ExplainOpenGroup("Distributed Query", NULL, true, es);
	}

	routerExecutablePlan = RouterExecutablePlan(multiPlan, TaskExecutorType);

	if (routerExecutablePlan)
	{
		ExplainPropertyText("Executor", "Router", es);
	}
	else
	{
		switch (TaskExecutorType)
		{
			case MULTI_EXECUTOR_REAL_TIME:
			{
				ExplainPropertyText("Executor", "Real-Time", es);
			}
			break;

			case MULTI_EXECUTOR_TASK_TRACKER:
			{
				ExplainPropertyText("Executor", "Task-Tracker", es);
			}
			break;

			default:
			{
				ExplainPropertyText("Executor", "Other", es);
			}
			break;
		}
	}

	workerJob = multiPlan->workerJob;
	ExplainJob(workerJob, es);

	if (es->format == EXPLAIN_FORMAT_TEXT)
	{
		es->indent -= 1;
	}

	if (!routerExecutablePlan)
	{
		PlannedStmt *masterPlan = MultiQueryContainerNode(initialPlan, multiPlan);

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "Master Query\n");
			es->indent += 1;
		}
		else if (es->format == EXPLAIN_FORMAT_JSON)
		{
			ExplainJSONLineEnding(es);
			appendStringInfoSpaces(es->str, es->indent * 2);
			appendStringInfo(es->str, "\"Master Query\":");
			es->grouping_stack = lcons_int(0, es->grouping_stack);
		}

		ExplainMasterPlan(masterPlan, into, es, queryString, params, &planDuration);

		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			es->indent -= 1;
		}
	}

	if (es->format == EXPLAIN_FORMAT_JSON)
	{
		ExplainCloseGroup("Distributed Query", NULL, true, es);
	}
}


/*
 * ExplainMasterPlan generates EXPLAIN output for the master query that merges results.
 * When using EXPLAIN ANALYZE, this function shows the execution time of the master query
 * in isolation. Calling ExplainOnePlan directly would show the overall execution time of
 * the distributed query, which makes it hard to determine how much time the master query
 * took.
 *
 * Parts of this function are copied directly from ExplainOnePlan.
 */
static void
ExplainMasterPlan(PlannedStmt *masterPlan, IntoClause *into,
				  ExplainState *es, const char *queryString,
				  ParamListInfo params, const instr_time *planDuration)
{
	DestReceiver *dest = NULL;
	int eflags = 0;
	QueryDesc *queryDesc = NULL;
	int instrument_option = 0;

	if (es->analyze && es->timing)
	{
		instrument_option |= INSTRUMENT_TIMER;
	}
	else if (es->analyze)
	{
		instrument_option |= INSTRUMENT_ROWS;
	}

	if (es->buffers)
	{
		instrument_option |= INSTRUMENT_BUFFERS;
	}

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/*
	 * Normally we discard the query's output, but if explaining CREATE TABLE
	 * AS, we'd better use the appropriate tuple receiver.
	 */
	if (into)
	{
		dest = CreateIntoRelDestReceiver(into);
	}
	else
	{
		dest = None_Receiver;
	}

	/* Create a QueryDesc for the query */
	queryDesc = CreateQueryDesc(masterPlan, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, params, instrument_option);

	/* Select execution options */
	if (es->analyze)
	{
		eflags = 0;             /* default run-to-completion flags */
	}
	else
	{
		eflags = EXEC_FLAG_EXPLAIN_ONLY;
	}
	if (into)
	{
		eflags |= GetIntoRelEFlags(into);
	}

	/*
	 * ExecutorStart creates the merge table. If using ANALYZE, it also executes the
	 * worker job and populates the merge table.
	 */
	ExecutorStart(queryDesc, eflags);

	if (es->analyze)
	{
		ScanDirection dir;

		/* if using analyze, then finish query execution */

		/* EXPLAIN ANALYZE CREATE TABLE AS WITH NO DATA is weird */
		if (into && into->skipData)
		{
			dir = NoMovementScanDirection;
		}
		else
		{
			dir = ForwardScanDirection;
		}

		/* run the plan */
		ExecutorRun(queryDesc, dir, 0L);

		/* run cleanup too */
		ExecutorFinish(queryDesc);
	}

	/*
	 * ExplainOnePlan executes the master query again, which ensures that the execution
	 * time only shows the execution time of the master query itself, instead of the
	 * overall execution time.
	 */
	ExplainOnePlan(queryDesc->plannedstmt, into, es, queryString, params, planDuration);

	/*
	 * ExecutorEnd for the distributed query is deferred until after the master query
	 * is executed again, otherwise the merge table would be dropped.
	 */
	ExecutorEnd(queryDesc);

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();
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

	ExplainPropertyInteger("Task Count", taskCount, es);

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

	ExplainCloseGroup("Job", "Job", true, es);

	/* show explain output for depended jobs, if any */
	foreach(dependedJobCell, dependedJobList)
	{
		Job *dependedJob = (Job *) lfirst(dependedJobCell);

		if (CitusIsA(dependedJob, MapMergeJob))
		{
			ExplainMapMergeJob((MapMergeJob *) dependedJob, es);
		}
	}
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

	ExplainPropertyInteger("Map Task Count", mapTaskCount, es);
	ExplainPropertyInteger("Merge Task Count", mergeTaskCount, es);

	ExplainCloseGroup("Job", NULL, true, es);

	foreach(dependedJobCell, dependedJobList)
	{
		Job *dependedJob = (Job *) lfirst(dependedJobCell);

		if (CitusIsA(dependedJob, MapMergeJob))
		{
			ExplainMapMergeJob((MapMergeJob *) dependedJob, es);
		}
	}

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

	for (placementIndex = 0; placementIndex < placementCount; placementIndex++)
	{
		ShardPlacement *taskPlacement = list_nth(taskPlacementList, placementIndex);
		char *nodeName = taskPlacement->nodeName;
		uint32 nodePort = taskPlacement->nodePort;

		remotePlan->placementIndex = placementIndex;
		remotePlan->explainOutputList = ExecuteRemoteQuery(nodeName, nodePort,
														   NULL, explainQuery);
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
	char *nodeDatabase = get_database_name(MyDatabaseId);
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
		}
		break;

		case EXPLAIN_FORMAT_JSON:
		{
			formatStr = "JSON";
		}
		break;

		case EXPLAIN_FORMAT_YAML:
		{
			formatStr = "YAML";
		}
		break;

		default:
		{
			formatStr = "TEXT";
		}
		break;
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
