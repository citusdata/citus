/*-------------------------------------------------------------------------
 *
 * multi_explain.c
 *	  Citus explain support.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/prepare.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/multi_explain.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_physical_planner.h"
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "tcop/tcopprot.h"


/* Config variables that enable printing distributed query plans */
bool ExplainMultiLogicalPlan = false;
bool ExplainMultiPhysicalPlan = false;


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
	MultiTreeRoot *multiTree = NULL;
	MultiPlan *multiPlan = NULL;
	Query *queryCopy = NULL;
	CmdType commandType = query->commandType;

	/* if local query, run the standard explain and return */
	bool localQuery = !NeedsDistributedPlanning(query);
	if (localQuery)
	{
		PlannedStmt *plan = NULL;
		instr_time planstart;
		instr_time planduration;

		INSTR_TIME_SET_CURRENT(planstart);

		/* plan the query */
		plan = pg_plan_query(query, 0, params);

		INSTR_TIME_SET_CURRENT(planduration);
		INSTR_TIME_SUBTRACT(planduration, planstart);

		/* run it (if needed) and produce output */
		ExplainOnePlan(plan, into, es, queryString, params, &planduration);

		return;
	}

	/* error out early if the query is a modification */
	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot show execution plan for distributed modification"),
						errdetail("EXPLAIN commands are unsupported for distributed "
								  "modifications.")));
	}

	/* call standard planner to modify the query structure before multi planning */
	standard_planner(query, 0, params);
	queryCopy = copyObject(query);

	/* create the logical and physical plan */
	multiTree = MultiLogicalPlanCreate(queryCopy);
	MultiLogicalPlanOptimize(multiTree);
	multiPlan = MultiPhysicalPlanCreate(multiTree);

	if (ExplainMultiLogicalPlan)
	{
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

	/* if explain printing isn't enabled, print error only after planning */
	if (!ExplainMultiLogicalPlan && !ExplainMultiPhysicalPlan)
	{
		appendStringInfo(es->str, "explain statements for distributed queries ");
		appendStringInfo(es->str, "are currently unsupported\n");
	}
}
