/*-------------------------------------------------------------------------
 *
 * multi_master_planner.c
 *	  Routines for building create table and select into table statements on the
 *	  master node.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/multi_master_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/worker_protocol.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#include "optimizer/var.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"


/*
 * MasterTargetList uses the given worker target list's expressions, and creates
 * a target target list for the master node. This master target list keeps the
 * temporary table's columns on the master node.
 */
static List *
MasterTargetList(List *workerTargetList)
{
	List *masterTargetList = NIL;
	const Index tableId = 1;
	AttrNumber columnId = 1;

	ListCell *workerTargetCell = NULL;
	foreach(workerTargetCell, workerTargetList)
	{
		TargetEntry *workerTargetEntry = (TargetEntry *) lfirst(workerTargetCell);
		TargetEntry *masterTargetEntry = copyObject(workerTargetEntry);

		Var *masterColumn = makeVarFromTargetEntry(tableId, workerTargetEntry);
		masterColumn->varattno = columnId;
		masterColumn->varoattno = columnId;
		columnId++;

		/*
		 * The master target entry has two pieces to it. The first piece is the
		 * target entry's expression, which we set to the newly created column.
		 * The second piece is sort and group clauses that we implicitly copy
		 * from the worker target entry. Note that any changes to worker target
		 * entry's sort and group clauses will *break* us here.
		 */
		masterTargetEntry->expr = (Expr *) masterColumn;
		masterTargetList = lappend(masterTargetList, masterTargetEntry);
	}

	return masterTargetList;
}


/*
 * BuildAggregatePlan creates and returns an aggregate plan. This aggregate plan
 * builds aggreation and grouping operators (if any) that are to be executed on
 * the master node.
 */
static Agg *
BuildAggregatePlan(Query *masterQuery, Plan *subPlan)
{
	Agg *aggregatePlan = NULL;
	AggStrategy aggregateStrategy = AGG_PLAIN;
	AggClauseCosts aggregateCosts;
	AttrNumber *groupColumnIdArray = NULL;
	List *aggregateTargetList = NIL;
	List *groupColumnList = NIL;
	List *aggregateColumnList = NIL;
	List *havingColumnList = NIL;
	List *columnList = NIL;
	ListCell *columnCell = NULL;
	Node *havingQual = NULL;
	Oid *groupColumnOpArray = NULL;
	uint32 groupColumnCount = 0;
	const long rowEstimate = 10;

	/* assert that we need to build an aggregate plan */
	Assert(masterQuery->hasAggs || masterQuery->groupClause);

	aggregateTargetList = masterQuery->targetList;
	havingQual = masterQuery->havingQual;

	/* estimate aggregate execution costs */
	memset(&aggregateCosts, 0, sizeof(AggClauseCosts));
#if (PG_VERSION_NUM >= 90600)
	get_agg_clause_costs(NULL, (Node *) aggregateTargetList, AGGSPLIT_SIMPLE,
						 &aggregateCosts);
	get_agg_clause_costs(NULL, (Node *) havingQual, AGGSPLIT_SIMPLE, &aggregateCosts);
#else
	count_agg_clauses(NULL, (Node *) aggregateTargetList, &aggregateCosts);
	count_agg_clauses(NULL, (Node *) havingQual, &aggregateCosts);
#endif

	/*
	 * For upper level plans above the sequential scan, the planner expects the
	 * table id (varno) to be set to OUTER_VAR.
	 */
	aggregateColumnList = pull_var_clause_default((Node *) aggregateTargetList);
	havingColumnList = pull_var_clause_default(havingQual);

	columnList = list_concat(aggregateColumnList, havingColumnList);
	foreach(columnCell, columnList)
	{
		Var *column = (Var *) lfirst(columnCell);
		column->varno = OUTER_VAR;
	}

	groupColumnList = masterQuery->groupClause;
	groupColumnCount = list_length(groupColumnList);

	/* if we have grouping, then initialize appropriate information */
	if (groupColumnCount > 0)
	{
		if (!grouping_is_hashable(groupColumnList))
		{
			ereport(ERROR, (errmsg("grouped column list cannot be hashed")));
		}

		/* switch to hashed aggregate strategy to allow grouping */
		aggregateStrategy = AGG_HASHED;

		/* get column indexes that are being grouped */
		groupColumnIdArray = extract_grouping_cols(groupColumnList, subPlan->targetlist);
		groupColumnOpArray = extract_grouping_ops(groupColumnList);
	}

	/* finally create the plan */
#if (PG_VERSION_NUM >= 90600)
	aggregatePlan = make_agg(aggregateTargetList, (List *) havingQual, aggregateStrategy,
							 AGGSPLIT_SIMPLE, groupColumnCount, groupColumnIdArray,
							 groupColumnOpArray, NIL, NIL,
							 rowEstimate, subPlan);
#else
	aggregatePlan = make_agg(NULL, aggregateTargetList, (List *) havingQual,
							 aggregateStrategy,
							 &aggregateCosts, groupColumnCount, groupColumnIdArray,
							 groupColumnOpArray, NIL, rowEstimate, subPlan);
#endif

	/* just for reproducible costs between different PostgreSQL versions */
	aggregatePlan->plan.startup_cost = 0;
	aggregatePlan->plan.total_cost = 0;
	aggregatePlan->plan.plan_rows = 0;

	return aggregatePlan;
}


/*
 * BuildSelectStatement builds the final select statement to run on the master
 * node, before returning results to the user. The function first gets the custom
 * scan node for all results fetched to the master, and layers aggregation, sort
 * and limit plans on top of the scan statement if necessary.
 */
static PlannedStmt *
BuildSelectStatement(Query *masterQuery, List *masterTargetList, CustomScan *remoteScan)
{
	PlannedStmt *selectStatement = NULL;
	RangeTblEntry *customScanRangeTableEntry = NULL;
	Agg *aggregationPlan = NULL;
	Plan *topLevelPlan = NULL;
	ListCell *targetEntryCell = NULL;
	List *columnNameList = NULL;

	/* (1) make PlannedStmt and set basic information */
	selectStatement = makeNode(PlannedStmt);
	selectStatement->canSetTag = true;
	selectStatement->relationOids = NIL;
	selectStatement->commandType = CMD_SELECT;

	/* top level select query should have only one range table entry */
	Assert(list_length(masterQuery->rtable) == 1);

	/* compute column names for the custom range table entry */
	foreach(targetEntryCell, masterTargetList)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);
		columnNameList = lappend(columnNameList, makeString(targetEntry->resname));
	}

	customScanRangeTableEntry = RemoteScanRangeTableEntry(columnNameList);

	/* set the single element range table list */
	selectStatement->rtable = list_make1(customScanRangeTableEntry);

	/* (2) add an aggregation plan if needed */
	if (masterQuery->hasAggs || masterQuery->groupClause)
	{
		remoteScan->scan.plan.targetlist = masterTargetList;

		aggregationPlan = BuildAggregatePlan(masterQuery, &remoteScan->scan.plan);
		topLevelPlan = (Plan *) aggregationPlan;
	}
	else
	{
		/* otherwise set the final projections on the scan plan directly */
		remoteScan->scan.plan.targetlist = masterQuery->targetList;
		topLevelPlan = &remoteScan->scan.plan;
	}

	/* (3) add a sorting plan if needed */
	if (masterQuery->sortClause)
	{
		List *sortClauseList = masterQuery->sortClause;
#if (PG_VERSION_NUM >= 90600)
		Sort *sortPlan = make_sort_from_sortclauses(sortClauseList, topLevelPlan);
#else
		Sort *sortPlan = make_sort_from_sortclauses(NULL, sortClauseList, topLevelPlan);
#endif

		/* just for reproducible costs between different PostgreSQL versions */
		sortPlan->plan.startup_cost = 0;
		sortPlan->plan.total_cost = 0;
		sortPlan->plan.plan_rows = 0;

		topLevelPlan = (Plan *) sortPlan;
	}

	/* (4) add a limit plan if needed */
	if (masterQuery->limitCount || masterQuery->limitOffset)
	{
		Node *limitCount = masterQuery->limitCount;
		Node *limitOffset = masterQuery->limitOffset;
#if (PG_VERSION_NUM >= 90600)
		Limit *limitPlan = make_limit(topLevelPlan, limitOffset, limitCount);
#else
		int64 offsetEstimate = 0;
		int64 countEstimate = 0;

		Limit *limitPlan = make_limit(topLevelPlan, limitOffset, limitCount,
									  offsetEstimate, countEstimate);
#endif
		topLevelPlan = (Plan *) limitPlan;
	}

	/* (5) finally set our top level plan in the plan tree */
	selectStatement->planTree = topLevelPlan;

	return selectStatement;
}


/*
 * MasterNodeSelectPlan takes in a distributed plan and a custom scan node which
 * wraps remote part of the plan. This function finds the master node query
 * structure in the multi plan, and builds the final select plan to execute on
 * the tuples returned by remote scan on the master node. Note that this select
 * plan is executed after result files are retrieved from worker nodes and
 * filled into the tuple store inside provided custom scan.
 */
PlannedStmt *
MasterNodeSelectPlan(MultiPlan *multiPlan, CustomScan *remoteScan)
{
	Query *masterQuery = multiPlan->masterQuery;
	PlannedStmt *masterSelectPlan = NULL;

	Job *workerJob = multiPlan->workerJob;
	List *workerTargetList = workerJob->jobQuery->targetList;
	List *masterTargetList = MasterTargetList(workerTargetList);

	masterSelectPlan = BuildSelectStatement(masterQuery, masterTargetList, remoteScan);

	return masterSelectPlan;
}
