/*-------------------------------------------------------------------------
 *
 * multi_master_planner.c
 *	  Routines for building create table and select into table statements on the
 *	  master node.
 *
 * Copyright (c) 2012-2019, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/extension.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/function_utils.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "optimizer/clauses.h"
#include "optimizer/cost.h"
#include "optimizer/planmain.h"
#include "optimizer/tlist.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#else
#include "optimizer/var.h"
#endif
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"


static List * MasterTargetList(List *workerTargetList);
static PlannedStmt * BuildSelectStatement(Query *masterQuery, List *masterTargetList,
										  CustomScan *remoteScan);
static Agg * BuildAggregatePlan(Query *masterQuery, Plan *subPlan);
static bool HasDistinctAggregate(Query *masterQuery);
static bool UseGroupAggregateWithHLL(Query *masterQuery);
static bool QueryContainsAggregateWithHLL(Query *query);
static Plan * BuildDistinctPlan(Query *masterQuery, Plan *subPlan);
static List * PrepareTargetListForNextPlan(List *targetList);
static Agg * makeAggNode(List *groupClauseList, List *havingQual,
						 AggStrategy aggrStrategy, List *queryTargetList, Plan *subPlan);


/*
 * MasterNodeSelectPlan takes in a distributed plan and a custom scan node which
 * wraps remote part of the plan. This function finds the master node query
 * structure in the multi plan, and builds the final select plan to execute on
 * the tuples returned by remote scan on the master node. Note that this select
 * plan is executed after result files are retrieved from worker nodes and
 * filled into the tuple store inside provided custom scan.
 */
PlannedStmt *
MasterNodeSelectPlan(DistributedPlan *distributedPlan, CustomScan *remoteScan)
{
	Query *masterQuery = distributedPlan->masterQuery;
	PlannedStmt *masterSelectPlan = NULL;

	Job *workerJob = distributedPlan->workerJob;
	List *workerTargetList = workerJob->jobQuery->targetList;
	List *masterTargetList = MasterTargetList(workerTargetList);

	masterSelectPlan = BuildSelectStatement(masterQuery, masterTargetList, remoteScan);

	return masterSelectPlan;
}


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
		TargetEntry *masterTargetEntry = NULL;
		Var *masterColumn = NULL;

		if (workerTargetEntry->resjunk)
		{
			continue;
		}

		masterTargetEntry = copyObject(workerTargetEntry);

		masterColumn = makeVarFromTargetEntry(tableId, workerTargetEntry);
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
	List *sortClauseList = copyObject(masterQuery->sortClause);

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

	/*
	 * (3) create distinct plan if needed.
	 *
	 * distinct on() requires sort + unique plans. Unique itself is not enough
	 * as it only compares the current value with previous one when checking
	 * uniqueness, thus ordering is necessary. If already has order by
	 * clause we append distinct clauses to the end of it. Postgresql requires
	 * that if both distinct on() and order by exists, ordering shall start
	 * on distinct clauses. Therefore we can safely append distinct clauses to
	 * the end of order by clauses. Although the same column may appear more
	 * than once in order by clauses, created plan uses only one instance, for
	 * example order by a,b,a,a,b,c is translated to equivalent order by a,b,c.
	 *
	 * If the query has distinct clause but not distinct on, we first create
	 * distinct plan that is either HashAggreate or Sort + Unique plans depending
	 * on hashable property of columns in distinct clause. If there is order by
	 * clause, it is handled after distinct planning.
	 */
	if (masterQuery->hasDistinctOn)
	{
		ListCell *distinctCell = NULL;
		foreach(distinctCell, masterQuery->distinctClause)
		{
			SortGroupClause *singleDistinctClause = lfirst(distinctCell);
			Index sortGroupRef = singleDistinctClause->tleSortGroupRef;

			if (get_sortgroupref_clause_noerr(sortGroupRef, sortClauseList) == NULL)
			{
				sortClauseList = lappend(sortClauseList, singleDistinctClause);
			}
		}
	}
	else if (masterQuery->distinctClause)
	{
		Plan *distinctPlan = BuildDistinctPlan(masterQuery, topLevelPlan);
		topLevelPlan = distinctPlan;
	}

	/* (4) add a sorting plan if needed */
	if (sortClauseList)
	{
		Sort *sortPlan = make_sort_from_sortclauses(sortClauseList, topLevelPlan);

		/* just for reproducible costs between different PostgreSQL versions */
		sortPlan->plan.startup_cost = 0;
		sortPlan->plan.total_cost = 0;
		sortPlan->plan.plan_rows = 0;

		topLevelPlan = (Plan *) sortPlan;
	}

	/*
	 * (5) add a unique plan for distinctOn.
	 * If the query has distinct on we add a sort clause in step 3. Therefore
	 * Step 4 always creates a sort plan.
	 * */
	if (masterQuery->hasDistinctOn)
	{
		Assert(IsA(topLevelPlan, Sort));
		topLevelPlan =
			(Plan *) make_unique_from_sortclauses(topLevelPlan,
												  masterQuery->distinctClause);
	}

	/* (5) add a limit plan if needed */
	if (masterQuery->limitCount || masterQuery->limitOffset)
	{
		Node *limitCount = masterQuery->limitCount;
		Node *limitOffset = masterQuery->limitOffset;
		Limit *limitPlan = make_limit(topLevelPlan, limitOffset, limitCount);
		topLevelPlan = (Plan *) limitPlan;
	}

	/* (6) finally set our top level plan in the plan tree */
	selectStatement->planTree = topLevelPlan;

	return selectStatement;
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
	List *aggregateTargetList = NIL;
	List *groupColumnList = NIL;
	List *aggregateColumnList = NIL;
	List *havingColumnList = NIL;
	List *columnList = NIL;
	ListCell *columnCell = NULL;
	Node *havingQual = NULL;
	uint32 groupColumnCount = 0;

	/* assert that we need to build an aggregate plan */
	Assert(masterQuery->hasAggs || masterQuery->groupClause);

	aggregateTargetList = masterQuery->targetList;
	havingQual = masterQuery->havingQual;

	/* estimate aggregate execution costs */
	memset(&aggregateCosts, 0, sizeof(AggClauseCosts));
	get_agg_clause_costs(NULL, (Node *) aggregateTargetList, AGGSPLIT_SIMPLE,
						 &aggregateCosts);
	get_agg_clause_costs(NULL, (Node *) havingQual, AGGSPLIT_SIMPLE, &aggregateCosts);

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
		bool groupingIsHashable = grouping_is_hashable(groupColumnList);
		bool groupingIsSortable = grouping_is_sortable(groupColumnList);
		bool hasDistinctAggregate = HasDistinctAggregate(masterQuery);

		if (!groupingIsHashable && !groupingIsSortable)
		{
			ereport(ERROR, (errmsg("grouped column list cannot be hashed or sorted")));
		}

		/*
		 * Postgres hash aggregate strategy does not support distinct aggregates
		 * in group and order by with aggregate operations.
		 * see nodeAgg.c:build_pertrans_for_aggref(). In that case we use
		 * sorted agg strategy, otherwise we use hash strategy.
		 *
		 * If the master query contains hll aggregate functions and the client set
		 * hll.force_groupagg to on, then we choose to use group aggregation.
		 */
		if (!enable_hashagg || !groupingIsHashable || hasDistinctAggregate ||
			UseGroupAggregateWithHLL(masterQuery))
		{
			char *messageHint = NULL;
			if (!enable_hashagg && groupingIsHashable)
			{
				messageHint = "Consider setting enable_hashagg to on.";
			}

			if (!groupingIsSortable)
			{
				ereport(ERROR, (errmsg("grouped column list must cannot be sorted"),
								errdetail("Having a distinct aggregate requires "
										  "grouped column list to be sortable."),
								messageHint ? errhint("%s", messageHint) : 0));
			}

			aggregateStrategy = AGG_SORTED;
			subPlan = (Plan *) make_sort_from_sortclauses(groupColumnList, subPlan);
		}
		else
		{
			aggregateStrategy = AGG_HASHED;
		}
	}

	/* finally create the plan */
	aggregatePlan = makeAggNode(groupColumnList, (List *) havingQual,
								aggregateStrategy, aggregateTargetList, subPlan);

	/* just for reproducible costs between different PostgreSQL versions */
	aggregatePlan->plan.startup_cost = 0;
	aggregatePlan->plan.total_cost = 0;
	aggregatePlan->plan.plan_rows = 0;

	return aggregatePlan;
}


/*
 * HasDistinctAggregate returns true if the query has a distinct
 * aggregate in its target list or in having clause.
 */
static bool
HasDistinctAggregate(Query *masterQuery)
{
	List *targetVarList = NIL;
	List *havingVarList = NIL;
	List *allColumnList = NIL;
	ListCell *allColumnCell = NULL;

	targetVarList = pull_var_clause((Node *) masterQuery->targetList,
									PVC_INCLUDE_AGGREGATES);
	havingVarList = pull_var_clause(masterQuery->havingQual, PVC_INCLUDE_AGGREGATES);

	allColumnList = list_concat(targetVarList, havingVarList);
	foreach(allColumnCell, allColumnList)
	{
		Node *columnNode = lfirst(allColumnCell);
		if (IsA(columnNode, Aggref))
		{
			Aggref *aggref = (Aggref *) columnNode;
			if (aggref->aggdistinct != NIL)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * UseGroupAggregateWithHLL first checks whether the HLL extension is loaded, if
 * it is not then simply return false. Otherwise, checks whether the client set
 * the hll.force_groupagg to on. If it is enabled and the master query contains
 * hll aggregate function, it returns true.
 */
static bool
UseGroupAggregateWithHLL(Query *masterQuery)
{
	Oid hllId = get_extension_oid(HLL_EXTENSION_NAME, true);
	const char *gucStrValue = NULL;

	/* If HLL extension is not loaded, return false */
	if (!OidIsValid(hllId))
	{
		return false;
	}

	/* If HLL is loaded but related GUC is not set, return false */
	gucStrValue = GetConfigOption(HLL_FORCE_GROUPAGG_GUC_NAME, true, false);
	if (gucStrValue == NULL || strcmp(gucStrValue, "off") == 0)
	{
		return false;
	}

	return QueryContainsAggregateWithHLL(masterQuery);
}


/*
 * QueryContainsAggregateWithHLL returns true if the query has an hll aggregate
 * function in it's target list.
 */
static bool
QueryContainsAggregateWithHLL(Query *query)
{
	List *varList = NIL;
	ListCell *varCell = NULL;

	varList = pull_var_clause((Node *) query->targetList, PVC_INCLUDE_AGGREGATES);
	foreach(varCell, varList)
	{
		Var *var = (Var *) lfirst(varCell);
		if (nodeTag(var) == T_Aggref)
		{
			Aggref *aggref = (Aggref *) var;
			int argCount = list_length(aggref->args);
			Oid hllId = get_extension_oid(HLL_EXTENSION_NAME, false);
			Oid hllSchemaOid = get_extension_schema(hllId);
			const char *hllSchemaName = get_namespace_name(hllSchemaOid);

			/*
			 * If the obtained oid is InvalidOid for addFunctionId, that means
			 * we don't have an hll_add_agg function with the given argument count.
			 * So, we don't need to double check whether the obtained id is valid.
			 */
			Oid addFunctionId = FunctionOidExtended(hllSchemaName, HLL_ADD_AGGREGATE_NAME,
													argCount, true);
			Oid unionFunctionId = FunctionOid(hllSchemaName, HLL_UNION_AGGREGATE_NAME, 1);

			if (aggref->aggfnoid == addFunctionId || aggref->aggfnoid == unionFunctionId)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * BuildDistinctPlan creates an returns a plan for distinct. Depending on
 * availability of hash function it chooses HashAgg over Sort/Unique
 * plans.
 * This function has a potential performance issue since we blindly set
 * Plan nodes without looking at cost. We might need to revisit this
 * if we have performance issues with select distinct queries.
 */
static Plan *
BuildDistinctPlan(Query *masterQuery, Plan *subPlan)
{
	Plan *distinctPlan = NULL;
	bool distinctClausesHashable = true;
	List *distinctClauseList = masterQuery->distinctClause;
	List *targetList = copyObject(masterQuery->targetList);
	bool hasDistinctAggregate = false;

	/*
	 * We don't need to add distinct plan if all of the columns used in group by
	 * clause also used in distinct clause, since group by clause guarantees the
	 * uniqueness of the target list for every row.
	 */
	if (IsGroupBySubsetOfDistinct(masterQuery->groupClause, masterQuery->distinctClause))
	{
		return subPlan;
	}

	/*
	 * We need to adjust varno to OUTER_VAR, since planner expects that for upper
	 * level plans above the sequential scan. We also need to convert aggregations
	 * (if exists) to regular Vars since the aggregation would be applied by the
	 * previous aggregation plan and we don't want them to be applied again.
	 */
	targetList = PrepareTargetListForNextPlan(targetList);

	Assert(masterQuery->distinctClause);
	Assert(!masterQuery->hasDistinctOn);

	/*
	 * Create group by plan with HashAggregate if all distinct
	 * members are hashable, and not containing distinct aggregate.
	 * Otherwise create sort+unique plan.
	 */
	distinctClausesHashable = grouping_is_hashable(distinctClauseList);
	hasDistinctAggregate = HasDistinctAggregate(masterQuery);

	if (enable_hashagg && distinctClausesHashable && !hasDistinctAggregate)
	{
		distinctPlan = (Plan *) makeAggNode(distinctClauseList, NIL, AGG_HASHED,
											targetList, subPlan);
	}
	else
	{
		Sort *sortPlan = make_sort_from_sortclauses(masterQuery->distinctClause,
													subPlan);
		distinctPlan = (Plan *) make_unique_from_sortclauses((Plan *) sortPlan,
															 masterQuery->distinctClause);
	}

	return distinctPlan;
}


/*
 * PrepareTargetListForNextPlan handles both regular columns to have right varno
 * and convert aggregates to regular Vars in the target list.
 */
static List *
PrepareTargetListForNextPlan(List *targetList)
{
	List *newtargetList = NIL;
	ListCell *targetEntryCell = NULL;

	foreach(targetEntryCell, targetList)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);
		TargetEntry *newTargetEntry = NULL;
		Var *newVar = NULL;

		Assert(IsA(targetEntry, TargetEntry));

		/*
		 * For upper level plans above the sequential scan, the planner expects the
		 * table id (varno) to be set to OUTER_VAR.
		 */
		newVar = makeVarFromTargetEntry(OUTER_VAR, targetEntry);
		newTargetEntry = flatCopyTargetEntry(targetEntry);
		newTargetEntry->expr = (Expr *) newVar;
		newtargetList = lappend(newtargetList, newTargetEntry);
	}

	return newtargetList;
}


/*
 * makeAggNode creates a "Agg" plan node. groupClauseList is a list of
 * SortGroupClause's.
 */
static Agg *
makeAggNode(List *groupClauseList, List *havingQual, AggStrategy aggrStrategy,
			List *queryTargetList, Plan *subPlan)
{
	Agg *aggNode = NULL;
	int groupColumnCount = list_length(groupClauseList);
	AttrNumber *groupColumnIdArray =
		extract_grouping_cols(groupClauseList, subPlan->targetlist);
	Oid *groupColumnOpArray = extract_grouping_ops(groupClauseList);
	const int rowEstimate = 10;

#if (PG_VERSION_NUM >= 120000)
	aggNode = make_agg(queryTargetList, havingQual, aggrStrategy,
					   AGGSPLIT_SIMPLE, groupColumnCount, groupColumnIdArray,
					   groupColumnOpArray,
					   extract_grouping_collations(groupClauseList,
												   subPlan->targetlist),
					   NIL, NIL, rowEstimate, subPlan);
#else
	aggNode = make_agg(queryTargetList, havingQual, aggrStrategy,
					   AGGSPLIT_SIMPLE, groupColumnCount, groupColumnIdArray,
					   groupColumnOpArray,
					   NIL, NIL, rowEstimate, subPlan);
#endif

	return aggNode;
}
