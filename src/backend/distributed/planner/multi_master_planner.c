/*-------------------------------------------------------------------------
 *
 * multi_master_planner.c
 *	  Routines for building create table and select into table statements on the
 *	  master node.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/function_utils.h"
#include "distributed/listutils.h"
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
#include "optimizer/subselect.h"
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
static Agg * BuildAggregatePlan(PlannerInfo *root, Query *masterQuery, Plan *subPlan);
static bool HasDistinctOrOrderByAggregate(Query *masterQuery);
static bool UseGroupAggregateWithHLL(Query *masterQuery);
static bool QueryContainsAggregateWithHLL(Query *query);
static Plan * BuildDistinctPlan(Query *masterQuery, Plan *subPlan);
static Agg * makeAggNode(List *groupClauseList, List *havingQual,
						 AggStrategy aggrStrategy, List *queryTargetList, Plan *subPlan);
static void FinalizeStatement(PlannerInfo *root, PlannedStmt *stmt, Plan *topLevelPlan);


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

	Job *workerJob = distributedPlan->workerJob;
	List *workerTargetList = workerJob->jobQuery->targetList;
	List *masterTargetList = MasterTargetList(workerTargetList);

	PlannedStmt *masterSelectPlan = BuildSelectStatement(masterQuery, masterTargetList,
														 remoteScan);

	return masterSelectPlan;
}


/*
 * MasterTargetList uses the given worker target list's expressions, and creates
 * a target list for the master node. This master target list keeps the
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

		if (workerTargetEntry->resjunk)
		{
			continue;
		}

		Var *masterColumn = makeVarFromTargetEntry(tableId, workerTargetEntry);
		masterColumn->varattno = columnId;
		masterColumn->varoattno = columnId;
		columnId++;

		if (masterColumn->vartype == RECORDOID || masterColumn->vartype == RECORDARRAYOID)
		{
			masterColumn->vartypmod = BlessRecordExpression(workerTargetEntry->expr);
		}

		/*
		 * The master target entry has two pieces to it. The first piece is the
		 * target entry's expression, which we set to the newly created column.
		 * The second piece is sort and group clauses that we implicitly copy
		 * from the worker target entry. Note that any changes to worker target
		 * entry's sort and group clauses will *break* us here.
		 */
		TargetEntry *masterTargetEntry = flatCopyTargetEntry(workerTargetEntry);
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
	/* top level select query should have only one range table entry */
	Assert(list_length(masterQuery->rtable) == 1);
	Agg *aggregationPlan = NULL;
	Plan *topLevelPlan = NULL;
	List *sortClauseList = copyObject(masterQuery->sortClause);
	List *columnNameList = NIL;
	TargetEntry *targetEntry = NULL;

	PlannerGlobal *glob = makeNode(PlannerGlobal);
	PlannerInfo *root = makeNode(PlannerInfo);
	root->parse = masterQuery;
	root->glob = glob;
	root->query_level = 1;
	root->planner_cxt = CurrentMemoryContext;
	root->wt_param_id = -1;


	/* (1) make PlannedStmt and set basic information */
	PlannedStmt *selectStatement = makeNode(PlannedStmt);
	selectStatement->canSetTag = true;
	selectStatement->relationOids = NIL;
	selectStatement->commandType = CMD_SELECT;


	remoteScan->custom_scan_tlist = masterTargetList;

	/* (2) add an aggregation plan if needed */
	if (masterQuery->hasAggs || masterQuery->groupClause)
	{
		remoteScan->scan.plan.targetlist = masterTargetList;

		aggregationPlan = BuildAggregatePlan(root, masterQuery, &remoteScan->scan.plan);
		topLevelPlan = (Plan *) aggregationPlan;
		selectStatement->planTree = topLevelPlan;
	}
	else
	{
		/* otherwise set the final projections on the scan plan directly */

		/*
		 * The masterTargetList contains all columns that we fetch from
		 * the worker as non-resjunk.
		 *
		 * Here the output of the plan node determines the output of the query.
		 * We therefore use the targetList of masterQuery, which has non-output
		 * columns set as resjunk.
		 */
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

	/*
	 * (6) set top level plan in the plantree and copy over some things from
	 * PlannerInfo
	 */
	FinalizeStatement(root, selectStatement, topLevelPlan);

	/*
	 * (7) Replace rangetable with one with nice names to show in EXPLAIN plans
	 */
	foreach_ptr(targetEntry, masterTargetList)
	{
		columnNameList = lappend(columnNameList, makeString(targetEntry->resname));
	}

	RangeTblEntry *customScanRangeTableEntry = linitial(selectStatement->rtable);
	customScanRangeTableEntry->eref = makeAlias("remote_scan", columnNameList);

	return selectStatement;
}


/*
 * FinalizeStatement sets some necessary fields on the final statement and its
 * plan to make it work with the regular postgres executor. This code is copied
 * almost verbatim from standard_planner in the PG source code.
 *
 * Modifications from original code:
 * - Added SS_attach_initplans call
 */
static void
FinalizeStatement(PlannerInfo *root, PlannedStmt *result, Plan *top_plan)
{
	ListCell *lp,
			 *lr;
	PlannerGlobal *glob = root->glob;

	/* Taken from create_plan */
	SS_attach_initplans(root, top_plan);

	/*
	 * If any Params were generated, run through the plan tree and compute
	 * each plan node's extParam/allParam sets.  Ideally we'd merge this into
	 * set_plan_references' tree traversal, but for now it has to be separate
	 * because we need to visit subplans before not after main plan.
	 */
	if (glob->paramExecTypes != NIL)
	{
		Assert(list_length(glob->subplans) == list_length(glob->subroots));
		forboth(lp, glob->subplans, lr, glob->subroots)
		{
			Plan *subplan = (Plan *) lfirst(lp);
			PlannerInfo *subroot = lfirst_node(PlannerInfo, lr);

			SS_finalize_plan(subroot, subplan);
		}
		SS_finalize_plan(root, top_plan);
	}

	/* final cleanup of the plan */
	Assert(glob->finalrtable == NIL);
	Assert(glob->finalrowmarks == NIL);
	Assert(glob->resultRelations == NIL);
	Assert(glob->rootResultRelations == NIL);

	top_plan = set_plan_references(root, top_plan);

	/* ... and the subplans (both regular subplans and initplans) */
	Assert(list_length(glob->subplans) == list_length(glob->subroots));
	forboth(lp, glob->subplans, lr, glob->subroots)
	{
		Plan *subplan = (Plan *) lfirst(lp);
		PlannerInfo *subroot = lfirst_node(PlannerInfo, lr);

		lfirst(lp) = set_plan_references(subroot, subplan);
	}
	result->transientPlan = glob->transientPlan;
	result->dependsOnRole = glob->dependsOnRole;
	result->parallelModeNeeded = glob->parallelModeNeeded;
	result->planTree = top_plan;

	result->rtable = glob->finalrtable;
	result->resultRelations = glob->resultRelations;
#if PG_VERSION_NUM < 120000
	result->nonleafResultRelations = glob->nonleafResultRelations;
#endif
	result->rootResultRelations = glob->rootResultRelations;
	result->subplans = glob->subplans;
	result->rewindPlanIDs = glob->rewindPlanIDs;
	result->rowMarks = glob->finalrowmarks;
	result->relationOids = glob->relationOids;
	result->invalItems = glob->invalItems;
	result->paramExecTypes = glob->paramExecTypes;
}


/*
 * BuildAggregatePlan creates and returns an aggregate plan. This aggregate plan
 * builds aggregation and grouping operators (if any) that are to be executed on
 * the master node.
 */
static Agg *
BuildAggregatePlan(PlannerInfo *root, Query *masterQuery, Plan *subPlan)
{
	/* assert that we need to build an aggregate plan */
	Assert(masterQuery->hasAggs || masterQuery->groupClause);
	AggClauseCosts aggregateCosts;
	AggStrategy aggregateStrategy = AGG_PLAIN;
	List *groupColumnList = masterQuery->groupClause;
	List *aggregateTargetList = masterQuery->targetList;

	/*
	 * Replaces SubLink nodes with SubPlan nodes in the having section of the
	 * query. (and creates the subplans in root->subplans)
	 *
	 * Would be nice if we could use masterQuery->hasSubLinks to only call
	 * these when that is true. However, for some reason hasSubLinks is false
	 * even when there are SubLinks.
	 */
	Node *havingQual = SS_process_sublinks(root, masterQuery->havingQual, true);

	/*
	 * Right now this is not really needed, since we don't support correlated
	 * subqueries anyway. Once we do calling this is critical to do right after
	 * calling SS_process_sublinks, according to the postgres function comment.
	 */
	havingQual = SS_replace_correlation_vars(root, havingQual);


	/* estimate aggregate execution costs */
	memset(&aggregateCosts, 0, sizeof(AggClauseCosts));
	get_agg_clause_costs(root, (Node *) aggregateTargetList, AGGSPLIT_SIMPLE,
						 &aggregateCosts);

	get_agg_clause_costs(root, (Node *) havingQual, AGGSPLIT_SIMPLE, &aggregateCosts);


	/* if we have grouping, then initialize appropriate information */
	if (list_length(groupColumnList) > 0)
	{
		bool groupingIsHashable = grouping_is_hashable(groupColumnList);
		bool groupingIsSortable = grouping_is_sortable(groupColumnList);
		bool hasUnhashableAggregate = HasDistinctOrOrderByAggregate(masterQuery);

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
		if (!enable_hashagg || !groupingIsHashable || hasUnhashableAggregate ||
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
	Agg *aggregatePlan = makeAggNode(groupColumnList, (List *) havingQual,
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
HasDistinctOrOrderByAggregate(Query *masterQuery)
{
	ListCell *allColumnCell = NULL;

	List *targetVarList = pull_var_clause((Node *) masterQuery->targetList,
										  PVC_INCLUDE_AGGREGATES);
	List *havingVarList = pull_var_clause(masterQuery->havingQual,
										  PVC_INCLUDE_AGGREGATES);

	List *allColumnList = list_concat(targetVarList, havingVarList);
	foreach(allColumnCell, allColumnList)
	{
		Node *columnNode = lfirst(allColumnCell);
		if (IsA(columnNode, Aggref))
		{
			Aggref *aggref = (Aggref *) columnNode;
			if (aggref->aggdistinct != NIL || aggref->aggorder != NIL)
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

	/* If HLL extension is not loaded, return false */
	if (!OidIsValid(hllId))
	{
		return false;
	}

	/* If HLL is loaded but related GUC is not set, return false */
	const char *gucStrValue = GetConfigOption(HLL_FORCE_GROUPAGG_GUC_NAME, true, false);
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
	ListCell *varCell = NULL;

	List *varList = pull_var_clause((Node *) query->targetList, PVC_INCLUDE_AGGREGATES);
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
	List *distinctClauseList = masterQuery->distinctClause;
	List *targetList = copyObject(masterQuery->targetList);

	/*
	 * We don't need to add distinct plan if all of the columns used in group by
	 * clause also used in distinct clause, since group by clause guarantees the
	 * uniqueness of the target list for every row.
	 */
	if (IsGroupBySubsetOfDistinct(masterQuery->groupClause, masterQuery->distinctClause))
	{
		return subPlan;
	}

	Assert(masterQuery->distinctClause);
	Assert(!masterQuery->hasDistinctOn);

	/*
	 * Create group by plan with HashAggregate if all distinct
	 * members are hashable, and not containing distinct aggregate.
	 * Otherwise create sort+unique plan.
	 */
	bool distinctClausesHashable = grouping_is_hashable(distinctClauseList);
	bool hasUnhashableAggregate = HasDistinctOrOrderByAggregate(masterQuery);

	if (enable_hashagg && distinctClausesHashable && !hasUnhashableAggregate)
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
