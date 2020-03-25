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
#include "distributed/citus_ruleutils.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_physical_planner.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "rewrite/rewriteManip.h"

static List * MasterTargetList(List *workerTargetList);
static PlannedStmt * BuildSelectStatementViaStdPlanner(Query *masterQuery,
													   List *masterTargetList,
													   CustomScan *remoteScan);
static bool FindCitusExtradataContainerRTE(Node *node, RangeTblEntry **result);

static Plan * CitusCustomScanPathPlan(PlannerInfo *root, RelOptInfo *rel,
									  struct CustomPath *best_path, List *tlist,
									  List *clauses, List *custom_plans);

bool ReplaceCitusExtraDataContainer = false;
CustomScan *ReplaceCitusExtraDataContainerWithCustomScan = NULL;

/*
 * CitusCustomScanPathMethods defines the methods for a custom path we insert into the
 * planner during the planning of the query part that will be executed on the node
 * coordinating the query.
 */
static CustomPathMethods CitusCustomScanPathMethods = {
	.CustomName = "CitusCustomScanPath",
	.PlanCustomPath = CitusCustomScanPathPlan,
};

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
	return BuildSelectStatementViaStdPlanner(masterQuery, masterTargetList, remoteScan);
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
 * CreateCitusCustomScanPath creates a custom path node that will return the CustomScan if
 * the path ends up in the best_path during postgres planning. We use this function during
 * the set relation hook of postgres during the planning of the query part that will be
 * executed on the query coordinating node.
 */
Path *
CreateCitusCustomScanPath(PlannerInfo *root, RelOptInfo *relOptInfo,
						  Index restrictionIndex, RangeTblEntry *rte,
						  CustomScan *remoteScan)
{
	CitusCustomScanPath *path = (CitusCustomScanPath *) newNode(
		sizeof(CitusCustomScanPath), T_CustomPath);
	path->custom_path.methods = &CitusCustomScanPathMethods;
	path->custom_path.path.pathtype = T_CustomScan;
	path->custom_path.path.pathtarget = relOptInfo->reltarget;
	path->custom_path.path.parent = relOptInfo;

	/*
	 * The 100k rows we put on the cost of the path is kind of arbitrary and could be
	 * improved in accuracy to produce better plans.
	 *
	 * 100k on the row estimate causes the postgres planner to behave very much like the
	 * old citus planner in the plans it produces. Namely the old planner had hardcoded
	 * the use of Hash Aggregates for most of the operations, unless a postgres guc was
	 * set that would disallow hash aggregates to be used.
	 *
	 * Ideally we would be able to provide estimates close to postgres' estimates on the
	 * workers to let the standard planner choose an optimal solution for the masterQuery.
	 */
	path->custom_path.path.rows = 100000;
	path->remoteScan = remoteScan;

	return (Path *) path;
}


/*
 * CitusCustomScanPathPlan is called for the CitusCustomScanPath node in the best_path
 * after the postgres planner has evaluated all possible paths.
 *
 * This function returns a Plan node, more specifically the CustomScan Plan node that has
 * the ability to execute the distributed part of the query.
 *
 * When this function is called there is an extra list of clauses passed in that might not
 * already have been applied to the plan. We add these clauses to the quals this node will
 * execute. The quals are evaluated before returning the tuples scanned from the workers
 * to the plan above ours to make sure they do not end up in the final result.
 */
static Plan *
CitusCustomScanPathPlan(PlannerInfo *root,
						RelOptInfo *rel,
						struct CustomPath *best_path,
						List *tlist,
						List *clauses,
						List *custom_plans)
{
	CitusCustomScanPath *citusPath = (CitusCustomScanPath *) best_path;

	/* clauses might have been added by the planner, need to add them to our scan */
	RestrictInfo *restrictInfo = NULL;
	List **quals = &citusPath->remoteScan->scan.plan.qual;
	foreach_ptr(restrictInfo, clauses)
	{
		*quals = lappend(*quals, restrictInfo->clause);
	}
	return (Plan *) citusPath->remoteScan;
}


/*
 * BuildSelectStatementViaStdPlanner creates a PlannedStmt where it combines the
 * masterQuery and the remoteScan. It utilizes the standard_planner from postgres to
 * create a plan based on the masterQuery.
 */
static PlannedStmt *
BuildSelectStatementViaStdPlanner(Query *masterQuery, List *masterTargetList,
								  CustomScan *remoteScan)
{
	/*
	 * the standard planner will scribble on the target list. Since it is essential to not
	 * change the custom_scan_tlist we copy the target list before adding them to any.
	 * The masterTargetList is used in the end to extract the column names to be added to
	 * the alias we will create for the CustomScan, (expressed as the
	 * citus_extradata_container function call in the masterQuery).
	 */
	remoteScan->custom_scan_tlist = copyObject(masterTargetList);
	remoteScan->scan.plan.targetlist = copyObject(masterTargetList);

	/* probably want to do this where we add sublinks to the master plan */
	masterQuery->hasSubLinks = checkExprHasSubLink((Node *) masterQuery);
	Assert(masterQuery->hasWindowFuncs == contain_window_function((Node *) masterQuery));

	/*
	 * We will overwrite the alias of the rangetable which describes the custom scan.
	 * Ideally we would have set the correct column names and alias on the range table in
	 * the master query already when we inserted the extra data container. This could be
	 * improved in the future.
	 */

	/* find the rangetable entry for the extradata container and overwrite its alias */
	RangeTblEntry *extradataContainerRTE = NULL;
	FindCitusExtradataContainerRTE((Node *) masterQuery, &extradataContainerRTE);
	if (extradataContainerRTE != NULL)
	{
		/* extract column names from the masterTargetList */
		List *columnNameList = NIL;
		TargetEntry *targetEntry = NULL;
		foreach_ptr(targetEntry, masterTargetList)
		{
			columnNameList = lappend(columnNameList, makeString(targetEntry->resname));
		}
		extradataContainerRTE->eref = makeAlias("remote_scan", columnNameList);
	}

	/*
	 * Print the master query at debug level 4. Since serializing the query is relatively
	 * cpu intensive we only perform that if we are actually logging DEBUG4.
	 */
	const int logMasterQueryLevel = DEBUG4;
	if (IsLoggableLevel(logMasterQueryLevel))
	{
		StringInfo queryString = makeStringInfo();
		pg_get_query_def(masterQuery, queryString);
		elog(logMasterQueryLevel, "master query: %s", queryString->data);
	}

	PlannedStmt *standardStmt = NULL;
	PG_TRY();
	{
		/* This code should not be re-entrant, we check via asserts below */
		Assert(ReplaceCitusExtraDataContainer == false);
		Assert(ReplaceCitusExtraDataContainerWithCustomScan == NULL);
		ReplaceCitusExtraDataContainer = true;
		ReplaceCitusExtraDataContainerWithCustomScan = remoteScan;

		standardStmt = standard_planner(masterQuery, 0, NULL);

		ReplaceCitusExtraDataContainer = false;
		ReplaceCitusExtraDataContainerWithCustomScan = NULL;
	}
	PG_CATCH();
	{
		ReplaceCitusExtraDataContainer = false;
		ReplaceCitusExtraDataContainerWithCustomScan = NULL;
		PG_RE_THROW();
	}
	PG_END_TRY();

	Assert(standardStmt != NULL);
	return standardStmt;
}


/*
 * Finds the rangetable entry in the query that refers to the citus_extradata_container
 * and stores the pointer in result.
 */
static bool
FindCitusExtradataContainerRTE(Node *node, RangeTblEntry **result)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTblEntry = castNode(RangeTblEntry, node);
		if (rangeTblEntry->rtekind == RTE_FUNCTION &&
			list_length(rangeTblEntry->functions) == 1)
		{
			RangeTblFunction *rangeTblFunction = (RangeTblFunction *) linitial(
				rangeTblEntry->functions);
			FuncExpr *funcExpr = castNode(FuncExpr, rangeTblFunction->funcexpr);
			if (funcExpr->funcid == CitusExtraDataContainerFuncId())
			{
				*result = rangeTblEntry;
				return true;
			}
		}

		/* query_tree_walker descends into RTEs */
		return false;
	}
	else if (IsA(node, Query))
	{
#if PG_VERSION_NUM >= 120000
		const int flags = QTW_EXAMINE_RTES_BEFORE;
#else
		const int flags = QTW_EXAMINE_RTES;
#endif
		return query_tree_walker((Query *) node, FindCitusExtradataContainerRTE, result,
								 flags);
	}

	return expression_tree_walker(node, FindCitusExtradataContainerRTE, result);
}
