/*-------------------------------------------------------------------------
 *
 * combine_query_planner.c
 *	  Routines for planning the combine query that runs on the coordinator
 *    to combine results from the workers.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/clauses.h"
#include "optimizer/planner.h"
#include "rewrite/rewriteManip.h"

#include "pg_version_constants.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/combine_query_planner.h"
#include "distributed/insert_select_planner.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_physical_planner.h"

static List * RemoteScanTargetList(List *workerTargetList);
static PlannedStmt * BuildSelectStatementViaStdPlanner(Query *combineQuery,
													   List *remoteScanTargetList,
													   CustomScan *remoteScan);

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
 * PlanCombineQuery takes in a distributed plan and a custom scan node which
 * wraps remote part of the plan. This function finds the combine query structure
 * in the multi plan, and builds the final select plan to execute on the tuples
 * returned by remote scan on the coordinator node. Note that this select
 * plan is executed after result files are retrieved from worker nodes and
 * filled into the tuple store inside provided custom scan.
 */
PlannedStmt *
PlanCombineQuery(DistributedPlan *distributedPlan, CustomScan *remoteScan)
{
	Query *combineQuery = distributedPlan->combineQuery;

	Job *workerJob = distributedPlan->workerJob;
	List *workerTargetList = workerJob->jobQuery->targetList;
	List *remoteScanTargetList = RemoteScanTargetList(workerTargetList);
	return BuildSelectStatementViaStdPlanner(combineQuery, remoteScanTargetList,
											 remoteScan);
}


/*
 * RemoteScanTargetList uses the given worker target list's expressions, and creates
 * a target list for the remote scan on the coordinator node.
 */
static List *
RemoteScanTargetList(List *workerTargetList)
{
	List *remoteScanTargetList = NIL;
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

		Var *remoteScanColumn = makeVarFromTargetEntry(tableId, workerTargetEntry);
		remoteScanColumn->varattno = columnId;
		remoteScanColumn->varattnosyn = columnId;
		columnId++;

		if (remoteScanColumn->vartype == RECORDOID || remoteScanColumn->vartype ==
			RECORDARRAYOID)
		{
			remoteScanColumn->vartypmod = BlessRecordExpression(workerTargetEntry->expr);
		}

		/*
		 * The remote scan target entry has two pieces to it. The first piece is the
		 * target entry's expression, which we set to the newly created column.
		 * The second piece is sort and group clauses that we implicitly copy
		 * from the worker target entry. Note that any changes to worker target
		 * entry's sort and group clauses will *break* us here.
		 */
		TargetEntry *remoteScanTargetEntry = flatCopyTargetEntry(workerTargetEntry);
		remoteScanTargetEntry->expr = (Expr *) remoteScanColumn;
		remoteScanTargetList = lappend(remoteScanTargetList, remoteScanTargetEntry);
	}

	return remoteScanTargetList;
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

	/* necessary to avoid extra Result node in PG15 */
	path->custom_path.flags = CUSTOMPATH_SUPPORT_PROJECTION;

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
	 * workers to let the standard planner choose an optimal solution for the combineQuery.
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

	/*
	 * Columns could have been pruned from the target list by the standard planner.
	 * A situation in which this might happen is a CASE that is proven to be always the
	 * same causing the other column to become useless;
	 *   CASE WHEN ... <> NULL
	 *     THEN ...
	 *     ELSE ...
	 *   END
	 * Since nothing is equal to NULL it will always end up in the else branch. The final
	 * target list the planenr needs from our node is passed in as tlist. By placing that
	 * as the target list on our scan the internal rows will be projected to this one.
	 */
	citusPath->remoteScan->scan.plan.targetlist = tlist;

	/*
	 * The custom_scan_tlist contains target entries for to the "output" of the call
	 * to citus_extradata_container, which is actually replaced by a CustomScan.
	 * The target entries are initialized with varno 1 (see RemoteScanTargetList), since
	 * it's currently the only relation in the join tree of the combineQuery.
	 *
	 * If the citus_extradata_container function call is not the first relation to
	 * appear in the flattened rtable for the entire plan, then varno is now pointing
	 * to the wrong relation and needs to be updated.
	 *
	 * Example:
	 * When the combineQuery field of the DistributedPlan is
	 * INSERT INTO local SELECT .. FROM citus_extradata_container.
	 * In that case the varno of citusdata_extradata_container should be 3, because
	 * it is preceded range table entries for "local" and the subquery.
	 */
	if (rel->relid != 1)
	{
		TargetEntry *targetEntry = NULL;

		foreach_declared_ptr(targetEntry, citusPath->remoteScan->custom_scan_tlist)
		{
			/* we created this list, so we know it only contains Var */
			Assert(IsA(targetEntry->expr, Var));

			Var *var = (Var *) targetEntry->expr;

			var->varno = rel->relid;
		}
	}

	/* clauses might have been added by the planner, need to add them to our scan */
	RestrictInfo *restrictInfo = NULL;
	List **quals = &citusPath->remoteScan->scan.plan.qual;
	foreach_declared_ptr(restrictInfo, clauses)
	{
		*quals = lappend(*quals, restrictInfo->clause);
	}
	return (Plan *) citusPath->remoteScan;
}


/*
 * BuildSelectStatementViaStdPlanner creates a PlannedStmt where it combines the
 * combineQuery and the remoteScan. It utilizes the standard_planner from postgres to
 * create a plan based on the combineQuery.
 */
static PlannedStmt *
BuildSelectStatementViaStdPlanner(Query *combineQuery, List *remoteScanTargetList,
								  CustomScan *remoteScan)
{
	/*
	 * the standard planner will scribble on the target list. Since it is essential to not
	 * change the custom_scan_tlist we copy the target list before adding them to any.
	 * The remoteScanTargetList is used in the end to extract the column names to be added to
	 * the alias we will create for the CustomScan, (expressed as the
	 * citus_extradata_container function call in the combineQuery).
	 */
	remoteScan->custom_scan_tlist = copyObject(remoteScanTargetList);
	remoteScan->scan.plan.targetlist = copyObject(remoteScanTargetList);

	/*
	 * We will overwrite the alias of the rangetable which describes the custom scan.
	 * Ideally we would have set the correct column names and alias on the range table in
	 * the combine query already when we inserted the extra data container. This could be
	 * improved in the future.
	 */

	/* find the rangetable entry for the extradata container and overwrite its alias */
	RangeTblEntry *extradataContainerRTE = NULL;
	FindCitusExtradataContainerRTE((Node *) combineQuery, &extradataContainerRTE);
	if (extradataContainerRTE != NULL)
	{
		/* extract column names from the remoteScanTargetList */
		List *columnNameList = NIL;
		TargetEntry *targetEntry = NULL;
		foreach_declared_ptr(targetEntry, remoteScanTargetList)
		{
			columnNameList = lappend(columnNameList, makeString(targetEntry->resname));
		}
		extradataContainerRTE->eref = makeAlias("remote_scan", columnNameList);
	}

	/*
	 * Print the combine query at debug level 4. Since serializing the query is relatively
	 * cpu intensive we only perform that if we are actually logging DEBUG4.
	 */
	const int logCombineQueryLevel = DEBUG4;
	if (IsLoggableLevel(logCombineQueryLevel))
	{
		StringInfo queryString = makeStringInfo();
		pg_get_query_def(combineQuery, queryString);
		elog(logCombineQueryLevel, "combine query: %s", queryString->data);
	}

	PlannedStmt *standardStmt = NULL;
	PG_TRY();
	{
		/* This code should not be re-entrant, we check via asserts below */
		Assert(ReplaceCitusExtraDataContainer == false);
		Assert(ReplaceCitusExtraDataContainerWithCustomScan == NULL);
		ReplaceCitusExtraDataContainer = true;
		ReplaceCitusExtraDataContainerWithCustomScan = remoteScan;

		standardStmt = standard_planner(combineQuery, NULL, 0, NULL);

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
 * ExtractCitusExtradataContainerRTE is a helper function that stores rangeTblEntry
 * to result if it has citus extra data container.
 *
 * The function returns true if it finds the RTE, and false otherwise.
 */
bool
ExtractCitusExtradataContainerRTE(RangeTblEntry *rangeTblEntry, RangeTblEntry **result)
{
        if (rangeTblEntry->rtekind == RTE_FUNCTION &&
                list_length(rangeTblEntry->functions) == 1)
        {
                RangeTblFunction *rangeTblFunction = (RangeTblFunction *) linitial(
                        rangeTblEntry->functions);
                if (!IsA(rangeTblFunction->funcexpr, FuncExpr))
                {
                        return false;
                }
                FuncExpr *funcExpr = castNode(FuncExpr, rangeTblFunction->funcexpr);
                if (funcExpr->funcid == CitusExtraDataContainerFuncId())
                {
                        *result = rangeTblEntry;
                        return true;
                }
        }

        return false;
}


/*
 * Finds the rangetable entry in the query that refers to the citus_extradata_container
 * and stores the pointer in result.
 */
bool
FindCitusExtradataContainerRTE(Node *node, RangeTblEntry **result)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTblEntry = castNode(RangeTblEntry, node);
		return ExtractCitusExtradataContainerRTE(rangeTblEntry, result);
	}
	else if (IsA(node, Query))
	{
		const int flags = QTW_EXAMINE_RTES_BEFORE;
		return query_tree_walker((Query *) node, FindCitusExtradataContainerRTE, result,
								 flags);
	}

	return expression_tree_walker(node, FindCitusExtradataContainerRTE, result);
}
