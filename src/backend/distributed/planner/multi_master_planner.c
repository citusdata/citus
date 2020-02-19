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
#include "distributed/listutils.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_physical_planner.h"
#include "nodes/makefuncs.h"
#include "optimizer/planner.h"
#include "rewrite/rewriteManip.h"

static List * MasterTargetList(List *workerTargetList);
static PlannedStmt * BuildSelectStatementViaStdPlanner(Query *masterQuery,
													   List *masterTargetList,
													   CustomScan *remoteScan);

static Plan * CitusCustomScanPathPlan(PlannerInfo *root, RelOptInfo *rel,
									  struct CustomPath *best_path, List *tlist,
									  List *clauses, List *custom_plans);
struct List * CitusCustomScanPathReparameterize(PlannerInfo *root,
												List *custom_private,
												RelOptInfo *child_rel);


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


bool ReplaceCitusExtraDataContainer = false;
CustomScan *ReplaceCitusExtraDataContainerWithCustomScan = NULL;
static CustomPathMethods CitusCustomScanPathMethods = {
	.CustomName = "CitusCustomScanPath",
	.PlanCustomPath = CitusCustomScanPathPlan,
	.ReparameterizeCustomPathByChild = CitusCustomScanPathReparameterize
};


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

	/* TODO come up with reasonable row counts */
	path->custom_path.path.rows = 100000;
	path->remoteScan = remoteScan;

	return (Path *) path;
}


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


struct List *
CitusCustomScanPathReparameterize(PlannerInfo *root,
								  List *custom_private,
								  RelOptInfo *child_rel)
{
	return NIL;
}


static PlannedStmt *
BuildSelectStatementViaStdPlanner(Query *masterQuery, List *masterTargetList,
								  CustomScan *remoteScan)
{
	/*
	 * the standard planner will scribble on the target list, since we need the target
	 * list for alias creation we make a copy here.
	 */

	remoteScan->custom_scan_tlist = copyObject(masterTargetList);
	remoteScan->scan.plan.targetlist = copyObject(masterTargetList);

	/* probably want to do this where we add sublinks to the master plan */
	masterQuery->hasSubLinks = checkExprHasSubLink((Node *) masterQuery);

	/* This code should not be re-entrant */
	PlannedStmt *standardStmt = NULL;
	PG_TRY();
	{
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

	List *columnNameList = NIL;
	TargetEntry *targetEntry = NULL;

	/*
	 * (7) Replace rangetable with one with nice names to show in EXPLAIN plans
	 */
	foreach_ptr(targetEntry, masterTargetList)
	{
		columnNameList = lappend(columnNameList, makeString(targetEntry->resname));
	}

	RangeTblEntry *customScanRangeTableEntry = linitial(standardStmt->rtable);
	customScanRangeTableEntry->eref = makeAlias("remote_scan", columnNameList);

	return standardStmt;
}
