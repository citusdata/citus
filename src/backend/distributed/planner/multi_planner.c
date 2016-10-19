/*-------------------------------------------------------------------------
 *
 * multi_planner.c
 *	  General Citus planner code.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>

#include "catalog/pg_type.h"

#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_nodes.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"

#include "executor/executor.h"

#include "nodes/makefuncs.h"

#include "optimizer/planner.h"

#include "utils/memutils.h"


static List *relationRestrictionContextList = NIL;


/* local function forward declarations */
static void CheckNodeIsDumpable(Node *node);


/* local function forward declarations */
static char * GetMultiPlanString(PlannedStmt *result);


/* Distributed planner hook */
PlannedStmt *
multi_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result = NULL;
	bool needsDistributedPlanning = NeedsDistributedPlanning(parse);
	Query *originalQuery = NULL;
	RelationRestrictionContext *restrictionContext = NULL;

	/*
	 * standard_planner scribbles on it's input, but for deparsing we need the
	 * unmodified form. So copy once we're sure it's a distributed query.
	 */
	if (needsDistributedPlanning)
	{
		originalQuery = copyObject(parse);

		/*
		 * We implement INSERT INTO .. SELECT by pushing down the SELECT to
		 * each shard. To compute that we use the router planner, by adding
		 * an "uninstantiated" constraint that the partition column be equal to a
		 * certain value. standard_planner() distributes that constraint to
		 * the baserestrictinfos of the tables that they are connected
		 * via equi joins.
		 *
		 * The router planner then iterates over the target table's shards,
		 * for each we replace the "uninstantiated" restriction, with one that
		 * PruneShardList() handles, and then generate a query for that
		 * individual shard. If any of the involved tables don't prune down
		 * to a single shard, or if the pruned shards aren't colocated,
		 * we error out.
		 */
		if (InsertSelectQuery(parse))
		{
			AddUninstantiatedPartitionColumnEqualityQual(parse);
		}
	}

	/* create a restriction context and put it at the end if context list */
	restrictionContext = CreateAndPushRestrictionContext();

	PG_TRY();
	{
		/*
		 * First call into standard planner. This is required because the Citus
		 * planner relies on parse tree transformations made by postgres' planner.
		 */

		result = standard_planner(parse, cursorOptions, boundParams);

		if (needsDistributedPlanning)
		{
			MultiPlan *physicalPlan = CreatePhysicalPlan(originalQuery, parse,
														 restrictionContext);

			/* store required data into the planned statement */
			result = MultiQueryContainerNode(result, physicalPlan);
		}
	}
	PG_CATCH();
	{
		PopRestrictionContext();
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* remove the context from the context list */
	PopRestrictionContext();

	return result;
}


/*
 * CreatePhysicalPlan encapsulates the logic needed to transform a particular
 * query into a physical plan. For modifications, queries immediately enter
 * the physical planning stage, since they are essentially "routed" to remote
 * target shards. SELECT queries go through the full logical plan/optimize/
 * physical plan process needed to produce distributed query plans.
 */
MultiPlan *
CreatePhysicalPlan(Query *originalQuery, Query *query,
				   RelationRestrictionContext *restrictionContext)
{
	MultiPlan *physicalPlan = MultiRouterPlanCreate(originalQuery, query,
													TaskExecutorType, restrictionContext);
	if (physicalPlan == NULL)
	{
		/* Create and optimize logical plan */
		MultiTreeRoot *logicalPlan = MultiLogicalPlanCreate(query);
		MultiLogicalPlanOptimize(logicalPlan);

		/*
		 * This check is here to make it likely that all node types used in
		 * Citus are dumpable. Explain can dump logical and physical plans
		 * using the extended outfuncs infrastructure, but it's infeasible to
		 * test most plans. MultiQueryContainerNode always serializes the
		 * physical plan, so there's no need to check that separately.
		 */
		CheckNodeIsDumpable((Node *) logicalPlan);

		/* Create the physical plan */
		physicalPlan = MultiPhysicalPlanCreate(logicalPlan);
	}

	return physicalPlan;
}


/*
 * GetMultiPlan returns the associated MultiPlan for a PlannedStmt if the
 * statement requires distributed execution, NULL otherwise.
 */
MultiPlan *
GetMultiPlan(PlannedStmt *result)
{
	char *serializedMultiPlan = NULL;
	MultiPlan *multiPlan = NULL;

	serializedMultiPlan = GetMultiPlanString(result);
	multiPlan = (MultiPlan *) CitusStringToNode(serializedMultiPlan);
	Assert(CitusIsA(multiPlan, MultiPlan));

	return multiPlan;
}


/* Does the passed in statement require distributed execution? */
bool
HasCitusToplevelNode(PlannedStmt *result)
{
	/*
	 * Can't be a distributed query if the extension hasn't been loaded
	 * yet. Directly return false, part of the required infrastructure for
	 * further checks might not be present.
	 */
	if (!CitusHasBeenLoaded())
	{
		return false;
	}

	if (GetMultiPlanString(result) == NULL)
	{
		return false;
	}
	else
	{
		return true;
	}
}


/*
 * CreateCitusToplevelNode creates the top-level planTree node for a
 * distributed statement. That top-level node is a) recognizable by the
 * executor hooks, allowing them to redirect execution, b) contains the
 * parameters required for distributed execution.
 *
 * The exact representation of the top-level node is an implementation detail
 * which should not be referred to outside this file, as it's likely to become
 * version dependant. Use GetMultiPlan() and HasCitusToplevelNode() to access.
 *
 * Internally the data is stored as arguments to a 'citus_extradata_container'
 * function, which has to be removed from the really executed plan tree before
 * query execution.
 */
PlannedStmt *
MultiQueryContainerNode(PlannedStmt *result, MultiPlan *multiPlan)
{
	FunctionScan *fauxFunctionScan = NULL;
	RangeTblFunction *fauxFunction = NULL;
	FuncExpr *fauxFuncExpr = NULL;
	Const *multiPlanData = NULL;
	char *serializedPlan = NULL;

	/* pass multiPlan serialized as a constant function argument */
	serializedPlan = CitusNodeToString(multiPlan);
	multiPlanData = makeNode(Const);
	multiPlanData->consttype = CSTRINGOID;
	multiPlanData->constlen = strlen(serializedPlan);
	multiPlanData->constvalue = CStringGetDatum(serializedPlan);
	multiPlanData->constbyval = false;
	multiPlanData->location = -1;

	fauxFuncExpr = makeNode(FuncExpr);
	fauxFuncExpr->funcid = CitusExtraDataContainerFuncId();
	fauxFuncExpr->funcretset = true;
	fauxFuncExpr->location = -1;

	fauxFuncExpr->args = list_make1(multiPlanData);
	fauxFunction = makeNode(RangeTblFunction);
	fauxFunction->funcexpr = (Node *) fauxFuncExpr;

	fauxFunctionScan = makeNode(FunctionScan);
	fauxFunctionScan->functions = lappend(fauxFunctionScan->functions, fauxFunction);

	/* copy original targetlist, accessed for RETURNING queries  */
	fauxFunctionScan->scan.plan.targetlist = copyObject(result->planTree->targetlist);

	/*
	 * Add set returning function to target list if the original (postgres
	 * created) plan doesn't support backward scans; doing so prevents
	 * backward scans being supported by the new plantree as well.  This is
	 * ugly as hell, but until we can rely on custom scans (which can signal
	 * this via CUSTOMPATH_SUPPORT_BACKWARD_SCAN), there's not really a pretty
	 * method to achieve this.
	 *
	 * FIXME: This should really be done on the master select plan.
	 */
	if (!ExecSupportsBackwardScan(result->planTree))
	{
		FuncExpr *funcExpr = makeNode(FuncExpr);
		TargetEntry *targetEntry = NULL;
		bool resjunkAttribute = true;

		funcExpr->funcretset = true;

		targetEntry = makeTargetEntry((Expr *) funcExpr, InvalidAttrNumber, NULL,
									  resjunkAttribute);

		fauxFunctionScan->scan.plan.targetlist =
			lappend(fauxFunctionScan->scan.plan.targetlist,
					targetEntry);
	}

	result->planTree = (Plan *) fauxFunctionScan;

	return result;
}


/*
 * GetMultiPlanString returns either NULL, if the plan is not a distributed
 * one, or the string representing the distributed plan.
 */
static char *
GetMultiPlanString(PlannedStmt *result)
{
	FunctionScan *fauxFunctionScan = NULL;
	RangeTblFunction *fauxFunction = NULL;
	FuncExpr *fauxFuncExpr = NULL;
	Const *multiPlanData = NULL;

	if (!IsA(result->planTree, FunctionScan))
	{
		return NULL;
	}

	fauxFunctionScan = (FunctionScan *) result->planTree;

	if (list_length(fauxFunctionScan->functions) != 1)
	{
		return NULL;
	}

	fauxFunction = linitial(fauxFunctionScan->functions);

	if (!IsA(fauxFunction->funcexpr, FuncExpr))
	{
		return NULL;
	}

	fauxFuncExpr = (FuncExpr *) fauxFunction->funcexpr;

	if (fauxFuncExpr->funcid != CitusExtraDataContainerFuncId())
	{
		return NULL;
	}

	if (list_length(fauxFuncExpr->args) != 1)
	{
		ereport(ERROR, (errmsg("unexpected number of function arguments to "
							   "citus_extradata_container")));
	}

	multiPlanData = (Const *) linitial(fauxFuncExpr->args);
	Assert(IsA(multiPlanData, Const));
	Assert(multiPlanData->consttype == CSTRINGOID);

	return DatumGetCString(multiPlanData->constvalue);
}


/*
 * CheckNodeIsDumpable checks that the passed node can be dumped using
 * CitusNodeToString(). As this checks is expensive, it's only active when
 * assertions are enabled.
 */
static void
CheckNodeIsDumpable(Node *node)
{
#ifdef USE_ASSERT_CHECKING
	char *out = CitusNodeToString(node);
	pfree(out);
#endif
}


/*
 * multi_relation_restriction_hook is a hook called by postgresql standard planner
 * to notify us about various planning information regarding a relation. We use
 * it to retrieve restrictions on relations.
 */
void
multi_relation_restriction_hook(PlannerInfo *root, RelOptInfo *relOptInfo, Index index,
								RangeTblEntry *rte)
{
	RelationRestrictionContext *restrictionContext = NULL;
	RelationRestriction *relationRestriction = NULL;
	bool distributedTable = false;
	bool localTable = false;

	if (rte->rtekind != RTE_RELATION)
	{
		return;
	}

	distributedTable = IsDistributedTable(rte->relid);
	localTable = !distributedTable;

	restrictionContext = CurrentRestrictionContext();
	Assert(restrictionContext != NULL);

	relationRestriction = palloc0(sizeof(RelationRestriction));
	relationRestriction->index = index;
	relationRestriction->relationId = rte->relid;
	relationRestriction->rte = rte;
	relationRestriction->relOptInfo = relOptInfo;
	relationRestriction->distributedRelation = distributedTable;
	relationRestriction->plannerInfo = root;
	relationRestriction->prunedShardIntervalList = NIL;

	restrictionContext->hasDistributedRelation |= distributedTable;
	restrictionContext->hasLocalRelation |= localTable;

	restrictionContext->relationRestrictionList =
		lappend(restrictionContext->relationRestrictionList, relationRestriction);
}


/*
 * CreateAndPushRestrictionContext creates a new restriction context, inserts it to the
 * beginning of the context list, and returns the newly created context.
 */
RelationRestrictionContext *
CreateAndPushRestrictionContext(void)
{
	RelationRestrictionContext *restrictionContext =
		palloc0(sizeof(RelationRestrictionContext));
	relationRestrictionContextList = lcons(restrictionContext,
										   relationRestrictionContextList);

	return restrictionContext;
}


/*
 * CurrentRestrictionContext returns the the last restriction context from the
 * list.
 */
RelationRestrictionContext *
CurrentRestrictionContext(void)
{
	RelationRestrictionContext *restrictionContext = NULL;

	Assert(relationRestrictionContextList != NIL);

	restrictionContext =
		(RelationRestrictionContext *) linitial(relationRestrictionContextList);

	return restrictionContext;
}


/*
 * PopRestrictionContext removes the most recently added restriction context from
 * context list. The function assumes the list is not empty.
 */
void
PopRestrictionContext(void)
{
	relationRestrictionContextList = list_delete_first(relationRestrictionContextList);
}
