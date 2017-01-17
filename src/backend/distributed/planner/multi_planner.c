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

#include "executor/executor.h"

#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"

#include "optimizer/planner.h"

#include "utils/memutils.h"


static List *relationRestrictionContextList = NIL;


/* local function forward declarations */
static void CheckNodeIsDumpable(Node *node);
static char * GetMultiPlanString(PlannedStmt *result);
static PlannedStmt * MultiQueryContainerNode(PlannedStmt *result,
											 struct MultiPlan *multiPlan);
static struct PlannedStmt * CreateDistributedPlan(PlannedStmt *localPlan,
												  Query *originalQuery,
												  Query *query,
												  ParamListInfo boundParams,
												  RelationRestrictionContext *
												  restrictionContext);
static RelationRestrictionContext * CreateAndPushRestrictionContext(void);
static RelationRestrictionContext * CurrentRestrictionContext(void);
static void PopRestrictionContext(void);
static bool HasUnresolvedExternParamsWalker(Node *expression, ParamListInfo boundParams);


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
		 * the baserestrictinfos to all the tables where it knows how to push
		 * the restriction safely. An example is that the tables that are
		 * connected via equi joins.
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
			AddUninstantiatedPartitionRestriction(parse);
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
			result = CreateDistributedPlan(result, originalQuery, parse,
										   boundParams, restrictionContext);
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
 * IsModifyCommand returns true if the query performs modifications, false
 * otherwise.
 */
bool
IsModifyCommand(Query *query)
{
	CmdType commandType = query->commandType;

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE || query->hasModifyingCTE)
	{
		return true;
	}

	return false;
}


/*
 * VerifyMultiPlanValidity verifies that multiPlan is ready for execution, or
 * errors out if not.
 *
 * A plan may e.g. not be ready for execution because CreateDistributedPlan()
 * couldn't find a plan due to unresolved prepared statement parameters, but
 * didn't error out, because we expect custom plans to come to our rescue.
 * But sql (not plpgsql) functions unfortunately don't go through a codepath
 * supporting custom plans.
 */
void
VerifyMultiPlanValidity(MultiPlan *multiPlan)
{
	if (multiPlan->planningError)
	{
		RaiseDeferredError(multiPlan->planningError, ERROR);
	}
}


/*
 * CreateDistributedPlan encapsulates the logic needed to transform a particular
 * query into a distributed plan.
 */
static PlannedStmt *
CreateDistributedPlan(PlannedStmt *localPlan, Query *originalQuery, Query *query,
					  ParamListInfo boundParams,
					  RelationRestrictionContext *restrictionContext)
{
	MultiPlan *distributedPlan = NULL;
	PlannedStmt *resultPlan = NULL;
	bool hasUnresolvedParams = false;

	if (HasUnresolvedExternParamsWalker((Node *) query, boundParams))
	{
		hasUnresolvedParams = true;
	}

	if (IsModifyCommand(query))
	{
		/* modifications are always routed through the same planner/executor */
		distributedPlan = CreateModifyPlan(originalQuery, query, restrictionContext);
		Assert(distributedPlan);
	}
	else
	{
		/*
		 * For select queries we, if router executor is enabled, first try to
		 * plan the query as a router query. If not supported, otherwise try
		 * the full blown plan/optimize/physical planing process needed to
		 * produce distributed query plans.
		 */
		if (EnableRouterExecution)
		{
			distributedPlan = CreateRouterPlan(originalQuery, query, restrictionContext);

			/* for debugging it's useful to display why query was not router plannable */
			if (distributedPlan && distributedPlan->planningError)
			{
				RaiseDeferredError(distributedPlan->planningError, DEBUG1);
			}
		}

		/*
		 * Router didn't yield a plan, try the full distributed planner. As
		 * real-time/task-tracker don't support prepared statement parameters,
		 * skip planning in that case (we'll later trigger an error in that
		 * case if necessary).
		 */
		if ((!distributedPlan || distributedPlan->planningError) && !hasUnresolvedParams)
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
			distributedPlan = MultiPhysicalPlanCreate(logicalPlan);

			/* distributed plan currently should always succeed or error out */
			Assert(distributedPlan && distributedPlan->planningError == NULL);
		}
	}

	/*
	 * If no plan was generated, prepare a generic error to be emitted.
	 * Normally this error message will never returned to the user, as it's
	 * usually due to unresolved prepared statement parameters - in that case
	 * the logic below will force a custom plan (i.e. with parameters bound to
	 * specific values) to be generated.  But sql (not plpgsql) functions
	 * unfortunately don't go through a codepath supporting custom plans - so
	 * we still need to have an error prepared.
	 */
	if (!distributedPlan)
	{
		/* currently always should have a more specific error otherwise */
		Assert(hasUnresolvedParams);
		distributedPlan = CitusMakeNode(MultiPlan);
		distributedPlan->planningError =
			DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
						  "could not create distributed plan",
						  "Possibly this is caused by the use of parameters in SQL "
						  "functions, which is not supported in Citus.",
						  "Consider using PLPGSQL functions instead.");
	}

	/*
	 * Error out if none of the planners resulted in a usable plan, unless the
	 * error was possibly triggered by missing parameters.  In that case we'll
	 * not error out here, but instead rely on postgres' custom plan logic.
	 * Postgres re-plans prepared statements the first five executions
	 * (i.e. it produces custom plans), after that the cost of a generic plan
	 * is compared with the average custom plan cost.  We support otherwise
	 * unsupported prepared statement parameters by assigning an exorbitant
	 * cost to the unsupported query.  That'll lead to the custom plan being
	 * chosen.  But for that to be possible we can't error out here, as
	 * otherwise that logic is never reached.
	 */
	if (distributedPlan->planningError && !hasUnresolvedParams)
	{
		RaiseDeferredError(distributedPlan->planningError, ERROR);
	}

	/* store required data into the planned statement */
	resultPlan = MultiQueryContainerNode(localPlan, distributedPlan);

	/*
	 * As explained above, force planning costs to be unrealistically high if
	 * query planning failed (possibly) due to prepared statement parameters.
	 */
	if (distributedPlan->planningError && hasUnresolvedParams)
	{
		/*
		 * Arbitraryly high cost, but low enough that it can be added up
		 * without overflowing by choose_custom_plan().
		 */
		resultPlan->planTree->total_cost = FLT_MAX / 100000000;
	}

	return resultPlan;
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
	DistTableCacheEntry *cacheEntry = NULL;
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

	/*
	 * We're also keeping track of whether all participant
	 * tables are reference tables.
	 */
	if (distributedTable)
	{
		cacheEntry = DistributedTableCacheEntry(rte->relid);

		restrictionContext->allReferenceTables &=
			(cacheEntry->partitionMethod == DISTRIBUTE_BY_NONE);
	}

	restrictionContext->relationRestrictionList =
		lappend(restrictionContext->relationRestrictionList, relationRestriction);
}


/*
 * CreateAndPushRestrictionContext creates a new restriction context, inserts it to the
 * beginning of the context list, and returns the newly created context.
 */
static RelationRestrictionContext *
CreateAndPushRestrictionContext(void)
{
	RelationRestrictionContext *restrictionContext =
		palloc0(sizeof(RelationRestrictionContext));

	/* we'll apply logical AND as we add tables */
	restrictionContext->allReferenceTables = true;

	relationRestrictionContextList = lcons(restrictionContext,
										   relationRestrictionContextList);

	return restrictionContext;
}


/*
 * CurrentRestrictionContext returns the the last restriction context from the
 * list.
 */
static RelationRestrictionContext *
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
static void
PopRestrictionContext(void)
{
	relationRestrictionContextList = list_delete_first(relationRestrictionContextList);
}


/*
 * HasUnresolvedExternParamsWalker returns true if the passed in expression
 * has external parameters that are not contained in boundParams, false
 * otherwise.
 */
static bool
HasUnresolvedExternParamsWalker(Node *expression, ParamListInfo boundParams)
{
	if (expression == NULL)
	{
		return false;
	}

	if (IsA(expression, Param))
	{
		Param *param = (Param *) expression;
		int paramId = param->paramid;

		/* only care about user supplied parameters */
		if (param->paramkind != PARAM_EXTERN)
		{
			return false;
		}

		/* don't care about our special parameter, it'll be removed during planning */
		if (paramId == UNINSTANTIATED_PARAMETER_ID)
		{
			return false;
		}

		/* check whether parameter is available (and valid) */
		if (boundParams && paramId > 0 && paramId <= boundParams->numParams)
		{
			ParamExternData *externParam = &boundParams->params[paramId - 1];

			/* give hook a chance in case parameter is dynamic */
			if (!OidIsValid(externParam->ptype) && boundParams->paramFetch != NULL)
			{
				(*boundParams->paramFetch)(boundParams, paramId);
			}

			if (OidIsValid(externParam->ptype))
			{
				return false;
			}
		}

		return true;
	}

	/* keep traversing */
	if (IsA(expression, Query))
	{
		return query_tree_walker((Query *) expression,
								 HasUnresolvedExternParamsWalker,
								 boundParams,
								 0);
	}
	else
	{
		return expression_tree_walker(expression,
									  HasUnresolvedExternParamsWalker,
									  boundParams);
	}
}
