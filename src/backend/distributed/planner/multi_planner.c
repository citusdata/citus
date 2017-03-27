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
#include "distributed/multi_executor.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_router_planner.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "utils/memutils.h"


static List *plannerRestrictionContextList = NIL;

/* create custom scan methods for separate executors */
static CustomScanMethods RealTimeCustomScanMethods = {
	"Citus Real-Time",
	RealTimeCreateScan
};

static CustomScanMethods TaskTrackerCustomScanMethods = {
	"Citus Task-Tracker",
	TaskTrackerCreateScan
};

static CustomScanMethods RouterCustomScanMethods = {
	"Citus Router",
	RouterCreateScan
};

static CustomScanMethods DelayedErrorCustomScanMethods = {
	"Citus Delayed Error",
	DelayedErrorCreateScan
};


/* local function forward declarations */
static PlannedStmt * CreateDistributedPlan(PlannedStmt *localPlan, Query *originalQuery,
										   Query *query, ParamListInfo boundParams,
										   PlannerRestrictionContext *
										   plannerRestrictionContext);
static void AssignRTEIdentities(Query *queryTree);
static void AssignRTEIdentity(RangeTblEntry *rangeTableEntry, int rteIdentifier);
static Node * SerializeMultiPlan(struct MultiPlan *multiPlan);
static MultiPlan * DeserializeMultiPlan(Node *node);
static PlannedStmt * FinalizePlan(PlannedStmt *localPlan, MultiPlan *multiPlan);
static PlannedStmt * FinalizeNonRouterPlan(PlannedStmt *localPlan, MultiPlan *multiPlan,
										   CustomScan *customScan);
static PlannedStmt * FinalizeRouterPlan(PlannedStmt *localPlan, CustomScan *customScan);
static void CheckNodeIsDumpable(Node *node);
static List * CopyPlanParamList(List *originalPlanParamList);
static PlannerRestrictionContext * CreateAndPushPlannerRestrictionContext(void);
static RelationRestrictionContext * CurrentRelationRestrictionContext(void);
static JoinRestrictionContext * CurrentJoinRestrictionContext(void);
static void PopPlannerRestrictionContext(void);
static bool HasUnresolvedExternParamsWalker(Node *expression, ParamListInfo boundParams);


/* Distributed planner hook */
PlannedStmt *
multi_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result = NULL;
	bool needsDistributedPlanning = NeedsDistributedPlanning(parse);
	Query *originalQuery = NULL;
	PlannerRestrictionContext *plannerRestrictionContext = NULL;

	/*
	 * standard_planner scribbles on it's input, but for deparsing we need the
	 * unmodified form. So copy once we're sure it's a distributed query.
	 */
	if (needsDistributedPlanning)
	{
		originalQuery = copyObject(parse);

		AssignRTEIdentities(parse);
	}

	/* create a restriction context and put it at the end if context list */
	plannerRestrictionContext = CreateAndPushPlannerRestrictionContext();

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
										   boundParams, plannerRestrictionContext);
		}
	}
	PG_CATCH();
	{
		PopPlannerRestrictionContext();
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* remove the context from the context list */
	PopPlannerRestrictionContext();

	/*
	 * In some cases, for example; parameterized SQL functions, we may miss that
	 * there is a need for distributed planning. Such cases only become clear after
	 * standart_planner performs some modifications on parse tree. In such cases
	 * we will simply error out.
	 */
	if (!needsDistributedPlanning && NeedsDistributedPlanning(parse))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning on this "
							   "query because parameterized queries for SQL "
							   "functions referencing distributed tables are "
							   "not supported"),
						errhint("Consider using PL/pgSQL functions instead.")));
	}

	return result;
}


/*
 * AssignRTEIdentities assigns unique identities to the
 * RTE_RELATIONs in the given query.
 *
 * To be able to track individual RTEs through postgres' query
 * planning, we need to be able to figure out whether an RTE is
 * actually a copy of another, rather than a different one. We
 * simply number the RTEs starting from 1.
 *
 * Note that we're only interested in RTE_RELATIONs and thus assigning
 * identifiers to those RTEs only.
 */
static void
AssignRTEIdentities(Query *queryTree)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	int rteIdentifier = 1;

	/* extract range table entries for simple relations only */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (rangeTableEntry->rtekind != RTE_RELATION)
		{
			continue;
		}

		AssignRTEIdentity(rangeTableEntry, rteIdentifier++);
	}
}


/*
 * AssignRTEIdentity assigns the given rteIdentifier to the given range table
 * entry.
 *
 * To be able to track RTEs through postgres' query planning, which copies and
 * duplicate, and modifies them, we sometimes need to figure out whether two
 * RTEs are copies of the same original RTE. For that we, hackishly, use a
 * field normally unused in RTE_RELATION RTEs.
 *
 * The assigned identifier better be unique within a plantree.
 */
static void
AssignRTEIdentity(RangeTblEntry *rangeTableEntry, int rteIdentifier)
{
	Assert(rangeTableEntry->rtekind == RTE_RELATION);
	Assert(rangeTableEntry->values_lists == NIL);

	rangeTableEntry->values_lists = list_make1_int(rteIdentifier);
}


/* GetRTEIdentity returns the identity assigned with AssignRTEIdentity. */
int
GetRTEIdentity(RangeTblEntry *rte)
{
	Assert(rte->rtekind == RTE_RELATION);
	Assert(IsA(rte->values_lists, IntList));
	Assert(list_length(rte->values_lists) == 1);

	return linitial_int(rte->values_lists);
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
 * IsModifyMultiPlan returns true if the multi plan performs modifications,
 * false otherwise.
 */
bool
IsModifyMultiPlan(MultiPlan *multiPlan)
{
	bool isModifyMultiPlan = false;
	CmdType operation = multiPlan->operation;

	if (operation == CMD_INSERT || operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		isModifyMultiPlan = true;
	}

	return isModifyMultiPlan;
}


/*
 * CreateDistributedPlan encapsulates the logic needed to transform a particular
 * query into a distributed plan.
 */
static PlannedStmt *
CreateDistributedPlan(PlannedStmt *localPlan, Query *originalQuery, Query *query,
					  ParamListInfo boundParams,
					  PlannerRestrictionContext *plannerRestrictionContext)
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
		distributedPlan =
			CreateModifyPlan(originalQuery, query, plannerRestrictionContext);

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
			RelationRestrictionContext *relationRestrictionContext =
				plannerRestrictionContext->relationRestrictionContext;

			distributedPlan = CreateRouterPlan(originalQuery, query,
											   relationRestrictionContext);

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
			MultiTreeRoot *logicalPlan = MultiLogicalPlanCreate(originalQuery, query);
			MultiLogicalPlanOptimize(logicalPlan, plannerRestrictionContext);

			/*
			 * This check is here to make it likely that all node types used in
			 * Citus are dumpable. Explain can dump logical and physical plans
			 * using the extended outfuncs infrastructure, but it's infeasible to
			 * test most plans. MultiQueryContainerNode always serializes the
			 * physical plan, so there's no need to check that separately.
			 */
			CheckNodeIsDumpable((Node *) logicalPlan);

			/* Create the physical plan */
			distributedPlan = MultiPhysicalPlanCreate(logicalPlan,
													  plannerRestrictionContext);

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
						  "Consider using PL/pgSQL functions instead.");
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

	/* create final plan by combining local plan with distributed plan */
	resultPlan = FinalizePlan(localPlan, distributedPlan);

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
 * GetMultiPlan returns the associated MultiPlan for a CustomScan.
 */
MultiPlan *
GetMultiPlan(CustomScan *customScan)
{
	MultiPlan *multiPlan = NULL;

	Assert(list_length(customScan->custom_private) == 1);

	multiPlan = DeserializeMultiPlan(linitial(customScan->custom_private));

	return multiPlan;
}


/*
 * SerializeMultiPlan returns the string representing the distributed plan in a
 * Const node.
 *
 * Note that this should be improved for 9.6+, we we can copy trees efficiently.
 * I.e. we should introduce copy support for relevant node types, and just
 * return the MultiPlan as-is for 9.6.
 */
static Node *
SerializeMultiPlan(MultiPlan *multiPlan)
{
	char *serializedMultiPlan = NULL;
	Const *multiPlanData = NULL;

	serializedMultiPlan = CitusNodeToString(multiPlan);

	multiPlanData = makeNode(Const);
	multiPlanData->consttype = CSTRINGOID;
	multiPlanData->constlen = strlen(serializedMultiPlan);
	multiPlanData->constvalue = CStringGetDatum(serializedMultiPlan);
	multiPlanData->constbyval = false;
	multiPlanData->location = -1;

	return (Node *) multiPlanData;
}


/*
 * DeserializeMultiPlan returns the deserialized distributed plan from the string
 * representation in a Const node.
 */
static MultiPlan *
DeserializeMultiPlan(Node *node)
{
	Const *multiPlanData = NULL;
	char *serializedMultiPlan = NULL;
	MultiPlan *multiPlan = NULL;

	Assert(IsA(node, Const));
	multiPlanData = (Const *) node;
	serializedMultiPlan = DatumGetCString(multiPlanData->constvalue);

	multiPlan = (MultiPlan *) CitusStringToNode(serializedMultiPlan);
	Assert(CitusIsA(multiPlan, MultiPlan));

	return multiPlan;
}


/*
 * FinalizePlan combines local plan with distributed plan and creates a plan
 * which can be run by the PostgreSQL executor.
 */
static PlannedStmt *
FinalizePlan(PlannedStmt *localPlan, MultiPlan *multiPlan)
{
	PlannedStmt *finalPlan = NULL;
	CustomScan *customScan = makeNode(CustomScan);
	Node *multiPlanData = NULL;
	MultiExecutorType executorType = MULTI_EXECUTOR_INVALID_FIRST;

	if (!multiPlan->planningError)
	{
		executorType = JobExecutorType(multiPlan);
	}

	switch (executorType)
	{
		case MULTI_EXECUTOR_REAL_TIME:
		{
			customScan->methods = &RealTimeCustomScanMethods;
			break;
		}

		case MULTI_EXECUTOR_TASK_TRACKER:
		{
			customScan->methods = &TaskTrackerCustomScanMethods;
			break;
		}

		case MULTI_EXECUTOR_ROUTER:
		{
			customScan->methods = &RouterCustomScanMethods;
			break;
		}

		default:
		{
			customScan->methods = &DelayedErrorCustomScanMethods;
			break;
		}
	}

	multiPlanData = SerializeMultiPlan(multiPlan);

	customScan->custom_private = list_make1(multiPlanData);
	customScan->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;

	/* check if we have a master query */
	if (multiPlan->masterQuery)
	{
		finalPlan = FinalizeNonRouterPlan(localPlan, multiPlan, customScan);
	}
	else
	{
		finalPlan = FinalizeRouterPlan(localPlan, customScan);
	}

	return finalPlan;
}


/*
 * FinalizeNonRouterPlan gets the distributed custom scan plan, and creates the
 * final master select plan on the top of this distributed plan for real-time
 * and task-tracker executors.
 */
static PlannedStmt *
FinalizeNonRouterPlan(PlannedStmt *localPlan, MultiPlan *multiPlan,
					  CustomScan *customScan)
{
	PlannedStmt *finalPlan = NULL;

	finalPlan = MasterNodeSelectPlan(multiPlan, customScan);
	finalPlan->queryId = localPlan->queryId;
	finalPlan->utilityStmt = localPlan->utilityStmt;

	/* add original range table list for access permission checks */
	finalPlan->rtable = list_concat(finalPlan->rtable, localPlan->rtable);

	return finalPlan;
}


/*
 * FinalizeRouterPlan gets a CustomScan node which already wrapped distributed
 * part of a router plan and sets it as the direct child of the router plan
 * because we don't run any query on master node for router executable queries.
 * Here, we also rebuild the column list to read from the remote scan.
 */
static PlannedStmt *
FinalizeRouterPlan(PlannedStmt *localPlan, CustomScan *customScan)
{
	PlannedStmt *routerPlan = NULL;
	RangeTblEntry *remoteScanRangeTableEntry = NULL;
	ListCell *targetEntryCell = NULL;
	List *targetList = NIL;
	List *columnNameList = NIL;

	/* we will have custom scan range table entry as the first one in the list */
	int customScanRangeTableIndex = 1;

	/* build a targetlist to read from the custom scan output */
	foreach(targetEntryCell, localPlan->planTree->targetlist)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);
		TargetEntry *newTargetEntry = NULL;
		Var *newVar = NULL;
		Value *columnName = NULL;

		Assert(IsA(targetEntry, TargetEntry));

		/*
		 * This is unlikely to be hit because we would not need resjunk stuff
		 * at the toplevel of a router query - all things needing it have been
		 * pushed down.
		 */
		if (targetEntry->resjunk)
		{
			continue;
		}

		/* build target entry pointing to remote scan range table entry */
		newVar = makeVarFromTargetEntry(customScanRangeTableIndex, targetEntry);
		newTargetEntry = flatCopyTargetEntry(targetEntry);
		newTargetEntry->expr = (Expr *) newVar;
		targetList = lappend(targetList, newTargetEntry);

		columnName = makeString(targetEntry->resname);
		columnNameList = lappend(columnNameList, columnName);
	}

	customScan->scan.plan.targetlist = targetList;

	routerPlan = makeNode(PlannedStmt);
	routerPlan->planTree = (Plan *) customScan;

	remoteScanRangeTableEntry = RemoteScanRangeTableEntry(columnNameList);
	routerPlan->rtable = list_make1(remoteScanRangeTableEntry);

	/* add original range table list for access permission checks */
	routerPlan->rtable = list_concat(routerPlan->rtable, localPlan->rtable);

	routerPlan->canSetTag = true;
	routerPlan->relationOids = NIL;

	routerPlan->queryId = localPlan->queryId;
	routerPlan->utilityStmt = localPlan->utilityStmt;
	routerPlan->commandType = localPlan->commandType;
	routerPlan->hasReturning = localPlan->hasReturning;

	return routerPlan;
}


/*
 * RemoteScanRangeTableEntry creates a range table entry from given column name
 * list to represent a remote scan.
 */
RangeTblEntry *
RemoteScanRangeTableEntry(List *columnNameList)
{
	RangeTblEntry *remoteScanRangeTableEntry = makeNode(RangeTblEntry);

	/* we use RTE_VALUES for custom scan because we can't look up relation */
	remoteScanRangeTableEntry->rtekind = RTE_VALUES;
	remoteScanRangeTableEntry->eref = makeAlias("remote_scan", columnNameList);
	remoteScanRangeTableEntry->inh = false;
	remoteScanRangeTableEntry->inFromCl = true;

	return remoteScanRangeTableEntry;
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
 * multi_join_restriction_hook is a hook called by postgresql standard planner
 * to notify us about various planning information regarding joins. We use
 * it to learn about the joining column.
 */
void
multi_join_restriction_hook(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra)
{
	JoinRestrictionContext *joinContext = NULL;
	JoinRestriction *joinRestriction = palloc0(sizeof(JoinRestriction));
	List *restrictInfoList = NIL;

	restrictInfoList = extra->restrictlist;
	joinContext = CurrentJoinRestrictionContext();
	Assert(joinContext != NULL);

	joinRestriction->joinType = jointype;
	joinRestriction->joinRestrictInfoList = restrictInfoList;
	joinRestriction->plannerInfo = root;

	joinContext->joinRestrictionList =
		lappend(joinContext->joinRestrictionList, joinRestriction);
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

	restrictionContext = CurrentRelationRestrictionContext();
	Assert(restrictionContext != NULL);

	relationRestriction = palloc0(sizeof(RelationRestriction));
	relationRestriction->index = index;
	relationRestriction->relationId = rte->relid;
	relationRestriction->rte = rte;
	relationRestriction->relOptInfo = relOptInfo;
	relationRestriction->distributedRelation = distributedTable;
	relationRestriction->plannerInfo = root;
	relationRestriction->parentPlannerInfo = root->parent_root;
	relationRestriction->prunedShardIntervalList = NIL;

	/* see comments on GetVarFromAssignedParam() */
	if (relationRestriction->parentPlannerInfo)
	{
		relationRestriction->parentPlannerParamList =
			CopyPlanParamList(root->parent_root->plan_params);
	}

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
 * CopyPlanParamList deep copies the input PlannerParamItem list and returns the newly
 * allocated list.
 * Note that we cannot use copyObject() function directly since there is no support for
 * copying PlannerParamItem structs.
 */
static List *
CopyPlanParamList(List *originalPlanParamList)
{
	ListCell *planParamCell = NULL;
	List *copiedPlanParamList = NIL;

	foreach(planParamCell, originalPlanParamList)
	{
		PlannerParamItem *originalParamItem = lfirst(planParamCell);
		PlannerParamItem *copiedParamItem = makeNode(PlannerParamItem);

		copiedParamItem->paramId = originalParamItem->paramId;
		copiedParamItem->item = copyObject(originalParamItem->item);

		copiedPlanParamList = lappend(copiedPlanParamList, copiedParamItem);
	}

	return copiedPlanParamList;
}


/*
 * CreateAndPushPlannerRestrictionContext creates a new planner restriction context.
 * Later, it creates a relation restriction context and a join restriction
 * context, and sets those contexts in the planner restriction context. Finally,
 * the planner restriction context is inserted to the beginning of the
 * plannerRestrictionContextList and it is returned.
 */
static PlannerRestrictionContext *
CreateAndPushPlannerRestrictionContext(void)
{
	PlannerRestrictionContext *plannerRestrictionContext =
		palloc0(sizeof(PlannerRestrictionContext));

	plannerRestrictionContext->relationRestrictionContext =
		palloc0(sizeof(RelationRestrictionContext));

	plannerRestrictionContext->joinRestrictionContext =
		palloc0(sizeof(JoinRestrictionContext));

	/* we'll apply logical AND as we add tables */
	plannerRestrictionContext->relationRestrictionContext->allReferenceTables = true;

	plannerRestrictionContextList = lcons(plannerRestrictionContext,
										  plannerRestrictionContextList);

	return plannerRestrictionContext;
}


/*
 * CurrentRelationRestrictionContext returns the the last restriction context from the
 * relationRestrictionContext list.
 */
static RelationRestrictionContext *
CurrentRelationRestrictionContext(void)
{
	PlannerRestrictionContext *plannerRestrictionContext = NULL;
	RelationRestrictionContext *relationRestrictionContext = NULL;

	Assert(plannerRestrictionContextList != NIL);

	plannerRestrictionContext =
		(PlannerRestrictionContext *) linitial(plannerRestrictionContextList);

	relationRestrictionContext = plannerRestrictionContext->relationRestrictionContext;

	return relationRestrictionContext;
}


/*
 * CurrentJoinRestrictionContext returns the the last restriction context from the
 * list.
 */
static JoinRestrictionContext *
CurrentJoinRestrictionContext(void)
{
	PlannerRestrictionContext *plannerRestrictionContext = NULL;
	JoinRestrictionContext *joinRestrictionContext = NULL;

	Assert(plannerRestrictionContextList != NIL);

	plannerRestrictionContext =
		(PlannerRestrictionContext *) linitial(plannerRestrictionContextList);

	joinRestrictionContext = plannerRestrictionContext->joinRestrictionContext;

	return joinRestrictionContext;
}


/*
 * PopPlannerRestrictionContext removes the most recently added restriction contexts from
 * the planner restriction context list. The function assumes the list is not empty.
 */
static void
PopPlannerRestrictionContext(void)
{
	plannerRestrictionContextList = list_delete_first(plannerRestrictionContextList);
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
