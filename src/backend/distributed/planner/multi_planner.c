/*-------------------------------------------------------------------------
 *
 * multi_planner.c
 *	  General Citus planner code.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <float.h>
#include <limits.h>

#include "catalog/pg_type.h"

#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_nodes.h"
#include "distributed/insert_select_planner.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_router_planner.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


static List *plannerRestrictionContextList = NIL;
int MultiTaskQueryLogLevel = MULTI_TASK_QUERY_INFO_OFF; /* multi-task query log level */

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

static CustomScanMethods CoordinatorInsertSelectCustomScanMethods = {
	"Citus INSERT ... SELECT via coordinator",
	CoordinatorInsertSelectCreateScan
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
static void AdjustParseTree(Query *parse, bool assignRTEIdentities,
							bool setPartitionedTablesInherited);
static void AssignRTEIdentity(RangeTblEntry *rangeTableEntry, int rteIdentifier);
static PlannedStmt * FinalizePlan(PlannedStmt *localPlan, MultiPlan *multiPlan);
static PlannedStmt * FinalizeNonRouterPlan(PlannedStmt *localPlan, MultiPlan *multiPlan,
										   CustomScan *customScan);
static PlannedStmt * FinalizeRouterPlan(PlannedStmt *localPlan, CustomScan *customScan);
static void CheckNodeIsDumpable(Node *node);
static Node * CheckNodeCopyAndSerialization(Node *node);
static List * CopyPlanParamList(List *originalPlanParamList);
static PlannerRestrictionContext * CreateAndPushPlannerRestrictionContext(void);
static PlannerRestrictionContext * CurrentPlannerRestrictionContext(void);
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
	bool assignRTEIdentities = false;
	bool setPartitionedTablesInherited = false;

	/*
	 * standard_planner scribbles on it's input, but for deparsing we need the
	 * unmodified form. So copy once we're sure it's a distributed query.
	 */
	if (needsDistributedPlanning)
	{
		originalQuery = copyObject(parse);
		assignRTEIdentities = true;
		setPartitionedTablesInherited = false;

		AdjustParseTree(parse, assignRTEIdentities, setPartitionedTablesInherited);
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

			assignRTEIdentities = false;
			setPartitionedTablesInherited = true;

			AdjustParseTree(parse, assignRTEIdentities, setPartitionedTablesInherited);
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
 * AdjustParseTree function modifies query tree by adding RTE identities to the
 * RTE_RELATIONs and changing inh flag and relkind of partitioned tables. We
 * perform these operations to ensure PostgreSQL's standard planner behaves as
 * we need.
 *
 * Please note that, we want to avoid modifying query tree as much as possible
 * because if PostgreSQL changes the way it uses modified fields, that may break
 * our logic.
 */
static void
AdjustParseTree(Query *queryTree, bool assignRTEIdentities,
				bool setPartitionedTablesInherited)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	int rteIdentifier = 1;

	/* extract range table entries for simple relations only */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		/*
		 * To be able to track individual RTEs through PostgreSQL's query
		 * planning, we need to be able to figure out whether an RTE is
		 * actually a copy of another, rather than a different one. We
		 * simply number the RTEs starting from 1.
		 *
		 * Note that we're only interested in RTE_RELATIONs and thus assigning
		 * identifiers to those RTEs only.
		 */
		if (assignRTEIdentities && rangeTableEntry->rtekind == RTE_RELATION)
		{
			AssignRTEIdentity(rangeTableEntry, rteIdentifier++);
		}

		/*
		 * We want Postgres to behave partitioned tables as regular relations
		 * (i.e. we do not want to expand them to their partitions). To do this
		 * we set each distributed partitioned table's inh flag to appropriate
		 * value before and after dropping to the standart_planner.
		 */
		if (IsDistributedTable(rangeTableEntry->relid) &&
			PartitionedTable(rangeTableEntry->relid))
		{
			rangeTableEntry->inh = setPartitionedTablesInherited;

#if (PG_VERSION_NUM >= 100000)
			if (setPartitionedTablesInherited)
			{
				rangeTableEntry->relkind = RELKIND_PARTITIONED_TABLE;
			}
			else
			{
				rangeTableEntry->relkind = RELKIND_RELATION;
			}
#endif
		}
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
		commandType == CMD_DELETE)
	{
		return true;
	}

	return false;
}


/*
 * IsMultiShardModifyPlan returns true if the given plan was generated for
 * multi shard update or delete query.
 */
bool
IsMultiShardModifyPlan(MultiPlan *multiPlan)
{
	if (IsUpdateOrDelete(multiPlan) && IsMultiTaskPlan(multiPlan))
	{
		return true;
	}

	return false;
}


/*
 * IsMultiTaskPlan returns true if job contains multiple tasks.
 */
bool
IsMultiTaskPlan(MultiPlan *multiPlan)
{
	Job *workerJob = multiPlan->workerJob;

	if (workerJob != NULL && list_length(workerJob->taskList) > 1)
	{
		return true;
	}

	return false;
}


/*
 * IsUpdateOrDelete returns true if the query performs update or delete.
 */
bool
IsUpdateOrDelete(MultiPlan *multiPlan)
{
	CmdType commandType = multiPlan->operation;

	if (commandType == CMD_UPDATE || commandType == CMD_DELETE)
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

	if (HasUnresolvedExternParamsWalker((Node *) originalQuery, boundParams))
	{
		hasUnresolvedParams = true;
	}

	if (IsModifyCommand(query))
	{
		EnsureModificationsCanRun();

		if (InsertSelectIntoDistributedTable(originalQuery))
		{
			distributedPlan =
				CreateInsertSelectPlan(originalQuery, plannerRestrictionContext);
		}
		else
		{
			/* modifications are always routed through the same planner/executor */
			distributedPlan =
				CreateModifyPlan(originalQuery, query, plannerRestrictionContext);
		}

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
			MultiTreeRoot *logicalPlan = MultiLogicalPlanCreate(originalQuery, query,
																plannerRestrictionContext,
																boundParams);
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
	 * query planning failed (possibly) due to prepared statement parameters or
	 * if it is planned as a multi shard modify query.
	 */
	if ((distributedPlan->planningError || IsMultiShardModifyPlan(distributedPlan)) &&
		hasUnresolvedParams)
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
	Node *node = NULL;
	MultiPlan *multiPlan = NULL;

	Assert(list_length(customScan->custom_private) == 1);

	node = (Node *) linitial(customScan->custom_private);
	Assert(CitusIsA(node, MultiPlan));

	node = CheckNodeCopyAndSerialization(node);

	/*
	 * When using prepared statements the same plan gets reused across
	 * multiple statements and transactions. We make several modifications
	 * to the MultiPlan during execution such as assigning task placements
	 * and evaluating functions and parameters. These changes should not
	 * persist, so we always work on a copy.
	 */
	multiPlan = (MultiPlan *) copyObject(node);

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

		case MULTI_EXECUTOR_COORDINATOR_INSERT_SELECT:
		{
			customScan->methods = &CoordinatorInsertSelectCustomScanMethods;
			break;
		}

		default:
		{
			customScan->methods = &DelayedErrorCustomScanMethods;
			break;
		}
	}

	if (IsMultiTaskPlan(multiPlan))
	{
		/* if it is not a single task executable plan, inform user according to the log level */
		if (MultiTaskQueryLogLevel != MULTI_TASK_QUERY_INFO_OFF)
		{
			ereport(MultiTaskQueryLogLevel, (errmsg(
												 "multi-task query about to be executed"),
											 errhint(
												 "Queries are split to multiple tasks "
												 "if they have to be split into several"
												 " queries on the workers.")));
		}
	}

	multiPlan->relationIdList = localPlan->relationOids;

	multiPlanData = (Node *) multiPlan;

	customScan->custom_private = list_make1(multiPlanData);
	customScan->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;

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
 * nodeToString(). As this checks is expensive, it's only active when
 * assertions are enabled.
 */
static void
CheckNodeIsDumpable(Node *node)
{
#ifdef USE_ASSERT_CHECKING
	char *out = nodeToString(node);
	pfree(out);
#endif
}


/*
 * CheckNodeCopyAndSerialization checks copy/dump/read functions
 * for nodes and returns copy of the input.
 *
 * It is only active when assertions are enabled, otherwise it returns
 * the input directly. We use this to confirm that our serialization
 * and copy logic produces the correct plan during regression tests.
 *
 * It does not check string equality on node dumps due to differences
 * in some Postgres types.
 */
static Node *
CheckNodeCopyAndSerialization(Node *node)
{
#ifdef USE_ASSERT_CHECKING
	char *out = nodeToString(node);
	Node *deserializedNode = (Node *) stringToNode(out);
	Node *nodeCopy = copyObject(deserializedNode);
	char *outCopy = nodeToString(nodeCopy);

	pfree(out);
	pfree(outCopy);

	return nodeCopy;
#else
	return node;
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
	PlannerRestrictionContext *plannerRestrictionContext = NULL;
	JoinRestrictionContext *joinRestrictionContext = NULL;
	JoinRestriction *joinRestriction = NULL;
	MemoryContext restrictionsMemoryContext = NULL;
	MemoryContext oldMemoryContext = NULL;
	List *restrictInfoList = NIL;

	/*
	 * Use a memory context that's guaranteed to live long enough, could be
	 * called in a more shorted lived one (e.g. with GEQO).
	 */
	plannerRestrictionContext = CurrentPlannerRestrictionContext();
	restrictionsMemoryContext = plannerRestrictionContext->memoryContext;
	oldMemoryContext = MemoryContextSwitchTo(restrictionsMemoryContext);

	/*
	 * We create a copy of restrictInfoList because it may be created in a memory
	 * context which will be deleted when we still need it, thus we create a copy
	 * of it in our memory context.
	 */
	restrictInfoList = copyObject(extra->restrictlist);

	joinRestrictionContext = plannerRestrictionContext->joinRestrictionContext;
	Assert(joinRestrictionContext != NULL);

	joinRestriction = palloc0(sizeof(JoinRestriction));
	joinRestriction->joinType = jointype;
	joinRestriction->joinRestrictInfoList = restrictInfoList;
	joinRestriction->plannerInfo = root;
	joinRestriction->innerrel = innerrel;
	joinRestriction->outerrel = outerrel;

	joinRestrictionContext->joinRestrictionList =
		lappend(joinRestrictionContext->joinRestrictionList, joinRestriction);

	MemoryContextSwitchTo(oldMemoryContext);
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
	PlannerRestrictionContext *plannerRestrictionContext = NULL;
	RelationRestrictionContext *relationRestrictionContext = NULL;
	MemoryContext restrictionsMemoryContext = NULL;
	MemoryContext oldMemoryContext = NULL;
	RelationRestriction *relationRestriction = NULL;
	DistTableCacheEntry *cacheEntry = NULL;
	bool distributedTable = false;
	bool localTable = false;

	if (rte->rtekind != RTE_RELATION)
	{
		return;
	}

	/*
	 * Use a memory context that's guaranteed to live long enough, could be
	 * called in a more shorted lived one (e.g. with GEQO).
	 */
	plannerRestrictionContext = CurrentPlannerRestrictionContext();
	restrictionsMemoryContext = plannerRestrictionContext->memoryContext;
	oldMemoryContext = MemoryContextSwitchTo(restrictionsMemoryContext);

	distributedTable = IsDistributedTable(rte->relid);
	localTable = !distributedTable;

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

	relationRestrictionContext = plannerRestrictionContext->relationRestrictionContext;
	relationRestrictionContext->hasDistributedRelation |= distributedTable;
	relationRestrictionContext->hasLocalRelation |= localTable;

	/*
	 * We're also keeping track of whether all participant
	 * tables are reference tables.
	 */
	if (distributedTable)
	{
		cacheEntry = DistributedTableCacheEntry(rte->relid);

		relationRestrictionContext->allReferenceTables &=
			(cacheEntry->partitionMethod == DISTRIBUTE_BY_NONE);
	}

	relationRestrictionContext->relationRestrictionList =
		lappend(relationRestrictionContext->relationRestrictionList, relationRestriction);

	MemoryContextSwitchTo(oldMemoryContext);
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
 * CreateAndPushPlannerRestrictionContext creates a new relation restriction context
 * and a new join context, inserts it to the beginning of the
 * plannerRestrictionContextList. Finally, the planner restriction context is
 * inserted to the beginning of the plannerRestrictionContextList and it is returned.
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

	plannerRestrictionContext->memoryContext = CurrentMemoryContext;

	/* we'll apply logical AND as we add tables */
	plannerRestrictionContext->relationRestrictionContext->allReferenceTables = true;

	plannerRestrictionContextList = lcons(plannerRestrictionContext,
										  plannerRestrictionContextList);

	return plannerRestrictionContext;
}


/*
 * CurrentRestrictionContext returns the the most recently added
 * PlannerRestrictionContext from the plannerRestrictionContextList list.
 */
static PlannerRestrictionContext *
CurrentPlannerRestrictionContext(void)
{
	PlannerRestrictionContext *plannerRestrictionContext = NULL;

	Assert(plannerRestrictionContextList != NIL);

	plannerRestrictionContext =
		(PlannerRestrictionContext *) linitial(plannerRestrictionContextList);

	if (plannerRestrictionContext == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("planner restriction context stack was empty"),
						errdetail("Please report this to the Citus core team.")));
	}

	return plannerRestrictionContext;
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
