/*-------------------------------------------------------------------------
 *
 * distributed_planner.c
 *	  General Citus planner code.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "funcapi.h"

#include <float.h>
#include <limits.h>

#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/cte_inline.h"
#include "distributed/function_call_delegation.h"
#include "distributed/insert_select_planner.h"
#include "distributed/intermediate_result_pruning.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/distributed_planner.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/combine_query_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/query_utils.h"
#include "distributed/recursive_planning.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shard_utils.h"
#include "distributed/version_compat.h"
#include "distributed/worker_shard_visibility.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "parser/parsetree.h"
#include "parser/parse_type.h"
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "optimizer/planmain.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


static List *plannerRestrictionContextList = NIL;
int MultiTaskQueryLogLevel = CITUS_LOG_LEVEL_OFF; /* multi-task query log level */
static uint64 NextPlanId = 1;

/* keep track of planner call stack levels */
int PlannerLevel = 0;

static bool ListContainsDistributedTableRTE(List *rangeTableList,
											bool *maybeHasForeignDistributedTable);
static bool IsUpdateOrDelete(Query *query);
static PlannedStmt * CreateDistributedPlannedStmt(
	DistributedPlanningContext *planContext);
static PlannedStmt * InlineCtesAndCreateDistributedPlannedStmt(uint64 planId,
															   DistributedPlanningContext
															   *planContext);
static PlannedStmt * TryCreateDistributedPlannedStmt(PlannedStmt *localPlan,
													 Query *originalQuery,
													 Query *query, ParamListInfo
													 boundParams,
													 PlannerRestrictionContext *
													 plannerRestrictionContext);
static DeferredErrorMessage * DeferErrorIfPartitionTableNotSingleReplicated(Oid
																			relationId);

static int AssignRTEIdentities(List *rangeTableList, int rteIdCounter);
static void AssignRTEIdentity(RangeTblEntry *rangeTableEntry, int rteIdentifier);
static void AdjustPartitioningForDistributedPlanning(List *rangeTableList,
													 bool setPartitionedTablesInherited);
static PlannedStmt * FinalizeNonRouterPlan(PlannedStmt *localPlan,
										   DistributedPlan *distributedPlan,
										   CustomScan *customScan);
static PlannedStmt * FinalizeRouterPlan(PlannedStmt *localPlan, CustomScan *customScan);
static AppendRelInfo * FindTargetAppendRelInfo(PlannerInfo *root, int relationRteIndex);
static List * makeTargetListFromCustomScanList(List *custom_scan_tlist);
static List * makeCustomScanTargetlistFromExistingTargetList(List *existingTargetlist);
static int32 BlessRecordExpressionList(List *exprs);
static void CheckNodeIsDumpable(Node *node);
static Node * CheckNodeCopyAndSerialization(Node *node);
static void AdjustReadIntermediateResultCost(RangeTblEntry *rangeTableEntry,
											 RelOptInfo *relOptInfo);
static void AdjustReadIntermediateResultArrayCost(RangeTblEntry *rangeTableEntry,
												  RelOptInfo *relOptInfo);
static void AdjustReadIntermediateResultsCostInternal(RelOptInfo *relOptInfo,
													  List *columnTypes,
													  int resultIdCount,
													  Datum *resultIds,
													  Const *resultFormatConst);
static List * OuterPlanParamsList(PlannerInfo *root);
static List * CopyPlanParamList(List *originalPlanParamList);
static PlannerRestrictionContext * CreateAndPushPlannerRestrictionContext(void);
static PlannerRestrictionContext * CurrentPlannerRestrictionContext(void);
static void PopPlannerRestrictionContext(void);
static void ResetPlannerRestrictionContext(
	PlannerRestrictionContext *plannerRestrictionContext);
static PlannedStmt * PlanFastPathDistributedStmt(DistributedPlanningContext *planContext,
												 Node *distributionKeyValue);
static PlannedStmt * PlanDistributedStmt(DistributedPlanningContext *planContext,
										 int rteIdCounter);
static RTEListProperties * GetRTEListProperties(List *rangeTableList);
static List * TranslatedVars(PlannerInfo *root, int relationIndex);
static void WarnIfListHasForeignDistributedTable(List *rangeTableList);


/* Distributed planner hook */
PlannedStmt *
distributed_planner(Query *parse,
	#if PG_VERSION_NUM >= PG_VERSION_13
					const char *query_string,
	#endif
					int cursorOptions,
					ParamListInfo boundParams)
{
	bool needsDistributedPlanning = false;
	bool fastPathRouterQuery = false;
	Node *distributionKeyValue = NULL;

	List *rangeTableList = ExtractRangeTableEntryList(parse);

	if (cursorOptions & CURSOR_OPT_FORCE_DISTRIBUTED)
	{
		/* this cursor flag could only be set when Citus has been loaded */
		Assert(CitusHasBeenLoaded());

		needsDistributedPlanning = true;
	}
	else if (CitusHasBeenLoaded())
	{
		bool maybeHasForeignDistributedTable = false;
		needsDistributedPlanning =
			ListContainsDistributedTableRTE(rangeTableList,
											&maybeHasForeignDistributedTable);
		if (needsDistributedPlanning)
		{
			fastPathRouterQuery = FastPathRouterQuery(parse, &distributionKeyValue);

			if (maybeHasForeignDistributedTable)
			{
				WarnIfListHasForeignDistributedTable(rangeTableList);
			}
		}
	}

	int rteIdCounter = 1;

	DistributedPlanningContext planContext = {
		.query = parse,
		.cursorOptions = cursorOptions,
		.boundParams = boundParams,
	};

	if (needsDistributedPlanning)
	{
		/*
		 * standard_planner scribbles on it's input, but for deparsing we need the
		 * unmodified form. Before copying we call AssignRTEIdentities to be able
		 * to match RTEs in the rewritten query tree with those in the original
		 * tree.
		 */
		rteIdCounter = AssignRTEIdentities(rangeTableList, rteIdCounter);

		planContext.originalQuery = copyObject(parse);

		/*
		 * When there are partitioned tables (not applicable to fast path),
		 * pretend that they are regular tables to avoid unnecessary work
		 * in standard_planner.
		 */
		if (!fastPathRouterQuery)
		{
			bool setPartitionedTablesInherited = false;
			AdjustPartitioningForDistributedPlanning(rangeTableList,
													 setPartitionedTablesInherited);
		}
	}

	/*
	 * Make sure that we hide shard names on the Citus MX worker nodes. See comments in
	 * HideShardsFromSomeApplications() for the details.
	 */
	HideShardsFromSomeApplications(parse);

	/* create a restriction context and put it at the end if context list */
	planContext.plannerRestrictionContext = CreateAndPushPlannerRestrictionContext();

	/*
	 * We keep track of how many times we've recursed into the planner, primarily
	 * to detect whether we are in a function call. We need to make sure that the
	 * PlannerLevel is decremented exactly once at the end of the next PG_TRY
	 * block, both in the happy case and when an error occurs.
	 */
	PlannerLevel++;

	PlannedStmt *result = NULL;

	PG_TRY();
	{
		if (fastPathRouterQuery)
		{
			result = PlanFastPathDistributedStmt(&planContext, distributionKeyValue);
		}
		else
		{
			/*
			 * Call into standard_planner because the Citus planner relies on both the
			 * restriction information per table and parse tree transformations made by
			 * postgres' planner.
			 */
			planContext.plan = standard_planner_compat(planContext.query,
													   planContext.cursorOptions,
													   planContext.boundParams);
			if (needsDistributedPlanning)
			{
				result = PlanDistributedStmt(&planContext, rteIdCounter);
			}
			else if ((result = TryToDelegateFunctionCall(&planContext)) == NULL)
			{
				result = planContext.plan;
			}
		}
	}
	PG_CATCH();
	{
		PopPlannerRestrictionContext();

		PlannerLevel--;

		PG_RE_THROW();
	}
	PG_END_TRY();

	PlannerLevel--;

	/* remove the context from the context list */
	PopPlannerRestrictionContext();

	/*
	 * In some cases, for example; parameterized SQL functions, we may miss that
	 * there is a need for distributed planning. Such cases only become clear after
	 * standard_planner performs some modifications on parse tree. In such cases
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
 * ExtractRangeTableEntryList is a wrapper around ExtractRangeTableEntryWalker.
 * The function traverses the input query and returns all the range table
 * entries that are in the query tree.
 */
List *
ExtractRangeTableEntryList(Query *query)
{
	List *rteList = NIL;

	ExtractRangeTableEntryWalker((Node *) query, &rteList);

	return rteList;
}


/*
 * NeedsDistributedPlanning returns true if the Citus extension is loaded and
 * the query contains a distributed table.
 *
 * This function allows queries containing local tables to pass through the
 * distributed planner. How to handle local tables is a decision that should
 * be made within the planner
 */
bool
NeedsDistributedPlanning(Query *query)
{
	if (!CitusHasBeenLoaded())
	{
		return false;
	}

	CmdType commandType = query->commandType;

	if (commandType != CMD_SELECT && commandType != CMD_INSERT &&
		commandType != CMD_UPDATE && commandType != CMD_DELETE)
	{
		return false;
	}

	List *allRTEs = ExtractRangeTableEntryList(query);

	return ListContainsDistributedTableRTE(allRTEs, NULL);
}


/*
 * ListContainsDistributedTableRTE gets a list of range table entries
 * and returns true if there is at least one distributed relation range
 * table entry in the list. The boolean maybeHasForeignDistributedTable
 * variable is set to true if the list contains a foreign table.
 */
static bool
ListContainsDistributedTableRTE(List *rangeTableList,
								bool *maybeHasForeignDistributedTable)
{
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (rangeTableEntry->rtekind != RTE_RELATION)
		{
			continue;
		}

		if (IsCitusTable(rangeTableEntry->relid))
		{
			if (maybeHasForeignDistributedTable != NULL &&
				IsForeignTable(rangeTableEntry->relid))
			{
				*maybeHasForeignDistributedTable = true;
			}

			return true;
		}
	}

	return false;
}


/*
 * AssignRTEIdentities function modifies query tree by adding RTE identities to the
 * RTE_RELATIONs.
 *
 * Please note that, we want to avoid modifying query tree as much as possible
 * because if PostgreSQL changes the way it uses modified fields, that may break
 * our logic.
 *
 * Returns the next id. This can be used to call on a rangeTableList that may've
 * been partially assigned. Should be set to 1 initially.
 */
static int
AssignRTEIdentities(List *rangeTableList, int rteIdCounter)
{
	ListCell *rangeTableCell = NULL;

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
		if (rangeTableEntry->rtekind == RTE_RELATION &&
			rangeTableEntry->values_lists == NIL)
		{
			AssignRTEIdentity(rangeTableEntry, rteIdCounter++);
		}
	}

	return rteIdCounter;
}


/*
 * AdjustPartitioningForDistributedPlanning function modifies query tree by
 * changing inh flag and relkind of partitioned tables. We want Postgres to
 * treat partitioned tables as regular relations (i.e. we do not want to
 * expand them to their partitions) since it breaks Citus planning in different
 * ways. We let anything related to partitioning happen on the shards.
 *
 * Please note that, we want to avoid modifying query tree as much as possible
 * because if PostgreSQL changes the way it uses modified fields, that may break
 * our logic.
 */
static void
AdjustPartitioningForDistributedPlanning(List *rangeTableList,
										 bool setPartitionedTablesInherited)
{
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		/*
		 * We want Postgres to behave partitioned tables as regular relations
		 * (i.e. we do not want to expand them to their partitions). To do this
		 * we set each partitioned table's inh flag to appropriate
		 * value before and after dropping to the standart_planner.
		 */
		if (rangeTableEntry->rtekind == RTE_RELATION &&
			PartitionedTable(rangeTableEntry->relid))
		{
			rangeTableEntry->inh = setPartitionedTablesInherited;

			if (setPartitionedTablesInherited)
			{
				rangeTableEntry->relkind = RELKIND_PARTITIONED_TABLE;
			}
			else
			{
				rangeTableEntry->relkind = RELKIND_RELATION;
			}
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

	rangeTableEntry->values_lists = list_make2_int(rteIdentifier, rangeTableEntry->inh);
}


/* GetRTEIdentity returns the identity assigned with AssignRTEIdentity. */
int
GetRTEIdentity(RangeTblEntry *rte)
{
	Assert(rte->rtekind == RTE_RELATION);
	Assert(rte->values_lists != NIL);
	Assert(IsA(rte->values_lists, IntList));
	Assert(list_length(rte->values_lists) == 2);

	return linitial_int(rte->values_lists);
}


/*
 * GetOriginalInh gets the original value of the inheritance flag set by
 * AssignRTEIdentity. The planner resets this flag in the rewritten query,
 * but we need it during deparsing.
 */
bool
GetOriginalInh(RangeTblEntry *rte)
{
	return lsecond_int(rte->values_lists);
}


/*
 * GetQueryLockMode returns the necessary lock mode to be acquired for the
 * given query. (See comment written in RangeTblEntry->rellockmode)
 */
LOCKMODE
GetQueryLockMode(Query *query)
{
	if (IsModifyCommand(query))
	{
		return RowExclusiveLock;
	}
	else if (query->hasForUpdate)
	{
		return RowShareLock;
	}
	else
	{
		return AccessShareLock;
	}
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
 * IsMultiTaskPlan returns true if job contains multiple tasks.
 */
bool
IsMultiTaskPlan(DistributedPlan *distributedPlan)
{
	Job *workerJob = distributedPlan->workerJob;

	if (workerJob != NULL && list_length(workerJob->taskList) > 1)
	{
		return true;
	}

	return false;
}


/*
 * IsUpdateOrDelete returns true if the query performs an update or delete.
 */
bool
IsUpdateOrDelete(Query *query)
{
	return query->commandType == CMD_UPDATE ||
		   query->commandType == CMD_DELETE;
}


/*
 * PlanFastPathDistributedStmt creates a distributed planned statement using
 * the FastPathPlanner.
 */
static PlannedStmt *
PlanFastPathDistributedStmt(DistributedPlanningContext *planContext,
							Node *distributionKeyValue)
{
	FastPathRestrictionContext *fastPathContext =
		planContext->plannerRestrictionContext->fastPathRestrictionContext;

	planContext->plannerRestrictionContext->fastPathRestrictionContext->
	fastPathRouterQuery = true;

	if (distributionKeyValue == NULL)
	{
		/* nothing to record */
	}
	else if (IsA(distributionKeyValue, Const))
	{
		fastPathContext->distributionKeyValue = (Const *) distributionKeyValue;
	}
	else if (IsA(distributionKeyValue, Param))
	{
		fastPathContext->distributionKeyHasParam = true;
	}

	planContext->plan = FastPathPlanner(planContext->originalQuery, planContext->query,
										planContext->boundParams);

	return CreateDistributedPlannedStmt(planContext);
}


/*
 * PlanDistributedStmt creates a distributed planned statement using the PG
 * planner.
 */
static PlannedStmt *
PlanDistributedStmt(DistributedPlanningContext *planContext,
					int rteIdCounter)
{
	/* may've inlined new relation rtes */
	List *rangeTableList = ExtractRangeTableEntryList(planContext->query);
	rteIdCounter = AssignRTEIdentities(rangeTableList, rteIdCounter);


	PlannedStmt *result = CreateDistributedPlannedStmt(planContext);

	bool setPartitionedTablesInherited = true;
	AdjustPartitioningForDistributedPlanning(rangeTableList,
											 setPartitionedTablesInherited);

	return result;
}


/*
 * DissuadePlannerFromUsingPlan try dissuade planner when planning a plan that
 * potentially failed due to unresolved prepared statement parameters.
 */
void
DissuadePlannerFromUsingPlan(PlannedStmt *plan)
{
	/*
	 * Arbitrarily high cost, but low enough that it can be added up
	 * without overflowing by choose_custom_plan().
	 */
	plan->planTree->total_cost = FLT_MAX / 100000000;
}


/*
 * CreateDistributedPlannedStmt encapsulates the logic needed to transform a particular
 * query into a distributed plan that is encapsulated by a PlannedStmt.
 */
static PlannedStmt *
CreateDistributedPlannedStmt(DistributedPlanningContext *planContext)
{
	uint64 planId = NextPlanId++;
	bool hasUnresolvedParams = false;

	PlannedStmt *resultPlan = NULL;

	if (QueryTreeContainsInlinableCTE(planContext->originalQuery))
	{
		/*
		 * Inlining CTEs as subqueries in the query can avoid recursively
		 * planning some (or all) of the CTEs. In other words, the inlined
		 * CTEs could become part of query pushdown planning, which is much
		 * more efficient than recursively planning. So, first try distributed
		 * planning on the inlined CTEs in the query tree.
		 *
		 * We also should fallback to distributed planning with non-inlined CTEs
		 * if the distributed planning fails with inlined CTEs, because recursively
		 * planning CTEs can provide full SQL coverage, although it might be slow.
		 */
		resultPlan = InlineCtesAndCreateDistributedPlannedStmt(planId, planContext);
		if (resultPlan != NULL)
		{
			return resultPlan;
		}
	}

	if (HasUnresolvedExternParamsWalker((Node *) planContext->originalQuery,
										planContext->boundParams))
	{
		hasUnresolvedParams = true;
	}

	DistributedPlan *distributedPlan =
		CreateDistributedPlan(planId, planContext->originalQuery, planContext->query,
							  planContext->boundParams,
							  hasUnresolvedParams,
							  planContext->plannerRestrictionContext);

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
		distributedPlan = CitusMakeNode(DistributedPlan);
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

	/* remember the plan's identifier for identifying subplans */
	distributedPlan->planId = planId;

	/* create final plan by combining local plan with distributed plan */
	resultPlan = FinalizePlan(planContext->plan, distributedPlan);

	/*
	 * As explained above, force planning costs to be unrealistically high if
	 * query planning failed (possibly) due to prepared statement parameters or
	 * if it is planned as a multi shard modify query.
	 */
	if ((distributedPlan->planningError ||
		 (IsUpdateOrDelete(planContext->originalQuery) && IsMultiTaskPlan(
			  distributedPlan))) &&
		hasUnresolvedParams)
	{
		DissuadePlannerFromUsingPlan(resultPlan);
	}

	return resultPlan;
}


/*
 * InlineCtesAndCreateDistributedPlannedStmt gets all the parameters required
 * for creating a distributed planned statement. The function is primarily a
 * wrapper on top of CreateDistributedPlannedStmt(), by first inlining the
 * CTEs and calling CreateDistributedPlannedStmt() in PG_TRY() block. The
 * function returns NULL if the planning fails on the query where eligable
 * CTEs are inlined.
 */
static PlannedStmt *
InlineCtesAndCreateDistributedPlannedStmt(uint64 planId,
										  DistributedPlanningContext *planContext)
{
	/*
	 * We'll inline the CTEs and try distributed planning, preserve the original
	 * query in case the planning fails and we fallback to recursive planning of
	 * CTEs.
	 */
	Query *copyOfOriginalQuery = copyObject(planContext->originalQuery);

	RecursivelyInlineCtesInQueryTree(copyOfOriginalQuery);

	/* after inlining, we shouldn't have any inlinable CTEs */
	Assert(!QueryTreeContainsInlinableCTE(copyOfOriginalQuery));

	/* simply recurse into CreateDistributedPlannedStmt() in a PG_TRY() block */
	PlannedStmt *result = TryCreateDistributedPlannedStmt(planContext->plan,
														  copyOfOriginalQuery,
														  planContext->query,
														  planContext->boundParams,
														  planContext->
														  plannerRestrictionContext);

	return result;
}


/*
 * TryCreateDistributedPlannedStmt is a wrapper around CreateDistributedPlannedStmt, simply
 * calling it in PG_TRY()/PG_CATCH() block. The function returns a PlannedStmt if the input
 * query can be planned by Citus. If not, the function returns NULL and generates a DEBUG4
 * message with the reason for the failure.
 */
static PlannedStmt *
TryCreateDistributedPlannedStmt(PlannedStmt *localPlan,
								Query *originalQuery,
								Query *query, ParamListInfo boundParams,
								PlannerRestrictionContext *plannerRestrictionContext)
{
	MemoryContext savedContext = CurrentMemoryContext;
	PlannedStmt *result = NULL;

	DistributedPlanningContext *planContext = palloc0(sizeof(DistributedPlanningContext));

	planContext->plan = localPlan;
	planContext->boundParams = boundParams;
	planContext->originalQuery = originalQuery;
	planContext->query = query;
	planContext->plannerRestrictionContext = plannerRestrictionContext;


	PG_TRY();
	{
		result = CreateDistributedPlannedStmt(planContext);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData *edata = CopyErrorData();
		FlushErrorState();

		/* don't try to intercept PANIC or FATAL, let those breeze past us */
		if (edata->elevel != ERROR)
		{
			PG_RE_THROW();
		}

		ereport(DEBUG4, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("Planning after CTEs inlined failed with "
								"\nmessage: %s\ndetail: %s\nhint: %s",
								edata->message ? edata->message : "",
								edata->detail ? edata->detail : "",
								edata->hint ? edata->hint : "")));

		/* leave the error handling system */
		FreeErrorData(edata);

		result = NULL;
	}
	PG_END_TRY();

	return result;
}


/*
 * CreateDistributedPlan generates a distributed plan for a query.
 * It goes through 3 steps:
 *
 * 1. Try router planner
 * 2. Generate subplans for CTEs and complex subqueries
 *    - If any, go back to step 1 by calling itself recursively
 * 3. Logical planner
 */
DistributedPlan *
CreateDistributedPlan(uint64 planId, Query *originalQuery, Query *query, ParamListInfo
					  boundParams, bool hasUnresolvedParams,
					  PlannerRestrictionContext *plannerRestrictionContext)
{
	DistributedPlan *distributedPlan = NULL;
	bool hasCtes = originalQuery->cteList != NIL;

	if (IsModifyCommand(originalQuery))
	{
		Oid targetRelationId = ModifyQueryResultRelationId(query);

		EnsureModificationsCanRunOnRelation(targetRelationId);

		EnsurePartitionTableNotReplicated(targetRelationId);

		if (InsertSelectIntoCitusTable(originalQuery))
		{
			if (hasUnresolvedParams)
			{
				/*
				 * Unresolved parameters can cause performance regressions in
				 * INSERT...SELECT when the partition column is a parameter
				 * because we don't perform any additional pruning in the executor.
				 */
				return NULL;
			}

			distributedPlan =
				CreateInsertSelectPlan(planId, originalQuery, plannerRestrictionContext,
									   boundParams);
		}
		else if (InsertSelectIntoLocalTable(originalQuery))
		{
			if (hasUnresolvedParams)
			{
				/*
				 * Unresolved parameters can cause performance regressions in
				 * INSERT...SELECT when the partition column is a parameter
				 * because we don't perform any additional pruning in the executor.
				 */
				return NULL;
			}
			distributedPlan =
				CreateInsertSelectIntoLocalTablePlan(planId, originalQuery, boundParams,
													 hasUnresolvedParams,
													 plannerRestrictionContext);
		}
		else
		{
			/* modifications are always routed through the same planner/executor */
			distributedPlan =
				CreateModifyPlan(originalQuery, query, plannerRestrictionContext);
		}

		/* the functions above always return a plan, possibly with an error */
		Assert(distributedPlan);

		if (distributedPlan->planningError == NULL)
		{
			return distributedPlan;
		}
		else
		{
			RaiseDeferredError(distributedPlan->planningError, DEBUG2);
		}
	}
	else
	{
		/*
		 * For select queries we, if router executor is enabled, first try to
		 * plan the query as a router query. If not supported, otherwise try
		 * the full blown plan/optimize/physical planning process needed to
		 * produce distributed query plans.
		 */

		distributedPlan = CreateRouterPlan(originalQuery, query,
										   plannerRestrictionContext);
		if (distributedPlan->planningError == NULL)
		{
			return distributedPlan;
		}
		else
		{
			/*
			 * For debugging it's useful to display why query was not
			 * router plannable.
			 */
			RaiseDeferredError(distributedPlan->planningError, DEBUG2);
		}
	}

	if (hasUnresolvedParams)
	{
		/*
		 * There are parameters that don't have a value in boundParams.
		 *
		 * The remainder of the planning logic cannot handle unbound
		 * parameters. We return a NULL plan, which will have an
		 * extremely high cost, such that postgres will replan with
		 * bound parameters.
		 */
		return NULL;
	}

	/* force evaluation of bound params */
	boundParams = copyParamList(boundParams);

	/*
	 * If there are parameters that do have a value in boundParams, replace
	 * them in the original query. This allows us to more easily cut the
	 * query into pieces (during recursive planning) or deparse parts of
	 * the query (during subquery pushdown planning).
	 */
	originalQuery = (Query *) ResolveExternalParams((Node *) originalQuery,
													boundParams);
	Assert(originalQuery != NULL);

	/*
	 * Plan subqueries and CTEs that cannot be pushed down by recursively
	 * calling the planner and return the resulting plans to subPlanList.
	 */
	List *subPlanList = GenerateSubplansForSubqueriesAndCTEs(planId, originalQuery,
															 plannerRestrictionContext);

	/*
	 * If subqueries were recursively planned then we need to replan the query
	 * to get the new planner restriction context and apply planner transformations.
	 *
	 * We could simplify this code if the logical planner was capable of dealing
	 * with an original query. In that case, we would only have to filter the
	 * planner restriction context.
	 *
	 * Note that we check both for subplans and whether the query had CTEs
	 * prior to calling GenerateSubplansForSubqueriesAndCTEs. If none of
	 * the CTEs are referenced then there are no subplans, but we still want
	 * to retry the router planner.
	 */
	if (list_length(subPlanList) > 0 || hasCtes)
	{
		Query *newQuery = copyObject(originalQuery);
		bool setPartitionedTablesInherited = false;
		PlannerRestrictionContext *currentPlannerRestrictionContext =
			CurrentPlannerRestrictionContext();

		/* reset the current planner restrictions context */
		ResetPlannerRestrictionContext(currentPlannerRestrictionContext);

		/*
		 * We force standard_planner to treat partitioned tables as regular tables
		 * by clearing the inh flag on RTEs. We already did this at the start of
		 * distributed_planner, but on a copy of the original query, so we need
		 * to do it again here.
		 */
		AdjustPartitioningForDistributedPlanning(ExtractRangeTableEntryList(newQuery),
												 setPartitionedTablesInherited);

		/*
		 * Some relations may have been removed from the query, but we can skip
		 * AssignRTEIdentities since we currently do not rely on RTE identities
		 * being contiguous.
		 */

		standard_planner_compat(newQuery, 0, boundParams);

		/* overwrite the old transformed query with the new transformed query */
		*query = *newQuery;

		/* recurse into CreateDistributedPlan with subqueries/CTEs replaced */
		distributedPlan = CreateDistributedPlan(planId, originalQuery, query, NULL, false,
												plannerRestrictionContext);

		/* distributedPlan cannot be null since hasUnresolvedParams argument was false */
		Assert(distributedPlan != NULL);
		distributedPlan->subPlanList = subPlanList;

		return distributedPlan;
	}

	/*
	 * DML command returns a planning error, even after recursive planning. The
	 * logical planner cannot handle DML commands so return the plan with the
	 * error.
	 */
	if (IsModifyCommand(originalQuery))
	{
		return distributedPlan;
	}

	/*
	 * CTEs are stripped from the original query by RecursivelyPlanSubqueriesAndCTEs.
	 * If we get here and there are still CTEs that means that none of the CTEs are
	 * referenced. We therefore also strip the CTEs from the rewritten query.
	 */
	query->cteList = NIL;
	Assert(originalQuery->cteList == NIL);

	MultiTreeRoot *logicalPlan = MultiLogicalPlanCreate(originalQuery, query,
														plannerRestrictionContext);
	MultiLogicalPlanOptimize(logicalPlan);

	/*
	 * This check is here to make it likely that all node types used in
	 * Citus are dumpable. Explain can dump logical and physical plans
	 * using the extended outfuncs infrastructure, but it's infeasible to
	 * test most plans. MultiQueryContainerNode always serializes the
	 * physical plan, so there's no need to check that separately
	 */
	CheckNodeIsDumpable((Node *) logicalPlan);

	/* Create the physical plan */
	distributedPlan = CreatePhysicalDistributedPlan(logicalPlan,
													plannerRestrictionContext);

	/* distributed plan currently should always succeed or error out */
	Assert(distributedPlan && distributedPlan->planningError == NULL);

	return distributedPlan;
}


/*
 * EnsurePartitionTableNotReplicated errors out if the infput relation is
 * a partition table and the table has a replication factor greater than
 * one.
 *
 * If the table is not a partition or replication factor is 1, the function
 * becomes a no-op.
 */
void
EnsurePartitionTableNotReplicated(Oid relationId)
{
	DeferredErrorMessage *deferredError =
		DeferErrorIfPartitionTableNotSingleReplicated(relationId);
	if (deferredError != NULL)
	{
		RaiseDeferredError(deferredError, ERROR);
	}
}


/*
 * DeferErrorIfPartitionTableNotSingleReplicated defers error if the input relation
 * is a partition table with replication factor > 1. Otherwise, the function returns
 * NULL.
 */
static DeferredErrorMessage *
DeferErrorIfPartitionTableNotSingleReplicated(Oid relationId)
{
	if (PartitionTableNoLock(relationId) && !SingleReplicatedTable(relationId))
	{
		Oid parentOid = PartitionParentOid(relationId);
		char *parentRelationTest = get_rel_name(parentOid);
		StringInfo errorHint = makeStringInfo();

		appendStringInfo(errorHint, "Run the query on the parent table "
									"\"%s\" instead.", parentRelationTest);

		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "modifications on partitions when replication "
							 "factor is greater than 1 is not supported",
							 NULL, errorHint->data);
	}

	return NULL;
}


/*
 * ResolveExternalParams replaces the external parameters that appears
 * in the query with the corresponding entries in the boundParams.
 *
 * Note that this function is inspired by eval_const_expr() on Postgres.
 * We cannot use that function because it requires access to PlannerInfo.
 */
Node *
ResolveExternalParams(Node *inputNode, ParamListInfo boundParams)
{
	/* consider resolving external parameters only when boundParams exists */
	if (!boundParams)
	{
		return inputNode;
	}

	if (inputNode == NULL)
	{
		return NULL;
	}

	if (IsA(inputNode, Param))
	{
		Param *paramToProcess = (Param *) inputNode;
		int numberOfParameters = boundParams->numParams;
		int parameterId = paramToProcess->paramid;
		int16 typeLength = 0;
		bool typeByValue = false;
		Datum constValue = 0;

		if (paramToProcess->paramkind != PARAM_EXTERN)
		{
			return inputNode;
		}

		if (parameterId < 0)
		{
			return inputNode;
		}

		/* parameterId starts from 1 */
		int parameterIndex = parameterId - 1;
		if (parameterIndex >= numberOfParameters)
		{
			return inputNode;
		}

		ParamExternData *correspondingParameterData =
			&boundParams->params[parameterIndex];

		if (!(correspondingParameterData->pflags & PARAM_FLAG_CONST))
		{
			return inputNode;
		}

		get_typlenbyval(paramToProcess->paramtype, &typeLength, &typeByValue);

		bool paramIsNull = correspondingParameterData->isnull;
		if (paramIsNull)
		{
			constValue = 0;
		}
		else if (typeByValue)
		{
			constValue = correspondingParameterData->value;
		}
		else
		{
			/*
			 * Out of paranoia ensure that datum lives long enough,
			 * although bind params currently should always live
			 * long enough.
			 */
			constValue = datumCopy(correspondingParameterData->value, typeByValue,
								   typeLength);
		}

		return (Node *) makeConst(paramToProcess->paramtype, paramToProcess->paramtypmod,
								  paramToProcess->paramcollid, typeLength, constValue,
								  paramIsNull, typeByValue);
	}
	else if (IsA(inputNode, Query))
	{
		return (Node *) query_tree_mutator((Query *) inputNode, ResolveExternalParams,
										   boundParams, 0);
	}

	return expression_tree_mutator(inputNode, ResolveExternalParams, boundParams);
}


/*
 * GetDistributedPlan returns the associated DistributedPlan for a CustomScan.
 *
 * Callers should only read from the returned data structure, since it may be
 * the plan of a prepared statement and may therefore be reused.
 */
DistributedPlan *
GetDistributedPlan(CustomScan *customScan)
{
	Assert(list_length(customScan->custom_private) == 1);

	Node *node = (Node *) linitial(customScan->custom_private);
	Assert(CitusIsA(node, DistributedPlan));

	CheckNodeCopyAndSerialization(node);

	DistributedPlan *distributedPlan = (DistributedPlan *) node;

	return distributedPlan;
}


/*
 * FinalizePlan combines local plan with distributed plan and creates a plan
 * which can be run by the PostgreSQL executor.
 */
PlannedStmt *
FinalizePlan(PlannedStmt *localPlan, DistributedPlan *distributedPlan)
{
	PlannedStmt *finalPlan = NULL;
	CustomScan *customScan = makeNode(CustomScan);
	MultiExecutorType executorType = MULTI_EXECUTOR_INVALID_FIRST;

	/* this field is used in JobExecutorType */
	distributedPlan->relationIdList = localPlan->relationOids;

	if (!distributedPlan->planningError)
	{
		executorType = JobExecutorType(distributedPlan);
	}

	switch (executorType)
	{
		case MULTI_EXECUTOR_ADAPTIVE:
		{
			customScan->methods = &AdaptiveExecutorCustomScanMethods;
			break;
		}

		case MULTI_EXECUTOR_NON_PUSHABLE_INSERT_SELECT:
		{
			customScan->methods = &NonPushableInsertSelectCustomScanMethods;
			break;
		}

		default:
		{
			customScan->methods = &DelayedErrorCustomScanMethods;
			break;
		}
	}

	if (IsMultiTaskPlan(distributedPlan))
	{
		/* if it is not a single task executable plan, inform user according to the log level */
		if (MultiTaskQueryLogLevel != CITUS_LOG_LEVEL_OFF)
		{
			ereport(MultiTaskQueryLogLevel, (errmsg(
												 "multi-task query about to be executed"),
											 errhint(
												 "Queries are split to multiple tasks "
												 "if they have to be split into several"
												 " queries on the workers.")));
		}
	}

	distributedPlan->queryId = localPlan->queryId;

	Node *distributedPlanData = (Node *) distributedPlan;

	customScan->custom_private = list_make1(distributedPlanData);
	customScan->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;

	/*
	 * Fast path queries cannot have any subplans by definition, so skip
	 * expensive traversals.
	 */
	if (!distributedPlan->fastPathRouterPlan)
	{
		/*
		 * Record subplans used by distributed plan to make intermediate result
		 * pruning easier.
		 *
		 * We do this before finalizing the plan, because the combineQuery is
		 * rewritten by standard_planner in FinalizeNonRouterPlan.
		 */
		distributedPlan->usedSubPlanNodeList = FindSubPlanUsages(distributedPlan);
	}

	if (distributedPlan->combineQuery)
	{
		finalPlan = FinalizeNonRouterPlan(localPlan, distributedPlan, customScan);
	}
	else
	{
		finalPlan = FinalizeRouterPlan(localPlan, customScan);
	}

	return finalPlan;
}


/*
 * FinalizeNonRouterPlan gets the distributed custom scan plan, and creates the
 * final master select plan on the top of this distributed plan for adaptive executor.
 */
static PlannedStmt *
FinalizeNonRouterPlan(PlannedStmt *localPlan, DistributedPlan *distributedPlan,
					  CustomScan *customScan)
{
	PlannedStmt *finalPlan = PlanCombineQuery(distributedPlan, customScan);
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
	List *columnNameList = NIL;

	customScan->custom_scan_tlist =
		makeCustomScanTargetlistFromExistingTargetList(localPlan->planTree->targetlist);
	customScan->scan.plan.targetlist =
		makeTargetListFromCustomScanList(customScan->custom_scan_tlist);

	/* extract the column names from the final targetlist*/
	TargetEntry *targetEntry = NULL;
	foreach_ptr(targetEntry, customScan->scan.plan.targetlist)
	{
		String *columnName = makeString(targetEntry->resname);
		columnNameList = lappend(columnNameList, columnName);
	}

	PlannedStmt *routerPlan = makeNode(PlannedStmt);
	routerPlan->planTree = (Plan *) customScan;

	RangeTblEntry *remoteScanRangeTableEntry = RemoteScanRangeTableEntry(columnNameList);
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
 * makeCustomScanTargetlistFromExistingTargetList rebuilds the targetlist from the remote
 * query into a list that can be used as the custom_scan_tlist for our Citus Custom Scan.
 */
static List *
makeCustomScanTargetlistFromExistingTargetList(List *existingTargetlist)
{
	List *custom_scan_tlist = NIL;

	/* we will have custom scan range table entry as the first one in the list */
	const int customScanRangeTableIndex = 1;

	/* build a targetlist to read from the custom scan output */
	TargetEntry *targetEntry = NULL;
	foreach_ptr(targetEntry, existingTargetlist)
	{
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
		Var *newVar = makeVarFromTargetEntry(customScanRangeTableIndex, targetEntry);

		if (newVar->vartype == RECORDOID || newVar->vartype == RECORDARRAYOID)
		{
			/*
			 * Add the anonymous composite type to the type cache and store
			 * the key in vartypmod. Eventually this makes its way into the
			 * TupleDesc used by the executor, which uses it to parse the
			 * query results from the workers in BuildTupleFromCStrings.
			 */
			newVar->vartypmod = BlessRecordExpression(targetEntry->expr);
		}

		TargetEntry *newTargetEntry = flatCopyTargetEntry(targetEntry);
		newTargetEntry->expr = (Expr *) newVar;
		custom_scan_tlist = lappend(custom_scan_tlist, newTargetEntry);
	}

	return custom_scan_tlist;
}


/*
 * makeTargetListFromCustomScanList based on a custom_scan_tlist create the target list to
 * use on the Citus Custom Scan Node. The targetlist differs from the custom_scan_tlist in
 * a way that the expressions in the targetlist all are references to the index (resno) in
 * the custom_scan_tlist in their varattno while the varno is replaced with INDEX_VAR
 * instead of the range table entry index.
 */
static List *
makeTargetListFromCustomScanList(List *custom_scan_tlist)
{
	List *targetList = NIL;
	TargetEntry *targetEntry = NULL;
	int resno = 1;
	foreach_ptr(targetEntry, custom_scan_tlist)
	{
		/*
		 * INDEX_VAR is used to reference back to the TargetEntry in custom_scan_tlist by
		 * its resno (index)
		 */
		Var *newVar = makeVarFromTargetEntry(INDEX_VAR, targetEntry);
		TargetEntry *newTargetEntry = makeTargetEntry((Expr *) newVar, resno,
													  targetEntry->resname,
													  targetEntry->resjunk);
		targetList = lappend(targetList, newTargetEntry);
		resno++;
	}
	return targetList;
}


/*
 * BlessRecordExpression ensures we can parse an anonymous composite type on the
 * target list of a query that is sent to the worker.
 *
 * We cannot normally parse record types coming from the workers unless we
 * "bless" the tuple descriptor, which adds a transient type to the type cache
 * and assigns it a type mod value, which is the key in the type cache.
 */
int32
BlessRecordExpression(Expr *expr)
{
	int32 typeMod = -1;

	if (IsA(expr, FuncExpr) || IsA(expr, OpExpr))
	{
		/*
		 * Handle functions that return records on the target
		 * list, e.g. SELECT function_call(1,2);
		 */
		Oid resultTypeId = InvalidOid;
		TupleDesc resultTupleDesc = NULL;

		/* get_expr_result_type blesses the tuple descriptor */
		TypeFuncClass typeClass = get_expr_result_type((Node *) expr, &resultTypeId,
													   &resultTupleDesc);

		if (typeClass == TYPEFUNC_COMPOSITE)
		{
			typeMod = resultTupleDesc->tdtypmod;
		}
	}
	else if (IsA(expr, RowExpr))
	{
		/*
		 * Handle row expressions, e.g. SELECT (1,2);
		 */
		RowExpr *rowExpr = (RowExpr *) expr;
		ListCell *argCell = NULL;
		int currentResno = 1;

		TupleDesc rowTupleDesc = CreateTemplateTupleDesc(list_length(rowExpr->args));

		foreach(argCell, rowExpr->args)
		{
			Node *rowArg = (Node *) lfirst(argCell);
			Oid rowArgTypeId = exprType(rowArg);
			int rowArgTypeMod = exprTypmod(rowArg);

			if (rowArgTypeId == RECORDOID || rowArgTypeId == RECORDARRAYOID)
			{
				/* ensure nested rows are blessed as well */
				rowArgTypeMod = BlessRecordExpression((Expr *) rowArg);
			}

			TupleDescInitEntry(rowTupleDesc, currentResno, NULL,
							   rowArgTypeId, rowArgTypeMod, 0);
			TupleDescInitEntryCollation(rowTupleDesc, currentResno,
										exprCollation(rowArg));

			currentResno++;
		}

		BlessTupleDesc(rowTupleDesc);

		typeMod = rowTupleDesc->tdtypmod;
	}
	else if (IsA(expr, ArrayExpr))
	{
		/*
		 * Handle row array expressions, e.g. SELECT ARRAY[(1,2)];
		 * Postgres allows ARRAY[(1,2),(1,2,3)]. We do not.
		 */
		ArrayExpr *arrayExpr = (ArrayExpr *) expr;

		typeMod = BlessRecordExpressionList(arrayExpr->elements);
	}
	else if (IsA(expr, NullIfExpr))
	{
		NullIfExpr *nullIfExpr = (NullIfExpr *) expr;

		typeMod = BlessRecordExpressionList(nullIfExpr->args);
	}
	else if (IsA(expr, MinMaxExpr))
	{
		MinMaxExpr *minMaxExpr = (MinMaxExpr *) expr;

		typeMod = BlessRecordExpressionList(minMaxExpr->args);
	}
	else if (IsA(expr, CoalesceExpr))
	{
		CoalesceExpr *coalesceExpr = (CoalesceExpr *) expr;

		typeMod = BlessRecordExpressionList(coalesceExpr->args);
	}
	else if (IsA(expr, CaseExpr))
	{
		CaseExpr *caseExpr = (CaseExpr *) expr;
		List *results = NIL;
		ListCell *whenCell = NULL;

		foreach(whenCell, caseExpr->args)
		{
			CaseWhen *whenArg = (CaseWhen *) lfirst(whenCell);

			results = lappend(results, whenArg->result);
		}

		if (caseExpr->defresult != NULL)
		{
			results = lappend(results, caseExpr->defresult);
		}

		typeMod = BlessRecordExpressionList(results);
	}

	return typeMod;
}


/*
 * BlessRecordExpressionList maps BlessRecordExpression over a list.
 * Returns typmod of all expressions, or -1 if they are not all the same.
 * Ignores expressions with a typmod of -1.
 */
static int32
BlessRecordExpressionList(List *exprs)
{
	int32 finalTypeMod = -1;
	ListCell *exprCell = NULL;
	foreach(exprCell, exprs)
	{
		Node *exprArg = (Node *) lfirst(exprCell);
		int32 exprTypeMod = BlessRecordExpression((Expr *) exprArg);

		if (exprTypeMod == -1)
		{
			continue;
		}
		else if (finalTypeMod == -1)
		{
			finalTypeMod = exprTypeMod;
		}
		else if (finalTypeMod != exprTypeMod)
		{
			return -1;
		}
	}
	return finalTypeMod;
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
	Node *nodeCopy = copyObject(node);
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
	if (bms_is_empty(innerrel->relids) || bms_is_empty(outerrel->relids))
	{
		/*
		 * We do not expect empty relids. Still, ignoring such JoinRestriction is
		 * preferable for two reasons:
		 * 1. This might be a query that doesn't rely on JoinRestrictions at all (e.g.,
		 * local query).
		 * 2. We cannot process them when they are empty (and likely to segfault if
		 * we allow as-is).
		 */
		ereport(DEBUG1, (errmsg("Join restriction information is NULL")));
	}

	/*
	 * Use a memory context that's guaranteed to live long enough, could be
	 * called in a more shortly lived one (e.g. with GEQO).
	 */
	PlannerRestrictionContext *plannerRestrictionContext =
		CurrentPlannerRestrictionContext();
	MemoryContext restrictionsMemoryContext = plannerRestrictionContext->memoryContext;
	MemoryContext oldMemoryContext = MemoryContextSwitchTo(restrictionsMemoryContext);

	JoinRestrictionContext *joinRestrictionContext =
		plannerRestrictionContext->joinRestrictionContext;
	Assert(joinRestrictionContext != NULL);

	JoinRestriction *joinRestriction = palloc0(sizeof(JoinRestriction));
	joinRestriction->joinType = jointype;
	joinRestriction->plannerInfo = root;

	/*
	 * We create a copy of restrictInfoList and relids because with geqo they may
	 * be created in a memory context which will be deleted when we still need it,
	 * thus we create a copy of it in our memory context.
	 */
	joinRestriction->joinRestrictInfoList = copyObject(extra->restrictlist);
	joinRestriction->innerrelRelids = bms_copy(innerrel->relids);
	joinRestriction->outerrelRelids = bms_copy(outerrel->relids);

	joinRestrictionContext->joinRestrictionList =
		lappend(joinRestrictionContext->joinRestrictionList, joinRestriction);

	/*
	 * Keep track if we received any semi joins here. If we didn't we can
	 * later safely convert any semi joins in the rewritten query to inner
	 * joins.
	 */
	joinRestrictionContext->hasSemiJoin = joinRestrictionContext->hasSemiJoin ||
										  extra->sjinfo->jointype == JOIN_SEMI;

	MemoryContextSwitchTo(oldMemoryContext);
}


/*
 * multi_relation_restriction_hook is a hook called by postgresql standard planner
 * to notify us about various planning information regarding a relation. We use
 * it to retrieve restrictions on relations.
 */
void
multi_relation_restriction_hook(PlannerInfo *root, RelOptInfo *relOptInfo,
								Index restrictionIndex, RangeTblEntry *rte)
{
	CitusTableCacheEntry *cacheEntry = NULL;

	if (ReplaceCitusExtraDataContainer && IsCitusExtraDataContainerRelation(rte))
	{
		/*
		 * We got here by planning the query part that needs to be executed on the query
		 * coordinator node.
		 * We have verified the occurrence of the citus_extra_datacontainer function
		 * encoding the remote scan we plan to execute here. We will replace all paths
		 * with a path describing our custom scan.
		 */
		Path *path = CreateCitusCustomScanPath(root, relOptInfo, restrictionIndex, rte,
											   ReplaceCitusExtraDataContainerWithCustomScan);

		/* replace all paths with our custom scan and recalculate cheapest */
		relOptInfo->pathlist = list_make1(path);
		set_cheapest(relOptInfo);

		return;
	}

	AdjustReadIntermediateResultCost(rte, relOptInfo);
	AdjustReadIntermediateResultArrayCost(rte, relOptInfo);

	if (rte->rtekind != RTE_RELATION)
	{
		return;
	}

	/*
	 * Use a memory context that's guaranteed to live long enough, could be
	 * called in a more shortly lived one (e.g. with GEQO).
	 */
	PlannerRestrictionContext *plannerRestrictionContext =
		CurrentPlannerRestrictionContext();
	MemoryContext restrictionsMemoryContext = plannerRestrictionContext->memoryContext;
	MemoryContext oldMemoryContext = MemoryContextSwitchTo(restrictionsMemoryContext);

	bool distributedTable = IsCitusTable(rte->relid);

	RelationRestriction *relationRestriction = palloc0(sizeof(RelationRestriction));
	relationRestriction->index = restrictionIndex;
	relationRestriction->relationId = rte->relid;
	relationRestriction->rte = rte;
	relationRestriction->relOptInfo = relOptInfo;
	relationRestriction->distributedRelation = distributedTable;
	relationRestriction->plannerInfo = root;

	/* see comments on GetVarFromAssignedParam() */
	relationRestriction->outerPlanParamsList = OuterPlanParamsList(root);
	relationRestriction->translatedVars = TranslatedVars(root,
														 relationRestriction->index);

	RelationRestrictionContext *relationRestrictionContext =
		plannerRestrictionContext->relationRestrictionContext;

	/*
	 * We're also keeping track of whether all participant
	 * tables are reference tables.
	 */
	if (distributedTable)
	{
		cacheEntry = GetCitusTableCacheEntry(rte->relid);

		relationRestrictionContext->allReferenceTables &=
			IsCitusTableTypeCacheEntry(cacheEntry, REFERENCE_TABLE);
	}

	relationRestrictionContext->relationRestrictionList =
		lappend(relationRestrictionContext->relationRestrictionList, relationRestriction);

	MemoryContextSwitchTo(oldMemoryContext);
}


/*
 * TranslatedVars deep copies the translated vars for the given relation index
 * if there is any append rel list.
 */
static List *
TranslatedVars(PlannerInfo *root, int relationIndex)
{
	List *translatedVars = NIL;

	if (root->append_rel_list != NIL)
	{
		AppendRelInfo *targetAppendRelInfo =
			FindTargetAppendRelInfo(root, relationIndex);
		if (targetAppendRelInfo != NULL)
		{
			/* postgres deletes translated_vars after pg13, hence we deep copy them here */
			Node *targetNode = NULL;
			foreach_ptr(targetNode, targetAppendRelInfo->translated_vars)
			{
				translatedVars =
					lappend(translatedVars, copyObject(targetNode));
			}
		}
	}
	return translatedVars;
}


/*
 * FindTargetAppendRelInfo finds the target append rel info for the given
 * relation rte index.
 */
static AppendRelInfo *
FindTargetAppendRelInfo(PlannerInfo *root, int relationRteIndex)
{
	AppendRelInfo *appendRelInfo = NULL;

	/* iterate on the queries that are part of UNION ALL subselects */
	foreach_ptr(appendRelInfo, root->append_rel_list)
	{
		/*
		 * We're only interested in the child rel that is equal to the
		 * relation we're investigating. Here we don't need to find the offset
		 * because postgres adds an offset to child_relid and parent_relid after
		 * calling multi_relation_restriction_hook.
		 */
		if (appendRelInfo->child_relid == relationRteIndex)
		{
			return appendRelInfo;
		}
	}
	return NULL;
}


/*
 * AdjustReadIntermediateResultCost adjusts the row count and total cost
 * of a read_intermediate_result call based on the file size.
 */
static void
AdjustReadIntermediateResultCost(RangeTblEntry *rangeTableEntry, RelOptInfo *relOptInfo)
{
	if (rangeTableEntry->rtekind != RTE_FUNCTION ||
		list_length(rangeTableEntry->functions) != 1)
	{
		/* avoid more expensive checks below for non-functions */
		return;
	}

	if (!CitusHasBeenLoaded() || !CheckCitusVersion(DEBUG5))
	{
		/* read_intermediate_result may not exist */
		return;
	}

	if (!ContainsReadIntermediateResultFunction((Node *) rangeTableEntry->functions))
	{
		return;
	}

	RangeTblFunction *rangeTableFunction = (RangeTblFunction *) linitial(
		rangeTableEntry->functions);
	FuncExpr *funcExpression = (FuncExpr *) rangeTableFunction->funcexpr;
	Const *resultIdConst = (Const *) linitial(funcExpression->args);
	if (!IsA(resultIdConst, Const))
	{
		/* not sure how to interpret non-const */
		return;
	}

	Datum resultIdDatum = resultIdConst->constvalue;

	Const *resultFormatConst = (Const *) lsecond(funcExpression->args);
	if (!IsA(resultFormatConst, Const))
	{
		/* not sure how to interpret non-const */
		return;
	}

	AdjustReadIntermediateResultsCostInternal(relOptInfo,
											  rangeTableFunction->funccoltypes,
											  1, &resultIdDatum, resultFormatConst);
}


/*
 * AdjustReadIntermediateResultArrayCost adjusts the row count and total cost
 * of a read_intermediate_results(resultIds, format) call based on the file size.
 */
static void
AdjustReadIntermediateResultArrayCost(RangeTblEntry *rangeTableEntry,
									  RelOptInfo *relOptInfo)
{
	Datum *resultIdArray = NULL;
	int resultIdCount = 0;

	if (rangeTableEntry->rtekind != RTE_FUNCTION ||
		list_length(rangeTableEntry->functions) != 1)
	{
		/* avoid more expensive checks below for non-functions */
		return;
	}

	if (!CitusHasBeenLoaded() || !CheckCitusVersion(DEBUG5))
	{
		/* read_intermediate_result may not exist */
		return;
	}

	if (!ContainsReadIntermediateResultArrayFunction((Node *) rangeTableEntry->functions))
	{
		return;
	}

	RangeTblFunction *rangeTableFunction =
		(RangeTblFunction *) linitial(rangeTableEntry->functions);
	FuncExpr *funcExpression = (FuncExpr *) rangeTableFunction->funcexpr;
	Const *resultIdConst = (Const *) linitial(funcExpression->args);
	if (!IsA(resultIdConst, Const))
	{
		/* not sure how to interpret non-const */
		return;
	}

	Datum resultIdArrayDatum = resultIdConst->constvalue;
	deconstruct_array(DatumGetArrayTypeP(resultIdArrayDatum), TEXTOID, -1, false,
					  'i', &resultIdArray, NULL, &resultIdCount);

	Const *resultFormatConst = (Const *) lsecond(funcExpression->args);
	if (!IsA(resultFormatConst, Const))
	{
		/* not sure how to interpret non-const */
		return;
	}

	AdjustReadIntermediateResultsCostInternal(relOptInfo,
											  rangeTableFunction->funccoltypes,
											  resultIdCount, resultIdArray,
											  resultFormatConst);
}


/*
 * AdjustReadIntermediateResultsCostInternal adjusts the row count and total cost
 * of reading intermediate results based on file sizes.
 */
static void
AdjustReadIntermediateResultsCostInternal(RelOptInfo *relOptInfo, List *columnTypes,
										  int resultIdCount, Datum *resultIds,
										  Const *resultFormatConst)
{
	PathTarget *reltarget = relOptInfo->reltarget;
	List *pathList = relOptInfo->pathlist;
	double rowCost = 0.;
	double rowSizeEstimate = 0;
	double rowCountEstimate = 0.;
	double ioCost = 0.;
	QualCost funcCost = { 0., 0. };
	int64 totalResultSize = 0;
	ListCell *typeCell = NULL;

	Datum resultFormatDatum = resultFormatConst->constvalue;
	Oid resultFormatId = DatumGetObjectId(resultFormatDatum);
	bool binaryFormat = (resultFormatId == BinaryCopyFormatId());

	for (int index = 0; index < resultIdCount; index++)
	{
		char *resultId = TextDatumGetCString(resultIds[index]);
		int64 resultSize = IntermediateResultSize(resultId);
		if (resultSize < 0)
		{
			/* result does not exist, will probably error out later on */
			return;
		}

		if (binaryFormat)
		{
			/* subtract 11-byte signature + 8 byte header + 2-byte footer */
			totalResultSize -= 21;
		}

		totalResultSize += resultSize;
	}

	/* start with the cost of evaluating quals */
	rowCost += relOptInfo->baserestrictcost.per_tuple;

	/* postgres' estimate for the width of the rows */
	rowSizeEstimate += reltarget->width;

	/* add 2 bytes for column count (binary) or line separator (text) */
	rowSizeEstimate += 2;

	foreach(typeCell, columnTypes)
	{
		Oid columnTypeId = lfirst_oid(typeCell);
		Oid inputFunctionId = InvalidOid;
		Oid typeIOParam = InvalidOid;

		if (binaryFormat)
		{
			getTypeBinaryInputInfo(columnTypeId, &inputFunctionId, &typeIOParam);

			/* binary format: 4 bytes for field size */
			rowSizeEstimate += 4;
		}
		else
		{
			getTypeInputInfo(columnTypeId, &inputFunctionId, &typeIOParam);

			/* text format: 1 byte for tab separator */
			rowSizeEstimate += 1;
		}


		/* add the cost of parsing a column */
		add_function_cost(NULL, inputFunctionId, NULL, &funcCost);
	}
	rowCost += funcCost.per_tuple;

	/* estimate the number of rows based on the file size and estimated row size */
	rowCountEstimate = Max(1, (double) totalResultSize / rowSizeEstimate);

	/* cost of reading the data */
	ioCost = seq_page_cost * totalResultSize / BLCKSZ;

	Assert(pathList != NIL);

	/* tell the planner about the cost and row count of the function */
	Path *path = (Path *) linitial(pathList);
	path->rows = rowCountEstimate;
	path->total_cost = rowCountEstimate * rowCost + ioCost;

	path->startup_cost = funcCost.startup + relOptInfo->baserestrictcost.startup;
}


/*
 * OuterPlanParamsList creates a list of RootPlanParams for outer nodes of the
 * given root. The first item in the list corresponds to parent_root, and the
 * last item corresponds to the outer most node.
 */
static List *
OuterPlanParamsList(PlannerInfo *root)
{
	List *planParamsList = NIL;

	for (PlannerInfo *outerNodeRoot = root->parent_root; outerNodeRoot != NULL;
		 outerNodeRoot = outerNodeRoot->parent_root)
	{
		RootPlanParams *rootPlanParams = palloc0(sizeof(RootPlanParams));
		rootPlanParams->root = outerNodeRoot;

		/*
		 * TODO: In SearchPlannerParamList() we are only interested in Var plan
		 * params, consider copying just them here.
		 */
		rootPlanParams->plan_params = CopyPlanParamList(outerNodeRoot->plan_params);

		planParamsList = lappend(planParamsList, rootPlanParams);
	}

	return planParamsList;
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

	plannerRestrictionContext->fastPathRestrictionContext =
		palloc0(sizeof(FastPathRestrictionContext));

	plannerRestrictionContext->memoryContext = CurrentMemoryContext;

	/* we'll apply logical AND as we add tables */
	plannerRestrictionContext->relationRestrictionContext->allReferenceTables = true;

	plannerRestrictionContextList = lcons(plannerRestrictionContext,
										  plannerRestrictionContextList);

	return plannerRestrictionContext;
}


/*
 * TranslatedVarsForRteIdentity gets an rteIdentity and returns the
 * translatedVars that belong to the range table relation. If no
 * translatedVars found, the function returns NIL;
 */
List *
TranslatedVarsForRteIdentity(int rteIdentity)
{
	PlannerRestrictionContext *currentPlannerRestrictionContext =
		CurrentPlannerRestrictionContext();

	List *relationRestrictionList =
		currentPlannerRestrictionContext->relationRestrictionContext->
		relationRestrictionList;
	RelationRestriction *relationRestriction = NULL;
	foreach_ptr(relationRestriction, relationRestrictionList)
	{
		if (GetRTEIdentity(relationRestriction->rte) == rteIdentity)
		{
			return relationRestriction->translatedVars;
		}
	}

	return NIL;
}


/*
 * CurrentRestrictionContext returns the most recently added
 * PlannerRestrictionContext from the plannerRestrictionContextList list.
 */
static PlannerRestrictionContext *
CurrentPlannerRestrictionContext(void)
{
	Assert(plannerRestrictionContextList != NIL);

	PlannerRestrictionContext *plannerRestrictionContext =
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
 * ResetPlannerRestrictionContext resets the element of the given planner
 * restriction context.
 */
static void
ResetPlannerRestrictionContext(PlannerRestrictionContext *plannerRestrictionContext)
{
	plannerRestrictionContext->relationRestrictionContext =
		palloc0(sizeof(RelationRestrictionContext));

	plannerRestrictionContext->joinRestrictionContext =
		palloc0(sizeof(JoinRestrictionContext));

	plannerRestrictionContext->fastPathRestrictionContext =
		palloc0(sizeof(FastPathRestrictionContext));


	/* we'll apply logical AND as we add tables */
	plannerRestrictionContext->relationRestrictionContext->allReferenceTables = true;
}


/*
 * HasUnresolvedExternParamsWalker returns true if the passed in expression
 * has external parameters that are not contained in boundParams, false
 * otherwise.
 */
bool
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
			ParamExternData *externParam = NULL;

			/* give hook a chance in case parameter is dynamic */
			if (boundParams->paramFetch != NULL)
			{
				ParamExternData externParamPlaceholder;
				externParam = (*boundParams->paramFetch)(boundParams, paramId, false,
														 &externParamPlaceholder);
			}
			else
			{
				externParam = &boundParams->params[paramId - 1];
			}

			Oid paramType = externParam->ptype;
			if (OidIsValid(paramType))
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


/*
 * GetRTEListPropertiesForQuery is a wrapper around GetRTEListProperties that
 * returns RTEListProperties for the rte list retrieved from query.
 */
RTEListProperties *
GetRTEListPropertiesForQuery(Query *query)
{
	List *rteList = ExtractRangeTableEntryList(query);
	return GetRTEListProperties(rteList);
}


/*
 * GetRTEListProperties returns RTEListProperties struct processing the given
 * rangeTableList.
 */
static RTEListProperties *
GetRTEListProperties(List *rangeTableList)
{
	RTEListProperties *rteListProperties = palloc0(sizeof(RTEListProperties));

	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, rangeTableList)
	{
		if (rangeTableEntry->rtekind != RTE_RELATION)
		{
			continue;
		}
		else if (rangeTableEntry->relkind == RELKIND_VIEW)
		{
			/*
			 * Skip over views, distributed tables within (regular) views are
			 * already in rangeTableList.
			 */
			continue;
		}


		if (rangeTableEntry->relkind == RELKIND_MATVIEW)
		{
			/*
			 * Record materialized views as they are similar to postgres local tables
			 * but it is nice to record them separately.
			 *
			 * Regular tables, partitioned tables or foreign tables can be a local or
			 * distributed tables and we can qualify them accurately.
			 *
			 * For regular views, we don't care because their definitions are already
			 * in the same query tree and we can detect what is inside the view definition.
			 *
			 * For materialized views, they are just local tables in the queries. But, when
			 * REFRESH MATERIALIZED VIEW is used, they behave similar to regular views, adds
			 * the view definition to the query. Hence, it is useful to record it seperately
			 * and let the callers decide on what to do.
			 */
			rteListProperties->hasMaterializedView = true;
			continue;
		}

		Oid relationId = rangeTableEntry->relid;
		CitusTableCacheEntry *cacheEntry = LookupCitusTableCacheEntry(relationId);
		if (!cacheEntry)
		{
			rteListProperties->hasPostgresLocalTable = true;
		}
		else if (IsCitusTableTypeCacheEntry(cacheEntry, REFERENCE_TABLE))
		{
			rteListProperties->hasReferenceTable = true;
		}
		else if (IsCitusTableTypeCacheEntry(cacheEntry, CITUS_LOCAL_TABLE))
		{
			rteListProperties->hasCitusLocalTable = true;
		}
		else if (IsCitusTableTypeCacheEntry(cacheEntry, DISTRIBUTED_TABLE))
		{
			rteListProperties->hasDistributedTable = true;
		}
		else
		{
			/* it's not expected, but let's do a bug catch here */
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("encountered with an unexpected citus "
								   "table type while processing range table "
								   "entries of query")));
		}
	}

	rteListProperties->hasCitusTable = (rteListProperties->hasDistributedTable ||
										rteListProperties->hasReferenceTable ||
										rteListProperties->hasCitusLocalTable);

	return rteListProperties;
}


/*
 * WarnIfListHasForeignDistributedTable iterates the given list and logs a WARNING
 * if the given relation is a distributed foreign table.
 * We do that because now we only support Citus Local Tables for foreign tables.
 */
static void
WarnIfListHasForeignDistributedTable(List *rangeTableList)
{
	static bool DistributedForeignTableWarningPrompted = false;

	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, rangeTableList)
	{
		if (DistributedForeignTableWarningPrompted)
		{
			return;
		}

		Oid relationId = rangeTableEntry->relid;
		if (IsForeignTable(relationId) && IsCitusTable(relationId) &&
			!IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
		{
			DistributedForeignTableWarningPrompted = true;
			ereport(WARNING, (errmsg(
								  "support for distributed foreign tables are deprecated, "
								  "please use Citus managed local tables"),
							  (errdetail(
								   "Foreign tables can be added to metadata using UDF: "
								   "citus_add_local_table_to_metadata()"))));
		}
	}
}
