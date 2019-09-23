/*-------------------------------------------------------------------------
 *
 * distributed_planner.c
 *	  General Citus planner code.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"

#include <float.h>
#include <limits.h>

#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_nodes.h"
#include "distributed/function_call_delegation.h"
#include "distributed/insert_select_planner.h"
#include "distributed/intermediate_results.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_master_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/worker_shard_visibility.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "parser/parse_type.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#include "optimizer/plancat.h"
#else
#include "optimizer/cost.h"
#endif
#include "optimizer/pathnode.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


static List *plannerRestrictionContextList = NIL;
int MultiTaskQueryLogLevel = MULTI_TASK_QUERY_INFO_OFF; /* multi-task query log level */
static uint64 NextPlanId = 1;


static bool ListContainsDistributedTableRTE(List *rangeTableList);
static bool IsUpdateOrDelete(Query *query);
static PlannedStmt * CreateDistributedPlannedStmt(uint64 planId, PlannedStmt *localPlan,
												  Query *originalQuery, Query *query,
												  ParamListInfo boundParams,
												  PlannerRestrictionContext *
												  plannerRestrictionContext);
static DistributedPlan * CreateDistributedPlan(uint64 planId, Query *originalQuery,
											   Query *query, ParamListInfo boundParams,
											   bool hasUnresolvedParams,
											   PlannerRestrictionContext *
											   plannerRestrictionContext);
static DeferredErrorMessage * DeferErrorIfPartitionTableNotSingleReplicated(Oid
																			relationId);

static void AssignRTEIdentities(List *rangeTableList);
static void AssignRTEIdentity(RangeTblEntry *rangeTableEntry, int rteIdentifier);
static void AdjustPartitioningForDistributedPlanning(List *rangeTableList,
													 bool setPartitionedTablesInherited);
static PlannedStmt * FinalizePlan(PlannedStmt *localPlan,
								  DistributedPlan *distributedPlan);
static PlannedStmt * FinalizeNonRouterPlan(PlannedStmt *localPlan,
										   DistributedPlan *distributedPlan,
										   CustomScan *customScan);
static PlannedStmt * FinalizeRouterPlan(PlannedStmt *localPlan, CustomScan *customScan);
static int32 BlessRecordExpression(Expr *expr);
static void CheckNodeIsDumpable(Node *node);
static Node * CheckNodeCopyAndSerialization(Node *node);
static void AdjustReadIntermediateResultCost(RangeTblEntry *rangeTableEntry,
											 RelOptInfo *relOptInfo);
static List * OuterPlanParamsList(PlannerInfo *root);
static List * CopyPlanParamList(List *originalPlanParamList);
static PlannerRestrictionContext * CreateAndPushPlannerRestrictionContext(void);
static PlannerRestrictionContext * CurrentPlannerRestrictionContext(void);
static void PopPlannerRestrictionContext(void);
static void ResetPlannerRestrictionContext(
	PlannerRestrictionContext *plannerRestrictionContext);
static bool HasUnresolvedExternParamsWalker(Node *expression, ParamListInfo boundParams);


/* Distributed planner hook */
PlannedStmt *
distributed_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	PlannedStmt *result = NULL;
	bool needsDistributedPlanning = false;
	Query *originalQuery = NULL;
	PlannerRestrictionContext *plannerRestrictionContext = NULL;
	bool setPartitionedTablesInherited = false;
	List *rangeTableList = ExtractRangeTableEntryList(parse);

	if (cursorOptions & CURSOR_OPT_FORCE_DISTRIBUTED)
	{
		/* this cursor flag could only be set when Citus has been loaded */
		Assert(CitusHasBeenLoaded());

		needsDistributedPlanning = true;
	}
	else if (CitusHasBeenLoaded())
	{
		needsDistributedPlanning = ListContainsDistributedTableRTE(rangeTableList);
	}

	if (needsDistributedPlanning)
	{
		/*
		 * Inserting into a local table needs to go through the regular postgres
		 * planner/executor, but the SELECT needs to go through Citus. We currently
		 * don't have a way of doing both things and therefore error out, but do
		 * have a handy tip for users.
		 */
		if (InsertSelectIntoLocalTable(parse))
		{
			ereport(ERROR, (errmsg("cannot INSERT rows from a distributed query into a "
								   "local table"),
							errhint("Consider using CREATE TEMPORARY TABLE tmp AS "
									"SELECT ... and inserting from the temporary "
									"table.")));
		}

		/*
		 * standard_planner scribbles on it's input, but for deparsing we need the
		 * unmodified form. Note that we keep RTE_RELATIONs with their identities
		 * set, which doesn't break our goals, but, prevents us keeping an extra copy
		 * of the query tree. Note that we copy the query tree once we're sure it's a
		 * distributed query.
		 */
		AssignRTEIdentities(rangeTableList);
		originalQuery = copyObject(parse);

		setPartitionedTablesInherited = false;
		AdjustPartitioningForDistributedPlanning(rangeTableList,
												 setPartitionedTablesInherited);
	}

	/*
	 * Make sure that we hide shard names on the Citus MX worker nodes. See comments in
	 * ReplaceTableVisibleFunction() for the details.
	 */
	ReplaceTableVisibleFunction((Node *) parse);

	/* create a restriction context and put it at the end if context list */
	plannerRestrictionContext = CreateAndPushPlannerRestrictionContext();

	PG_TRY();
	{
		/*
		 * For trivial queries, we're skipping the standard_planner() in
		 * order to eliminate its overhead.
		 *
		 * Otherwise, call into standard planner. This is required because the Citus
		 * planner relies on both the restriction information per table and parse tree
		 * transformations made by postgres' planner.
		 */

		if (needsDistributedPlanning && FastPathRouterQuery(originalQuery))
		{
			result = FastPathPlanner(originalQuery, parse, boundParams);
		}
		else
		{
			result = standard_planner(parse, cursorOptions, boundParams);
		}

		if (needsDistributedPlanning)
		{
			uint64 planId = NextPlanId++;

			result = CreateDistributedPlannedStmt(planId, result, originalQuery, parse,
												  boundParams, plannerRestrictionContext);

			setPartitionedTablesInherited = true;
			AdjustPartitioningForDistributedPlanning(rangeTableList,
													 setPartitionedTablesInherited);
		}
		else
		{
			DistributedPlan *delegatePlan = TryToDelegateFunctionCall(parse);
			if (delegatePlan != NULL)
			{
				result = FinalizePlan(result, delegatePlan);
			}
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
 * ExtractRangeTableEntryList is a wrapper around ExtractRangeTableEntryWalker.
 * The function traverses the input query and returns all the range table
 * entries that are in the query tree.
 */
List *
ExtractRangeTableEntryList(Query *query)
{
	List *rangeTblList = NIL;

	ExtractRangeTableEntryWalker((Node *) query, &rangeTblList);

	return rangeTblList;
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
	List *allRTEs = NIL;
	CmdType commandType = query->commandType;

	if (!CitusHasBeenLoaded())
	{
		return false;
	}

	if (commandType != CMD_SELECT && commandType != CMD_INSERT &&
		commandType != CMD_UPDATE && commandType != CMD_DELETE)
	{
		return false;
	}

	ExtractRangeTableEntryWalker((Node *) query, &allRTEs);

	return ListContainsDistributedTableRTE(allRTEs);
}


/*
 * ListContainsDistributedTableRTE gets a list of range table entries
 * and returns true if there is at least one distributed relation range
 * table entry in the list.
 */
static bool
ListContainsDistributedTableRTE(List *rangeTableList)
{
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (rangeTableEntry->rtekind != RTE_RELATION)
		{
			continue;
		}

		if (IsDistributedTable(rangeTableEntry->relid))
		{
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
 */
static void
AssignRTEIdentities(List *rangeTableList)
{
	ListCell *rangeTableCell = NULL;
	int rteIdentifier = 1;

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
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			AssignRTEIdentity(rangeTableEntry, rteIdentifier++);
		}
	}
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
		 * we set each distributed partitioned table's inh flag to appropriate
		 * value before and after dropping to the standart_planner.
		 */
		if (rangeTableEntry->rtekind == RTE_RELATION &&
			IsDistributedTable(rangeTableEntry->relid) &&
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
 * IsModifyDistributedPlan returns true if the multi plan performs modifications,
 * false otherwise.
 */
bool
IsModifyDistributedPlan(DistributedPlan *distributedPlan)
{
	return distributedPlan->modLevel > ROW_MODIFY_READONLY;
}


/*
 * CreateDistributedPlannedStmt encapsulates the logic needed to transform a particular
 * query into a distributed plan that is encapsulated by a PlannedStmt.
 */
static PlannedStmt *
CreateDistributedPlannedStmt(uint64 planId, PlannedStmt *localPlan, Query *originalQuery,
							 Query *query, ParamListInfo boundParams,
							 PlannerRestrictionContext *plannerRestrictionContext)
{
	DistributedPlan *distributedPlan = NULL;
	PlannedStmt *resultPlan = NULL;
	bool hasUnresolvedParams = false;
	JoinRestrictionContext *joinRestrictionContext =
		plannerRestrictionContext->joinRestrictionContext;

	if (HasUnresolvedExternParamsWalker((Node *) originalQuery, boundParams))
	{
		hasUnresolvedParams = true;
	}

	plannerRestrictionContext->joinRestrictionContext =
		RemoveDuplicateJoinRestrictions(joinRestrictionContext);

	distributedPlan =
		CreateDistributedPlan(planId, originalQuery, query, boundParams,
							  hasUnresolvedParams, plannerRestrictionContext);

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
	resultPlan = FinalizePlan(localPlan, distributedPlan);

	/*
	 * As explained above, force planning costs to be unrealistically high if
	 * query planning failed (possibly) due to prepared statement parameters or
	 * if it is planned as a multi shard modify query.
	 */
	if ((distributedPlan->planningError ||
		 (IsUpdateOrDelete(originalQuery) && IsMultiTaskPlan(distributedPlan))) &&
		hasUnresolvedParams)
	{
		/*
		 * Arbitrarily high cost, but low enough that it can be added up
		 * without overflowing by choose_custom_plan().
		 */
		resultPlan->planTree->total_cost = FLT_MAX / 100000000;
	}

	return resultPlan;
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
static DistributedPlan *
CreateDistributedPlan(uint64 planId, Query *originalQuery, Query *query, ParamListInfo
					  boundParams, bool hasUnresolvedParams,
					  PlannerRestrictionContext *plannerRestrictionContext)
{
	DistributedPlan *distributedPlan = NULL;
	MultiTreeRoot *logicalPlan = NULL;
	List *subPlanList = NIL;
	bool hasCtes = originalQuery->cteList != NIL;

	if (IsModifyCommand(originalQuery))
	{
		Oid targetRelationId = InvalidOid;
		EnsureModificationsCanRun();

		targetRelationId = ModifyQueryResultRelationId(query);
		EnsurePartitionTableNotReplicated(targetRelationId);

		if (InsertSelectIntoDistributedTable(originalQuery))
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
				CreateInsertSelectPlan(planId, originalQuery, plannerRestrictionContext);
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
		 * the full blown plan/optimize/physical planing process needed to
		 * produce distributed query plans.
		 */

		distributedPlan = CreateRouterPlan(originalQuery, query,
										   plannerRestrictionContext);
		if (distributedPlan->planningError == NULL)
		{
			/* successfully created a router plan */
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

	/*
	 * Plan subqueries and CTEs that cannot be pushed down by recursively
	 * calling the planner and return the resulting plans to subPlanList.
	 */
	subPlanList = GenerateSubplansForSubqueriesAndCTEs(planId, originalQuery,
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

		standard_planner(newQuery, 0, boundParams);

		/* overwrite the old transformed query with the new transformed query */
		memcpy(query, newQuery, sizeof(Query));

		/* recurse into CreateDistributedPlan with subqueries/CTEs replaced */
		distributedPlan = CreateDistributedPlan(planId, originalQuery, query, NULL, false,
												plannerRestrictionContext);
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

	logicalPlan = MultiLogicalPlanCreate(originalQuery, query,
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
		ParamExternData *correspondingParameterData = NULL;
		int numberOfParameters = boundParams->numParams;
		int parameterId = paramToProcess->paramid;
		int16 typeLength = 0;
		bool typeByValue = false;
		Datum constValue = 0;
		bool paramIsNull = false;
		int parameterIndex = 0;

		if (paramToProcess->paramkind != PARAM_EXTERN)
		{
			return inputNode;
		}

		if (parameterId < 0)
		{
			return inputNode;
		}

		/* parameterId starts from 1 */
		parameterIndex = parameterId - 1;
		if (parameterIndex >= numberOfParameters)
		{
			return inputNode;
		}

		correspondingParameterData = &boundParams->params[parameterIndex];

		if (!(correspondingParameterData->pflags & PARAM_FLAG_CONST))
		{
			return inputNode;
		}

		get_typlenbyval(paramToProcess->paramtype, &typeLength, &typeByValue);

		paramIsNull = correspondingParameterData->isnull;
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
	Node *node = NULL;
	DistributedPlan *distributedPlan = NULL;

	Assert(list_length(customScan->custom_private) == 1);

	node = (Node *) linitial(customScan->custom_private);
	Assert(CitusIsA(node, DistributedPlan));

	CheckNodeCopyAndSerialization(node);

	distributedPlan = (DistributedPlan *) node;

	return distributedPlan;
}


/*
 * FinalizePlan combines local plan with distributed plan and creates a plan
 * which can be run by the PostgreSQL executor.
 */
static PlannedStmt *
FinalizePlan(PlannedStmt *localPlan, DistributedPlan *distributedPlan)
{
	PlannedStmt *finalPlan = NULL;
	CustomScan *customScan = makeNode(CustomScan);
	Node *distributedPlanData = NULL;
	MultiExecutorType executorType = MULTI_EXECUTOR_INVALID_FIRST;

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

	if (IsMultiTaskPlan(distributedPlan))
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

	distributedPlan->relationIdList = localPlan->relationOids;
	distributedPlan->queryId = localPlan->queryId;

	distributedPlanData = (Node *) distributedPlan;

	customScan->custom_private = list_make1(distributedPlanData);
	customScan->flags = CUSTOMPATH_SUPPORT_BACKWARD_SCAN;

	if (distributedPlan->masterQuery)
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
 * final master select plan on the top of this distributed plan for real-time
 * and task-tracker executors.
 */
static PlannedStmt *
FinalizeNonRouterPlan(PlannedStmt *localPlan, DistributedPlan *distributedPlan,
					  CustomScan *customScan)
{
	PlannedStmt *finalPlan = NULL;

	finalPlan = MasterNodeSelectPlan(distributedPlan, customScan);
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

		if (newVar->vartype == RECORDOID)
		{
			/*
			 * Add the anonymous composite type to the type cache and store
			 * the key in vartypmod. Eventually this makes its way into the
			 * TupleDesc used by the executor, which uses it to parse the
			 * query results from the workers in BuildTupleFromCStrings.
			 */
			newVar->vartypmod = BlessRecordExpression(targetEntry->expr);
		}

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
 * BlessRecordExpression ensures we can parse an anonymous composite type on the
 * target list of a query that is sent to the worker.
 *
 * We cannot normally parse record types coming from the workers unless we
 * "bless" the tuple descriptor, which adds a transient type to the type cache
 * and assigns it a type mod value, which is the key in the type cache.
 */
static int32
BlessRecordExpression(Expr *expr)
{
	int32 typeMod = -1;

	if (IsA(expr, FuncExpr))
	{
		/*
		 * Handle functions that return records on the target
		 * list, e.g. SELECT function_call(1,2);
		 */
		Oid resultTypeId = InvalidOid;
		TupleDesc resultTupleDesc = NULL;
		TypeFuncClass typeClass;

		/* get_expr_result_type blesses the tuple descriptor */
		typeClass = get_expr_result_type((Node *) expr, &resultTypeId,
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
		TupleDesc rowTupleDesc = NULL;
		ListCell *argCell = NULL;
		int currentResno = 1;

#if PG_VERSION_NUM >= 120000
		rowTupleDesc = CreateTemplateTupleDesc(list_length(rowExpr->args));
#else
		rowTupleDesc = CreateTemplateTupleDesc(list_length(rowExpr->args), false);
#endif

		foreach(argCell, rowExpr->args)
		{
			Node *rowArg = (Node *) lfirst(argCell);
			Oid rowArgTypeId = exprType(rowArg);
			int rowArgTypeMod = exprTypmod(rowArg);

			if (rowArgTypeId == RECORDOID)
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

	return typeMod;
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

	AdjustReadIntermediateResultCost(rte, relOptInfo);

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
	relationRestriction->prunedShardIntervalList = NIL;

	/* see comments on GetVarFromAssignedParam() */
	relationRestriction->outerPlanParamsList = OuterPlanParamsList(root);

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
 * AdjustReadIntermediateResultCost adjusts the row count and total cost
 * of a read_intermediate_result call based on the file size.
 */
static void
AdjustReadIntermediateResultCost(RangeTblEntry *rangeTableEntry, RelOptInfo *relOptInfo)
{
	PathTarget *reltarget = relOptInfo->reltarget;
	List *pathList = relOptInfo->pathlist;
	Path *path = NULL;
	RangeTblFunction *rangeTableFunction = NULL;
	FuncExpr *funcExpression = NULL;
	Const *resultFormatConst = NULL;
	Datum resultFormatDatum = 0;
	Oid resultFormatId = InvalidOid;
	Const *resultIdConst = NULL;
	Datum resultIdDatum = 0;
	char *resultId = NULL;
	int64 resultSize = 0;
	ListCell *typeCell = NULL;
	bool binaryFormat = false;
	double rowCost = 0.;
	double rowSizeEstimate = 0;
	double rowCountEstimate = 0.;
	double ioCost = 0.;
#if PG_VERSION_NUM >= 120000
	QualCost funcCost = { 0., 0. };
#else
	double funcCost = 0.;
#endif

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

	rangeTableFunction = (RangeTblFunction *) linitial(rangeTableEntry->functions);
	funcExpression = (FuncExpr *) rangeTableFunction->funcexpr;
	resultIdConst = (Const *) linitial(funcExpression->args);
	if (!IsA(resultIdConst, Const))
	{
		/* not sure how to interpret non-const */
		return;
	}

	resultIdDatum = resultIdConst->constvalue;
	resultId = TextDatumGetCString(resultIdDatum);

	resultSize = IntermediateResultSize(resultId);
	if (resultSize < 0)
	{
		/* result does not exist, will probably error out later on */
		return;
	}

	resultFormatConst = (Const *) lsecond(funcExpression->args);
	if (!IsA(resultFormatConst, Const))
	{
		/* not sure how to interpret non-const */
		return;
	}

	resultFormatDatum = resultFormatConst->constvalue;
	resultFormatId = DatumGetObjectId(resultFormatDatum);

	if (resultFormatId == BinaryCopyFormatId())
	{
		binaryFormat = true;

		/* subtract 11-byte signature + 8 byte header + 2-byte footer */
		resultSize -= 21;
	}

	/* start with the cost of evaluating quals */
	rowCost += relOptInfo->baserestrictcost.per_tuple;

	/* postgres' estimate for the width of the rows */
	rowSizeEstimate += reltarget->width;

	/* add 2 bytes for column count (binary) or line separator (text) */
	rowSizeEstimate += 2;

	foreach(typeCell, rangeTableFunction->funccoltypes)
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
#if PG_VERSION_NUM >= 120000
		add_function_cost(NULL, inputFunctionId, NULL, &funcCost);
#else
		funcCost += get_func_cost(inputFunctionId);
#endif
	}
#if PG_VERSION_NUM >= 120000
	rowCost += funcCost.per_tuple;
#else
	rowCost += funcCost * cpu_operator_cost;
#endif

	/* estimate the number of rows based on the file size and estimated row size */
	rowCountEstimate = Max(1, (double) resultSize / rowSizeEstimate);

	/* cost of reading the data */
	ioCost = seq_page_cost * resultSize / BLCKSZ;

	Assert(pathList != NIL);

	/* tell the planner about the cost and row count of the function */
	path = (Path *) linitial(pathList);
	path->rows = rowCountEstimate;
	path->total_cost = rowCountEstimate * rowCost + ioCost;

#if PG_VERSION_NUM >= 120000
	path->startup_cost = funcCost.startup + relOptInfo->baserestrictcost.startup;
#endif
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
	PlannerInfo *outerNodeRoot = NULL;

	for (outerNodeRoot = root->parent_root; outerNodeRoot != NULL;
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

	/* we'll apply logical AND as we add tables */
	plannerRestrictionContext->relationRestrictionContext->allReferenceTables = true;
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
			ParamExternData *externParam = NULL;
			Oid paramType = InvalidOid;

			/* give hook a chance in case parameter is dynamic */
			if (boundParams->paramFetch != NULL)
			{
#if (PG_VERSION_NUM >= 110000)
				ParamExternData externParamPlaceholder;
				externParam = (*boundParams->paramFetch)(boundParams, paramId, false,
														 &externParamPlaceholder);
#else
				externParam = &boundParams->params[paramId - 1];
				if (!OidIsValid(externParam->ptype))
				{
					(*boundParams->paramFetch)(boundParams, paramId);
				}
#endif
			}
			else
			{
				externParam = &boundParams->params[paramId - 1];
			}

			paramType = externParam->ptype;
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
