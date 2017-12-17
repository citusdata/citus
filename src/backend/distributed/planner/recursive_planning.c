/*-------------------------------------------------------------------------
 *
 * recursive_planning.c
 *
 * Logic for calling the postgres planner recursively for CTEs and
 * non-pushdownable subqueries in distributed queries.
 *
 * PostgreSQL with Citus can execute 4 types of queries:
 *
 * - Postgres queries on local tables and functions.
 *
 *   These queries can use all SQL features, but they may not reference
 *   distributed tables.
 *
 * - Router queries that can be executed on a single by node by replacing
 *   table names with shard names.
 *
 *   These queries can use nearly all SQL features, but only if they have
 *   a single-valued filter on the distribution column.
 *
 * - Real-time queries that can be executed by performing a task for each
 *   shard in a distributed table and performing a merge step.
 *
 *   These queries have limited SQL support. They may only include
 *   subqueries if the subquery can be executed on each shard by replacing
 *   table names with shard names and concatenating the result.
 *
 * - Task-tracker queries that can be executed through a tree of
 *   re-partitioning operations.
 *
 *   These queries have very limited SQL support and only support basic
 *   inner joins and subqueries without joins.
 *
 * To work around the limitations of these planners, we recursively call
 * the planner for CTEs and unsupported subqueries to obtain a list of
 * subplans.
 *
 * During execution, each subplan is executed separately through the method
 * that is appropriate for that query. The results are written to temporary
 * files on the workers. In the original query, the CTEs and subqueries are
 * replaced by mini-subqueries that read from the temporary files.
 *
 * This allows almost all SQL to be directly or indirectly supported,
 * because if all subqueries that contain distributed tables have been
 * replaced then what remains is a router query which can use nearly all
 * SQL features.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/distributed_planner.h"
#include "distributed/errormessage.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/recursive_planning.h"
#include "distributed/relation_restriction_equivalence.h"
#include "lib/stringinfo.h"
#include "optimizer/planner.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "utils/builtins.h"
#include "utils/guc.h"


/*
 * RecursivePlanningContext is used to recursively plan subqueries
 * and CTEs, pull results to the coordinator, and push it back into
 * the workers.
 */
typedef struct RecursivePlanningContext
{
	int level;
	uint64 planId;
	List *subPlanList;
	PlannerRestrictionContext *plannerRestrictionContext;
} RecursivePlanningContext;

/*
 * CteReferenceWalkerContext is used to collect CTE references in
 * CteReferenceListWalker.
 */
typedef struct CteReferenceWalkerContext
{
	int level;
	List *cteReferenceList;
} CteReferenceWalkerContext;

/*
 * VarLevelsUpWalkerContext is used to find Vars in a (sub)query that
 * refer to upper levels and therefore cannot be planned separately.
 */
typedef struct VarLevelsUpWalkerContext
{
	int level;
} VarLevelsUpWalkerContext;


/* local function forward declarations */
static DeferredErrorMessage * RecursivelyPlanCTEs(Query *query,
												  RecursivePlanningContext *context);
static DistributedSubPlan * CreateDistributedSubPlan(uint32 subPlanId,
													 Query *subPlanQuery);
static bool CteReferenceListWalker(Node *node, CteReferenceWalkerContext *context);
static bool ContainsReferencesToOuterQuery(Query *query);
static bool ContainsReferencesToOuterQueryWalker(Node *node,
												 VarLevelsUpWalkerContext *context);
static Query * BuildSubPlanResultQuery(Query *subquery, uint64 planId, uint32 subPlanId);


/*
 * RecursivelyPlanSubqueriesAndCTEs finds subqueries and CTEs that cannot be pushed down to
 * workers directly and instead plans them by recursively calling the planner and
 * adding the subplan to subPlanList.
 *
 * Subplans are executed prior to the distributed plan and the results are written
 * to temporary files on workers.
 *
 * CTE references are replaced by a subquery on the read_intermediate_result
 * function, which reads from the temporary file.
 *
 * If recursive planning results in an error then the error is returned. Otherwise, the
 * subplans will be added to subPlanList.
 */
DeferredErrorMessage *
RecursivelyPlanSubqueriesAndCTEs(Query *query,
								 PlannerRestrictionContext *plannerRestrictionContext,
								 uint64 planId, List **subPlanList)
{
	DeferredErrorMessage *error = NULL;
	RecursivePlanningContext context;

	if (SubqueryPushdown)
	{
		/*
		 * When the subquery_pushdown flag is enabled we make some hacks
		 * to push down subqueries with LIMIT. Recursive planning would
		 * valiantly do the right thing and try to recursively plan the
		 * inner subqueries, but we don't really want it to because those
		 * subqueries might not be supported and would be much slower.
		 *
		 * Instead, we skip recursive planning altogether when
		 * subquery_pushdown is enabled.
		 */
		return NULL;
	}

	context.level = 0;
	context.planId = planId;
	context.subPlanList = NIL;
	context.plannerRestrictionContext = plannerRestrictionContext;

	error = RecursivelyPlanCTEs(query, &context);
	if (error != NULL)
	{
		return error;
	}

	/* XXX: plan subqueries */

	*subPlanList = context.subPlanList;

	return NULL;
}


/*
 * RecursivelyPlanCTEs plans all CTEs in the query by recursively calling the planner
 * The resulting plan is added to planningContext->subPlanList and CTE references
 * are replaced by subqueries that call read_intermediate_result, which reads the
 * intermediate result of the CTE after it is executed.
 *
 * Recursive and modifying CTEs are not yet supported and return an error.
 */
static DeferredErrorMessage *
RecursivelyPlanCTEs(Query *query, RecursivePlanningContext *planningContext)
{
	ListCell *cteCell = NULL;
	CteReferenceWalkerContext context = { -1, NIL };

	if (query->cteList == NIL)
	{
		/* no CTEs, nothing to do */
		return NULL;
	}

	if (query->hasModifyingCTE)
	{
		/* we could easily support these, but it's a little scary */
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "data-modifying statements are not supported in "
							 "the WITH clauses of distributed queries",
							 NULL, NULL);
	}

	if (query->hasRecursive)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "recursive CTEs are not supported in distributed "
							 "queries",
							 NULL, NULL);
	}

	/* get all RTE_CTEs that point to CTEs from cteList */
	CteReferenceListWalker((Node *) query, &context);

	foreach(cteCell, query->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);
		char *cteName = cte->ctename;
		Query *subquery = (Query *) cte->ctequery;
		uint64 planId = planningContext->planId;
		uint32 subPlanId = 0;
		Query *resultQuery = NULL;
		DistributedSubPlan *subPlan = NULL;
		ListCell *rteCell = NULL;
		int replacedCtesCount = 0;

		if (ContainsReferencesToOuterQuery(subquery))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "CTEs that refer to other subqueries are not "
								 "supported in multi-shard queries",
								 NULL, NULL);
		}

		if (cte->cterefcount == 0)
		{
			/*
			 * CTEs that aren't referenced aren't executed in postgres. We
			 * don't need to generate a subplan for it and can take the rest
			 * of this iteration off.
			 */
			continue;
		}

		subPlanId = list_length(planningContext->subPlanList) + 1;

		if (log_min_messages >= DEBUG1)
		{
			StringInfo subPlanString = makeStringInfo();
			pg_get_query_def(subquery, subPlanString);
			ereport(DEBUG1, (errmsg("generating subplan " UINT64_FORMAT "_%u for "
																		"CTE %s: %s",
									planId, subPlanId, cteName, subPlanString->data)));
		}

		/* build a sub plan for the CTE */
		subPlan = CreateDistributedSubPlan(subPlanId, subquery);
		planningContext->subPlanList = lappend(planningContext->subPlanList, subPlan);

		/* replace references to the CTE with a subquery that reads results */
		resultQuery = BuildSubPlanResultQuery(subquery, planId, subPlanId);

		foreach(rteCell, context.cteReferenceList)
		{
			RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rteCell);

			if (rangeTableEntry->rtekind != RTE_CTE)
			{
				/*
				 * This RTE pointed to a preceding CTE that was already replaced
				 * by a subplan.
				 */
				continue;
			}

			if (strncmp(rangeTableEntry->ctename, cteName, NAMEDATALEN) == 0)
			{
				/* change the RTE_CTE into an RTE_SUBQUERY */
				rangeTableEntry->rtekind = RTE_SUBQUERY;
				rangeTableEntry->ctename = NULL;
				rangeTableEntry->ctelevelsup = 0;

				if (replacedCtesCount == 0)
				{
					/*
					 * Replace the first CTE reference with the result query directly.
					 */
					rangeTableEntry->subquery = resultQuery;
				}
				else
				{
					/*
					 * Replace subsequent CTE references with a copy of the result
					 * query.
					 */
					rangeTableEntry->subquery = copyObject(resultQuery);
				}

				replacedCtesCount++;
			}
		}

		Assert(cte->cterefcount == replacedCtesCount);
	}

	/*
	 * All CTEs are now executed through subplans and RTE_CTEs pointing
	 * to the CTE list have been replaced with subqueries. We can now
	 * clear the cteList.
	 */
	query->cteList = NIL;

	return NULL;
}


/*
 * CreateDistributedSubPlan creates a distributed subplan by recursively calling
 * the planner from the top, which may either generate a local plan or another
 * distributed plan, which can itself contain subplans.
 */
static DistributedSubPlan *
CreateDistributedSubPlan(uint32 subPlanId, Query *subPlanQuery)
{
	DistributedSubPlan *subPlan = NULL;
	int cursorOptions = 0;

	if (ContainsReadIntermediateResultFunction((Node *) subPlanQuery))
	{
		/*
		 * Make sure we go through distributed planning if there are
		 * read_intermediate_result calls, even if there are no distributed
		 * tables in the query anymore.
		 *
		 * We cannot perform this check in the planner itself, since that
		 * would also cause the workers to attempt distributed planning.
		 */
		cursorOptions |= CURSOR_OPT_FORCE_DISTRIBUTED;
	}

	subPlan = CitusMakeNode(DistributedSubPlan);
	subPlan->plan = planner(subPlanQuery, cursorOptions, NULL);
	subPlan->subPlanId = subPlanId;

	return subPlan;
}


/*
 * CteReferenceListWalker finds all references to CTEs in the top level of a query
 * and adds them to context->cteReferenceList.
 */
static bool
CteReferenceListWalker(Node *node, CteReferenceWalkerContext *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblEntry))
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) node;

		if (rangeTableEntry->rtekind == RTE_CTE &&
			rangeTableEntry->ctelevelsup == context->level)
		{
			context->cteReferenceList = lappend(context->cteReferenceList,
												rangeTableEntry);
		}

		/* caller will descend into range table entry */
		return false;
	}
	else if (IsA(node, Query))
	{
		Query *query = (Query *) node;

		context->level += 1;
		query_tree_walker(query, CteReferenceListWalker, context, QTW_EXAMINE_RTES);
		context->level -= 1;

		return false;
	}
	else
	{
		return expression_tree_walker(node, CteReferenceListWalker, context);
	}
}


/*
 * ContainsReferencesToOuterQuery determines whether the given query contains
 * any Vars that point outside of the query itself. Such queries cannot be
 * planned recursively.
 */
static bool
ContainsReferencesToOuterQuery(Query *query)
{
	VarLevelsUpWalkerContext context = { 0 };
	int flags = 0;

	return query_tree_walker(query, ContainsReferencesToOuterQueryWalker,
							 &context, flags);
}


/*
 * ContainsReferencesToOuterQueryWalker determines whether the given query
 * contains any Vars that point more than context->level levels up.
 *
 * ContainsReferencesToOuterQueryWalker recursively descends into subqueries
 * and increases the level by 1 before recursing.
 */
static bool
ContainsReferencesToOuterQueryWalker(Node *node, VarLevelsUpWalkerContext *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Var))
	{
		if (((Var *) node)->varlevelsup > context->level)
		{
			return true;
		}

		return false;
	}
	else if (IsA(node, Aggref))
	{
		if (((Aggref *) node)->agglevelsup > context->level)
		{
			return true;
		}
	}
	else if (IsA(node, GroupingFunc))
	{
		if (((GroupingFunc *) node)->agglevelsup > context->level)
		{
			return true;
		}

		return false;
	}
	else if (IsA(node, PlaceHolderVar))
	{
		if (((PlaceHolderVar *) node)->phlevelsup > context->level)
		{
			return true;
		}
	}
	else if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		bool found = false;
		int flags = 0;

		context->level += 1;
		found = query_tree_walker(query, ContainsReferencesToOuterQueryWalker,
								  context, flags);
		context->level -= 1;

		return found;
	}

	return expression_tree_walker(node, ContainsReferencesToOuterQueryWalker,
								  context);
}


/*
 * BuildSubPlanResultQuery returns a query of the form:
 *
 * SELECT
 *   <target list>
 * FROM
 *   read_intermediate_result('<planId>_<subPlanId>', '<copy format'>)
 *   AS res (<column definition list>);
 *
 * The target list and column definition list are derived from the given subquery.
 *
 * If any of the types in the target list cannot be used in the binary copy format,
 * then the copy format 'text' is used, otherwise 'binary' is used.
 */
static Query *
BuildSubPlanResultQuery(Query *subquery, uint64 planId, uint32 subPlanId)
{
	Query *resultQuery = NULL;
	char *resultIdString = NULL;
	Const *resultIdConst = NULL;
	Const *resultFormatConst = NULL;
	FuncExpr *funcExpr = NULL;
	Alias *funcAlias = NULL;
	List *funcColNames = NIL;
	List *funcColTypes = NIL;
	List *funcColTypMods = NIL;
	List *funcColCollations = NIL;
	RangeTblFunction *rangeTableFunction = NULL;
	RangeTblEntry *rangeTableEntry = NULL;
	RangeTblRef *rangeTableRef = NULL;
	FromExpr *joinTree = NULL;
	ListCell *targetEntryCell = NULL;
	List *targetList = NIL;
	int columnNumber = 1;
	bool useBinaryCopyFormat = true;
	Oid copyFormatId = BinaryCopyFormatId();

	/* build the target list and column definition list */
	foreach(targetEntryCell, subquery->targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Node *targetExpr = (Node *) targetEntry->expr;
		char *columnName = targetEntry->resname;
		Oid columnType = exprType(targetExpr);
		Oid columnTypMod = exprTypmod(targetExpr);
		Oid columnCollation = exprCollation(targetExpr);
		Var *functionColumnVar = NULL;
		TargetEntry *newTargetEntry = NULL;

		if (targetEntry->resjunk)
		{
			continue;
		}

		funcColNames = lappend(funcColNames, makeString(columnName));
		funcColTypes = lappend_int(funcColTypes, columnType);
		funcColTypMods = lappend_int(funcColTypMods, columnTypMod);
		funcColCollations = lappend_int(funcColCollations, columnCollation);

		functionColumnVar = makeNode(Var);
		functionColumnVar->varno = 1;
		functionColumnVar->varattno = columnNumber;
		functionColumnVar->vartype = columnType;
		functionColumnVar->vartypmod = columnTypMod;
		functionColumnVar->varcollid = columnCollation;
		functionColumnVar->varlevelsup = 0;
		functionColumnVar->varnoold = 1;
		functionColumnVar->varoattno = columnNumber;
		functionColumnVar->location = -1;

		newTargetEntry = makeNode(TargetEntry);
		newTargetEntry->expr = (Expr *) functionColumnVar;
		newTargetEntry->resno = columnNumber;
		newTargetEntry->resname = columnName;
		newTargetEntry->resjunk = false;

		targetList = lappend(targetList, newTargetEntry);

		if (useBinaryCopyFormat && !CanUseBinaryCopyFormatForType(columnType))
		{
			useBinaryCopyFormat = false;
		}

		columnNumber++;
	}

	/* build the result_id parameter for the call to read_intermediate_result */
	resultIdString = GenerateResultId(planId, subPlanId);

	resultIdConst = makeNode(Const);
	resultIdConst->consttype = TEXTOID;
	resultIdConst->consttypmod = -1;
	resultIdConst->constlen = -1;
	resultIdConst->constvalue = CStringGetTextDatum(resultIdString);
	resultIdConst->constbyval = false;
	resultIdConst->constisnull = false;
	resultIdConst->location = -1;

	/* build the citus_copy_format parameter for the call to read_intermediate_result */
	if (!useBinaryCopyFormat)
	{
		copyFormatId = TextCopyFormatId();
	}

	resultFormatConst = makeNode(Const);
	resultFormatConst->consttype = CitusCopyFormatTypeId();
	resultFormatConst->consttypmod = -1;
	resultFormatConst->constlen = 4;
	resultFormatConst->constvalue = ObjectIdGetDatum(copyFormatId);
	resultFormatConst->constbyval = true;
	resultFormatConst->constisnull = false;
	resultFormatConst->location = -1;

	/* build the call to read_intermediate_result */
	funcExpr = makeNode(FuncExpr);
	funcExpr->funcid = CitusReadIntermediateResultFuncId();
	funcExpr->funcretset = true;
	funcExpr->funcvariadic = false;
	funcExpr->funcformat = 0;
	funcExpr->funccollid = 0;
	funcExpr->inputcollid = 0;
	funcExpr->location = -1;
	funcExpr->args = list_make2(resultIdConst, resultFormatConst);

	/* build the RTE for the call to read_intermediate_result */
	rangeTableFunction = makeNode(RangeTblFunction);
	rangeTableFunction->funccolcount = list_length(funcColNames);
	rangeTableFunction->funccolnames = funcColNames;
	rangeTableFunction->funccoltypes = funcColTypes;
	rangeTableFunction->funccoltypmods = funcColTypMods;
	rangeTableFunction->funccolcollations = funcColCollations;
	rangeTableFunction->funcparams = NULL;
	rangeTableFunction->funcexpr = (Node *) funcExpr;

	funcAlias = makeNode(Alias);
	funcAlias->aliasname = "intermediate_result";
	funcAlias->colnames = funcColNames;

	rangeTableEntry = makeNode(RangeTblEntry);
	rangeTableEntry->rtekind = RTE_FUNCTION;
	rangeTableEntry->functions = list_make1(rangeTableFunction);
	rangeTableEntry->inFromCl = true;
	rangeTableEntry->eref = funcAlias;

	/* build the join tree using the read_intermediate_result RTE */
	rangeTableRef = makeNode(RangeTblRef);
	rangeTableRef->rtindex = 1;

	joinTree = makeNode(FromExpr);
	joinTree->fromlist = list_make1(rangeTableRef);

	/* build the SELECT query */
	resultQuery = makeNode(Query);
	resultQuery->commandType = CMD_SELECT;
	resultQuery->rtable = list_make1(rangeTableEntry);
	resultQuery->jointree = joinTree;
	resultQuery->targetList = targetList;

	return resultQuery;
}


/*
 * GenerateResultId generates the result ID that is used to identify an intermediate
 * result of the subplan with the given plan ID and subplan ID.
 */
char *
GenerateResultId(uint64 planId, uint32 subPlanId)
{
	StringInfo resultId = makeStringInfo();

	appendStringInfo(resultId, UINT64_FORMAT "_%u", planId, subPlanId);

	return resultId->data;
}
