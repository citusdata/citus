#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "funcapi.h"

#include "catalog/pg_type.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/distributed_planner.h"
#include "distributed/errormessage.h"
#include "distributed/local_distributed_join_planner.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/query_colocation_checker.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/recursive_planning.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/log_utils.h"
#include "distributed/shard_pruning.h"
#include "distributed/version_compat.h"
#include "lib/stringinfo.h"
#include "optimizer/clauses.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "optimizer/optimizer.h"
#else
#include "optimizer/var.h"
#endif
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "parser/parsetree.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#if PG_VERSION_NUM >= PG_VERSION_12
#include "nodes/pathnodes.h"
#else
#include "nodes/relation.h"
#endif
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

typedef struct RTEToSubqueryConverterReference
{
	RangeTblEntry *rangeTableEntry;
	Index rteIndex;
	List *restrictionList;
	List *requiredAttributeNumbers;
} RTEToSubqueryConverterReference;

typedef struct RTEToSubqueryConverterContext
{
	List *distributedTableList; /* reference or distributed table */
	List *localTableList; /* local or citus local table */
	bool hasSubqueryRTE;
}RTEToSubqueryConverterContext;

static Oid GetResultRelationId(Query *query);
static Oid GetRTEToSubqueryConverterReferenceRelId(
	RTEToSubqueryConverterReference *rteToSubqueryConverterReference);
static bool ShouldConvertLocalTableJoinsToSubqueries(List *rangeTableList, Oid
													 resultRelationId);
static bool HasUniqueFilter(RangeTblEntry *distRTE, List *distRTERestrictionList,
							List *requiredAttrNumbersForDistRTE);
static bool ShouldConvertDistributedTable(FromExpr *joinTree,
										  RTEToSubqueryConverterReference *distRTEContext);
static List * RequiredAttrNumbersForRelation(RangeTblEntry *relationRte,
											 RecursivePlanningContext *planningContext);
static RTEToSubqueryConverterContext * CreateRTEToSubqueryConverterContext(
	RecursivePlanningContext *context,
	List *
	rangeTableList);
static void GetAllUniqueIndexes(Form_pg_index indexForm, List **uniqueIndexes);
static RTEToSubqueryConverterReference * GetNextRTEToConvertToSubquery(FromExpr *joinTree,
																	   RTEToSubqueryConverterContext
																	   *
																	   rteToSubqueryConverterContext,
																	   PlannerRestrictionContext
																	   *
																	   plannerRestrictionContext,
																	   Oid
																	   resultRelationId);
static void GetRangeTableEntriesFromJoinTree(Node *joinNode, List *rangeTableList,
											 List **joinRangeTableEntries);
static void RemoveFromRTEToSubqueryConverterContext(
	RTEToSubqueryConverterContext *rteToSubqueryConverterContext, Oid relationId);
static bool FillLocalAndDistributedRTECandidates(
	RTEToSubqueryConverterContext *rteToSubqueryConverterContext,
	RTEToSubqueryConverterReference **
	localRTECandidate,
	RTEToSubqueryConverterReference **
	distributedRTECandidate);

/*
 * ConvertUnplannableTableJoinsToSubqueries gets a query and the planner
 * restrictions. As long as the query is not plannable by router planner,
 * it converts either a local or distributed table to a subquery.
 */
void
ConvertUnplannableTableJoinsToSubqueries(Query *query,
										 RecursivePlanningContext *context)
{
	List *rangeTableList = NIL;
	GetRangeTableEntriesFromJoinTree((Node *) query->jointree, query->rtable,
									 &rangeTableList);
	Oid resultRelationId = GetResultRelationId(query);
	if (!ShouldConvertLocalTableJoinsToSubqueries(rangeTableList, resultRelationId))
	{
		return;
	}

	RTEToSubqueryConverterContext *rteToSubqueryConverterContext =
		CreateRTEToSubqueryConverterContext(
			context, rangeTableList);

	RTEToSubqueryConverterReference *rteToSubqueryConverterReference =
		GetNextRTEToConvertToSubquery(query->jointree, rteToSubqueryConverterContext,
									  context->plannerRestrictionContext,
									  resultRelationId);

	PlannerRestrictionContext *plannerRestrictionContext =
		FilterPlannerRestrictionForQuery(context->plannerRestrictionContext, query);
	while (rteToSubqueryConverterReference && !IsRouterPlannable(query,
																 plannerRestrictionContext))
	{
		ReplaceRTERelationWithRteSubquery(
			rteToSubqueryConverterReference->rangeTableEntry,
			rteToSubqueryConverterReference->restrictionList,
			rteToSubqueryConverterReference->
			requiredAttributeNumbers,
			context);
		RemoveFromRTEToSubqueryConverterContext(rteToSubqueryConverterContext,
												rteToSubqueryConverterReference->
												rangeTableEntry->relid);
		rteToSubqueryConverterReference =
			GetNextRTEToConvertToSubquery(query->jointree, rteToSubqueryConverterContext,
										  context->plannerRestrictionContext,
										  resultRelationId);
	}
}


/*
 * GetResultRelationId gets the result relation id from query
 * if it exists.
 */
static Oid
GetResultRelationId(Query *query)
{
	RangeTblEntry *resultRelation = ExtractResultRelationRTE(query);
	Oid resultRelationId = InvalidOid;
	if (resultRelation)
	{
		resultRelationId = resultRelation->relid;
	}
	return resultRelationId;
}


/*
 * GetRangeTableEntriesFromJoinTree gets the range table entries that are
 * on the given join tree.
 */
static void
GetRangeTableEntriesFromJoinTree(Node *joinNode, List *rangeTableList,
								 List **joinRangeTableEntries)
{
	if (joinNode == NULL)
	{
		return;
	}
	else if (IsA(joinNode, FromExpr))
	{
		FromExpr *fromExpr = (FromExpr *) joinNode;
		Node *fromElement;

		foreach_ptr(fromElement, fromExpr->fromlist)
		{
			GetRangeTableEntriesFromJoinTree(fromElement, rangeTableList,
											 joinRangeTableEntries);
		}
	}
	else if (IsA(joinNode, JoinExpr))
	{
		JoinExpr *joinExpr = (JoinExpr *) joinNode;
		GetRangeTableEntriesFromJoinTree(joinExpr->larg, rangeTableList,
										 joinRangeTableEntries);
		GetRangeTableEntriesFromJoinTree(joinExpr->rarg, rangeTableList,
										 joinRangeTableEntries);
	}
	else if (IsA(joinNode, RangeTblRef))
	{
		int rangeTableIndex = ((RangeTblRef *) joinNode)->rtindex;
		RangeTblEntry *rte = rt_fetch(rangeTableIndex, rangeTableList);
		*joinRangeTableEntries = lappend(*joinRangeTableEntries, rte);
	}
	else
	{
		pg_unreachable();
	}
}


/*
 * GetNextRTEToConvertToSubquery returns the range table entry
 * which should be converted to a subquery. It considers the local join policy
 * and result relation.
 */
static RTEToSubqueryConverterReference *
GetNextRTEToConvertToSubquery(FromExpr *joinTree,
							  RTEToSubqueryConverterContext *rteToSubqueryConverterContext,
							  PlannerRestrictionContext *plannerRestrictionContext, Oid
							  resultRelationId)
{
	RTEToSubqueryConverterReference *localRTECandidate = NULL;
	RTEToSubqueryConverterReference *distributedRTECandidate = NULL;
	if (!FillLocalAndDistributedRTECandidates(rteToSubqueryConverterContext,
											  &localRTECandidate,
											  &distributedRTECandidate))
	{
		return NULL;
	}

	if (OidIsValid(resultRelationId))
	{
		if (resultRelationId == GetRTEToSubqueryConverterReferenceRelId(
				localRTECandidate))
		{
			return distributedRTECandidate;
		}
		if (resultRelationId == GetRTEToSubqueryConverterReferenceRelId(
				distributedRTECandidate))
		{
			return localRTECandidate;
		}
	}

	if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_PREFER_LOCAL)
	{
		return localRTECandidate;
	}
	else if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_PREFER_DISTRIBUTED)
	{
		if (distributedRTECandidate)
		{
			return distributedRTECandidate;
		}
		else
		{
			return localRTECandidate;
		}
	}
	else
	{
		if (ShouldConvertDistributedTable(joinTree, distributedRTECandidate))
		{
			return distributedRTECandidate;
		}
		else
		{
			return localRTECandidate;
		}
	}
}


/*
 * FillLocalAndDistributedRTECandidates fills the local and distributed RTE candidates.
 * It returns true if we should continue converting tables to subqueries.
 */
static bool
FillLocalAndDistributedRTECandidates(
	RTEToSubqueryConverterContext *rteToSubqueryConverterContext,
	RTEToSubqueryConverterReference **localRTECandidate,
	RTEToSubqueryConverterReference **
	distributedRTECandidate)
{
	if (list_length(rteToSubqueryConverterContext->localTableList) > 0)
	{
		*localRTECandidate = linitial(rteToSubqueryConverterContext->localTableList);
	}
	if (*localRTECandidate == NULL)
	{
		return false;
	}

	if (list_length(rteToSubqueryConverterContext->distributedTableList) > 0)
	{
		*distributedRTECandidate = linitial(
			rteToSubqueryConverterContext->distributedTableList);
	}
	return *distributedRTECandidate != NULL ||
		   rteToSubqueryConverterContext->hasSubqueryRTE;
}


/*
 * GetRTEToSubqueryConverterReferenceRelId returns the underlying relation id
 * if it is a valid one.
 */
static Oid
GetRTEToSubqueryConverterReferenceRelId(
	RTEToSubqueryConverterReference *rteToSubqueryConverterReference)
{
	if (rteToSubqueryConverterReference &&
		rteToSubqueryConverterReference->rangeTableEntry)
	{
		return rteToSubqueryConverterReference->rangeTableEntry->relid;
	}
	return InvalidOid;
}


/*
 * RemoveFromRTEToSubqueryConverterContext removes an element from
 * the relevant list based on the relation id.
 */
static void
RemoveFromRTEToSubqueryConverterContext(
	RTEToSubqueryConverterContext *rteToSubqueryConverterContext, Oid relationId)
{
	if (IsLocalOrCitusLocalTable(relationId))
	{
		rteToSubqueryConverterContext->localTableList =
			list_delete_first(rteToSubqueryConverterContext->localTableList);
	}
	else
	{
		rteToSubqueryConverterContext->distributedTableList =
			list_delete_first(rteToSubqueryConverterContext->distributedTableList);
	}
}


/*
 * ShouldConvertLocalTableJoinsToSubqueries returns true if we should
 * convert local-dist table joins to subqueries.
 */
static bool
ShouldConvertLocalTableJoinsToSubqueries(List *rangeTableList, Oid resultRelationId)
{
	if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_NEVER)
	{
		/* user doesn't want Citus to enable local table joins */
		return false;
	}
	if (!ContainsTableToBeConvertedToSubquery(rangeTableList, resultRelationId))
	{
		return false;
	}
	return true;
}


/*
 * ShouldConvertDistributedTable returns true if we should convert the
 * distributed table rte to a subquery. This will be the case if the distributed
 * table has a unique index on a column that appears in filter.
 */
static bool
ShouldConvertDistributedTable(FromExpr *joinTree,
							  RTEToSubqueryConverterReference *
							  rteToSubqueryConverterReference)
{
	if (rteToSubqueryConverterReference == NULL)
	{
		return false;
	}
	List *distRTEEqualityQuals =
		FetchEqualityAttrNumsForRTEFromQuals(joinTree->quals,
											 rteToSubqueryConverterReference->rteIndex);

	Node *join = NULL;
	foreach_ptr(join, joinTree->fromlist)
	{
		if (IsA(join, JoinExpr))
		{
			JoinExpr *joinExpr = (JoinExpr *) join;
			distRTEEqualityQuals = list_concat(distRTEEqualityQuals,
											   FetchEqualityAttrNumsForRTEFromQuals(
												   joinExpr->quals,
												   rteToSubqueryConverterReference->
												   rteIndex)
											   );
		}
	}

	bool hasUniqueFilter = HasUniqueFilter(
		rteToSubqueryConverterReference->rangeTableEntry,
		rteToSubqueryConverterReference->
		restrictionList, distRTEEqualityQuals);
	return hasUniqueFilter;
}


/*
 * HasUniqueFilter returns true if the given RTE has a unique filter
 * on a column, which is a member of the given requiredAttrNumbersForDistRTE.
 */
static bool
HasUniqueFilter(RangeTblEntry *distRTE, List *distRTERestrictionList,
				List *requiredAttrNumbersForDistRTE)
{
	List *uniqueIndexes = ExecuteFunctionOnEachTableIndex(distRTE->relid,
														  GetAllUniqueIndexes);
	int columnNumber = 0;
	foreach_int(columnNumber, uniqueIndexes)
	{
		if (list_member_int(requiredAttrNumbersForDistRTE, columnNumber))
		{
			return true;
		}
	}
	return false;
}


/*
 * GetAllUniqueIndexes adds the given index's column numbers if it is a
 * unique index.
 * TODO:: if there is a unique index on a multiple column, then we should
 * probably return true only if all the columns in the index exist in the filter.
 */
static void
GetAllUniqueIndexes(Form_pg_index indexForm, List **uniqueIndexes)
{
	if (indexForm->indisunique || indexForm->indisprimary)
	{
		for (int i = 0; i < indexForm->indkey.dim1; i++)
		{
			*uniqueIndexes = list_append_unique_int(*uniqueIndexes,
													indexForm->indkey.values[i]);
		}
	}
}


/*
 * RequiredAttrNumbersForRelation returns the required attribute numbers for
 * the input RTE relation in order for the planning to succeed.
 *
 * The function could be optimized by not adding the columns that only appear
 * WHERE clause as a filter (e.g., not a join clause).
 */
static List *
RequiredAttrNumbersForRelation(RangeTblEntry *relationRte,
							   RecursivePlanningContext *planningContext)
{
	PlannerRestrictionContext *plannerRestrictionContext =
		planningContext->plannerRestrictionContext;

	/* TODO: Get rid of this hack, find relation restriction information directly */
	PlannerRestrictionContext *filteredPlannerRestrictionContext =
		FilterPlannerRestrictionForQuery(plannerRestrictionContext,
										 WrapRteRelationIntoSubquery(relationRte, NIL));

	RelationRestrictionContext *relationRestrictionContext =
		filteredPlannerRestrictionContext->relationRestrictionContext;
	List *filteredRelationRestrictionList =
		relationRestrictionContext->relationRestrictionList;

	if (list_length(filteredRelationRestrictionList) == 0)
	{
		return NIL;
	}
	RelationRestriction *relationRestriction =
		(RelationRestriction *) linitial(filteredRelationRestrictionList);

	PlannerInfo *plannerInfo = relationRestriction->plannerInfo;
	Query *queryToProcess = plannerInfo->parse;
	int rteIndex = relationRestriction->index;

	List *allVarsInQuery = pull_vars_of_level((Node *) queryToProcess, 0);
	ListCell *varCell = NULL;

	List *requiredAttrNumbers = NIL;

	foreach(varCell, allVarsInQuery)
	{
		Var *var = (Var *) lfirst(varCell);

		if (var->varno == rteIndex)
		{
			requiredAttrNumbers = list_append_unique_int(requiredAttrNumbers,
														 var->varattno);
		}
	}

	return requiredAttrNumbers;
}


/*
 * CreateRTEToSubqueryConverterContext returns a range table entry which has the most filters
 * on it along with the restrictions (e.g., fills **restrictionList).
 *
 * The function also gets a boolean localTable parameter, so the caller
 * can choose to run the function for only local tables or distributed tables.
 */
static RTEToSubqueryConverterContext *
CreateRTEToSubqueryConverterContext(RecursivePlanningContext *context,
									List *rangeTableList)
{
	RTEToSubqueryConverterContext *rteToSubqueryConverterContext = palloc0(
		sizeof(RTEToSubqueryConverterContext));

	int rteIndex = 0;
	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, rangeTableList)
	{
		rteIndex++;
		if (rangeTableEntry->rtekind == RTE_SUBQUERY)
		{
			rteToSubqueryConverterContext->hasSubqueryRTE = true;
		}

		/* we're only interested in tables */
		if (!SubqueryConvertableRelationForJoin(rangeTableEntry))
		{
			continue;
		}

		RTEToSubqueryConverterReference *rteToSubqueryConverter = palloc(
			sizeof(RTEToSubqueryConverterReference));
		rteToSubqueryConverter->rangeTableEntry = rangeTableEntry;
		rteToSubqueryConverter->rteIndex = rteIndex;
		rteToSubqueryConverter->restrictionList = GetRestrictInfoListForRelation(
			rangeTableEntry,
			context->
			plannerRestrictionContext, 1);
		rteToSubqueryConverter->requiredAttributeNumbers = RequiredAttrNumbersForRelation(
			rangeTableEntry, context);

		bool referenceOrDistributedTable = IsCitusTableType(rangeTableEntry->relid,
															REFERENCE_TABLE) ||
										   IsCitusTableType(rangeTableEntry->relid,
															DISTRIBUTED_TABLE);
		if (referenceOrDistributedTable)
		{
			rteToSubqueryConverterContext->distributedTableList =
				lappend(rteToSubqueryConverterContext->distributedTableList,
						rteToSubqueryConverter);
		}
		else
		{
			rteToSubqueryConverterContext->localTableList =
				lappend(rteToSubqueryConverterContext->localTableList,
						rteToSubqueryConverter);
		}
	}
	return rteToSubqueryConverterContext;
}
