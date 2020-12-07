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


/*
 * Managed via a GUC
 */
int LocalTableJoinPolicy = LOCAL_JOIN_POLICY_AUTO;

typedef struct RangeTableEntryDetails
{
	RangeTblEntry *rangeTableEntry;
	int rteIdentity;
	List *requiredAttributeNumbers;
	bool hasConstantFilterOnUniqueColumn;
} RangeTableEntryDetails;

typedef struct ConversionCandidates
{
	List *distributedTableList; /* reference or distributed table */
	List *localTableList; /* local or citus local table */
}ConversionCandidates;

static bool HasConstantFilterOnUniqueColumn(RangeTblEntry *rangeTableEntry,
											RelationRestriction *relationRestriction);
static List * RequiredAttrNumbersForRelation(RangeTblEntry *relationRte,
											 PlannerRestrictionContext *
											 plannerRestrictionContext);
static ConversionCandidates * CreateConversionCandidates(FromExpr *joinTree,
														 PlannerRestrictionContext *
														 plannerRestrictionContext,
														 List *rangeTableList,
														 Oid resultRelationId);
static void GetAllUniqueIndexes(Form_pg_index indexForm, List **uniqueIndexes);
static RangeTableEntryDetails * GetNextRTEToConvertToSubquery(FromExpr *joinTree,
															  ConversionCandidates *
															  conversionCandidates,
															  PlannerRestrictionContext *
															  plannerRestrictionContext);
static void RemoveFromConversionCandidates(ConversionCandidates *conversionCandidates,
										   int rteIdentity);
static bool AllRangeTableEntriesHaveUniqueIndex(List *rangeTableEntryDetailsList);

/*
 * RecursivelyPlanLocalTableJoins gets a query and the planner
 * restrictions. As long as the query is not plannable by router planner,
 * it converts either a local or distributed table to a subquery.
 */
void
RecursivelyPlanLocalTableJoins(Query *query,
							   RecursivePlanningContext *context, List *rangeTableList)
{
	PlannerRestrictionContext *plannerRestrictionContext =
		GetPlannerRestrictionContext(context);

	Oid resultRelationId = InvalidOid;
	if (IsModifyCommand(query))
	{
		resultRelationId = ModifyQueryResultRelationId(query);
	}
	ConversionCandidates *conversionCandidates =
		CreateConversionCandidates(query->jointree, plannerRestrictionContext,
								   rangeTableList, resultRelationId);

	while (ShouldConvertLocalTableJoinsToSubqueries(query, rangeTableList,
													plannerRestrictionContext))
	{
		FromExpr *joinTree = query->jointree;
		RangeTableEntryDetails *rangeTableEntryDetails =
			GetNextRTEToConvertToSubquery(joinTree, conversionCandidates,
										  plannerRestrictionContext);
		if (rangeTableEntryDetails == NULL)
		{
			break;
		}

		RangeTblEntry *rangeTableEntry = rangeTableEntryDetails->rangeTableEntry;
		List *requiredAttributeNumbers = rangeTableEntryDetails->requiredAttributeNumbers;
		ReplaceRTERelationWithRteSubquery(rangeTableEntry,
										  requiredAttributeNumbers, context);
		int rteIdentity = rangeTableEntryDetails->rteIdentity;
		RemoveFromConversionCandidates(conversionCandidates, rteIdentity);
	}
}


/*
 * GetNextRTEToConvertToSubquery returns the range table entry
 * which should be converted to a subquery. It considers the local join policy
 * and result relation.
 */
static RangeTableEntryDetails *
GetNextRTEToConvertToSubquery(FromExpr *joinTree,
							  ConversionCandidates *conversionCandidates,
							  PlannerRestrictionContext *plannerRestrictionContext)
{
	RangeTableEntryDetails *localRTECandidate = NULL;
	RangeTableEntryDetails *distributedRTECandidate = NULL;

	if (list_length(conversionCandidates->localTableList) > 0)
	{
		localRTECandidate = linitial(conversionCandidates->localTableList);
	}
	if (list_length(conversionCandidates->distributedTableList) > 0)
	{
		distributedRTECandidate = linitial(conversionCandidates->distributedTableList);
	}

	if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_PREFER_LOCAL)
	{
		return localRTECandidate ? localRTECandidate : distributedRTECandidate;
	}
	else if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_PREFER_DISTRIBUTED)
	{
		return distributedRTECandidate ? distributedRTECandidate : localRTECandidate;
	}
	else
	{
		bool allRangeTableEntriesHaveUniqueIndex = AllRangeTableEntriesHaveUniqueIndex(
			conversionCandidates->distributedTableList);

		if (allRangeTableEntriesHaveUniqueIndex)
		{
			return distributedRTECandidate ? distributedRTECandidate : localRTECandidate;
		}
		else
		{
			return localRTECandidate ? localRTECandidate : distributedRTECandidate;
		}
	}
}


/*
 * AllRangeTableEntriesHaveUniqueIndex returns true if all of the RTE's in the given
 * list have a unique index.
 */
static bool
AllRangeTableEntriesHaveUniqueIndex(List *rangeTableEntryDetailsList)
{
	RangeTableEntryDetails *rangeTableEntryDetails = NULL;
	foreach_ptr(rangeTableEntryDetails, rangeTableEntryDetailsList)
	{
		if (!rangeTableEntryDetails->hasConstantFilterOnUniqueColumn)
		{
			return false;
		}
	}
	return true;
}


/*
 * RemoveFromConversionCandidates removes an element from
 * the relevant list based on the relation id.
 */
static void
RemoveFromConversionCandidates(ConversionCandidates *conversionCandidates, int
							   rteIdentity)
{
	RangeTableEntryDetails *rangeTableEntryDetails = NULL;
	foreach_ptr(rangeTableEntryDetails, conversionCandidates->localTableList)
	{
		if (rangeTableEntryDetails->rteIdentity == rteIdentity)
		{
			conversionCandidates->localTableList =
				list_delete_ptr(conversionCandidates->localTableList,
								rangeTableEntryDetails);
			return;
		}
	}

	foreach_ptr(rangeTableEntryDetails, conversionCandidates->distributedTableList)
	{
		if (rangeTableEntryDetails->rteIdentity == rteIdentity)
		{
			conversionCandidates->distributedTableList =
				list_delete_ptr(conversionCandidates->distributedTableList,
								rangeTableEntryDetails);
			return;
		}
	}

	ereport(ERROR, (errmsg("invalid rte index is given :%d", rteIdentity)));
}


/*
 * ShouldConvertLocalTableJoinsToSubqueries returns true if we should
 * convert local-dist table joins to subqueries.
 */
bool
ShouldConvertLocalTableJoinsToSubqueries(Query *query, List *rangeTableList,
										 PlannerRestrictionContext *
										 plannerRestrictionContext)
{
	if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_NEVER)
	{
		/* user doesn't want Citus to enable local table joins */
		return false;
	}

	if (!ContainsTableToBeConvertedToSubquery(rangeTableList))
	{
		return false;
	}

	plannerRestrictionContext = FilterPlannerRestrictionForQuery(
		plannerRestrictionContext, query);
	if (IsRouterPlannable(query, plannerRestrictionContext))
	{
		ereport(DEBUG1, (errmsg("local-distributed table joins will not be converted, "
								"as the query is router plannable")));
		return false;
	}
	return true;
}


/*
 * HasConstantFilterOnUniqueColumn returns true if the given rangeTableEntry has a constant
 * filter on a unique column.
 */
static bool
HasConstantFilterOnUniqueColumn(RangeTblEntry *rangeTableEntry,
								RelationRestriction *relationRestriction)
{
	if (rangeTableEntry == NULL)
	{
		return false;
	}
	List *baseRestrictionList = relationRestriction->relOptInfo->baserestrictinfo;
	List *restrictClauseList = get_all_actual_clauses(baseRestrictionList);
	List *rteEqualityQuals =
		FetchEqualityAttrNumsForRTE((Node *) restrictClauseList);

	List *uniqueIndexAttrNumbers = ExecuteFunctionOnEachTableIndex(rangeTableEntry->relid,
																   GetAllUniqueIndexes);
	int columnNumber = 0;
	foreach_int(columnNumber, uniqueIndexAttrNumbers)
	{
		if (list_member_int(rteEqualityQuals, columnNumber))
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
RequiredAttrNumbersForRelation(RangeTblEntry *rangeTableEntry,
							   PlannerRestrictionContext *plannerRestrictionContext)
{
	RelationRestriction *relationRestriction =
		RelationRestrictionForRelation(rangeTableEntry, plannerRestrictionContext);

	if (relationRestriction == NULL)
	{
		return NIL;
	}

	PlannerInfo *plannerInfo = relationRestriction->plannerInfo;
	Query *queryToProcess = plannerInfo->parse;
	int rteIndex = relationRestriction->index;

	List *allVarsInQuery = pull_vars_of_level((Node *) queryToProcess, 0);

	List *requiredAttrNumbers = NIL;

	Var *var = NULL;
	foreach_ptr(var, allVarsInQuery)
	{
		if (var->varno == rteIndex)
		{
			requiredAttrNumbers = list_append_unique_int(requiredAttrNumbers,
														 var->varattno);
		}
	}

	return requiredAttrNumbers;
}


/*
 * CreateConversionCandidates creates the conversion candidates that might
 * be converted to a subquery so that citus planners can work.
 */
static ConversionCandidates *
CreateConversionCandidates(FromExpr *joinTree,
						   PlannerRestrictionContext *plannerRestrictionContext,
						   List *rangeTableList, Oid resultRelationId)
{
	ConversionCandidates *conversionCandidates =
		palloc0(sizeof(ConversionCandidates));

	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, rangeTableList)
	{
		/* we're only interested in tables */
		if (!IsRecursivelyPlannableRelation(rangeTableEntry))
		{
			continue;
		}

		/* result relation cannot converted to a subquery */
		if (resultRelationId == rangeTableEntry->relid)
		{
			continue;
		}

		RelationRestriction *relationRestriction =
			RelationRestrictionForRelation(rangeTableEntry, plannerRestrictionContext);
		if (relationRestriction == NULL)
		{
			continue;
		}
		int rteIdentity = GetRTEIdentity(rangeTableEntry);

		RangeTableEntryDetails *rangeTableEntryDetails =
			palloc0(sizeof(RangeTableEntryDetails));

		rangeTableEntryDetails->rangeTableEntry = rangeTableEntry;
		rangeTableEntryDetails->rteIdentity = rteIdentity;
		rangeTableEntryDetails->requiredAttributeNumbers =
			RequiredAttrNumbersForRelation(rangeTableEntry, plannerRestrictionContext);
		rangeTableEntryDetails->hasConstantFilterOnUniqueColumn =
			HasConstantFilterOnUniqueColumn(rangeTableEntry, relationRestriction);

		bool referenceOrDistributedTable =
			IsCitusTableType(rangeTableEntry->relid, REFERENCE_TABLE) ||
			IsCitusTableType(rangeTableEntry->relid, DISTRIBUTED_TABLE);
		if (referenceOrDistributedTable)
		{
			conversionCandidates->distributedTableList =
				lappend(conversionCandidates->distributedTableList,
						rangeTableEntryDetails);
		}
		else
		{
			conversionCandidates->localTableList =
				lappend(conversionCandidates->localTableList,
						rangeTableEntryDetails);
		}
	}
	return conversionCandidates;
}
