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

typedef struct RTEToSubqueryConverterReference {
	RangeTblEntry* rangeTableEntry;
	Index rteIndex;
	List* restrictionList;
	List* requiredAttributeNumbers;
} RTEToSubqueryConverterReference;

typedef struct RTEToSubqueryConverterContext{
	List* distributedTableList; /* reference or distributed table */
	List* localTableList;
	List* citusLocalTableList;
	bool hasSubqueryRTE;
}RTEToSubqueryConverterContext;

static bool ShouldConvertLocalTableJoinsToSubqueries(List* rangeTableList, Oid resultRelationId);
static bool HasUniqueFilter(RangeTblEntry* distRTE, List* distRTERestrictionList, List* requiredAttrNumbersForDistRTE);
static bool AutoConvertLocalTableJoinToSubquery(FromExpr* joinTree,
 RTEToSubqueryConverterReference* distRTEContext);
static List * RequiredAttrNumbersForRelation(RangeTblEntry *relationRte,
											 RecursivePlanningContext *planningContext);
static RTEToSubqueryConverterContext * CreateRTEToSubqueryConverterContext(RecursivePlanningContext *context,
				List *rangeTableList);
static void GetAllUniqueIndexes(Form_pg_index indexForm, List** uniqueIndexes);
static RTEToSubqueryConverterReference* 
	GetNextRTEToConvertToSubquery(FromExpr* joinTree, RTEToSubqueryConverterContext* rteToSubqueryConverterContext,
		PlannerRestrictionContext *plannerRestrictionContext, RangeTblEntry* resultRelation);
static void PopFromRTEToSubqueryConverterContext(RTEToSubqueryConverterContext* rteToSubqueryConverterContext,
	bool isCitusLocalTable);		
/*
 * ConvertLocalTableJoinsToSubqueries gets a query and the planner
 * restrictions. As long as there is a join between a local table
 * and distributed table, the function wraps one table in a
 * subquery (by also pushing the filters on the table down
 * to the subquery).
 *
 * Once this function returns, there are no direct joins between
 * local and distributed tables.
 */
void
ConvertLocalTableJoinsToSubqueries(Query *query,
								   RecursivePlanningContext *context)
{
	List *rangeTableList = query->rtable;
	RangeTblEntry *resultRelation = ExtractResultRelationRTE(query);
	Oid resultRelationId = InvalidOid;
	if (resultRelation) {
		resultRelationId = resultRelation->relid;
	}
	if (!ShouldConvertLocalTableJoinsToSubqueries(rangeTableList, resultRelationId)) {
		return;
	}
	RTEToSubqueryConverterContext* rteToSubqueryConverterContext = CreateRTEToSubqueryConverterContext(
		context, rangeTableList);


	RTEToSubqueryConverterReference* rteToSubqueryConverterReference = 
			GetNextRTEToConvertToSubquery(query->jointree, rteToSubqueryConverterContext,
		 context->plannerRestrictionContext, resultRelation);
	while (rteToSubqueryConverterReference)
	{
		ReplaceRTERelationWithRteSubquery(rteToSubqueryConverterReference->rangeTableEntry,
									rteToSubqueryConverterReference->restrictionList,
									rteToSubqueryConverterReference->requiredAttributeNumbers);
		rteToSubqueryConverterReference = 
			GetNextRTEToConvertToSubquery(query->jointree, rteToSubqueryConverterContext,
		 		context->plannerRestrictionContext, resultRelation);
	}
}

static RTEToSubqueryConverterReference* 
	GetNextRTEToConvertToSubquery(FromExpr* joinTree, RTEToSubqueryConverterContext* rteToSubqueryConverterContext,
		PlannerRestrictionContext *plannerRestrictionContext, RangeTblEntry* resultRelation) {	

	RTEToSubqueryConverterReference* localRTECandidate = NULL;
	RTEToSubqueryConverterReference* nonLocalRTECandidate = NULL;
	bool citusLocalTableChosen = false;

	if (list_length(rteToSubqueryConverterContext->localTableList) > 0) {
		localRTECandidate = linitial(rteToSubqueryConverterContext->localTableList);
	}else if (list_length(rteToSubqueryConverterContext->citusLocalTableList) > 0) {
		localRTECandidate = linitial(rteToSubqueryConverterContext->citusLocalTableList);
		citusLocalTableChosen = true;
	}
	if (localRTECandidate == NULL) {
		return NULL;
	}

	if (list_length(rteToSubqueryConverterContext->distributedTableList) > 0) {
		nonLocalRTECandidate = linitial(rteToSubqueryConverterContext->distributedTableList);
	}
	if (nonLocalRTECandidate == NULL && !rteToSubqueryConverterContext->hasSubqueryRTE) {
		return NULL;
	}

	if (resultRelation) {
		if(resultRelation == localRTECandidate->rangeTableEntry) {
			rteToSubqueryConverterContext->distributedTableList = list_delete_first(
				rteToSubqueryConverterContext->distributedTableList
			);
			return nonLocalRTECandidate;
		}
		if (resultRelation == nonLocalRTECandidate->rangeTableEntry) {
			PopFromRTEToSubqueryConverterContext(rteToSubqueryConverterContext,citusLocalTableChosen);
			return localRTECandidate;
		}
	}

	if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_PREFER_LOCAL) {
		PopFromRTEToSubqueryConverterContext(rteToSubqueryConverterContext,citusLocalTableChosen);
		return localRTECandidate;
	}else if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_PREFER_DISTRIBUTED) {
		if (nonLocalRTECandidate) {
			rteToSubqueryConverterContext->distributedTableList = list_delete_first(
				rteToSubqueryConverterContext->distributedTableList
			);
			return nonLocalRTECandidate;
		}else {
			PopFromRTEToSubqueryConverterContext(rteToSubqueryConverterContext,citusLocalTableChosen);
			return localRTECandidate;
		}

	}else if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_AUTO) {
		bool shouldConvertNonLocalTable = AutoConvertLocalTableJoinToSubquery(joinTree, nonLocalRTECandidate);
		if (shouldConvertNonLocalTable) {
			rteToSubqueryConverterContext->distributedTableList = list_delete_first(
				rteToSubqueryConverterContext->distributedTableList
			);
			return nonLocalRTECandidate;
		}else {
			PopFromRTEToSubqueryConverterContext(rteToSubqueryConverterContext,citusLocalTableChosen);
			return localRTECandidate;
		}
	}else {
		elog(ERROR, "unexpected local table join policy: %d", LocalTableJoinPolicy);
	}
	return NULL;
}

static void PopFromRTEToSubqueryConverterContext(RTEToSubqueryConverterContext* rteToSubqueryConverterContext,
	bool isCitusLocalTable) {
	if (isCitusLocalTable) {
		rteToSubqueryConverterContext->citusLocalTableList = 
			list_delete_first(rteToSubqueryConverterContext->citusLocalTableList);
	}else {
		rteToSubqueryConverterContext->localTableList = 
			list_delete_first(rteToSubqueryConverterContext->localTableList);
	}
}

/*
 * ShouldConvertLocalTableJoinsToSubqueries returns true if we should
 * convert local-dist table joins to subqueries.
 */
static bool ShouldConvertLocalTableJoinsToSubqueries(List* rangeTableList, Oid resultRelationId) {
	if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_NEVER)
	{
		/* user doesn't want Citus to enable local table joins */
		return false;
	}
	if (!ContainsTableToBeConvertedToSubquery(rangeTableList, resultRelationId)) {
		return false;
	}
	return true;
}

static bool AutoConvertLocalTableJoinToSubquery(FromExpr* joinTree,
 		RTEToSubqueryConverterReference* rteToSubqueryConverterReference) {
	if (rteToSubqueryConverterReference == NULL) {
		return false;
	}
	List* distRTEEqualityQuals =
		FetchAttributeNumsForRTEFromQuals(joinTree->quals, rteToSubqueryConverterReference->rteIndex);

	Node* join = NULL;
	foreach_ptr(join, joinTree->fromlist) {
		if (IsA(join, JoinExpr)) {
			JoinExpr* joinExpr = (JoinExpr*) join;
			distRTEEqualityQuals = list_concat(distRTEEqualityQuals, 
				FetchAttributeNumsForRTEFromQuals(joinExpr->quals, rteToSubqueryConverterReference->rteIndex)
			);
		}
	}

	bool hasUniqueFilter = HasUniqueFilter(rteToSubqueryConverterReference->rangeTableEntry, 
		rteToSubqueryConverterReference->restrictionList, distRTEEqualityQuals);	
	return hasUniqueFilter;	
	
}

static bool HasUniqueFilter(RangeTblEntry* distRTE, List* distRTERestrictionList, List* requiredAttrNumbersForDistRTE) {
	List* uniqueIndexes = ExecuteFunctionOnEachTableIndex(distRTE->relid, GetAllUniqueIndexes);
    int columnNumber = 0;
    foreach_int(columnNumber, uniqueIndexes) {
        if (list_member_int(requiredAttrNumbersForDistRTE, columnNumber)) {
            return true;
        }
    }
    return false;
}

static void GetAllUniqueIndexes(Form_pg_index indexForm, List** uniqueIndexes) {
    if (indexForm->indisunique || indexForm->indisprimary) {
        for(int i = 0; i < indexForm->indkey.dim1; i++) {
            *uniqueIndexes = list_append_unique_int(*uniqueIndexes, indexForm->indkey.values[i]);
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

	if (list_length(filteredRelationRestrictionList) == 0) {
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
static RTEToSubqueryConverterContext*
CreateRTEToSubqueryConverterContext(RecursivePlanningContext *context,
				List *rangeTableList)
{
	
	RTEToSubqueryConverterContext* rteToSubqueryConverterContext = palloc0(sizeof(RTEToSubqueryConverterContext));

	int rteIndex = 0;
	RangeTblEntry* rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, rangeTableList)
	{
		rteIndex++;
		if (rangeTableEntry->rtekind == RTE_SUBQUERY) {
			rteToSubqueryConverterContext->hasSubqueryRTE = true;
		}
		/* we're only interested in tables */
		if (!SubqueryConvertableRelationForJoin(rangeTableEntry))
		{
			continue;
		}

		RTEToSubqueryConverterReference* rteToSubqueryConverter = palloc(sizeof(RTEToSubqueryConverterReference));
		rteToSubqueryConverter->rangeTableEntry = rangeTableEntry;
		rteToSubqueryConverter->rteIndex = rteIndex;
		rteToSubqueryConverter->restrictionList = GetRestrictInfoListForRelation(rangeTableEntry,
										   context->plannerRestrictionContext, 1);
		rteToSubqueryConverter->requiredAttributeNumbers = RequiredAttrNumbersForRelation(rangeTableEntry, context);								   

		bool referenceOrDistributedTable = IsCitusTableType(rangeTableEntry->relid, REFERENCE_TABLE) || 
			IsCitusTableType(rangeTableEntry->relid, DISTRIBUTED_TABLE);
		if (referenceOrDistributedTable) {
			rteToSubqueryConverterContext->distributedTableList = 
				lappend(rteToSubqueryConverterContext->distributedTableList, rteToSubqueryConverter);
		}else if (IsCitusTableType(rangeTableEntry->relid, CITUS_LOCAL_TABLE)) {
			rteToSubqueryConverterContext->citusLocalTableList = 
				lappend(rteToSubqueryConverterContext->citusLocalTableList, rteToSubqueryConverter);
		}else {
			rteToSubqueryConverterContext->localTableList = 
				lappend(rteToSubqueryConverterContext->localTableList, rteToSubqueryConverter);
		}
	}
	return rteToSubqueryConverterContext;
}