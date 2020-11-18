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

static bool ShouldConvertLocalTableJoinsToSubqueries(List* rangeTableList);
static bool HasUniqueFilter(RangeTblEntry* distRTE, List* distRTERestrictionList, List* requiredAttrNumbersForDistRTE);
static void AutoConvertLocalTableJoinToSubquery(RangeTblEntry* localRTE, RangeTblEntry* distRTE,
		List* localRTERestrictionList, List* distRTERestrictionList,
		List *requiredAttrNumbersForLocalRTE, List *requiredAttrNumbersForDistRTE);
static List * RequiredAttrNumbersForRelation(RangeTblEntry *relationRte,
											 RecursivePlanningContext *planningContext);
static RangeTblEntry * FindNextRTECandidate(PlannerRestrictionContext *
									   plannerRestrictionContext,
									   List *rangeTableList, List **restrictionList,
									   bool localTable);
static bool AllDataLocallyAccessible(List *rangeTableList);
static void GetAllUniqueIndexes(Form_pg_index indexForm, List** uniqueIndexes);

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
	if(!ShouldConvertLocalTableJoinsToSubqueries(rangeTableList)) {
		return;
	}

	RangeTblEntry *resultRelation = ExtractResultRelationRTE(query);

	while (ContainsLocalTableDistributedTableJoin(rangeTableList))
	{
		List *localTableRestrictList = NIL;
		List *distributedTableRestrictList = NIL;

		bool localTable = true;

		PlannerRestrictionContext *plannerRestrictionContext =
			context->plannerRestrictionContext;
		RangeTblEntry *localRTECandidate =
			FindNextRTECandidate(plannerRestrictionContext, rangeTableList,
							&localTableRestrictList, localTable);
		RangeTblEntry *distributedRTECandidate =
			FindNextRTECandidate(plannerRestrictionContext, rangeTableList,
							&distributedTableRestrictList, !localTable);

		List *requiredAttrNumbersForLocalRte =
			RequiredAttrNumbersForRelation(localRTECandidate, context);
		List *requiredAttrNumbersForDistributedRte =
			RequiredAttrNumbersForRelation(distributedRTECandidate, context);

		if (resultRelation) {

			if (resultRelation->relid == localRTECandidate->relid) {
				ReplaceRTERelationWithRteSubquery(distributedRTECandidate,
										distributedTableRestrictList,
										requiredAttrNumbersForDistributedRte);
				continue;						
			}else if (resultRelation->relid == distributedRTECandidate->relid) {
				ReplaceRTERelationWithRteSubquery(localRTECandidate,
										localTableRestrictList,
										requiredAttrNumbersForLocalRte);
				continue;						
			}
		}

		if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_PREFER_LOCAL)
		{
			ReplaceRTERelationWithRteSubquery(localRTECandidate,
											  localTableRestrictList,
											  requiredAttrNumbersForLocalRte);
		}
		else if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_PREFER_DISTRIBUTED)
		{
			ReplaceRTERelationWithRteSubquery(distributedRTECandidate,
											  distributedTableRestrictList,
											  requiredAttrNumbersForDistributedRte);
		}
		else if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_AUTO)
		{
			AutoConvertLocalTableJoinToSubquery(localRTECandidate, distributedRTECandidate,
				localTableRestrictList, distributedTableRestrictList, 
				requiredAttrNumbersForLocalRte, requiredAttrNumbersForDistributedRte);
		}
		else
		{
			elog(ERROR, "unexpected local table join policy: %d", LocalTableJoinPolicy);
		}
	}
}

/*
 * ShouldConvertLocalTableJoinsToSubqueries returns true if we should
 * convert local-dist table joins to subqueries.
 */
static bool ShouldConvertLocalTableJoinsToSubqueries(List* rangeTableList) {
	if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_NEVER)
	{
		/* user doesn't want Citus to enable local table joins */
		return false;
	}

	if (!ContainsLocalTableDistributedTableJoin(rangeTableList))
	{
		/* nothing to do as there are no relevant joins */
		return false;
	}

	if (AllDataLocallyAccessible(rangeTableList))
	{
		/* recursively planning is overkill, router planner can already handle this */
		return false;
	}
	return true;
}

static void AutoConvertLocalTableJoinToSubquery(RangeTblEntry* localRTE, RangeTblEntry* distRTE,
		List* localRTERestrictionList, List* distRTERestrictionList,
		List *requiredAttrNumbersForLocalRTE, List *requiredAttrNumbersForDistRTE) {

	bool hasUniqueFilter = HasUniqueFilter(distRTE, distRTERestrictionList, requiredAttrNumbersForDistRTE);
	if (hasUniqueFilter) {
		ReplaceRTERelationWithRteSubquery(distRTE,
									distRTERestrictionList,
									requiredAttrNumbersForDistRTE);
	}else {
		ReplaceRTERelationWithRteSubquery(localRTE,
											localRTERestrictionList,
											requiredAttrNumbersForLocalRTE);
	}
	
}

// TODO:: This function should only consider equality,
// currently it will return true for dist.a > 5. We should check this from join->quals.
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
 * FindNextRTECandidate returns a range table entry which has the most filters
 * on it along with the restrictions (e.g., fills **restrictionList).
 *
 * The function also gets a boolean localTable parameter, so the caller
 * can choose to run the function for only local tables or distributed tables.
 */
static RangeTblEntry *
FindNextRTECandidate(PlannerRestrictionContext *plannerRestrictionContext,
				List *rangeTableList, List **restrictionList,
				bool localTable)
{
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		/* we're only interested in tables */
		if (!(rangeTableEntry->rtekind == RTE_RELATION &&
			  rangeTableEntry->relkind == RELKIND_RELATION))
		{
			continue;
		}

		if (localTable && IsCitusTable(rangeTableEntry->relid))
		{
			continue;
		}

		if (!localTable && !IsCitusTable(rangeTableEntry->relid))
		{
			continue;
		}

		List *currentRestrictionList =
			GetRestrictInfoListForRelation(rangeTableEntry,
										   plannerRestrictionContext, 1);

		*restrictionList = currentRestrictionList;
		return rangeTableEntry;
	}
	// TODO:: Put Illegal state error code
	ereport(ERROR, (errmsg("unexpected state: could not find any RTE to convert to subquery in range table list")));
	return NULL;
}

/*
 * AllDataLocallyAccessible return true if all data for the relations in the
 * rangeTableList is locally accessible.
 */
static bool
AllDataLocallyAccessible(List *rangeTableList)
{
	ListCell *rangeTableCell = NULL;
	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		/* we're only interested in tables */
		if (!(rangeTableEntry->rtekind == RTE_RELATION &&
			  rangeTableEntry->relkind == RELKIND_RELATION))
		{
			continue;
		}


		Oid relationId = rangeTableEntry->relid;

		if (!IsCitusTable(relationId))
		{
			/* local tables are locally accessible */
			continue;
		}

		List *shardIntervalList = LoadShardIntervalList(relationId);
		if (list_length(shardIntervalList) > 1)
		{
			/* we currently only consider single placement tables */
			return false;
		}

		ShardInterval *shardInterval = linitial(shardIntervalList);
		uint64 shardId = shardInterval->shardId;
		ShardPlacement *localShardPlacement =
			ShardPlacementOnGroup(shardId, GetLocalGroupId());
		if (localShardPlacement == NULL)
		{
			/* the table doesn't have a placement on this node */
			return false;
		}
	}

	return true;
}