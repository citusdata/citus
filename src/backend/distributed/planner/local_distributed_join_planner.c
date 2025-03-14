/*-------------------------------------------------------------------------
 *
 * local_distributed_join_planner.c
 *
 * This file contains functions to convert convert local-distributed
 * tables to subqueries so that they can be planned by the router planner.
 *
 *
 * The current algorithm checks if there is any table in the `jointree` that
 * should be converted, if so it creates conversion candidates.
 * With conversion candidates, it will convert either a distributed table or a local table to a
 * subquery until it is plannable by router planner. It will choose a distributed table if we
 * expect it to return few rows, such as a constant equality filter on a unique column.
 *
 * ```sql
 * -- assuming dist.a is a unique column, this will convert distributed table
 * SELECT * FROM dist join local ON(a) where dist.a = 5;
 * ```
 *
 * If the uniqueness is defined on multiple columns such as `dist.a, dist.b`
 * then distributed table will only be chosen if there is a constant equality in all of the columns such as:
 *
 * ```sql
 * SELECT * FROM dist join local ON(a) where dist.a = 5 AND dist.b =10; -- this will choose distributed table
 * SELECT * FROM dist join local ON(a) where dist.a = 5 AND dist.b >10; -- this won't since no equality on dist.b
 * SELECT * FROM dist join local ON(a) where dist.a = 5; -- this won't since no equality on dist.b
 * ```
 *
 * The algorithm will also not favor distributed tables if there exists a
 * distributed table which is expected to return many rows, because in that
 * case we will already plan local tables hence there is no point in converting some distributed tables.
 *
 * ```sql
 * -- here only the local table will be chosen
 * SELECT * FROM dist_without_unique JOIN dist_with_unique USING(a) join local USING (a);
 * ```
 *
 * this also makes the algorithm consistent.
 *
 * The algorithm can understand `OR` and `AND` expressions in the filters.
 *
 * There is a GUC called `local_table_join_policy` consisting of 4 modes:
 * `none`: don't do any conversion
 * `prefer-local`: prefer converting local tables if there is
 * `prefer-distributed`: prefer converting distributed tables if there is
 * `auto`: use the above mechanism to decide (constant equality on unique column)
 *
 * `auto` mode is the default.
 *
 * While converting to a subquery, we use a trick to avoid unnecessary network bandwidth,
 * if there are columns that are not required in a table that will be converted to a subquery, We do:
 *
 * ```sql
 * SELECT t.a, NULL, NULL (SELECT a FROM table) t
 * ```
 *
 * instead of
 *
 * ```sql
 * SELECT a, NULL, NULL FROM table
 * ```
 *
 * There are NULLs in the query because we currently don't have an easy way to update the Vars
 * that reference the non-required ones and we don't want to break the postgres query.
 *
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"

#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "optimizer/prep.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

#include "pg_version_constants.h"

#include "distributed/citus_nodes.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/distributed_planner.h"
#include "distributed/errormessage.h"
#include "distributed/listutils.h"
#include "distributed/local_distributed_join_planner.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/query_colocation_checker.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/recursive_planning.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/shard_pruning.h"
#include "distributed/version_compat.h"

#define INVALID_RTE_IDENTITY -1

/*
 * Managed via a GUC
 */
int LocalTableJoinPolicy = LOCAL_JOIN_POLICY_AUTO;

/*
 * RangeTableEntryDetails contains some information about
 * a range table entry so that we don't need to calculate
 * them over and over.
 */
typedef struct RangeTableEntryDetails
{
	RangeTblEntry *rangeTableEntry;
	List *requiredAttributeNumbers;
	bool hasConstantFilterOnUniqueColumn;
#if PG_VERSION_NUM >= PG_VERSION_16
	RTEPermissionInfo *perminfo;
#endif
} RangeTableEntryDetails;

/*
 * ConversionCandidates contains candidates that could
 * be converted to a subquery. This is used as a convenience to
 * first generate all the candidates and then choose which ones to convert.
 */
typedef struct ConversionCandidates
{
	List *distributedTableList; /* reference or distributed table */
	List *localTableList; /* local or citus local table */
}ConversionCandidates;


/*
 * IndexColumns contains the column numbers for an index.
 * For example if there is an index on (a, b) then it will contain
 * their column numbers (1,2).
 */
typedef struct IndexColumns
{
	List *indexColumnNos;
}IndexColumns;

/*
 * ConversionChoice represents which conversion group
 * to convert to a subquery. Currently we either convert all
 * local tables, or distributed tables.
 */
typedef enum ConversionChoice
{
	CONVERT_LOCAL_TABLES = 1,
	CONVERT_DISTRIBUTED_TABLES = 2
}ConversionChoice;

static bool HasConstantFilterOnUniqueColumn(RangeTblEntry *rangeTableEntry,
											RelationRestriction *relationRestriction);
static ConversionCandidates * CreateConversionCandidates(PlannerRestrictionContext *
														 plannerRestrictionContext,
														 List *rangeTableList,
														 int resultRTEIdentity,
														 List *rteperminfos);
static void AppendUniqueIndexColumnsToList(Form_pg_index indexForm, List **uniqueIndexes,
										   int flags);
static ConversionChoice GetConversionChoice(ConversionCandidates *
											conversionCandidates,
											PlannerRestrictionContext *
											plannerRestrictionContext);
static bool AllRangeTableEntriesHaveUniqueIndex(List *rangeTableEntryDetailsList);
static bool FirstIsSuperSetOfSecond(List *firstIntList, List *secondIntList);
static void ConvertRTEsToSubquery(List *rangeTableEntryDetailsList,
								  RecursivePlanningContext *context);
static int ResultRTEIdentity(Query *query);
static List * RTEListToConvert(ConversionCandidates *conversionCandidates,
							   ConversionChoice conversionChoice);


/*
 * RecursivelyPlanLocalTableJoins gets a query and the planner
 * restrictions. As long as the query is not plannable by router planner,
 * it converts either a local or distributed table to a subquery.
 */
void
RecursivelyPlanLocalTableJoins(Query *query,
							   RecursivePlanningContext *context)
{
	PlannerRestrictionContext *plannerRestrictionContext =
		GetPlannerRestrictionContext(context);

	List *rangeTableList = query->rtable;
#if PG_VERSION_NUM >= PG_VERSION_16
	List *rteperminfos = query->rteperminfos;
#endif
	int resultRTEIdentity = ResultRTEIdentity(query);
	ConversionCandidates *conversionCandidates =
		CreateConversionCandidates(plannerRestrictionContext,
#if PG_VERSION_NUM >= PG_VERSION_16
								   rangeTableList, resultRTEIdentity, rteperminfos);
#else
								   rangeTableList, resultRTEIdentity, NIL);
#endif

	ConversionChoice conversionChoise =
		GetConversionChoice(conversionCandidates, plannerRestrictionContext);


	List *rteListToConvert = RTEListToConvert(conversionCandidates, conversionChoise);
	ConvertRTEsToSubquery(rteListToConvert, context);
}


/*
 * ResultRTEIdentity returns the result RTE's identity if it exists,
 * otherwise it returns INVALID_RTE_INDENTITY
 */
static int
ResultRTEIdentity(Query *query)
{
	int resultRTEIdentity = INVALID_RTE_IDENTITY;
	if (IsModifyCommand(query))
	{
		RangeTblEntry *resultRTE = ExtractResultRelationRTEOrError(query);
		resultRTEIdentity = GetRTEIdentity(resultRTE);
	}
	return resultRTEIdentity;
}


/*
 * RTEListToConvert to converts returns a list of RTEs that should
 * be converted to a subquery.
 */
static List *
RTEListToConvert(ConversionCandidates *conversionCandidates, ConversionChoice
				 conversionChoice)
{
	List *rtesToConvert = NIL;
	if (conversionChoice == CONVERT_LOCAL_TABLES)
	{
		rtesToConvert = list_concat(rtesToConvert, conversionCandidates->localTableList);
	}
	else
	{
		rtesToConvert = list_concat(rtesToConvert,
									conversionCandidates->distributedTableList);
	}
	return rtesToConvert;
}


/*
 * GetConversionChoice returns the conversion choice considering the local table
 * join policy.
 */
static ConversionChoice
GetConversionChoice(ConversionCandidates *conversionCandidates,
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
		return localRTECandidate ? CONVERT_LOCAL_TABLES : CONVERT_DISTRIBUTED_TABLES;
	}
	else if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_PREFER_DISTRIBUTED)
	{
		return distributedRTECandidate ? CONVERT_DISTRIBUTED_TABLES :
			   CONVERT_LOCAL_TABLES;
	}
	else
	{
		/*
		 * We want to convert distributed tables only if all the distributed tables
		 * have a constant filter on a unique index, otherwise we would be redundantly
		 * converting a distributed table as we will convert all the other local tables.
		 */
		bool allRangeTableEntriesHaveUniqueIndex = AllRangeTableEntriesHaveUniqueIndex(
			conversionCandidates->distributedTableList);

		if (allRangeTableEntriesHaveUniqueIndex)
		{
			return distributedRTECandidate ? CONVERT_DISTRIBUTED_TABLES :
				   CONVERT_LOCAL_TABLES;
		}
		else
		{
			return localRTECandidate ? CONVERT_LOCAL_TABLES : CONVERT_DISTRIBUTED_TABLES;
		}
	}
}


/*
 * ConvertRTEsToSubquery converts all the given range table entries
 * to a subquery.
 */
static void
ConvertRTEsToSubquery(List *rangeTableEntryDetailsList, RecursivePlanningContext *context)
{
	RangeTableEntryDetails *rangeTableEntryDetails = NULL;
	foreach_declared_ptr(rangeTableEntryDetails, rangeTableEntryDetailsList)
	{
		RangeTblEntry *rangeTableEntry = rangeTableEntryDetails->rangeTableEntry;
		List *requiredAttributeNumbers = rangeTableEntryDetails->requiredAttributeNumbers;
		ReplaceRTERelationWithRteSubquery(rangeTableEntry,
#if PG_VERSION_NUM >= PG_VERSION_16
										  requiredAttributeNumbers, context,
										  rangeTableEntryDetails->perminfo);
#else
										  requiredAttributeNumbers, context, NULL);
#endif
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
	foreach_declared_ptr(rangeTableEntryDetails, rangeTableEntryDetailsList)
	{
		if (!rangeTableEntryDetails->hasConstantFilterOnUniqueColumn)
		{
			return false;
		}
	}
	return true;
}


/*
 * ShouldConvertLocalTableJoinsToSubqueries returns true if we should
 * convert local-dist table joins to subqueries.
 */
bool
ShouldConvertLocalTableJoinsToSubqueries(List *rangeTableList)
{
	if (LocalTableJoinPolicy == LOCAL_JOIN_POLICY_NEVER)
	{
		/* user doesn't want Citus to enable local table joins */
		return false;
	}

	if (!ContainsLocalTableDistributedTableJoin(rangeTableList))
	{
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
	if (rangeTableEntry == NULL || relationRestriction == NULL)
	{
		/*
		 * Postgres might not pass relationRestriction info with hooks if
		 * the table doesn't contribute to the result, and in that case
		 * relationRestriction will be NULL. Ideally it doesn't make sense
		 * to recursively plan such tables but for the time being we don't
		 * add any special logic for these tables as it might introduce bugs.
		 */
		return false;
	}

	bool joinOnFalse = JoinConditionIsOnFalse(relationRestriction->relOptInfo->joininfo);
	if (joinOnFalse)
	{
		/* If there is a WHERE FALSE, we consider it as a constant filter. */
		return true;
	}

	List *baseRestrictionList = relationRestriction->relOptInfo->baserestrictinfo;
	List *restrictClauseList = get_all_actual_clauses(baseRestrictionList);

	List *rteEqualityColumnsNos =
		FetchEqualityAttrNumsForRTE((Node *) restrictClauseList);

	List *uniqueIndexColumnsList = ExecuteFunctionOnEachTableIndex(rangeTableEntry->relid,
																   AppendUniqueIndexColumnsToList,
																   INCLUDE_INDEX_ALL_STATEMENTS);
	IndexColumns *indexColumns = NULL;
	foreach_declared_ptr(indexColumns, uniqueIndexColumnsList)
	{
		List *uniqueIndexColumnNos = indexColumns->indexColumnNos;
		if (FirstIsSuperSetOfSecond(rteEqualityColumnsNos,
									uniqueIndexColumnNos))
		{
			return true;
		}
	}
	return false;
}


/*
 * FirstIsSuperSetOfSecond returns true if the first int List
 * contains every element of the second int List.
 */
static bool
FirstIsSuperSetOfSecond(List *firstIntList, List *secondIntList)
{
	int curInt = 0;
	foreach_declared_int(curInt, secondIntList)
	{
		if (!list_member_int(firstIntList, curInt))
		{
			return false;
		}
	}
	return true;
}


/*
 * AppendUniqueIndexColumnsToList adds the given index's column numbers if it is a
 * unique index.
 */
static void
AppendUniqueIndexColumnsToList(Form_pg_index indexForm, List **uniqueIndexGroups,
							   int flags)
{
	if (indexForm->indisunique || indexForm->indisprimary)
	{
		IndexColumns *indexColumns = palloc0(sizeof(IndexColumns));
		List *uniqueIndexes = NIL;
		for (int i = 0; i < indexForm->indkey.dim1; i++)
		{
			uniqueIndexes = list_append_unique_int(uniqueIndexes,
												   indexForm->indkey.values[i]);
		}
		if (list_length(uniqueIndexes) == 0)
		{
			return;
		}
		indexColumns->indexColumnNos = uniqueIndexes;
		*uniqueIndexGroups = lappend(*uniqueIndexGroups, indexColumns);
	}
}


/*
 * RequiredAttrNumbersForRelation returns the required attribute numbers for
 * the input RTE relation in order for the planning to succeed.
 *
 * The function could be optimized by not adding the columns that only appear
 * WHERE clause as a filter (e.g., not a join clause).
 */
List *
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

	int rteIndex = relationRestriction->index;

	/*
	 * Here we used the query from plannerInfo because it has the optimizations
	 * so that it doesn't have unnecessary columns. The original query doesn't have
	 * some of these optimizations hence if we use it here, we don't get the
	 * 'required' attributes.
	 */
	Query *queryToProcess = plannerInfo->parse;

	return RequiredAttrNumbersForRelationInternal(queryToProcess, rteIndex);
}


/*
 * RequiredAttrNumbersForRelationInternal returns the required attribute numbers
 * for the input range-table-index in the query parameter.
 */
List *
RequiredAttrNumbersForRelationInternal(Query *queryToProcess, int rteIndex)
{
	List *allVarsInQuery = pull_vars_of_level((Node *) queryToProcess, 0);

	List *requiredAttrNumbers = NIL;

	Var *var = NULL;
	foreach_declared_ptr(var, allVarsInQuery)
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
CreateConversionCandidates(PlannerRestrictionContext *plannerRestrictionContext,
						   List *rangeTableList,
						   int resultRTEIdentity,
						   List *rteperminfos)
{
	ConversionCandidates *conversionCandidates =
		palloc0(sizeof(ConversionCandidates));


	RangeTblEntry *rangeTableEntry = NULL;
	foreach_declared_ptr(rangeTableEntry, rangeTableList)
	{
		/* we're only interested in tables */
		if (!IsRecursivelyPlannableRelation(rangeTableEntry))
		{
			continue;
		}

		int rteIdentity = GetRTEIdentity(rangeTableEntry);

		/* result relation cannot converted to a subquery */
		if (resultRTEIdentity == rteIdentity)
		{
			continue;
		}

		RelationRestriction *relationRestriction =
			RelationRestrictionForRelation(rangeTableEntry, plannerRestrictionContext);

		RangeTableEntryDetails *rangeTableEntryDetails =
			palloc0(sizeof(RangeTableEntryDetails));

		rangeTableEntryDetails->rangeTableEntry = rangeTableEntry;
		rangeTableEntryDetails->requiredAttributeNumbers =
			RequiredAttrNumbersForRelation(rangeTableEntry, plannerRestrictionContext);
		rangeTableEntryDetails->hasConstantFilterOnUniqueColumn =
			HasConstantFilterOnUniqueColumn(rangeTableEntry, relationRestriction);
#if PG_VERSION_NUM >= PG_VERSION_16
		rangeTableEntryDetails->perminfo = NULL;
		if (rangeTableEntry->perminfoindex)
		{
			rangeTableEntryDetails->perminfo = getRTEPermissionInfo(rteperminfos,
																	rangeTableEntry);
		}
#endif

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
