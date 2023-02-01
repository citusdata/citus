/*-------------------------------------------------------------------------
 *
 * merge_planner.c
 *
 * This file contains functions to help plan MERGE queries.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include <stddef.h>

#include "postgres.h"
#include "parser/parsetree.h"

#include "distributed/pg_version_constants.h"
#include "distributed/merge_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_router_planner.h"
#include "distributed/listutils.h"


static bool QueryHasMergeCommand(Query *queryTree);
static DeferredErrorMessage * ErrorIfMergeHasUnsupportedTables(Query *parse,
															   List *rangeTableList,
															   PlannerRestrictionContext *
															   restrictionContext);
static DeferredErrorMessage * ErrorIfDistTablesNotColocated(Query *parse,
															List *distTablesList,
															PlannerRestrictionContext *
															plannerRestrictionContext);
static bool IsPartitionColumnInMergeSource(Expr *columnExpression, Query *query, bool
											skipOuterVars);
#if PG_VERSION_NUM >= PG_VERSION_15
static DeferredErrorMessage * InsertPartitionColumnMatchesSource(Query *query, RangeTblEntry *resultRte);
#endif


/*
 * MergeQuerySupported does check for a MERGE command in the query, if it finds
 * one, it will verify the below criteria
 * - Supported tables and combinations in ErrorIfMergeHasUnsupportedTables
 * - Distributed tables requirements in ErrorIfDistTablesNotColocated
 * - Checks target-lists and functions-in-quals in TargetlistAndFunctionsSupported
 */
DeferredErrorMessage *
MergeQuerySupported(Query *originalQuery,
					PlannerRestrictionContext *plannerRestrictionContext)
{
	/* For non-MERGE commands it's a no-op */
	if (!QueryHasMergeCommand(originalQuery))
	{
		return NULL;
	}

	List *rangeTableList = ExtractRangeTableEntryList(originalQuery);
	RangeTblEntry *resultRte = ExtractResultRelationRTE(originalQuery);

	/*
	 * Fast path queries cannot have merge command, and we prevent the remaining here.
	 * In Citus we have limited support for MERGE, it's allowed only if all
	 * the tables(target, source or any CTE) tables are are local i.e. a
	 * combination of Citus local and Non-Citus tables (regular Postgres tables)
	 * or distributed tables with some restrictions, please see header of routine
	 * ErrorIfDistTablesNotColocated for details.
	 */
	DeferredErrorMessage *deferredError =
		ErrorIfMergeHasUnsupportedTables(originalQuery,
										 rangeTableList,
										 plannerRestrictionContext);
	if (deferredError)
	{
		return deferredError;
	}

	Oid resultRelationId = resultRte->relid;
	deferredError =
		TargetlistAndFunctionsSupported(resultRelationId,
										originalQuery->jointree,
										originalQuery->jointree->quals,
										originalQuery->targetList,
										originalQuery->commandType,
										originalQuery->returningList);
	if (deferredError)
	{
		return deferredError;
	}

	#if PG_VERSION_NUM >= PG_VERSION_15

	/*
	 * MERGE is a special case where we have multiple modify statements
	 * within itself. Check each INSERT/UPDATE/DELETE individually.
	 */
	MergeAction *action = NULL;
	foreach_ptr(action, originalQuery->mergeActionList)
	{
		Assert(originalQuery->returningList == NULL);
		deferredError =
			TargetlistAndFunctionsSupported(resultRelationId,
											originalQuery->jointree,
											action->qual,
											action->targetList,
											action->commandType,
											originalQuery->returningList);
		if (deferredError)
		{
			return deferredError;
		}
	}

	deferredError =
		InsertPartitionColumnMatchesSource(originalQuery, resultRte);
	if (deferredError)
	{
		return deferredError;
	}

	#endif

	return NULL;
}

/*
 * IsMergeAllowedOnRelation takes a relation entry and checks if MERGE command is
 * permitted on special relations, such as materialized view, returns true only if
 * it's a "source" relation.
 */
bool
IsMergeAllowedOnRelation(Query *parse, RangeTblEntry *rte)
{
	if (!IsMergeQuery(parse))
	{
		return false;
	}

	RangeTblEntry *targetRte = rt_fetch(parse->resultRelation, parse->rtable);

	/* Is it a target relation? */
	if (targetRte->relid == rte->relid)
	{
		return false;
	}

	return true;
}


/*
 * ErrorIfDistTablesNotColocated Checks to see if
 *
 *   - There are a minimum of two distributed tables (source and a target).
 *   - All the distributed tables are indeed colocated.
 *   - MERGE relations are joined on the distribution column
 *          MERGE .. USING .. ON target.dist_key = source.dist_key
 *
 * If any of the conditions are not met, it raises an exception.
 */
static DeferredErrorMessage *
ErrorIfDistTablesNotColocated(Query *parse, List *distTablesList,
							  PlannerRestrictionContext *plannerRestrictionContext)
{
	/* All MERGE tables must be distributed */
	if (list_length(distTablesList) < 2)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "For MERGE command, both the source and target "
							 "must be distributed", NULL, NULL);
	}

	/* All distributed tables must be colocated */
	if (!AllRelationsInListColocated(distTablesList, RANGETABLE_ENTRY))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "For MERGE command, all the distributed tables "
							 "must be colocated", NULL, NULL);
	}

	/* Are source and target tables joined on distribution column? */
	if (!RestrictionEquivalenceForPartitionKeys(plannerRestrictionContext))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "MERGE command is only supported when distributed "
							 "tables are joined on their distribution column",
							 NULL, NULL);
	}

	return NULL;
}


/*
 * ErrorIfMergeHasUnsupportedTables checks if all the tables(target, source or any CTE
 * present) in the MERGE command are local i.e. a combination of Citus local and Non-Citus
 * tables (regular Postgres tables), or distributed tables with some restrictions, please
 * see header of routine ErrorIfDistTablesNotColocated for details, raises an exception
 * for all other combinations.
 */
static DeferredErrorMessage *
ErrorIfMergeHasUnsupportedTables(Query *parse, List *rangeTableList,
								 PlannerRestrictionContext *restrictionContext)
{
	List *distTablesList = NIL;
	bool foundLocalTables = false;

	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, rangeTableList)
	{
		Oid relationId = rangeTableEntry->relid;

		switch (rangeTableEntry->rtekind)
		{
			case RTE_RELATION:
			{
				/* Check the relation type */
				break;
			}

			case RTE_SUBQUERY:
			case RTE_FUNCTION:
			case RTE_TABLEFUNC:
			case RTE_VALUES:
			case RTE_JOIN:
			case RTE_CTE:
			{
				/* Skip them as base table(s) will be checked */
				continue;
			}

			/*
			 * RTE_NAMEDTUPLESTORE is typically used in ephmeral named relations,
			 * such as, trigger data; until we find a genuine use case, raise an
			 * exception.
			 * RTE_RESULT is a node added by the planner and we shouldn't
			 * encounter it in the parse tree.
			 */
			case RTE_NAMEDTUPLESTORE:
			case RTE_RESULT:
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "MERGE command is not supported with "
									 "Tuplestores and results",
									 NULL, NULL);
			}

			default:
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "MERGE command: Unrecognized range table entry.",
									 NULL, NULL);
			}
		}

		/* RTE Relation can be of various types, check them now */

		/* skip the regular views as they are replaced with subqueries */
		if (rangeTableEntry->relkind == RELKIND_VIEW)
		{
			continue;
		}

		if (rangeTableEntry->relkind == RELKIND_MATVIEW ||
			rangeTableEntry->relkind == RELKIND_FOREIGN_TABLE)
		{
			/* Materialized view or Foreign table as target is not allowed */
			if (IsMergeAllowedOnRelation(parse, rangeTableEntry))
			{
				/* Non target relation is ok */
				continue;
			}
			else
			{
				/* Usually we don't reach this exception as the Postgres parser catches it */
				StringInfo errorMessage = makeStringInfo();
				appendStringInfo(errorMessage,
								 "MERGE command is not allowed on "
								 "relation type(relkind:%c)", rangeTableEntry->relkind);
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED, errorMessage->data,
									 NULL, NULL);
			}
		}

		if (rangeTableEntry->relkind != RELKIND_RELATION &&
			rangeTableEntry->relkind != RELKIND_PARTITIONED_TABLE)
		{
			StringInfo errorMessage = makeStringInfo();
			appendStringInfo(errorMessage, "Unexpected table type(relkind:%c) "
										   "in MERGE command", rangeTableEntry->relkind);
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED, errorMessage->data,
								 NULL, NULL);
		}

		Assert(rangeTableEntry->relid != 0);

		/* Reference tables are not supported yet */
		if (IsCitusTableType(relationId, REFERENCE_TABLE))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "MERGE command is not supported on reference "
								 "tables yet", NULL, NULL);
		}

		/* Append/Range tables are not supported */
		if (IsCitusTableType(relationId, APPEND_DISTRIBUTED) ||
			IsCitusTableType(relationId, RANGE_DISTRIBUTED))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "For MERGE command, all the distributed tables "
								 "must be colocated, for append/range distribution, "
								 "colocation is not supported", NULL,
								 "Consider using hash distribution instead");
		}

		/*
		 * For now, save all distributed tables, later (below) we will
		 * check for supported combination(s).
		 */
		if (IsCitusTableType(relationId, DISTRIBUTED_TABLE))
		{
			distTablesList = lappend(distTablesList, rangeTableEntry);
			continue;
		}

		/* Regular Postgres tables and Citus local tables are allowed */
		if (!IsCitusTable(relationId) ||
			IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
		{
			foundLocalTables = true;
			continue;
		}

		/* Any other Citus table type missing ? */
	}

	/* Ensure all tables are indeed local */
	if (foundLocalTables && list_length(distTablesList) == 0)
	{
		/* All the tables are local, supported */
		return NULL;
	}
	else if (foundLocalTables && list_length(distTablesList) > 0)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "MERGE command is not supported with "
							 "combination of distributed/local tables yet",
							 NULL, NULL);
	}

	/* Ensure all distributed tables are indeed co-located */
	return ErrorIfDistTablesNotColocated(parse, distTablesList, restrictionContext);
}


/*
 * QueryHasMergeCommand walks over the query tree and returns false if there
 * is no Merge command (e.g., CMD_MERGE), true otherwise.
 */
static bool
QueryHasMergeCommand(Query *queryTree)
{
	/* function is void for pre-15 versions of Postgres */
	#if PG_VERSION_NUM < PG_VERSION_15
	return false;
	#else

	/*
	 * Postgres currently doesn't support Merge queries inside subqueries and
	 * ctes, but lets be defensive and do query tree walk anyway.
	 *
	 * We do not call this path for fast-path queries to avoid this additional
	 * overhead.
	 */
	if (!ContainsMergeCommandWalker((Node *) queryTree))
	{
		/* No MERGE found */
		return false;
	}

	return true;
	#endif
}


/*
 * IsPartitionColumnInMerge returns true if the given column is a partition column.
 * The function uses FindReferencedTableColumn to find the original relation
 * id and column that the column expression refers to. It then checks whether
 * that column is a partition column of the relation.
 *
 * Also, the function returns always false for reference tables given that
 * reference tables do not have partition column.
 *
 * If skipOuterVars is true, then it doesn't process the outervars.
 */
bool
IsPartitionColumnInMergeSource(Expr *columnExpression, Query *query, bool skipOuterVars)
{
	bool isPartitionColumn = false;
	Var *column = NULL;
	RangeTblEntry *relationRTE = NULL;

	/* ParentQueryList is same as the original query for MERGE */
	FindReferencedTableColumn(columnExpression, list_make1(query), query, &column,
							  &relationRTE,
							  skipOuterVars);
	Oid relationId = relationRTE ? relationRTE->relid : InvalidOid;
	if (relationId != InvalidOid && column != NULL)
	{
		Var *partitionColumn = DistPartitionKey(relationId);

		/* not all distributed tables have partition column */
		if (partitionColumn != NULL && column->varattno == partitionColumn->varattno)
		{
			isPartitionColumn = true;
		}
	}

	return isPartitionColumn;
}


#if PG_VERSION_NUM >= PG_VERSION_15

/*
 * InsertPartitionColumnMatchesSource check to see if MERGE is inserting a
 * value into the target which is not from the source table, if so, it
 * raises an exception.
 * Note: Inserting random values other than the joined column values will
 * result in unexpected behaviour of rows ending up in incorrect shards.
 */
static DeferredErrorMessage *
InsertPartitionColumnMatchesSource(Query *query, RangeTblEntry *resultRte)
{
	if (!IsCitusTableType(resultRte->relid, DISTRIBUTED_TABLE))
	{
		return NULL;
	}

	bool foundDistributionColumn = false;
	MergeAction *action = NULL;
	foreach_ptr(action, query->mergeActionList)
	{
		/* Skip MATCHED clauses */
		if (action->matched)
		{
			continue;
		}

		/* NOT MATCHED can have either INSERT or DO NOTHING */
		if (action->commandType == CMD_NOTHING)
		{
			return NULL;
		}

		if (action->targetList == NIL)
		{
			/* INSERT DEFAULT VALUES is not allowed */
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform MERGE INSERT with DEFAULTS",
								 NULL, NULL);
		}

		Assert(action->commandType == CMD_INSERT);
		Var *targetKey = PartitionColumn(resultRte->relid, 1);

		TargetEntry *targetEntry = NULL;
		foreach_ptr(targetEntry, action->targetList)
		{
			if (targetEntry->resjunk)
			{
				continue;
			}

			AttrNumber originalAttrNo = targetEntry->resno;

			/* skip processing of target table non-partition columns */
			if (originalAttrNo != targetKey->varattno)
			{
				continue;
			}

			foundDistributionColumn = true;

			if (targetEntry->expr->type == T_Var)
			{
				if (IsPartitionColumnInMergeSource(targetEntry->expr, query, true))
				{
					return NULL;
				}
				else
				{
					return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 "MERGE INSERT must use the source table "
										 "distribution column value",
										 NULL, NULL);
				}
			}
			else
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "MERGE INSERT must refer a source column "
									 "for distribution column ",
									 NULL, NULL);
			}
		}

		if (!foundDistributionColumn)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "MERGE INSERT must have distribution column as value",
								 NULL, NULL);
		}
	}

	return NULL;
}

#endif
