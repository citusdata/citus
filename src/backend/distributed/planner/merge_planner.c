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
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "utils/lsyscache.h"

#include "distributed/citus_clauses.h"
#include "distributed/listutils.h"
#include "distributed/merge_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_version_constants.h"
#include "distributed/query_pushdown_planning.h"

#if PG_VERSION_NUM >= PG_VERSION_15

static DeferredErrorMessage * CheckIfRTETypeIsUnsupported(Query *parse,
														  RangeTblEntry *rangeTableEntry);
static DeferredErrorMessage * ErrorIfDistTablesNotColocated(Query *parse,
															List *
															distTablesList,
															PlannerRestrictionContext
															*
															plannerRestrictionContext);
static DeferredErrorMessage * ErrorIfMergeHasUnsupportedTables(Query *parse,
															   List *rangeTableList,
															   PlannerRestrictionContext *
															   restrictionContext);
static bool IsDistributionColumnInMergeSource(Expr *columnExpression, Query *query, bool
											  skipOuterVars);
static DeferredErrorMessage * InsertDistributionColumnMatchesSource(Query *query,
																	RangeTblEntry *
																	resultRte);

static DeferredErrorMessage * MergeQualAndTargetListFunctionsSupported(Oid
																	   resultRelationId,
																	   FromExpr *joinTree,
																	   Node *quals,
																	   List *targetList,
																	   CmdType commandType);
#endif


/*
 * MergeQuerySupported does check for a MERGE command in the query, if it finds
 * one, it will verify the below criteria
 * - Supported tables and combinations in ErrorIfMergeHasUnsupportedTables
 * - Distributed tables requirements in ErrorIfDistTablesNotColocated
 * - Checks target-lists and functions-in-quals in TargetlistAndFunctionsSupported
 */
DeferredErrorMessage *
MergeQuerySupported(Query *originalQuery, bool multiShardQuery,
					PlannerRestrictionContext *plannerRestrictionContext)
{
	/* function is void for pre-15 versions of Postgres */
	#if PG_VERSION_NUM < PG_VERSION_15

	return NULL;

	#else

	/* For non-MERGE commands it's a no-op */
	if (!IsMergeQuery(originalQuery))
	{
		return NULL;
	}

	/*
	 * TODO: For now, we are adding an exception where any volatile or stable
	 * functions are not allowed in the MERGE query, but this will become too
	 * restrictive as this will prevent many useful and simple cases, such as,
	 * INSERT VALUES(ts::timestamp), bigserial column inserts etc. But without
	 * this restriction, we have a potential danger of some of the function(s)
	 * getting executed at the worker which will result in incorrect behavior.
	 */
	if (contain_mutable_functions((Node *) originalQuery))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "non-IMMUTABLE functions are not yet supported "
							 "in MERGE sql with distributed tables ",
							 NULL, NULL);
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
		/* MERGE's unsupported combination, raise the exception */
		RaiseDeferredError(deferredError, ERROR);
	}

	Oid resultRelationId = resultRte->relid;
	deferredError = MergeQualAndTargetListFunctionsSupported(resultRelationId,
															 originalQuery->jointree,
															 originalQuery->jointree->
															 quals,
															 originalQuery->targetList,
															 originalQuery->commandType);
	if (deferredError)
	{
		return deferredError;
	}

	/*
	 * MERGE is a special case where we have multiple modify statements
	 * within itself. Check each INSERT/UPDATE/DELETE individually.
	 */
	MergeAction *action = NULL;
	foreach_ptr(action, originalQuery->mergeActionList)
	{
		Assert(originalQuery->returningList == NULL);
		deferredError = MergeQualAndTargetListFunctionsSupported(resultRelationId,
																 originalQuery->jointree,
																 action->qual,
																 action->targetList,
																 action->commandType);
		if (deferredError)
		{
			/* MERGE's unsupported scenario, raise the exception */
			RaiseDeferredError(deferredError, ERROR);
		}
	}

	deferredError =
		InsertDistributionColumnMatchesSource(originalQuery, resultRte);
	if (deferredError)
	{
		/* MERGE's unsupported scenario, raise the exception */
		RaiseDeferredError(deferredError, ERROR);
	}

	if (multiShardQuery)
	{
		deferredError =
			DeferErrorIfUnsupportedSubqueryPushdown(originalQuery,
													plannerRestrictionContext);
		if (deferredError)
		{
			return deferredError;
		}
	}

	if (HasDangerousJoinUsing(originalQuery->rtable, (Node *) originalQuery->jointree))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "a join with USING causes an internal naming "
							 "conflict, use ON instead", NULL, NULL);
	}

	return NULL;

	#endif
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

	/* Fetch the MERGE target relation */
	RangeTblEntry *targetRte = rt_fetch(parse->resultRelation, parse->rtable);

	/* Is it a target relation? */
	if (targetRte->relid == rte->relid)
	{
		return false;
	}

	return true;
}


#if PG_VERSION_NUM >= PG_VERSION_15

/*
 * ErrorIfDistTablesNotColocated Checks to see if
 *
 *   - There are a minimum of two distributed tables (source and a target).
 *   - All the distributed tables are indeed colocated.
 *
 * If any of the conditions are not met, it raises an exception.
 */
static DeferredErrorMessage *
ErrorIfDistTablesNotColocated(Query *parse, List *distTablesList,
							  PlannerRestrictionContext *
							  plannerRestrictionContext)
{
	/* All MERGE tables must be distributed */
	if (list_length(distTablesList) < 2)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "For MERGE command, both the source and target "
							 "must be distributed", NULL, NULL);
	}

	/* All distributed tables must be colocated */
	if (!AllRelationsInRTEListColocated(distTablesList))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "For MERGE command, all the distributed tables "
							 "must be colocated", NULL, NULL);
	}

	return NULL;
}


/*
 * ErrorIfRTETypeIsUnsupported Checks for types of tables that are not supported, such
 * as, reference tables, append-distributed tables and materialized view as target relation.
 * Routine returns NULL for the supported types, error message for everything else.
 */
static DeferredErrorMessage *
CheckIfRTETypeIsUnsupported(Query *parse, RangeTblEntry *rangeTableEntry)
{
	if (rangeTableEntry->relkind == RELKIND_MATVIEW ||
		rangeTableEntry->relkind == RELKIND_FOREIGN_TABLE)
	{
		/* Materialized view or Foreign table as target is not allowed */
		if (IsMergeAllowedOnRelation(parse, rangeTableEntry))
		{
			/* Non target relation is ok */
			return NULL;
		}
		else
		{
			/* Usually we don't reach this exception as the Postgres parser catches it */
			StringInfo errorMessage = makeStringInfo();
			appendStringInfo(errorMessage, "MERGE command is not allowed on "
										   "relation type(relkind:%c)",
							 rangeTableEntry->relkind);
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 errorMessage->data, NULL, NULL);
		}
	}

	if (rangeTableEntry->relkind != RELKIND_RELATION &&
		rangeTableEntry->relkind != RELKIND_PARTITIONED_TABLE)
	{
		StringInfo errorMessage = makeStringInfo();
		appendStringInfo(errorMessage, "Unexpected table type(relkind:%c) "
									   "in MERGE command", rangeTableEntry->relkind);
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 errorMessage->data, NULL, NULL);
	}

	Assert(rangeTableEntry->relid != 0);

	/* Reference tables are not supported yet */
	if (IsCitusTableType(rangeTableEntry->relid, REFERENCE_TABLE))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "MERGE command is not supported on reference "
							 "tables yet", NULL, NULL);
	}

	/* Append/Range tables are not supported */
	if (IsCitusTableType(rangeTableEntry->relid, APPEND_DISTRIBUTED) ||
		IsCitusTableType(rangeTableEntry->relid, RANGE_DISTRIBUTED))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "For MERGE command, all the distributed tables "
							 "must be colocated, for append/range distribution, "
							 "colocation is not supported", NULL,
							 "Consider using hash distribution instead");
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

		DeferredErrorMessage *errorMessage =
			CheckIfRTETypeIsUnsupported(parse, rangeTableEntry);
		if (errorMessage)
		{
			return errorMessage;
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
	return ErrorIfDistTablesNotColocated(parse,
										 distTablesList,
										 restrictionContext);
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
IsDistributionColumnInMergeSource(Expr *columnExpression, Query *query, bool
								  skipOuterVars)
{
	bool isDistributionColumn = false;
	Var *column = NULL;
	RangeTblEntry *relationRTE = NULL;

	/* ParentQueryList is same as the original query for MERGE */
	FindReferencedTableColumn(columnExpression, list_make1(query), query, &column,
							  &relationRTE,
							  skipOuterVars);
	Oid relationId = relationRTE ? relationRTE->relid : InvalidOid;
	if (relationId != InvalidOid && column != NULL)
	{
		Var *distributionColumn = DistPartitionKey(relationId);

		/* not all distributed tables have partition column */
		if (distributionColumn != NULL && column->varattno ==
			distributionColumn->varattno)
		{
			isDistributionColumn = true;
		}
	}

	return isDistributionColumn;
}


/*
 * InsertDistributionColumnMatchesSource check to see if MERGE is inserting a
 * value into the target which is not from the source table, if so, it
 * raises an exception.
 * Note: Inserting random values other than the joined column values will
 * result in unexpected behaviour of rows ending up in incorrect shards, to
 * prevent such mishaps, we disallow such inserts here.
 */
static DeferredErrorMessage *
InsertDistributionColumnMatchesSource(Query *query, RangeTblEntry *resultRte)
{
	Assert(IsMergeQuery(query));

	if (!IsCitusTableType(resultRte->relid, DISTRIBUTED_TABLE))
	{
		return NULL;
	}

	bool foundDistributionColumn = false;
	MergeAction *action = NULL;
	foreach_ptr(action, query->mergeActionList)
	{
		/* Skip MATCHED clause as INSERTS are not allowed in it*/
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
			AttrNumber originalAttrNo = targetEntry->resno;

			/* skip processing of target table non-partition columns */
			if (originalAttrNo != targetKey->varattno)
			{
				continue;
			}

			foundDistributionColumn = true;

			if (IsA(targetEntry->expr, Var))
			{
				if (IsDistributionColumnInMergeSource(targetEntry->expr, query, true))
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


/*
 * MergeQualAndTargetListFunctionsSupported Checks WHEN/ON clause actions to see what functions
 * are allowed, if we are updating distribution column, etc.
 */
static DeferredErrorMessage *
MergeQualAndTargetListFunctionsSupported(Oid resultRelationId, FromExpr *joinTree,
										 Node *quals,
										 List *targetList, CmdType commandType)
{
	uint32 rangeTableId = 1;
	Var *distributionColumn = NULL;
	if (IsCitusTable(resultRelationId) && HasDistributionKey(resultRelationId))
	{
		distributionColumn = PartitionColumn(resultRelationId, rangeTableId);
	}

	ListCell *targetEntryCell = NULL;
	bool hasVarArgument = false; /* A STABLE function is passed a Var argument */
	bool hasBadCoalesce = false; /* CASE/COALESCE passed a mutable function */
	foreach(targetEntryCell, targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

		/* skip resjunk entries: UPDATE adds some for ctid, etc. */
		if (targetEntry->resjunk)
		{
			continue;
		}

		bool targetEntryDistributionColumn = false;
		AttrNumber targetColumnAttrNumber = InvalidAttrNumber;

		if (distributionColumn)
		{
			if (commandType == CMD_UPDATE)
			{
				/*
				 * Note that it is not possible to give an alias to
				 * UPDATE table SET ...
				 */
				if (targetEntry->resname)
				{
					targetColumnAttrNumber = get_attnum(resultRelationId,
														targetEntry->resname);
					if (targetColumnAttrNumber == distributionColumn->varattno)
					{
						targetEntryDistributionColumn = true;
					}
				}
			}
		}

		if (targetEntryDistributionColumn &&
			TargetEntryChangesValue(targetEntry, distributionColumn, joinTree))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "updating the distribution column is not "
								 "allowed in MERGE actions",
								 NULL, NULL);
		}

		if (FindNodeMatchingCheckFunction((Node *) targetEntry->expr,
										  CitusIsVolatileFunction))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "functions used in MERGE actions on distributed "
								 "tables must not be VOLATILE",
								 NULL, NULL);
		}

		if (MasterIrreducibleExpression((Node *) targetEntry->expr,
										&hasVarArgument, &hasBadCoalesce))
		{
			Assert(hasVarArgument || hasBadCoalesce);
		}

		if (FindNodeMatchingCheckFunction((Node *) targetEntry->expr,
										  NodeIsFieldStore))
		{
			/* DELETE cannot do field indirection already */
			Assert(commandType == CMD_UPDATE || commandType == CMD_INSERT);
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "inserting or modifying composite type fields is not "
								 "supported", NULL,
								 "Use the column name to insert or update the composite "
								 "type as a single value");
		}
	}


	/*
	 * Check the condition, convert list of expressions into expression tree for further processing
	 */
	if (quals)
	{
		if (IsA(quals, List))
		{
			quals = (Node *) make_ands_explicit((List *) quals);
		}

		if (FindNodeMatchingCheckFunction((Node *) quals, CitusIsVolatileFunction))
		{
			StringInfo errorMessage = makeStringInfo();
			appendStringInfo(errorMessage, "functions used in the %s clause of MERGE "
										   "queries on distributed tables must not be VOLATILE",
							 (commandType == CMD_MERGE) ? "ON" : "WHEN");
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 errorMessage->data, NULL, NULL);
		}
		else if (MasterIrreducibleExpression(quals, &hasVarArgument, &hasBadCoalesce))
		{
			Assert(hasVarArgument || hasBadCoalesce);
		}
	}

	if (hasVarArgument)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "STABLE functions used in MERGE queries "
							 "cannot be called with column references",
							 NULL, NULL);
	}

	if (hasBadCoalesce)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "non-IMMUTABLE functions are not allowed in CASE or "
							 "COALESCE statements",
							 NULL, NULL);
	}

	if (quals != NULL && nodeTag(quals) == T_CurrentOfExpr)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "cannot run MERGE actions with cursors",
							 NULL, NULL);
	}

	return NULL;
}


#endif
