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
#include "distributed/pg_dist_node_metadata.h"
#include "distributed/pg_version_constants.h"
#include "distributed/query_pushdown_planning.h"

#if PG_VERSION_NUM >= PG_VERSION_15

static DeferredErrorMessage * ErrorIfDistTablesNotColocated(Query *parse,
															List *
															distTablesList,
															PlannerRestrictionContext
															*
															plannerRestrictionContext);
static DeferredErrorMessage * ErrorIfMergeHasUnsupportedTables(Oid targetRelationId,
															   Query *parse,
															   List *rangeTableList,
															   PlannerRestrictionContext *
															   restrictionContext);
static bool IsDistributionColumnInMergeSource(Expr *columnExpression, Query *query, bool
											  skipOuterVars);
static DeferredErrorMessage * InsertDistributionColumnMatchesSource(Oid targetRelationId,
																	Query *query);
static DeferredErrorMessage * MergeQualAndTargetListFunctionsSupported(Oid
																	   resultRelationId,
																	   FromExpr *joinTree,
																	   Node *quals,
																	   List *targetList,
																	   CmdType commandType);


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
	if (!AllDistributedRelationsInRTEListColocated(distTablesList))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "For MERGE command, all the distributed tables "
							 "must be colocated", NULL, NULL);
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
ErrorIfMergeHasUnsupportedTables(Oid targetRelationId, Query *parse, List *rangeTableList,
								 PlannerRestrictionContext *restrictionContext)
{
	List *distTablesList = NIL;
	bool foundLocalTables = false;
	bool foundReferenceTables = false;

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
		switch (rangeTableEntry->relkind)
		{
			/* skip the regular views as they are replaced with subqueries */
			case RELKIND_VIEW:
			{
				continue;
			}

			case RELKIND_MATVIEW:
			case RELKIND_FOREIGN_TABLE:
			{
				/* These two cases as a target is not allowed */
				if (relationId == targetRelationId)
				{
					/* Usually we don't reach this exception as the Postgres parser catches it */
					StringInfo errorMessage = makeStringInfo();
					appendStringInfo(errorMessage, "MERGE command is not allowed on "
												   "relation type(relkind:%c)",
									 rangeTableEntry->relkind);
					return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 errorMessage->data, NULL, NULL);
				}
				break;
			}

			case RELKIND_RELATION:
			case RELKIND_PARTITIONED_TABLE:
			{
				/* Check for citus/postgres table types */
				Assert(OidIsValid(relationId));
				break;
			}

			default:
			{
				StringInfo errorMessage = makeStringInfo();
				appendStringInfo(errorMessage, "Unexpected table type(relkind:%c) "
											   "in MERGE command",
								 rangeTableEntry->relkind);
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 errorMessage->data, NULL, NULL);
			}
		}

		/*
		 * For now, save all distributed tables, later (below) we will
		 * check for supported combination(s).
		 */
		if (IsCitusTableType(relationId, DISTRIBUTED_TABLE))
		{
			/* Append/Range distributed tables are not supported */
			if (IsCitusTableType(relationId, APPEND_DISTRIBUTED) ||
				IsCitusTableType(relationId, RANGE_DISTRIBUTED))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "For MERGE command, all the distributed tables "
									 "must be colocated, for append/range distribution, "
									 "colocation is not supported", NULL,
									 "Consider using hash distribution instead");
			}

			distTablesList = lappend(distTablesList, rangeTableEntry);
		}
		else if (IsCitusTableType(relationId, REFERENCE_TABLE))
		{
			/* Reference table as a target is not allowed */
			if (relationId == targetRelationId)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "Reference table as target "
									 "is not allowed in "
									 "MERGE command", NULL, NULL);
			}

			foundReferenceTables = true;
		}
		else if (IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
		{
			/* Citus local tables */
			foundLocalTables = true;
		}
		else if (!IsCitusTable(relationId))
		{
			/* Regular Postgres table */
			foundLocalTables = true;
		}

		/* Any other Citus table type missing ? */
	}

	/* Ensure all tables are indeed local (or a combination of reference and local) */
	if (list_length(distTablesList) == 0)
	{
		/*
		 * All the tables are local/reference, supported as long as
		 * coordinator is in the metadata.
		 */
		if (FindCoordinatorNodeId() == -1)
		{
			elog(ERROR, "Coordinator node is not in the metadata. TODO better meesage");
		}

		/* All the tables are local/reference, supported */
		return NULL;
	}

	if (foundLocalTables)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "MERGE command is not supported with "
							 "combination of distributed/local tables yet",
							 NULL, NULL);
	}

	if (foundReferenceTables)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "MERGE command is not supported with "
							 "combination of distributed/reference yet",
							 NULL,
							 "If target is distributed, source "
							 "must be distributed and co-located");
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
InsertDistributionColumnMatchesSource(Oid targetRelationId, Query *query)
{
	Assert(IsMergeQuery(query));

	if (!IsCitusTableType(targetRelationId, DISTRIBUTED_TABLE))
	{
		return NULL;
	}

	if (!HasDistributionKey(targetRelationId))
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
		Var *targetKey = PartitionColumn(targetRelationId, 1);

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


/*
 * MergeQuerySupported does check for a MERGE command in the query, if it finds
 * one, it will verify the below criteria
 * - Supported tables and combinations in ErrorIfMergeHasUnsupportedTables
 * - Distributed tables requirements in ErrorIfDistTablesNotColocated
 * - Checks target-lists and functions-in-quals in TargetlistAndFunctionsSupported
 */
DeferredErrorMessage *
MergeQuerySupported(Oid resultRelationId, Query *originalQuery, bool multiShardQuery,
					PlannerRestrictionContext *plannerRestrictionContext)
{
	/* function is void for pre-15 versions of Postgres */
	#if PG_VERSION_NUM < PG_VERSION_15

	return NULL;

	#else

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

	/*
	 * Fast path queries cannot have merge command, and we prevent the remaining here.
	 * In Citus we have limited support for MERGE, it's allowed only if all
	 * the tables(target, source or any CTE) tables are are local i.e. a
	 * combination of Citus local and Non-Citus tables (regular Postgres tables)
	 * or distributed tables with some restrictions, please see header of routine
	 * ErrorIfDistTablesNotColocated for details.
	 */
	DeferredErrorMessage *deferredError =
		ErrorIfMergeHasUnsupportedTables(resultRelationId,
										 originalQuery,
										 rangeTableList,
										 plannerRestrictionContext);
	if (deferredError)
	{
		/* MERGE's unsupported combination, raise the exception */
		RaiseDeferredError(deferredError, ERROR);
	}

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
		InsertDistributionColumnMatchesSource(resultRelationId, originalQuery);
	if (deferredError)
	{
		/* MERGE's unsupported scenario, raise the exception */
		RaiseDeferredError(deferredError, ERROR);
	}

	if (multiShardQuery)
	{
		bool requireSubqueryPushdownCondsForTopLevel = false;
		deferredError =
			DeferErrorIfUnsupportedSubqueryPushdown(originalQuery,
													plannerRestrictionContext,
													requireSubqueryPushdownCondsForTopLevel);
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
 * CreateMergePlan attempts to create a plan for the given MERGE SQL
 * statement. If planning fails ->planningError is set to a description
 * of the failure.
 */
DistributedPlan *
CreateMergePlan(Query *originalQuery, Query *query,
				PlannerRestrictionContext *plannerRestrictionContext)
{
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	bool multiShardQuery = false;
	Oid targetRelationId = ModifyQueryResultRelationId(originalQuery);

	Assert(originalQuery->commandType == CMD_MERGE);
	Assert(OidIsValid(targetRelationId));

	distributedPlan->targetRelationId = targetRelationId;
	distributedPlan->modLevel = RowModifyLevelForQuery(query);
	distributedPlan->planningError = MergeQuerySupported(targetRelationId,
														 originalQuery,
														 multiShardQuery,
														 plannerRestrictionContext);

	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
	}

	Job *job = RouterJob(originalQuery, plannerRestrictionContext,
						 &distributedPlan->planningError);

	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
	}

	ereport(DEBUG1, (errmsg("Creating MERGE router plan")));

	distributedPlan->workerJob = job;
	distributedPlan->combineQuery = NULL;

	/* MERGE doesn't support RETURNING clause */
	distributedPlan->expectResults = false;
	distributedPlan->fastPathRouterPlan =
		plannerRestrictionContext->fastPathRestrictionContext->fastPathRouterQuery;

	return distributedPlan;
}


/*
 * IsLocalTableModification returns true if the table modified is a Postgres table.
 * We do not support recursive planning for MERGE yet, so we could have a join
 * between local and Citus tables. Only allow local tables when it is the target table.
 */
bool
IsLocalTableModification(Oid targetRelationId, Query *query, uint64 shardId,
						 RTEListProperties *rteProperties)
{
	/* No-op for SELECT command */
	if (!IsModifyCommand(query))
	{
		return false;
	}

	/* For MERGE, we have to check only the target relation */
	if (IsMergeQuery(query) && !IsCitusTable(targetRelationId))
	{
		/* Postgres table */
		return true;
	}

	if (shardId == INVALID_SHARD_ID && ContainsOnlyLocalTables(rteProperties))
	{
		return true;
	}

	return false;
}
