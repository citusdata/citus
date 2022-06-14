
/*-------------------------------------------------------------------------
 *
 * multi_router_planner.c
 *
 * This file contains functions to plan multiple shard queries without any
 * aggregation step including distributed table modifications.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include <stddef.h>

#include "access/stratnum.h"
#include "access/xact.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_type.h"
#include "distributed/colocation_utils.h"
#include "distributed/citus_clauses.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distribution_column.h"
#include "distributed/errormessage.h"
#include "distributed/log_utils.h"
#include "distributed/insert_select_planner.h"
#include "distributed/intermediate_result_pruning.h"
#include "distributed/metadata_utility.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/listutils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/query_pushdown_planning.h"
#include "distributed/query_utils.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/relay_utility.h"
#include "distributed/recursive_planning.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/shard_pruning.h"
#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/joininfo.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/optimizer.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
#include "postmaster/postmaster.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/typcache.h"

#include "catalog/pg_proc.h"
#include "optimizer/planmain.h"

/* intermediate value for INSERT processing */
typedef struct InsertValues
{
	Expr *partitionValueExpr; /* partition value provided in INSERT row */
	List *rowValues;          /* full values list of INSERT row, possibly NIL */
	int64 shardId;            /* target shard for this row, possibly invalid */
	Index listIndex;          /* index to make our sorting stable */
} InsertValues;


/*
 * A ModifyRoute encapsulates the information needed to route modifications
 * to the appropriate shard. For a single-shard modification, only one route
 * is needed, but in the case of e.g. a multi-row INSERT, lists of these values
 * will help divide the rows by their destination shards, permitting later
 * shard-and-row-specific extension of the original SQL.
 */
typedef struct ModifyRoute
{
	int64 shardId;        /* identifier of target shard */
	List *rowValuesLists; /* for multi-row INSERTs, list of rows to be inserted */
} ModifyRoute;


typedef struct WalkerState
{
	bool containsVar;
	bool varArgument;
	bool badCoalesce;
} WalkerState;

bool EnableRouterExecution = true;


/* planner functions forward declarations */
static void CreateSingleTaskRouterSelectPlan(DistributedPlan *distributedPlan,
											 Query *originalQuery,
											 Query *query,
											 PlannerRestrictionContext *
											 plannerRestrictionContext);
static Oid ResultRelationOidForQuery(Query *query);
static bool IsTidColumn(Node *node);
static DeferredErrorMessage * ModifyPartialQuerySupported(Query *queryTree, bool
														  multiShardQuery,
														  Oid *distributedTableId);
static bool NodeIsFieldStore(Node *node);
static DeferredErrorMessage * MultiShardUpdateDeleteSupported(Query *originalQuery,
															  PlannerRestrictionContext *
															  plannerRestrictionContext);
static DeferredErrorMessage * SingleShardUpdateDeleteSupported(Query *originalQuery,
															   PlannerRestrictionContext *
															   plannerRestrictionContext);
static bool HasDangerousJoinUsing(List *rtableList, Node *jtnode);
static bool MasterIrreducibleExpression(Node *expression, bool *varArgument,
										bool *badCoalesce);
static bool MasterIrreducibleExpressionWalker(Node *expression, WalkerState *state);
static bool MasterIrreducibleExpressionFunctionChecker(Oid func_id, void *context);
static bool TargetEntryChangesValue(TargetEntry *targetEntry, Var *column,
									FromExpr *joinTree);
static Job * RouterInsertJob(Query *originalQuery);
static void ErrorIfNoShardsExist(CitusTableCacheEntry *cacheEntry);
static DeferredErrorMessage * DeferErrorIfModifyView(Query *queryTree);
static Job * CreateJob(Query *query);
static Task * CreateTask(TaskType taskType);
static Job * RouterJob(Query *originalQuery,
					   PlannerRestrictionContext *plannerRestrictionContext,
					   DeferredErrorMessage **planningError);
static bool RelationPrunesToMultipleShards(List *relationShardList);
static void NormalizeMultiRowInsertTargetList(Query *query);
static void AppendNextDummyColReference(Alias *expendedReferenceNames);
static String * MakeDummyColumnString(int dummyColumnId);
static List * BuildRoutesForInsert(Query *query, DeferredErrorMessage **planningError);
static List * GroupInsertValuesByShardId(List *insertValuesList);
static List * ExtractInsertValuesList(Query *query, Var *partitionColumn);
static DeferredErrorMessage * DeferErrorIfUnsupportedRouterPlannableSelectQuery(
	Query *query);
static DeferredErrorMessage * ErrorIfQueryHasUnroutableModifyingCTE(Query *queryTree);
#if PG_VERSION_NUM >= PG_VERSION_14
static DeferredErrorMessage * ErrorIfQueryHasCTEWithSearchClause(Query *queryTree);
static bool ContainsSearchClauseWalker(Node *node);
#endif
static bool SelectsFromDistributedTable(List *rangeTableList, Query *query);
static ShardPlacement * CreateDummyPlacement(bool hasLocalRelation);
static ShardPlacement * CreateLocalDummyPlacement();
static int CompareInsertValuesByShardId(const void *leftElement,
										const void *rightElement);
static List * SingleShardTaskList(Query *query, uint64 jobId,
								  List *relationShardList, List *placementList,
								  uint64 shardId, bool parametersInQueryResolved,
								  bool isLocalTableModification);
static bool RowLocksOnRelations(Node *node, List **rtiLockList);
static void ReorderTaskPlacementsByTaskAssignmentPolicy(Job *job,
														TaskAssignmentPolicyType
														taskAssignmentPolicy,
														List *placementList);
static bool ModifiesLocalTableWithRemoteCitusLocalTable(List *rangeTableList);
static DeferredErrorMessage * DeferErrorIfUnsupportedLocalTableJoin(List *rangeTableList);
static bool IsLocallyAccessibleCitusLocalTable(Oid relationId);


/*
 * CreateRouterPlan attempts to create a router executor plan for the given
 * SELECT statement. ->planningError is set if planning fails.
 */
DistributedPlan *
CreateRouterPlan(Query *originalQuery, Query *query,
				 PlannerRestrictionContext *plannerRestrictionContext)
{
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);

	distributedPlan->planningError = DeferErrorIfUnsupportedRouterPlannableSelectQuery(
		query);

	if (distributedPlan->planningError == NULL)
	{
		CreateSingleTaskRouterSelectPlan(distributedPlan, originalQuery, query,
										 plannerRestrictionContext);
	}

	distributedPlan->fastPathRouterPlan =
		plannerRestrictionContext->fastPathRestrictionContext->fastPathRouterQuery;

	return distributedPlan;
}


/*
 * CreateModifyPlan attempts to create a plan for the given modification
 * statement. If planning fails ->planningError is set to a description of
 * the failure.
 */
DistributedPlan *
CreateModifyPlan(Query *originalQuery, Query *query,
				 PlannerRestrictionContext *plannerRestrictionContext)
{
	Job *job = NULL;
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	bool multiShardQuery = false;

	Assert(originalQuery->commandType != CMD_SELECT);

	distributedPlan->modLevel = RowModifyLevelForQuery(query);

	distributedPlan->planningError = ModifyQuerySupported(query, originalQuery,
														  multiShardQuery,
														  plannerRestrictionContext);

	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
	}

	if (UpdateOrDeleteQuery(query))
	{
		job = RouterJob(originalQuery, plannerRestrictionContext,
						&distributedPlan->planningError);
	}
	else
	{
		job = RouterInsertJob(originalQuery);
	}

	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
	}

	ereport(DEBUG2, (errmsg("Creating router plan")));

	distributedPlan->workerJob = job;
	distributedPlan->combineQuery = NULL;
	distributedPlan->expectResults = originalQuery->returningList != NIL;
	distributedPlan->targetRelationId = ResultRelationOidForQuery(query);

	distributedPlan->fastPathRouterPlan =
		plannerRestrictionContext->fastPathRestrictionContext->fastPathRouterQuery;


	return distributedPlan;
}


/*
 * CreateSingleTaskRouterSelectPlan creates a physical plan for given SELECT query.
 * The returned plan is a router task that returns query results from a single worker.
 * If not router plannable, the returned plan's planningError describes the problem.
 */
static void
CreateSingleTaskRouterSelectPlan(DistributedPlan *distributedPlan, Query *originalQuery,
								 Query *query,
								 PlannerRestrictionContext *plannerRestrictionContext)
{
	Assert(query->commandType == CMD_SELECT);

	distributedPlan->modLevel = RowModifyLevelForQuery(query);

	Job *job = RouterJob(originalQuery, plannerRestrictionContext,
						 &distributedPlan->planningError);

	if (distributedPlan->planningError != NULL)
	{
		/* query cannot be handled by this planner */
		return;
	}

	ereport(DEBUG2, (errmsg("Creating router plan")));

	distributedPlan->workerJob = job;
	distributedPlan->combineQuery = NULL;
	distributedPlan->expectResults = true;
}


/*
 * ShardIntervalOpExpressions returns a list of OpExprs with exactly two
 * items in it. The list consists of shard interval ranges with partition columns
 * such as (partitionColumn >= shardMinValue) and (partitionColumn <= shardMaxValue).
 *
 * The function returns hashed columns generated by MakeInt4Column() for the hash
 * partitioned tables in place of partition columns.
 *
 * The function returns NIL if shard interval does not belong to a hash,
 * range and append distributed tables.
 *
 * NB: If you update this, also look at PrunableExpressionsWalker().
 */
List *
ShardIntervalOpExpressions(ShardInterval *shardInterval, Index rteIndex)
{
	Oid relationId = shardInterval->relationId;
	Var *partitionColumn = NULL;

	if (IsCitusTableType(relationId, HASH_DISTRIBUTED))
	{
		partitionColumn = MakeInt4Column();
	}
	else if (IsCitusTableType(relationId, RANGE_DISTRIBUTED) || IsCitusTableType(
				 relationId, APPEND_DISTRIBUTED))
	{
		Assert(rteIndex > 0);
		partitionColumn = PartitionColumn(relationId, rteIndex);
	}
	else
	{
		/* do not add any shard range interval for reference tables */
		return NIL;
	}

	/* build the base expression for constraint */
	Node *baseConstraint = BuildBaseConstraint(partitionColumn);

	/* walk over shard list and check if shards can be pruned */
	if (shardInterval->minValueExists && shardInterval->maxValueExists)
	{
		UpdateConstraint(baseConstraint, shardInterval);
	}

	return list_make1(baseConstraint);
}


/*
 * AddPartitionKeyNotNullFilterToSelect adds the following filters to a subquery:
 *
 *    partitionColumn IS NOT NULL
 *
 * The function expects and asserts that subquery's target list contains a partition
 * column value. Thus, this function should never be called with reference tables.
 */
void
AddPartitionKeyNotNullFilterToSelect(Query *subqery)
{
	List *targetList = subqery->targetList;
	ListCell *targetEntryCell = NULL;
	Var *targetPartitionColumnVar = NULL;

	/* iterate through the target entries */
	foreach(targetEntryCell, targetList)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);

		bool skipOuterVars = true;
		if (IsPartitionColumn(targetEntry->expr, subqery, skipOuterVars) &&
			IsA(targetEntry->expr, Var))
		{
			targetPartitionColumnVar = (Var *) targetEntry->expr;
			break;
		}
	}

	/* we should have found target partition column */
	Assert(targetPartitionColumnVar != NULL);

	/* create expression for partition_column IS NOT NULL */
	NullTest *nullTest = makeNode(NullTest);
	nullTest->nulltesttype = IS_NOT_NULL;
	nullTest->arg = (Expr *) targetPartitionColumnVar;
	nullTest->argisrow = false;

	/* finally add the quals */
	if (subqery->jointree->quals == NULL)
	{
		subqery->jointree->quals = (Node *) nullTest;
	}
	else
	{
		subqery->jointree->quals = make_and_qual(subqery->jointree->quals,
												 (Node *) nullTest);
	}
}


/*
 * ExtractSelectRangeTableEntry returns the range table entry of the subquery.
 * Note that the function expects and asserts that the input query be
 * an INSERT...SELECT query.
 */
RangeTblEntry *
ExtractSelectRangeTableEntry(Query *query)
{
	Assert(InsertSelectIntoCitusTable(query) || InsertSelectIntoLocalTable(query));

	/*
	 * Since we already asserted InsertSelectIntoCitusTable() it is safe to access
	 * both lists
	 */
	List *fromList = query->jointree->fromlist;
	RangeTblRef *reference = linitial(fromList);
	RangeTblEntry *subqueryRte = rt_fetch(reference->rtindex, query->rtable);

	return subqueryRte;
}


/*
 * ModifyQueryResultRelationId returns the result relation's Oid
 * for the given modification query.
 *
 * The function errors out if the input query is not a
 * modify query (e.g., INSERT, UPDATE or DELETE). So, this
 * function is not expected to be called on SELECT queries.
 */
Oid
ModifyQueryResultRelationId(Query *query)
{
	/* only modify queries have result relations */
	if (!IsModifyCommand(query))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("input query is not a modification query")));
	}

	RangeTblEntry *resultRte = ExtractResultRelationRTE(query);
	Assert(OidIsValid(resultRte->relid));

	return resultRte->relid;
}


/*
 * ResultRelationOidForQuery returns the OID of the relation this is modified
 * by a given query.
 */
static Oid
ResultRelationOidForQuery(Query *query)
{
	RangeTblEntry *resultRTE = rt_fetch(query->resultRelation, query->rtable);

	return resultRTE->relid;
}


/*
 * ExtractResultRelationRTE returns the table's resultRelation range table
 * entry. This returns NULL when there's no resultRelation, such as in a SELECT
 * query.
 */
RangeTblEntry *
ExtractResultRelationRTE(Query *query)
{
	if (query->resultRelation > 0)
	{
		return rt_fetch(query->resultRelation, query->rtable);
	}

	return NULL;
}


/*
 * ExtractResultRelationRTEOrError returns the table's resultRelation range table
 * entry and errors out if there's no result relation at all, e.g. like in a
 * SELECT query.
 *
 * This is a separate function (instead of using missingOk), so static analysis
 * reasons about NULL returns correctly.
 */
RangeTblEntry *
ExtractResultRelationRTEOrError(Query *query)
{
	RangeTblEntry *relation = ExtractResultRelationRTE(query);
	if (relation == NULL)
	{
		ereport(ERROR, (errmsg("no result relation could be found for the query"),
						errhint("is this a SELECT query?")));
	}

	return relation;
}


/*
 * IsTidColumn gets a node and returns true if the node is a Var type of TID.
 */
static bool
IsTidColumn(Node *node)
{
	if (IsA(node, Var))
	{
		Var *column = (Var *) node;
		if (column->vartype == TIDOID)
		{
			return true;
		}
	}

	return false;
}


/*
 * ModifyPartialQuerySupported implements a subset of what ModifyQuerySupported checks,
 * that subset being what's necessary to check modifying CTEs for.
 */
static DeferredErrorMessage *
ModifyPartialQuerySupported(Query *queryTree, bool multiShardQuery,
							Oid *distributedTableIdOutput)
{
	DeferredErrorMessage *deferredError = DeferErrorIfModifyView(queryTree);
	if (deferredError != NULL)
	{
		return deferredError;
	}
	CmdType commandType = queryTree->commandType;

	deferredError = DeferErrorIfUnsupportedLocalTableJoin(queryTree->rtable);
	if (deferredError != NULL)
	{
		return deferredError;
	}

	/*
	 * Reject subqueries which are in SELECT or WHERE clause.
	 * Queries which include subqueries in FROM clauses are rejected below.
	 */
	if (queryTree->hasSubLinks == true)
	{
		/* we support subqueries for INSERTs only via INSERT INTO ... SELECT */
		if (!UpdateOrDeleteQuery(queryTree))
		{
			Assert(queryTree->commandType == CMD_INSERT);

			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "subqueries are not supported within INSERT queries",
								 NULL, "Try rewriting your queries with 'INSERT "
									   "INTO ... SELECT' syntax.");
		}
	}

	/* reject queries which include CommonTableExpr which aren't routable */
	if (queryTree->cteList != NIL)
	{
		ListCell *cteCell = NULL;

		/* CTEs still not supported for INSERTs. */
		if (queryTree->commandType == CMD_INSERT)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "Router planner doesn't support common table expressions with INSERT queries.",
								 NULL, NULL);
		}

		foreach(cteCell, queryTree->cteList)
		{
			CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);
			Query *cteQuery = (Query *) cte->ctequery;

			if (cteQuery->commandType != CMD_SELECT)
			{
				/* Modifying CTEs still not supported for multi shard queries. */
				if (multiShardQuery)
				{
					return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 "Router planner doesn't support non-select common table expressions with multi shard queries.",
										 NULL, NULL);
				}
				/* Modifying CTEs exclude both INSERT CTEs & INSERT queries. */
				else if (cteQuery->commandType == CMD_INSERT)
				{
					return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 "Router planner doesn't support INSERT common table expressions.",
										 NULL, NULL);
				}
			}

			if (cteQuery->hasForUpdate &&
				FindNodeMatchingCheckFunctionInRangeTableList(cteQuery->rtable,
															  IsReferenceTableRTE))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "Router planner doesn't support SELECT FOR UPDATE"
									 " in common table expressions involving reference tables.",
									 NULL, NULL);
			}

			if (FindNodeMatchingCheckFunction((Node *) cteQuery, CitusIsVolatileFunction))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "Router planner doesn't support VOLATILE functions"
									 " in common table expressions.",
									 NULL, NULL);
			}

			if (cteQuery->commandType == CMD_SELECT)
			{
				DeferredErrorMessage *cteError =
					DeferErrorIfUnsupportedRouterPlannableSelectQuery(cteQuery);
				if (cteError)
				{
					return cteError;
				}
			}
		}
	}


	Oid resultRelationId = ModifyQueryResultRelationId(queryTree);
	*distributedTableIdOutput = resultRelationId;
	uint32 rangeTableId = 1;

	Var *partitionColumn = NULL;
	if (IsCitusTable(resultRelationId))
	{
		partitionColumn = PartitionColumn(resultRelationId, rangeTableId);
	}
	commandType = queryTree->commandType;
	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		bool hasVarArgument = false; /* A STABLE function is passed a Var argument */
		bool hasBadCoalesce = false; /* CASE/COALESCE passed a mutable function */
		FromExpr *joinTree = queryTree->jointree;
		ListCell *targetEntryCell = NULL;

		foreach(targetEntryCell, queryTree->targetList)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

			/* skip resjunk entries: UPDATE adds some for ctid, etc. */
			if (targetEntry->resjunk)
			{
				continue;
			}

			bool targetEntryPartitionColumn = false;
			AttrNumber targetColumnAttrNumber = InvalidAttrNumber;

			/* reference tables do not have partition column */
			if (partitionColumn == NULL)
			{
				targetEntryPartitionColumn = false;
			}
			else
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
						if (targetColumnAttrNumber == partitionColumn->varattno)
						{
							targetEntryPartitionColumn = true;
						}
					}
				}
			}


			if (commandType == CMD_UPDATE &&
				FindNodeMatchingCheckFunction((Node *) targetEntry->expr,
											  CitusIsVolatileFunction))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in UPDATE queries on distributed "
									 "tables must not be VOLATILE",
									 NULL, NULL);
			}

			if (commandType == CMD_UPDATE && targetEntryPartitionColumn &&
				TargetEntryChangesValue(targetEntry, partitionColumn,
										queryTree->jointree))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "modifying the partition value of rows is not "
									 "allowed",
									 NULL, NULL);
			}

			if (commandType == CMD_UPDATE &&
				MasterIrreducibleExpression((Node *) targetEntry->expr,
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

		if (joinTree != NULL)
		{
			if (FindNodeMatchingCheckFunction((Node *) joinTree->quals,
											  CitusIsVolatileFunction))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in the WHERE clause of modification "
									 "queries on distributed tables must not be VOLATILE",
									 NULL, NULL);
			}
			else if (MasterIrreducibleExpression(joinTree->quals, &hasVarArgument,
												 &hasBadCoalesce))
			{
				Assert(hasVarArgument || hasBadCoalesce);
			}
		}

		if (hasVarArgument)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "STABLE functions used in UPDATE queries "
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

		if (contain_mutable_functions((Node *) queryTree->returningList))
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "non-IMMUTABLE functions are not allowed in the "
								 "RETURNING clause",
								 NULL, NULL);
		}

		if (queryTree->jointree->quals != NULL &&
			nodeTag(queryTree->jointree->quals) == T_CurrentOfExpr)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot run DML queries with cursors", NULL,
								 NULL);
		}
	}

	deferredError = ErrorIfOnConflictNotSupported(queryTree);
	if (deferredError != NULL)
	{
		return deferredError;
	}


	/* set it for caller to use when we don't return any errors */
	*distributedTableIdOutput = resultRelationId;

	return NULL;
}


/*
 * DeferErrorIfUnsupportedLocalTableJoin returns an error message
 * if there is an unsupported join in the given range table list.
 */
static DeferredErrorMessage *
DeferErrorIfUnsupportedLocalTableJoin(List *rangeTableList)
{
	if (ModifiesLocalTableWithRemoteCitusLocalTable(rangeTableList))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "Modifying local tables with remote local tables is "
							 "not supported.",
							 NULL,
							 "Consider wrapping remote local table to a CTE, or subquery");
	}
	return NULL;
}


/*
 * ModifiesLocalTableWithRemoteCitusLocalTable returns true if a local
 * table is modified with a remote citus local table. This could be a case with
 * MX structure.
 */
static bool
ModifiesLocalTableWithRemoteCitusLocalTable(List *rangeTableList)
{
	bool containsLocalResultRelation = false;
	bool containsRemoteCitusLocalTable = false;

	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, rangeTableList)
	{
		if (!IsRecursivelyPlannableRelation(rangeTableEntry))
		{
			continue;
		}
		if (IsCitusTableType(rangeTableEntry->relid, CITUS_LOCAL_TABLE))
		{
			if (!IsLocallyAccessibleCitusLocalTable(rangeTableEntry->relid))
			{
				containsRemoteCitusLocalTable = true;
			}
		}
		else if (!IsCitusTable(rangeTableEntry->relid))
		{
			containsLocalResultRelation = true;
		}
	}
	return containsLocalResultRelation && containsRemoteCitusLocalTable;
}


/*
 * IsLocallyAccessibleCitusLocalTable returns true if the given table
 * is a citus local table that can be accessed using local execution.
 */
static bool
IsLocallyAccessibleCitusLocalTable(Oid relationId)
{
	if (!IsCitusTableType(relationId, CITUS_LOCAL_TABLE))
	{
		return false;
	}

	List *shardIntervalList = LoadShardIntervalList(relationId);

	/*
	 * Citus local tables should always have exactly one shard, but we have
	 * this check for safety.
	 */
	if (list_length(shardIntervalList) != 1)
	{
		return false;
	}

	ShardInterval *shardInterval = linitial(shardIntervalList);
	uint64 shardId = shardInterval->shardId;
	ShardPlacement *localShardPlacement =
		ActiveShardPlacementOnGroup(GetLocalGroupId(), shardId);
	return localShardPlacement != NULL;
}


/*
 * NodeIsFieldStore returns true if given Node is a FieldStore object.
 */
static bool
NodeIsFieldStore(Node *node)
{
	return node && IsA(node, FieldStore);
}


/*
 * ModifyQuerySupported returns NULL if the query only contains supported
 * features, otherwise it returns an error description.
 * Note that we need both the original query and the modified one because
 * different checks need different versions. In particular, we cannot
 * perform the ContainsReadIntermediateResultFunction check on the
 * rewritten query because it may have been replaced by a subplan,
 * while some of the checks for setting the partition column value rely
 * on the rewritten query.
 */
DeferredErrorMessage *
ModifyQuerySupported(Query *queryTree, Query *originalQuery, bool multiShardQuery,
					 PlannerRestrictionContext *plannerRestrictionContext)
{
	Oid distributedTableId = InvalidOid;
	DeferredErrorMessage *error = ModifyPartialQuerySupported(queryTree, multiShardQuery,
															  &distributedTableId);
	if (error)
	{
		return error;
	}

	List *rangeTableList = NIL;
	uint32 queryTableCount = 0;
	CmdType commandType = queryTree->commandType;
	bool fastPathRouterQuery =
		plannerRestrictionContext->fastPathRestrictionContext->fastPathRouterQuery;

	/*
	 * Here, we check if a recursively planned query tries to modify
	 * rows based on the ctid column. This is a bad idea because ctid of
	 * the rows could be changed before the modification part of
	 * the query is executed.
	 *
	 * We can exclude fast path queries since they cannot have intermediate
	 * results by definition.
	 */
	if (!fastPathRouterQuery &&
		ContainsReadIntermediateResultFunction((Node *) originalQuery))
	{
		bool hasTidColumn = FindNodeMatchingCheckFunction(
			(Node *) originalQuery->jointree, IsTidColumn);

		if (hasTidColumn)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform distributed planning for the given "
								 "modification",
								 "Recursively planned distributed modifications "
								 "with ctid on where clause are not supported.",
								 NULL);
		}
	}

	/*
	 * Extract range table entries for queries that are not fast path. We can skip fast
	 * path queries because their definition is a single RTE entry, which is a relation,
	 * so the following check doesn't apply for fast-path queries.
	 */
	if (!fastPathRouterQuery)
	{
		ExtractRangeTableEntryWalker((Node *) originalQuery, &rangeTableList);
	}
	bool containsLocalTableDistributedTableJoin =
		ContainsLocalTableDistributedTableJoin(queryTree->rtable);

	RangeTblEntry *rangeTableEntry = NULL;
	foreach_ptr(rangeTableEntry, rangeTableList)
	{
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			/* we do not expect to see a view in modify query */
			if (rangeTableEntry->relkind == RELKIND_VIEW)
			{
				/*
				 * we already check if modify is run on a view in DeferErrorIfModifyView
				 * function call. In addition, since Postgres replaced views in FROM
				 * clause with subqueries, encountering with a view should not be a problem here.
				 */
			}
			else if (rangeTableEntry->relkind == RELKIND_MATVIEW)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "materialized views in modify queries are not supported",
									 NULL, NULL);
			}
			/* for other kinds of relations, check if its distributed */
			else
			{
				if (IsRelationLocalTableOrMatView(rangeTableEntry->relid) &&
					containsLocalTableDistributedTableJoin)
				{
					StringInfo errorMessage = makeStringInfo();
					char *relationName = get_rel_name(rangeTableEntry->relid);
					if (IsCitusTable(rangeTableEntry->relid))
					{
						appendStringInfo(errorMessage,
										 "local table %s cannot be joined with these distributed tables",
										 relationName);
					}
					else
					{
						appendStringInfo(errorMessage, "relation %s is not distributed",
										 relationName);
					}
					return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										 errorMessage->data, NULL, NULL);
				}
			}

			queryTableCount++;
		}
		else if (rangeTableEntry->rtekind == RTE_VALUES ||
				 rangeTableEntry->rtekind == RTE_RESULT
				 )
		{
			/* do nothing, this type is supported */
		}
		else
		{
			char *rangeTableEntryErrorDetail = NULL;

			/*
			 * We support UPDATE and DELETE with subqueries and joins unless
			 * they are multi shard queries.
			 */
			if (UpdateOrDeleteQuery(queryTree))
			{
				continue;
			}

			/*
			 * Error out for rangeTableEntries that we do not support.
			 * We do not explicitly specify "in FROM clause" in the error detail
			 * for the features that we do not support at all (SUBQUERY, JOIN).
			 */
			if (rangeTableEntry->rtekind == RTE_SUBQUERY)
			{
				StringInfo errorHint = makeStringInfo();
				CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(
					distributedTableId);
				char *partitionColumnName =
					ColumnToColumnName(distributedTableId,
									   (Node *) cacheEntry->partitionColumn);

				appendStringInfo(errorHint, "Consider using an equality filter on "
											"partition column \"%s\" to target a single shard.",
								 partitionColumnName);

				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED, "subqueries are not "
																	"supported in modifications across multiple shards",
									 errorHint->data, NULL);
			}
			else if (rangeTableEntry->rtekind == RTE_JOIN)
			{
				rangeTableEntryErrorDetail = "Joins are not supported in distributed"
											 " modifications.";
			}
			else if (rangeTableEntry->rtekind == RTE_FUNCTION)
			{
				rangeTableEntryErrorDetail = "Functions must not appear in the FROM"
											 " clause of a distributed modifications.";
			}
			else if (rangeTableEntry->rtekind == RTE_CTE)
			{
				rangeTableEntryErrorDetail = "Common table expressions are not supported"
											 " in distributed modifications.";
			}
			else
			{
				rangeTableEntryErrorDetail = "Unrecognized range table entry.";
			}

			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform distributed planning for the given "
								 "modifications",
								 rangeTableEntryErrorDetail,
								 NULL);
		}
	}

	if (commandType != CMD_INSERT)
	{
		DeferredErrorMessage *errorMessage = NULL;

		if (multiShardQuery)
		{
			errorMessage = MultiShardUpdateDeleteSupported(originalQuery,
														   plannerRestrictionContext);
		}
		else
		{
			errorMessage = SingleShardUpdateDeleteSupported(originalQuery,
															plannerRestrictionContext);
		}

		if (errorMessage != NULL)
		{
			return errorMessage;
		}
	}

#if PG_VERSION_NUM >= PG_VERSION_14
	DeferredErrorMessage *CTEWithSearchClauseError =
		ErrorIfQueryHasCTEWithSearchClause(originalQuery);
	if (CTEWithSearchClauseError != NULL)
	{
		return CTEWithSearchClauseError;
	}
#endif

	return NULL;
}


/*
 * Modify statements on simple updetable views are not supported yet.
 * Actually, we need the original query (the query before postgres
 * pg_rewrite_query) to detect if the view sitting in rtable is to
 * be updated or just to be used in FROM clause.
 * Hence, tracing the postgres source code, we deduced that postgres
 * puts the relation to be modified to the first entry of rtable.
 * If first element of the range table list is a simple updatable
 * view and this view is not coming from FROM clause (inFromCl = False),
 * then update is run "on" that view.
 */
static DeferredErrorMessage *
DeferErrorIfModifyView(Query *queryTree)
{
	if (queryTree->rtable != NIL)
	{
		RangeTblEntry *firstRangeTableElement = (RangeTblEntry *) linitial(
			queryTree->rtable);

		if (firstRangeTableElement->rtekind == RTE_RELATION &&
			firstRangeTableElement->relkind == RELKIND_VIEW &&
			firstRangeTableElement->inFromCl == false)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot modify views when the query contains citus tables",
								 NULL,
								 NULL);
		}
	}

	return NULL;
}


/*
 * ErrorIfOnConflictNotSupprted returns an error if an INSERT query has an
 * unsupported ON CONFLICT clause. In particular, changing the partition
 * column value or using volatile functions is not allowed.
 */
DeferredErrorMessage *
ErrorIfOnConflictNotSupported(Query *queryTree)
{
	uint32 rangeTableId = 1;
	ListCell *setTargetCell = NULL;
	bool specifiesPartitionValue = false;

	CmdType commandType = queryTree->commandType;
	if (commandType != CMD_INSERT || queryTree->onConflict == NULL)
	{
		return NULL;
	}

	Oid distributedTableId = ExtractFirstCitusTableId(queryTree);
	Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);

	List *onConflictSet = queryTree->onConflict->onConflictSet;
	Node *arbiterWhere = queryTree->onConflict->arbiterWhere;
	Node *onConflictWhere = queryTree->onConflict->onConflictWhere;

	/*
	 * onConflictSet is expanded via expand_targetlist() on the standard planner.
	 * This ends up adding all the columns to the onConflictSet even if the user
	 * does not explicitly state the columns in the query.
	 *
	 * The following loop simply allows "DO UPDATE SET part_col = table.part_col"
	 * types of elements in the target list, which are added by expand_targetlist().
	 * Any other attempt to update partition column value is forbidden.
	 */
	foreach(setTargetCell, onConflictSet)
	{
		TargetEntry *setTargetEntry = (TargetEntry *) lfirst(setTargetCell);
		bool setTargetEntryPartitionColumn = false;

		/* reference tables do not have partition column */
		if (partitionColumn == NULL)
		{
			setTargetEntryPartitionColumn = false;
		}
		else
		{
			Oid resultRelationId = ModifyQueryResultRelationId(queryTree);

			AttrNumber targetColumnAttrNumber = InvalidAttrNumber;
			if (setTargetEntry->resname)
			{
				targetColumnAttrNumber = get_attnum(resultRelationId,
													setTargetEntry->resname);
				if (targetColumnAttrNumber == partitionColumn->varattno)
				{
					setTargetEntryPartitionColumn = true;
				}
			}
		}
		if (setTargetEntryPartitionColumn)
		{
			Expr *setExpr = setTargetEntry->expr;
			if (IsA(setExpr, Var) &&
				((Var *) setExpr)->varattno == partitionColumn->varattno)
			{
				specifiesPartitionValue = false;
			}
			else
			{
				specifiesPartitionValue = true;
			}
		}
		else
		{
			/*
			 * Similarly, allow  "DO UPDATE SET col_1 = table.col_1" types of
			 * target list elements. Note that, the following check allows
			 * "DO UPDATE SET col_1 = table.col_2", which is not harmful.
			 */
			if (IsA(setTargetEntry->expr, Var))
			{
				continue;
			}
			else if (contain_mutable_functions((Node *) setTargetEntry->expr))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in the DO UPDATE SET clause of "
									 "INSERTs on distributed tables must be marked "
									 "IMMUTABLE",
									 NULL, NULL);
			}
		}
	}

	/* error if either arbiter or on conflict WHERE contains a mutable function */
	if (contain_mutable_functions((Node *) arbiterWhere) ||
		contain_mutable_functions((Node *) onConflictWhere))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "functions used in the WHERE clause of the "
							 "ON CONFLICT clause of INSERTs on distributed "
							 "tables must be marked IMMUTABLE",
							 NULL, NULL);
	}

	if (specifiesPartitionValue)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "modifying the partition value of rows is not "
							 "allowed",
							 NULL, NULL);
	}

	return NULL;
}


/*
 * MultiShardUpdateDeleteSupported returns the error message if the update/delete is
 * not pushdownable, otherwise it returns NULL.
 */
static DeferredErrorMessage *
MultiShardUpdateDeleteSupported(Query *originalQuery,
								PlannerRestrictionContext *plannerRestrictionContext)
{
	DeferredErrorMessage *errorMessage = NULL;
	RangeTblEntry *resultRangeTable = ExtractResultRelationRTE(originalQuery);
	Oid resultRelationOid = resultRangeTable->relid;

	if (HasDangerousJoinUsing(originalQuery->rtable, (Node *) originalQuery->jointree))
	{
		errorMessage = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "a join with USING causes an internal naming conflict, use "
									 "ON instead",
									 NULL, NULL);
	}
	else if (FindNodeMatchingCheckFunction((Node *) originalQuery,
										   CitusIsVolatileFunction))
	{
		errorMessage = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in UPDATE queries on distributed "
									 "tables must not be VOLATILE",
									 NULL, NULL);
	}
	else if (IsCitusTableType(resultRelationOid, REFERENCE_TABLE))
	{
		errorMessage = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "only reference tables may be queried when targeting "
									 "a reference table with multi shard UPDATE/DELETE queries "
									 "with multiple tables ",
									 NULL, NULL);
	}
	else
	{
		errorMessage = DeferErrorIfUnsupportedSubqueryPushdown(originalQuery,
															   plannerRestrictionContext);
	}

	return errorMessage;
}


/*
 * SingleShardUpdateDeleteSupported returns the error message if the update/delete query is
 * not routable, otherwise it returns NULL.
 */
static DeferredErrorMessage *
SingleShardUpdateDeleteSupported(Query *originalQuery,
								 PlannerRestrictionContext *plannerRestrictionContext)
{
	DeferredErrorMessage *errorMessage = NULL;

	/*
	 * We currently do not support volatile functions in update/delete statements because
	 * the function evaluation logic does not know how to distinguish volatile functions
	 * (that need to be evaluated per row) from stable functions (that need to be evaluated per query),
	 * and it is also not safe to push the volatile functions down on replicated tables.
	 */
	if (FindNodeMatchingCheckFunction((Node *) originalQuery,
									  CitusIsVolatileFunction))
	{
		errorMessage = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in UPDATE queries on distributed "
									 "tables must not be VOLATILE",
									 NULL, NULL);
	}

	return errorMessage;
}


/*
 * HasDangerousJoinUsing search jointree for unnamed JOIN USING. Check the
 * implementation of has_dangerous_join_using in ruleutils.
 */
static bool
HasDangerousJoinUsing(List *rtableList, Node *joinTreeNode)
{
	if (IsA(joinTreeNode, RangeTblRef))
	{
		/* nothing to do here */
	}
	else if (IsA(joinTreeNode, FromExpr))
	{
		FromExpr *fromExpr = (FromExpr *) joinTreeNode;
		ListCell *listCell;

		foreach(listCell, fromExpr->fromlist)
		{
			if (HasDangerousJoinUsing(rtableList, (Node *) lfirst(listCell)))
			{
				return true;
			}
		}
	}
	else if (IsA(joinTreeNode, JoinExpr))
	{
		JoinExpr *joinExpr = (JoinExpr *) joinTreeNode;

		/* Is it an unnamed JOIN with USING? */
		if (joinExpr->alias == NULL && joinExpr->usingClause)
		{
			/*
			 * Yes, so check each join alias var to see if any of them are not
			 * simple references to underlying columns. If so, we have a
			 * dangerous situation and must pick unique aliases.
			 */
			RangeTblEntry *joinRTE = rt_fetch(joinExpr->rtindex, rtableList);
			ListCell *listCell;

			foreach(listCell, joinRTE->joinaliasvars)
			{
				Var *aliasVar = (Var *) lfirst(listCell);

				if (aliasVar != NULL && !IsA(aliasVar, Var))
				{
					return true;
				}
			}
		}

		/* Nope, but inspect children */
		if (HasDangerousJoinUsing(rtableList, joinExpr->larg))
		{
			return true;
		}
		if (HasDangerousJoinUsing(rtableList, joinExpr->rarg))
		{
			return true;
		}
	}
	else
	{
		elog(ERROR, "unrecognized node type: %d",
			 (int) nodeTag(joinTreeNode));
	}
	return false;
}


/*
 * UpdateOrDeleteQuery checks if the given query is an UPDATE or DELETE command.
 * If it is, it returns true otherwise it returns false.
 */
bool
UpdateOrDeleteQuery(Query *query)
{
	return query->commandType == CMD_UPDATE ||
		   query->commandType == CMD_DELETE;
}


/*
 * If the expression contains STABLE functions which accept any parameters derived from a
 * Var returns true and sets varArgument.
 *
 * If the expression contains a CASE or COALESCE which invoke non-IMMUTABLE functions
 * returns true and sets badCoalesce.
 *
 * Assumes the expression contains no VOLATILE functions.
 *
 * Var's are allowed, but only if they are passed solely to IMMUTABLE functions
 *
 * We special-case CASE/COALESCE because those are evaluated lazily. We could evaluate
 * CASE/COALESCE expressions which don't reference Vars, or partially evaluate some
 * which do, but for now we just error out. That makes both the code and user-education
 * easier.
 */
static bool
MasterIrreducibleExpression(Node *expression, bool *varArgument, bool *badCoalesce)
{
	WalkerState data;
	data.containsVar = data.varArgument = data.badCoalesce = false;

	bool result = MasterIrreducibleExpressionWalker(expression, &data);

	*varArgument |= data.varArgument;
	*badCoalesce |= data.badCoalesce;
	return result;
}


static bool
MasterIrreducibleExpressionWalker(Node *expression, WalkerState *state)
{
	char volatileFlag = 0;
	WalkerState childState = { false, false, false };
	bool containsDisallowedFunction = false;
	bool hasVolatileFunction PG_USED_FOR_ASSERTS_ONLY = false;

	if (expression == NULL)
	{
		return false;
	}

	if (IsA(expression, CoalesceExpr))
	{
		CoalesceExpr *expr = (CoalesceExpr *) expression;

		if (contain_mutable_functions((Node *) (expr->args)))
		{
			state->badCoalesce = true;
			return true;
		}
		else
		{
			/*
			 * There's no need to recurse. Since there are no STABLE functions
			 * varArgument will never be set.
			 */
			return false;
		}
	}

	if (IsA(expression, CaseExpr))
	{
		if (contain_mutable_functions(expression))
		{
			state->badCoalesce = true;
			return true;
		}

		return false;
	}

	if (IsA(expression, Var))
	{
		state->containsVar = true;
		return false;
	}

	/*
	 * In order for statement replication to give us consistent results it's important
	 * that we either disallow or evaluate on the coordinator anything which has a
	 * volatility category above IMMUTABLE. Newer versions of postgres might add node
	 * types which should be checked in this function.
	 *
	 * Look through contain_mutable_functions_walker or future PG's equivalent for new
	 * node types before bumping this version number to fix compilation; e.g. for any
	 * PostgreSQL after 9.5, see check_functions_in_node. Review
	 * MasterIrreducibleExpressionFunctionChecker for any changes in volatility
	 * permissibility ordering.
	 *
	 * Once you've added them to this check, make sure you also evaluate them in the
	 * executor!
	 */

	hasVolatileFunction =
		check_functions_in_node(expression, MasterIrreducibleExpressionFunctionChecker,
								&volatileFlag);

	/* the caller should have already checked for this */
	Assert(!hasVolatileFunction);
	Assert(volatileFlag != PROVOLATILE_VOLATILE);

	if (volatileFlag == PROVOLATILE_STABLE)
	{
		containsDisallowedFunction =
			expression_tree_walker(expression,
								   MasterIrreducibleExpressionWalker,
								   &childState);

		if (childState.containsVar)
		{
			state->varArgument = true;
		}

		state->badCoalesce |= childState.badCoalesce;
		state->varArgument |= childState.varArgument;

		return (containsDisallowedFunction || childState.containsVar);
	}

	/* keep traversing */
	return expression_tree_walker(expression,
								  MasterIrreducibleExpressionWalker,
								  state);
}


/*
 * MasterIrreducibleExpressionFunctionChecker returns true if a provided function
 * oid corresponds to a volatile function. It also updates provided context if
 * the current volatility flag is more permissive than the provided one. It is
 * only called from check_functions_in_node as checker function.
 */
static bool
MasterIrreducibleExpressionFunctionChecker(Oid func_id, void *context)
{
	char volatileFlag = func_volatile(func_id);
	char *volatileContext = (char *) context;

	if (volatileFlag == PROVOLATILE_VOLATILE || *volatileContext == PROVOLATILE_VOLATILE)
	{
		*volatileContext = PROVOLATILE_VOLATILE;
	}
	else if (volatileFlag == PROVOLATILE_STABLE || *volatileContext == PROVOLATILE_STABLE)
	{
		*volatileContext = PROVOLATILE_STABLE;
	}
	else
	{
		*volatileContext = PROVOLATILE_IMMUTABLE;
	}

	return (volatileFlag == PROVOLATILE_VOLATILE);
}


/*
 * TargetEntryChangesValue determines whether the given target entry may
 * change the value in a given column, given a join tree. The result is
 * true unless the expression refers directly to the column, or the
 * expression is a value that is implied by the qualifiers of the join
 * tree, or the target entry sets a different column.
 */
static bool
TargetEntryChangesValue(TargetEntry *targetEntry, Var *column, FromExpr *joinTree)
{
	bool isColumnValueChanged = true;
	Expr *setExpr = targetEntry->expr;

	if (IsA(setExpr, Var))
	{
		Var *newValue = (Var *) setExpr;
		if (newValue->varattno == column->varattno)
		{
			/* target entry of the form SET col = table.col */
			isColumnValueChanged = false;
		}
	}
	else if (IsA(setExpr, Const))
	{
		Const *newValue = (Const *) setExpr;
		List *restrictClauseList = WhereClauseList(joinTree);
		OpExpr *equalityExpr = MakeOpExpression(column, BTEqualStrategyNumber);
		Node *rightOp = get_rightop((Expr *) equalityExpr);

		Assert(rightOp != NULL);
		Assert(IsA(rightOp, Const));
		Const *rightConst = (Const *) rightOp;

		rightConst->constvalue = newValue->constvalue;
		rightConst->constisnull = newValue->constisnull;
		rightConst->constbyval = newValue->constbyval;

		bool predicateIsImplied = predicate_implied_by(list_make1(equalityExpr),
													   restrictClauseList, false);
		if (predicateIsImplied)
		{
			/* target entry of the form SET col = <x> WHERE col = <x> AND ... */
			isColumnValueChanged = false;
		}
	}

	return isColumnValueChanged;
}


/*
 * RouterInsertJob builds a Job to represent an insertion performed by the provided
 * query. For inserts we always defer shard pruning and generating the task list to
 * the executor.
 */
static Job *
RouterInsertJob(Query *originalQuery)
{
	Assert(originalQuery->commandType == CMD_INSERT);

	bool isMultiRowInsert = IsMultiRowInsert(originalQuery);
	if (isMultiRowInsert)
	{
		/* add default expressions to RTE_VALUES in multi-row INSERTs */
		NormalizeMultiRowInsertTargetList(originalQuery);
	}

	Job *job = CreateJob(originalQuery);
	job->requiresCoordinatorEvaluation = RequiresCoordinatorEvaluation(originalQuery);
	job->deferredPruning = true;
	job->partitionKeyValue = ExtractInsertPartitionKeyValue(originalQuery);

	return job;
}


/*
 * CreateJob returns a new Job for the given query.
 */
static Job *
CreateJob(Query *query)
{
	Job *job = CitusMakeNode(Job);
	job->jobId = UniqueJobId();
	job->jobQuery = query;
	job->taskList = NIL;
	job->dependentJobList = NIL;
	job->subqueryPushdown = false;
	job->requiresCoordinatorEvaluation = false;
	job->deferredPruning = false;

	return job;
}


/*
 * ErrorIfNoShardsExist throws an error if the given table has no shards.
 */
static void
ErrorIfNoShardsExist(CitusTableCacheEntry *cacheEntry)
{
	int shardCount = cacheEntry->shardIntervalArrayLength;
	if (shardCount == 0)
	{
		Oid distributedTableId = cacheEntry->relationId;
		char *relationName = get_rel_name(distributedTableId);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not find any shards"),
						errdetail("No shards exist for distributed table \"%s\".",
								  relationName),
						errhint("Run master_create_worker_shards to create shards "
								"and try again.")));
	}
}


/*
 * RouterInsertTaskList generates a list of tasks for performing an INSERT on
 * a distributed table via the router executor.
 */
List *
RouterInsertTaskList(Query *query, bool parametersInQueryResolved,
					 DeferredErrorMessage **planningError)
{
	List *insertTaskList = NIL;

	Oid distributedTableId = ExtractFirstCitusTableId(query);
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(distributedTableId);

	ErrorIfNoShardsExist(cacheEntry);

	Assert(query->commandType == CMD_INSERT);

	List *modifyRouteList = BuildRoutesForInsert(query, planningError);
	if (*planningError != NULL)
	{
		return NIL;
	}

	ModifyRoute *modifyRoute = NULL;
	foreach_ptr(modifyRoute, modifyRouteList)
	{
		Task *modifyTask = CreateTask(MODIFY_TASK);
		modifyTask->anchorShardId = modifyRoute->shardId;
		modifyTask->replicationModel = cacheEntry->replicationModel;
		modifyTask->rowValuesLists = modifyRoute->rowValuesLists;

		RelationShard *relationShard = CitusMakeNode(RelationShard);
		relationShard->shardId = modifyRoute->shardId;
		relationShard->relationId = distributedTableId;

		modifyTask->relationShardList = list_make1(relationShard);
		modifyTask->taskPlacementList = ActiveShardPlacementList(
			modifyRoute->shardId);
		modifyTask->parametersInQueryStringResolved = parametersInQueryResolved;

		insertTaskList = lappend(insertTaskList, modifyTask);
	}

	return insertTaskList;
}


/*
 * CreateTask returns a new Task with the given type.
 */
static Task *
CreateTask(TaskType taskType)
{
	Task *task = CitusMakeNode(Task);
	task->taskType = taskType;
	task->jobId = INVALID_JOB_ID;
	task->taskId = INVALID_TASK_ID;
	SetTaskQueryString(task, NULL);
	task->anchorShardId = INVALID_SHARD_ID;
	task->taskPlacementList = NIL;
	task->dependentTaskList = NIL;

	task->partitionId = 0;
	task->upstreamTaskId = INVALID_TASK_ID;
	task->shardInterval = NULL;
	task->assignmentConstrained = false;
	task->replicationModel = REPLICATION_MODEL_INVALID;
	task->relationRowLockList = NIL;

	task->modifyWithSubquery = false;
	task->partiallyLocalOrRemote = false;
	task->relationShardList = NIL;

	return task;
}


/*
 * ExtractFirstCitusTableId takes a given query, and finds the relationId
 * for the first distributed table in that query. If the function cannot find a
 * distributed table, it returns InvalidOid.
 *
 * We only use this function for modifications and fast path queries, which
 * should have the first distributed table in the top-level rtable.
 */
Oid
ExtractFirstCitusTableId(Query *query)
{
	List *rangeTableList = query->rtable;
	ListCell *rangeTableCell = NULL;
	Oid distributedTableId = InvalidOid;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (IsCitusTable(rangeTableEntry->relid))
		{
			distributedTableId = rangeTableEntry->relid;
			break;
		}
	}

	return distributedTableId;
}


/*
 * RouterJob builds a Job to represent a single shard select/update/delete and
 * multiple shard update/delete queries.
 */
static Job *
RouterJob(Query *originalQuery, PlannerRestrictionContext *plannerRestrictionContext,
		  DeferredErrorMessage **planningError)
{
	uint64 shardId = INVALID_SHARD_ID;
	List *placementList = NIL;
	List *relationShardList = NIL;
	List *prunedShardIntervalListList = NIL;
	bool isMultiShardModifyQuery = false;
	Const *partitionKeyValue = NULL;

	/* router planner should create task even if it doesn't hit a shard at all */
	bool replacePrunedQueryWithDummy = true;

	bool isLocalTableModification = false;

	/* check if this query requires coordinator evaluation */
	bool requiresCoordinatorEvaluation = RequiresCoordinatorEvaluation(originalQuery);
	FastPathRestrictionContext *fastPathRestrictionContext =
		plannerRestrictionContext->fastPathRestrictionContext;

	/*
	 * We prefer to defer shard pruning/task generation to the
	 * execution when the parameter on the distribution key
	 * cannot be resolved.
	 */
	if (fastPathRestrictionContext->fastPathRouterQuery &&
		fastPathRestrictionContext->distributionKeyHasParam)
	{
		Job *job = CreateJob(originalQuery);
		job->deferredPruning = true;

		ereport(DEBUG2, (errmsg("Deferred pruning for a fast-path router "
								"query")));
		return job;
	}
	else
	{
		(*planningError) = PlanRouterQuery(originalQuery, plannerRestrictionContext,
										   &placementList, &shardId, &relationShardList,
										   &prunedShardIntervalListList,
										   replacePrunedQueryWithDummy,
										   &isMultiShardModifyQuery,
										   &partitionKeyValue,
										   &isLocalTableModification);
	}

	if (*planningError)
	{
		return NULL;
	}

	Job *job = CreateJob(originalQuery);
	job->partitionKeyValue = partitionKeyValue;

	if (originalQuery->resultRelation > 0)
	{
		RangeTblEntry *updateOrDeleteRTE = ExtractResultRelationRTE(originalQuery);

		/*
		 * If all of the shards are pruned, we replace the relation RTE into
		 * subquery RTE that returns no results. However, this is not useful
		 * for UPDATE and DELETE queries. Therefore, if we detect a UPDATE or
		 * DELETE RTE with subquery type, we just set task list to empty and return
		 * the job.
		 */
		if (updateOrDeleteRTE->rtekind == RTE_SUBQUERY)
		{
			job->taskList = NIL;
			return job;
		}
	}

	if (isMultiShardModifyQuery)
	{
		job->taskList = QueryPushdownSqlTaskList(originalQuery, job->jobId,
												 plannerRestrictionContext->
												 relationRestrictionContext,
												 prunedShardIntervalListList,
												 MODIFY_TASK,
												 requiresCoordinatorEvaluation,
												 planningError);
		if (*planningError)
		{
			return NULL;
		}
	}
	else
	{
		GenerateSingleShardRouterTaskList(job, relationShardList,
										  placementList, shardId,
										  isLocalTableModification);
	}

	job->requiresCoordinatorEvaluation = requiresCoordinatorEvaluation;
	return job;
}


/*
 * GenerateSingleShardRouterTaskList is a wrapper around other corresponding task
 * list generation functions specific to single shard selects and modifications.
 *
 * The function updates the input job's taskList in-place.
 */
void
GenerateSingleShardRouterTaskList(Job *job, List *relationShardList,
								  List *placementList, uint64 shardId, bool
								  isLocalTableModification)
{
	Query *originalQuery = job->jobQuery;

	if (originalQuery->commandType == CMD_SELECT)
	{
		job->taskList = SingleShardTaskList(originalQuery, job->jobId,
											relationShardList, placementList,
											shardId,
											job->parametersInJobQueryResolved,
											isLocalTableModification);

		/*
		 * Queries to reference tables, or distributed tables with multiple replica's have
		 * their task placements reordered according to the configured
		 * task_assignment_policy. This is only applicable to select queries as the modify
		 * queries will _always_ be executed on all placements.
		 *
		 * We also ignore queries that are targeting only intermediate results (e.g., no
		 * valid anchorShardId).
		 */
		if (shardId != INVALID_SHARD_ID)
		{
			ReorderTaskPlacementsByTaskAssignmentPolicy(job, TaskAssignmentPolicy,
														placementList);
		}
	}
	else if (shardId == INVALID_SHARD_ID && !isLocalTableModification)
	{
		/* modification that prunes to 0 shards */
		job->taskList = NIL;
	}
	else
	{
		job->taskList = SingleShardTaskList(originalQuery, job->jobId,
											relationShardList, placementList,
											shardId,
											job->parametersInJobQueryResolved,
											isLocalTableModification);
	}
}


/*
 * ReorderTaskPlacementsByTaskAssignmentPolicy applies selective reordering for supported
 * TaskAssignmentPolicyTypes.
 *
 * Supported Types
 * - TASK_ASSIGNMENT_ROUND_ROBIN round robin schedule queries among placements
 *
 * By default it does not reorder the task list, implying a first-replica strategy.
 */
static void
ReorderTaskPlacementsByTaskAssignmentPolicy(Job *job,
											TaskAssignmentPolicyType taskAssignmentPolicy,
											List *placementList)
{
	if (taskAssignmentPolicy == TASK_ASSIGNMENT_ROUND_ROBIN)
	{
		/*
		 * We hit a single shard on router plans, and there should be only
		 * one task in the task list
		 */
		Assert(list_length(job->taskList) == 1);
		Task *task = (Task *) linitial(job->taskList);

		/*
		 * For round-robin SELECT queries, we don't want to include the coordinator
		 * because the user is trying to distributed the load across nodes via
		 * round-robin policy. Otherwise, the local execution would prioritize
		 * executing the local tasks and especially for reference tables on the
		 * coordinator this would prevent load balancing across nodes.
		 *
		 * For other worker nodes in Citus MX, we let the local execution to kick-in
		 * even for round-robin policy, that's because we expect the clients to evenly
		 * connect to the worker nodes.
		 */
		Assert(ReadOnlyTask(task->taskType));
		placementList = RemoveCoordinatorPlacementIfNotSingleNode(placementList);

		/* reorder the placement list */
		List *reorderedPlacementList = RoundRobinReorder(placementList);
		task->taskPlacementList = reorderedPlacementList;

		ShardPlacement *primaryPlacement = (ShardPlacement *) linitial(
			reorderedPlacementList);
		ereport(DEBUG3, (errmsg("assigned task %u to node %s:%u", task->taskId,
								primaryPlacement->nodeName,
								primaryPlacement->nodePort)));
	}
}


/*
 * RemoveCoordinatorPlacementIfNotSingleNode gets a task placement list and returns the list
 * by removing the placement belonging to the coordinator (if any).
 *
 * If the list has a single element or no placements on the coordinator, the list
 * returned is unmodified.
 */
List *
RemoveCoordinatorPlacementIfNotSingleNode(List *placementList)
{
	ListCell *placementCell = NULL;

	if (list_length(placementList) < 2)
	{
		return placementList;
	}

	foreach(placementCell, placementList)
	{
		ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);

		if (placement->groupId == COORDINATOR_GROUP_ID)
		{
			return list_delete_ptr(placementList, placement);
		}
	}

	return placementList;
}


/*
 * SingleShardTaskList generates a task for single shard query
 * and returns it as a list.
 */
static List *
SingleShardTaskList(Query *query, uint64 jobId, List *relationShardList,
					List *placementList, uint64 shardId,
					bool parametersInQueryResolved,
					bool isLocalTableModification)
{
	TaskType taskType = READ_TASK;
	char replicationModel = 0;

	if (query->commandType != CMD_SELECT)
	{
		List *rangeTableList = NIL;
		ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);

		RangeTblEntry *updateOrDeleteRTE = ExtractResultRelationRTE(query);
		Assert(updateOrDeleteRTE != NULL);

		CitusTableCacheEntry *modificationTableCacheEntry = NULL;
		if (IsCitusTable(updateOrDeleteRTE->relid))
		{
			modificationTableCacheEntry = GetCitusTableCacheEntry(
				updateOrDeleteRTE->relid);
		}

		if (IsCitusTableType(updateOrDeleteRTE->relid, REFERENCE_TABLE) &&
			SelectsFromDistributedTable(rangeTableList, query))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform select on a distributed table "
								   "and modify a reference table")));
		}

		taskType = MODIFY_TASK;
		if (modificationTableCacheEntry)
		{
			replicationModel = modificationTableCacheEntry->replicationModel;
		}
	}

	if (taskType == READ_TASK && query->hasModifyingCTE)
	{
		/* assume ErrorIfQueryHasUnroutableModifyingCTE checked query already */

		CommonTableExpr *cte = NULL;
		foreach_ptr(cte, query->cteList)
		{
			Query *cteQuery = (Query *) cte->ctequery;

			if (cteQuery->commandType != CMD_SELECT)
			{
				RangeTblEntry *updateOrDeleteRTE = ExtractResultRelationRTE(cteQuery);
				CitusTableCacheEntry *modificationTableCacheEntry =
					GetCitusTableCacheEntry(
						updateOrDeleteRTE->relid);

				taskType = MODIFY_TASK;
				replicationModel = modificationTableCacheEntry->replicationModel;
				break;
			}
		}
	}

	Task *task = CreateTask(taskType);
	task->isLocalTableModification = isLocalTableModification;
	List *relationRowLockList = NIL;

	RowLocksOnRelations((Node *) query, &relationRowLockList);

	/*
	 * For performance reasons, we skip generating the queryString. For local
	 * execution this is not needed, so we wait until the executor determines
	 * that the query cannot be executed locally.
	 */
	task->taskPlacementList = placementList;
	SetTaskQueryIfShouldLazyDeparse(task, query);
	task->anchorShardId = shardId;
	task->jobId = jobId;
	task->relationShardList = relationShardList;
	task->relationRowLockList = relationRowLockList;
	task->replicationModel = replicationModel;
	task->parametersInQueryStringResolved = parametersInQueryResolved;

	return list_make1(task);
}


/*
 * RowLocksOnRelations forms the list for range table IDs and corresponding
 * row lock modes.
 */
static bool
RowLocksOnRelations(Node *node, List **relationRowLockList)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		ListCell *rowMarkCell = NULL;

		foreach(rowMarkCell, query->rowMarks)
		{
			RowMarkClause *rowMarkClause = (RowMarkClause *) lfirst(rowMarkCell);
			RangeTblEntry *rangeTable = rt_fetch(rowMarkClause->rti, query->rtable);
			Oid relationId = rangeTable->relid;

			if (IsCitusTable(relationId))
			{
				RelationRowLock *relationRowLock = CitusMakeNode(RelationRowLock);
				relationRowLock->relationId = relationId;
				relationRowLock->rowLockStrength = rowMarkClause->strength;
				*relationRowLockList = lappend(*relationRowLockList, relationRowLock);
			}
		}

		return query_tree_walker(query, RowLocksOnRelations, relationRowLockList, 0);
	}
	else
	{
		return expression_tree_walker(node, RowLocksOnRelations, relationRowLockList);
	}
}


/*
 * SelectsFromDistributedTable checks if there is a select on a distributed
 * table by looking into range table entries.
 */
static bool
SelectsFromDistributedTable(List *rangeTableList, Query *query)
{
	ListCell *rangeTableCell = NULL;
	RangeTblEntry *resultRangeTableEntry = NULL;

	if (query->resultRelation > 0)
	{
		resultRangeTableEntry = ExtractResultRelationRTE(query);
	}

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (rangeTableEntry->relid == InvalidOid)
		{
			continue;
		}

		if (rangeTableEntry->relkind == RELKIND_VIEW ||
			rangeTableEntry->relkind == RELKIND_MATVIEW)
		{
			/*
			 * Skip over views, which would error out in GetCitusTableCacheEntry.
			 * Distributed tables within (regular) views are already in rangeTableList.
			 */
			continue;
		}

		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(
			rangeTableEntry->relid);
		if (IsCitusTableTypeCacheEntry(cacheEntry, DISTRIBUTED_TABLE) &&
			(resultRangeTableEntry == NULL || resultRangeTableEntry->relid !=
			 rangeTableEntry->relid))
		{
			return true;
		}
	}

	return false;
}


static bool ContainsOnlyLocalTables(RTEListProperties *rteProperties);

/*
 * RouterQuery runs router pruning logic for SELECT, UPDATE and DELETE queries.
 * If there are shards present and query is routable, all RTEs have been updated
 * to point to the relevant shards in the originalQuery. Also, placementList is
 * filled with the list of worker nodes that has all the required shard placements
 * for the query execution. anchorShardId is set to the first pruned shardId of
 * the given query. Finally, relationShardList is filled with the list of
 * relation-to-shard mappings for the query.
 *
 * If the given query is not routable, it fills planningError with the related
 * DeferredErrorMessage. The caller can check this error message to see if query
 * is routable or not.
 *
 * Note: If the query prunes down to 0 shards due to filters (e.g. WHERE false),
 * or the query has only read_intermediate_result calls (no relations left after
 * recursively planning CTEs and subqueries), then it will be assigned to an
 * arbitrary worker node in a round-robin fashion.
 *
 * Relations that prune down to 0 shards are replaced by subqueries returning
 * 0 values in UpdateRelationToShardNames.
 */
DeferredErrorMessage *
PlanRouterQuery(Query *originalQuery,
				PlannerRestrictionContext *plannerRestrictionContext,
				List **placementList, uint64 *anchorShardId, List **relationShardList,
				List **prunedShardIntervalListList,
				bool replacePrunedQueryWithDummy, bool *multiShardModifyQuery,
				Const **partitionValueConst,
				bool *isLocalTableModification)
{
	bool isMultiShardQuery = false;
	DeferredErrorMessage *planningError = NULL;
	bool shardsPresent = false;
	CmdType commandType = originalQuery->commandType;
	bool fastPathRouterQuery =
		plannerRestrictionContext->fastPathRestrictionContext->fastPathRouterQuery;

	*placementList = NIL;

	/*
	 * When FastPathRouterQuery() returns true, we know that standard_planner() has
	 * not been called. Thus, restriction information is not avaliable and we do the
	 * shard pruning based on the distribution column in the quals of the query.
	 */
	if (fastPathRouterQuery)
	{
		Const *distributionKeyValue =
			plannerRestrictionContext->fastPathRestrictionContext->distributionKeyValue;

		List *shardIntervalList =
			TargetShardIntervalForFastPathQuery(originalQuery, &isMultiShardQuery,
												distributionKeyValue,
												partitionValueConst);

		/*
		 * This could only happen when there is a parameter on the distribution key.
		 * We defer error here, later the planner is forced to use a generic plan
		 * by assigning arbitrarily high cost to the plan.
		 */
		if (UpdateOrDeleteQuery(originalQuery) && isMultiShardQuery)
		{
			planningError = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										  "Router planner cannot handle multi-shard "
										  "modify queries", NULL, NULL);
			return planningError;
		}

		*prunedShardIntervalListList = shardIntervalList;

		if (!isMultiShardQuery)
		{
			ereport(DEBUG2, (errmsg("Distributed planning for a fast-path router "
									"query")));
		}
	}
	else
	{
		*prunedShardIntervalListList =
			TargetShardIntervalsForRestrictInfo(plannerRestrictionContext->
												relationRestrictionContext,
												&isMultiShardQuery,
												partitionValueConst);
	}

	if (isMultiShardQuery)
	{
		/*
		 * If multiShardQuery is true and it is a type of SELECT query, then
		 * return deferred error. We do not support multi-shard SELECT queries
		 * with this code path.
		 */
		if (commandType == CMD_SELECT)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "Router planner cannot handle multi-shard select queries",
								 NULL, NULL);
		}

		Assert(UpdateOrDeleteQuery(originalQuery));
		planningError = ModifyQuerySupported(originalQuery, originalQuery,
											 isMultiShardQuery,
											 plannerRestrictionContext);
		if (planningError != NULL)
		{
			return planningError;
		}
		else
		{
			*multiShardModifyQuery = true;
			return planningError;
		}
	}

	*relationShardList =
		RelationShardListForShardIntervalList(*prunedShardIntervalListList,
											  &shardsPresent);

	if (!shardsPresent && !replacePrunedQueryWithDummy)
	{
		/*
		 * For INSERT ... SELECT, this query could be still a valid for some other target
		 * shard intervals. Thus, we should return empty list if there aren't any matching
		 * workers, so that the caller can decide what to do with this task.
		 */
		return NULL;
	}

	/*
	 * We bail out if there are RTEs that prune multiple shards above, but
	 * there can also be multiple RTEs that reference the same relation.
	 */
	if (RelationPrunesToMultipleShards(*relationShardList))
	{
		planningError = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									  "cannot run command which targets "
									  "multiple shards", NULL, NULL);
		return planningError;
	}

	/* we need anchor shard id for select queries with router planner */
	uint64 shardId = GetAnchorShardId(*prunedShardIntervalListList);

	/* both Postgres tables and materialized tables are locally avaliable */
	RTEListProperties *rteProperties = GetRTEListPropertiesForQuery(originalQuery);
	if (shardId == INVALID_SHARD_ID && ContainsOnlyLocalTables(rteProperties))
	{
		if (commandType != CMD_SELECT)
		{
			*isLocalTableModification = true;
		}
	}
	bool hasPostgresLocalRelation =
		rteProperties->hasPostgresLocalTable || rteProperties->hasMaterializedView;
	List *taskPlacementList =
		CreateTaskPlacementListForShardIntervals(*prunedShardIntervalListList,
												 shardsPresent,
												 replacePrunedQueryWithDummy,
												 hasPostgresLocalRelation);
	if (taskPlacementList == NIL)
	{
		planningError = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									  "found no worker with all shard placements",
									  NULL, NULL);
		return planningError;
	}

	/*
	 * If this is an UPDATE or DELETE query which requires coordinator evaluation,
	 * don't try update shard names, and postpone that to execution phase.
	 */
	bool isUpdateOrDelete = UpdateOrDeleteQuery(originalQuery);
	if (!(isUpdateOrDelete && RequiresCoordinatorEvaluation(originalQuery)))
	{
		UpdateRelationToShardNames((Node *) originalQuery, *relationShardList);
	}

	*multiShardModifyQuery = false;
	*placementList = taskPlacementList;
	*anchorShardId = shardId;

	return planningError;
}


/*
 * ContainsOnlyLocalTables returns true if there is only
 * local tables and not any distributed or reference table.
 */
static bool
ContainsOnlyLocalTables(RTEListProperties *rteProperties)
{
	return !rteProperties->hasDistributedTable && !rteProperties->hasReferenceTable;
}


/*
 * CreateTaskPlacementListForShardIntervals returns a list of shard placements
 * on which it can access all shards in shardIntervalListList, which contains
 * a list of shards for each relation in the query.
 *
 * If the query contains a local table then hasLocalRelation should be set to
 * true. In that case, CreateTaskPlacementListForShardIntervals only returns
 * a placement for the local node or an empty list if the shards cannot be
 * accessed locally.
 *
 * If generateDummyPlacement is true and there are no shards that need to be
 * accessed to answer the query (shardsPresent is false), then a single
 * placement is returned that is either local or follows a round-robin policy.
 * A typical example is a router query that only reads an intermediate result.
 * This will happen on the coordinator, unless the user wants to balance the
 * load by setting the citus.task_assignment_policy.
 */
List *
CreateTaskPlacementListForShardIntervals(List *shardIntervalListList, bool shardsPresent,
										 bool generateDummyPlacement,
										 bool hasLocalRelation)
{
	List *placementList = NIL;

	if (shardsPresent)
	{
		/*
		 * Determine the workers that have all shard placements, if any.
		 */
		List *shardPlacementList =
			PlacementsForWorkersContainingAllShards(shardIntervalListList);

		if (hasLocalRelation)
		{
			ShardPlacement *taskPlacement = NULL;

			/*
			 * If there is a local table, we only allow the local placement to
			 * be used. If there is none, we disallow the query.
			 */
			foreach_ptr(taskPlacement, shardPlacementList)
			{
				if (taskPlacement->groupId == GetLocalGroupId())
				{
					placementList = lappend(placementList, taskPlacement);
				}
			}
		}
		else
		{
			placementList = shardPlacementList;
		}
	}
	else if (generateDummyPlacement)
	{
		ShardPlacement *dummyPlacement = CreateDummyPlacement(hasLocalRelation);

		placementList = list_make1(dummyPlacement);
	}

	return placementList;
}


/*
 * CreateLocalDummyPlacement creates a dummy placement for the local node that
 * can be used for queries that don't involve any shards. The typical examples
 * are:
 *       (a) queries that consist of only intermediate results
 *       (b) queries that hit zero shards (... WHERE false;)
 */
static ShardPlacement *
CreateLocalDummyPlacement()
{
	ShardPlacement *dummyPlacement = CitusMakeNode(ShardPlacement);
	dummyPlacement->nodeId = LOCAL_NODE_ID;
	dummyPlacement->nodeName = LocalHostName;
	dummyPlacement->nodePort = PostPortNumber;
	dummyPlacement->groupId = GetLocalGroupId();
	return dummyPlacement;
}


/*
 * CreateDummyPlacement creates a dummy placement that can be used for queries
 * that don't involve any shards. The typical examples are:
 *       (a) queries that consist of only intermediate results
 *       (b) queries that hit zero shards (... WHERE false;)
 *
 * If round robin policy is set, the placement could be on any node in pg_dist_node.
 * Else, the local node is set for the placement.
 *
 * Queries can also involve local tables. In that case we always use the local
 * node.
 */
static ShardPlacement *
CreateDummyPlacement(bool hasLocalRelation)
{
	static uint32 zeroShardQueryRoundRobin = 0;

	if (TaskAssignmentPolicy != TASK_ASSIGNMENT_ROUND_ROBIN || hasLocalRelation)
	{
		return CreateLocalDummyPlacement();
	}

	List *workerNodeList = ActiveReadableNonCoordinatorNodeList();
	if (workerNodeList == NIL)
	{
		/*
		 * We want to round-robin over the workers, but there are no workers.
		 * To make sure the query can still succeed we fall back to returning
		 * a local dummy placement.
		 */
		return CreateLocalDummyPlacement();
	}

	int workerNodeCount = list_length(workerNodeList);
	int workerNodeIndex = zeroShardQueryRoundRobin % workerNodeCount;
	WorkerNode *workerNode = (WorkerNode *) list_nth(workerNodeList,
													 workerNodeIndex);

	ShardPlacement *dummyPlacement = CitusMakeNode(ShardPlacement);
	SetPlacementNodeMetadata(dummyPlacement, workerNode);

	zeroShardQueryRoundRobin++;

	return dummyPlacement;
}


/*
 * RelationShardListForShardIntervalList is a utility function which gets a list of
 * shardInterval, and returns a list of RelationShard.
 */
List *
RelationShardListForShardIntervalList(List *shardIntervalList, bool *shardsPresent)
{
	List *relationShardList = NIL;
	ListCell *shardIntervalListCell = NULL;

	foreach(shardIntervalListCell, shardIntervalList)
	{
		List *prunedShardIntervalList = (List *) lfirst(shardIntervalListCell);

		/* no shard is present or all shards are pruned out case will be handled later */
		if (prunedShardIntervalList == NIL)
		{
			continue;
		}

		*shardsPresent = true;

		ListCell *shardIntervalCell = NULL;
		foreach(shardIntervalCell, prunedShardIntervalList)
		{
			ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
			RelationShard *relationShard = CitusMakeNode(RelationShard);

			relationShard->relationId = shardInterval->relationId;
			relationShard->shardId = shardInterval->shardId;

			relationShardList = lappend(relationShardList, relationShard);
		}
	}

	return relationShardList;
}


/*
 * GetAnchorShardId returns the anchor shard id given relation shard list.
 * The desired anchor shard is found as follows:
 *
 * - Return the first distributed table shard id in the relationShardList if
 * there is any.
 * - Return a random reference table shard id if all the shards belong to
 * reference tables
 * - Return INVALID_SHARD_ID on empty lists
 */
uint64
GetAnchorShardId(List *prunedShardIntervalListList)
{
	ListCell *prunedShardIntervalListCell = NULL;
	uint64 referenceShardId = INVALID_SHARD_ID;

	foreach(prunedShardIntervalListCell, prunedShardIntervalListList)
	{
		List *prunedShardIntervalList = (List *) lfirst(prunedShardIntervalListCell);

		/* no shard is present or all shards are pruned out case will be handled later */
		if (prunedShardIntervalList == NIL)
		{
			continue;
		}

		ShardInterval *shardInterval = linitial(prunedShardIntervalList);

		if (ReferenceTableShardId(shardInterval->shardId))
		{
			referenceShardId = shardInterval->shardId;
		}
		else
		{
			return shardInterval->shardId;
		}
	}

	return referenceShardId;
}


/*
 * TargetShardIntervalForFastPathQuery gets a query which is in
 * the form defined by FastPathRouterQuery() and returns exactly
 * one list of a a one shard interval (see FastPathRouterQuery()
 * for the detail).
 *
 * If the caller requested the distributionKey value that this function
 * yields, set outputPartitionValueConst.
 */
List *
TargetShardIntervalForFastPathQuery(Query *query, bool *isMultiShardQuery,
									Const *inputDistributionKeyValue,
									Const **outputPartitionValueConst)
{
	Oid relationId = ExtractFirstCitusTableId(query);

	if (IsCitusTableType(relationId, CITUS_TABLE_WITH_NO_DIST_KEY))
	{
		/* we don't need to do shard pruning for non-distributed tables */
		return list_make1(LoadShardIntervalList(relationId));
	}

	if (inputDistributionKeyValue && !inputDistributionKeyValue->constisnull)
	{
		CitusTableCacheEntry *cache = GetCitusTableCacheEntry(relationId);
		Var *distributionKey = cache->partitionColumn;

		/*
		 * We currently don't allow implicitly coerced values to be handled by fast-
		 * path planner. Still, let's be defensive for any  future changes..
		 */
		if (inputDistributionKeyValue->consttype != distributionKey->vartype)
		{
			bool missingOk = false;
			inputDistributionKeyValue =
				TransformPartitionRestrictionValue(distributionKey,
												   inputDistributionKeyValue, missingOk);
		}

		ShardInterval *cachedShardInterval =
			FindShardInterval(inputDistributionKeyValue->constvalue, cache);
		if (cachedShardInterval == NULL)
		{
			ereport(ERROR, (errmsg(
								"could not find shardinterval to which to send the query")));
		}

		if (outputPartitionValueConst != NULL)
		{
			/* set the outgoing partition column value if requested */
			*outputPartitionValueConst = inputDistributionKeyValue;
		}
		ShardInterval *shardInterval = CopyShardInterval(cachedShardInterval);
		List *shardIntervalList = list_make1(shardInterval);

		return list_make1(shardIntervalList);
	}

	Node *quals = query->jointree->quals;
	int relationIndex = 1;

	/*
	 * We couldn't do the shard pruning based on inputDistributionKeyValue as it might
	 * be passed as NULL. Still, we can search the quals for distribution key.
	 */
	Const *distributionKeyValueInQuals = NULL;
	List *prunedShardIntervalList =
		PruneShards(relationId, relationIndex, make_ands_implicit((Expr *) quals),
					&distributionKeyValueInQuals);

	if (!distributionKeyValueInQuals || distributionKeyValueInQuals->constisnull)
	{
		/*
		 * If the distribution key equals to NULL, we prefer to treat it as a zero shard
		 * query as it cannot return any rows.
		 */
		return NIL;
	}

	/* we're only expecting single shard from a single table */
	Node *distKey PG_USED_FOR_ASSERTS_ONLY = NULL;
	Assert(FastPathRouterQuery(query, &distKey) || !EnableFastPathRouterPlanner);

	if (list_length(prunedShardIntervalList) > 1)
	{
		*isMultiShardQuery = true;
	}
	else if (list_length(prunedShardIntervalList) == 1 &&
			 outputPartitionValueConst != NULL)
	{
		/* set the outgoing partition column value if requested */
		*outputPartitionValueConst = distributionKeyValueInQuals;
	}

	return list_make1(prunedShardIntervalList);
}


/*
 * TargetShardIntervalsForRestrictInfo performs shard pruning for all referenced
 * relations in the relation restriction context and returns list of shards per
 * relation. Shard pruning is done based on provided restriction context per relation.
 * The function sets multiShardQuery to true if any of the relations pruned down to
 * more than one active shard. It also records pruned shard intervals in relation
 * restriction context to be used later on. Some queries may have contradiction
 * clauses like 'and false' or 'and 1=0', such queries are treated as if all of
 * the shards of joining relations are pruned out.
 */
List *
TargetShardIntervalsForRestrictInfo(RelationRestrictionContext *restrictionContext,
									bool *multiShardQuery, Const **partitionValueConst)
{
	List *prunedShardIntervalListList = NIL;
	ListCell *restrictionCell = NULL;
	bool multiplePartitionValuesExist = false;
	Const *queryPartitionValueConst = NULL;

	Assert(restrictionContext != NULL);

	foreach(restrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(restrictionCell);
		Oid relationId = relationRestriction->relationId;

		if (!IsCitusTable(relationId))
		{
			/* ignore local tables for shard pruning purposes */
			continue;
		}

		Index tableId = relationRestriction->index;
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
		int shardCount = cacheEntry->shardIntervalArrayLength;
		List *baseRestrictionList = relationRestriction->relOptInfo->baserestrictinfo;
		List *restrictClauseList = get_all_actual_clauses(baseRestrictionList);
		List *prunedShardIntervalList = NIL;

		/*
		 * Queries may have contradiction clauses like 'false', or '1=0' in
		 * their filters. Such queries would have pseudo constant 'false'
		 * inside relOptInfo->joininfo list. We treat such cases as if all
		 * shards of the table are pruned out.
		 */
		bool joinFalseQuery = JoinConditionIsOnFalse(
			relationRestriction->relOptInfo->joininfo);
		if (!joinFalseQuery && shardCount > 0)
		{
			Const *restrictionPartitionValueConst = NULL;
			prunedShardIntervalList = PruneShards(relationId, tableId, restrictClauseList,
												  &restrictionPartitionValueConst);

			if (list_length(prunedShardIntervalList) > 1)
			{
				(*multiShardQuery) = true;
			}
			if (restrictionPartitionValueConst != NULL &&
				queryPartitionValueConst == NULL)
			{
				queryPartitionValueConst = restrictionPartitionValueConst;
			}
			else if (restrictionPartitionValueConst != NULL &&
					 !equal(queryPartitionValueConst, restrictionPartitionValueConst))
			{
				multiplePartitionValuesExist = true;
			}
		}

		prunedShardIntervalListList = lappend(prunedShardIntervalListList,
											  prunedShardIntervalList);
	}

	/*
	 * Different resrictions might have different partition columns.
	 * We report partition column value if there is only one.
	 */
	if (multiplePartitionValuesExist)
	{
		queryPartitionValueConst = NULL;
	}

	/* set the outgoing partition column value if requested */
	if (partitionValueConst != NULL)
	{
		*partitionValueConst = queryPartitionValueConst;
	}

	return prunedShardIntervalListList;
}


/*
 * JoinConditionIsOnFalse returns true for queries that
 * have contradiction clauses like 'false', or '1=0' in
 * their filters. Such queries would have pseudo constant 'false'
 * inside joininfo list.
 */
bool
JoinConditionIsOnFalse(List *joinInfoList)
{
	List *pseudoJoinRestrictionList = extract_actual_clauses(joinInfoList, true);

	bool joinFalseQuery = ContainsFalseClause(pseudoJoinRestrictionList);
	return joinFalseQuery;
}


/*
 * RelationPrunesToMultipleShards returns true if the given list of
 * relation-to-shard mappings contains at least two mappings with
 * the same relation, but different shards.
 */
static bool
RelationPrunesToMultipleShards(List *relationShardList)
{
	ListCell *relationShardCell = NULL;
	RelationShard *previousRelationShard = NULL;

	relationShardList = SortList(relationShardList, CompareRelationShards);

	foreach(relationShardCell, relationShardList)
	{
		RelationShard *relationShard = (RelationShard *) lfirst(relationShardCell);

		if (previousRelationShard != NULL &&
			relationShard->relationId == previousRelationShard->relationId &&
			relationShard->shardId != previousRelationShard->shardId)
		{
			return true;
		}

		previousRelationShard = relationShard;
	}

	return false;
}


/*
 * PlacementsForWorkersContainingAllShards returns list of shard placements for workers
 * that contain all shard intervals in the given list of shard interval lists.
 */
List *
PlacementsForWorkersContainingAllShards(List *shardIntervalListList)
{
	bool firstShard = true;
	List *currentPlacementList = NIL;
	List *shardIntervalList = NIL;

	foreach_ptr(shardIntervalList, shardIntervalListList)
	{
		if (shardIntervalList == NIL)
		{
			continue;
		}

		Assert(list_length(shardIntervalList) == 1);

		ShardInterval *shardInterval = (ShardInterval *) linitial(shardIntervalList);
		uint64 shardId = shardInterval->shardId;

		/* retrieve all active shard placements for this shard */
		List *newPlacementList = ActiveShardPlacementList(shardId);

		if (firstShard)
		{
			firstShard = false;
			currentPlacementList = newPlacementList;
		}
		else
		{
			/* keep placements that still exists for this shard */
			currentPlacementList = IntersectPlacementList(currentPlacementList,
														  newPlacementList);
		}

		/*
		 * Bail out if placement list becomes empty. This means there is no worker
		 * containing all shards referenced by the query, hence we can not forward
		 * this query directly to any worker.
		 */
		if (currentPlacementList == NIL)
		{
			break;
		}
	}

	return currentPlacementList;
}


/*
 * BuildRoutesForInsert returns a list of ModifyRoute objects for an INSERT
 * query or an empty list if the partition column value is defined as an ex-
 * pression that still needs to be evaluated. If any partition column value
 * falls within 0 or multiple (overlapping) shards, the planning error is set.
 *
 * Multi-row INSERTs are handled by grouping their rows by target shard. These
 * groups are returned in ascending order by shard id, ready for later deparse
 * to shard-specific SQL.
 */
static List *
BuildRoutesForInsert(Query *query, DeferredErrorMessage **planningError)
{
	Oid distributedTableId = ExtractFirstCitusTableId(query);
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(distributedTableId);
	List *modifyRouteList = NIL;
	ListCell *insertValuesCell = NULL;

	Assert(query->commandType == CMD_INSERT);

	/* reference tables and citus local tables can only have one shard */
	if (IsCitusTableTypeCacheEntry(cacheEntry, CITUS_TABLE_WITH_NO_DIST_KEY))
	{
		List *shardIntervalList = LoadShardIntervalList(distributedTableId);

		int shardCount = list_length(shardIntervalList);
		if (shardCount != 1)
		{
			if (IsCitusTableTypeCacheEntry(cacheEntry, REFERENCE_TABLE))
			{
				ereport(ERROR, (errmsg("reference table cannot have %d shards",
									   shardCount)));
			}
			else if (IsCitusTableTypeCacheEntry(cacheEntry, CITUS_LOCAL_TABLE))
			{
				ereport(ERROR, (errmsg("local table cannot have %d shards",
									   shardCount)));
			}
		}

		ShardInterval *shardInterval = linitial(shardIntervalList);
		ModifyRoute *modifyRoute = palloc(sizeof(ModifyRoute));

		modifyRoute->shardId = shardInterval->shardId;

		RangeTblEntry *valuesRTE = ExtractDistributedInsertValuesRTE(query);
		if (valuesRTE != NULL)
		{
			/* add the values list for a multi-row INSERT */
			modifyRoute->rowValuesLists = valuesRTE->values_lists;
		}
		else
		{
			modifyRoute->rowValuesLists = NIL;
		}

		modifyRouteList = lappend(modifyRouteList, modifyRoute);

		return modifyRouteList;
	}

	Var *partitionColumn = cacheEntry->partitionColumn;

	/* get full list of insert values and iterate over them to prune */
	List *insertValuesList = ExtractInsertValuesList(query, partitionColumn);

	foreach(insertValuesCell, insertValuesList)
	{
		InsertValues *insertValues = (InsertValues *) lfirst(insertValuesCell);
		List *prunedShardIntervalList = NIL;
		Node *partitionValueExpr = (Node *) insertValues->partitionValueExpr;

		/*
		 * We only support constant partition values at this point. Sometimes
		 * they are wrappend in an implicit coercion though. Most notably
		 * FuncExpr coercions for casts created with CREATE CAST ... WITH
		 * FUNCTION .. AS IMPLICIT. To support this first we strip them here.
		 * Then we do the coercion manually below using
		 * TransformPartitionRestrictionValue, if the types are not the same.
		 *
		 * NOTE: eval_const_expressions below would do some of these removals
		 * too, but it's unclear if it would do all of them. It is possible
		 * that there are no cases where this strip_implicit_coercions call is
		 * really necessary at all, but currently that's hard to rule out.
		 * So to be on the safe side we call strip_implicit_coercions too, to
		 * be sure we support as much as possible.
		 */
		partitionValueExpr = strip_implicit_coercions(partitionValueExpr);

		/*
		 * By evaluating constant expressions an expression such as 2 + 4
		 * will become const 6. That way we can use them as a partition column
		 * value. Normally the planner evaluates constant expressions, but we
		 * may be working on the original query tree here. So we do it here
		 * explicitely before checking that the partition value is a const.
		 *
		 * NOTE: We do not use expression_planner here, since all it does
		 * apart from calling eval_const_expressions is call fix_opfuncids.
		 * This is not needed here, since it's a no-op for T_Const nodes and we
		 * error out below in all other cases.
		 */
		partitionValueExpr = eval_const_expressions(NULL, partitionValueExpr);

		if (!IsA(partitionValueExpr, Const))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("failed to evaluate partition key in insert"),
							errhint("try using constant values for partition column")));
		}

		Const *partitionValueConst = (Const *) partitionValueExpr;
		if (partitionValueConst->constisnull)
		{
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							errmsg("cannot perform an INSERT with NULL in the partition "
								   "column")));
		}

		/* actually do the coercions that we skipped before, if fails throw an
		 * error */
		if (partitionValueConst->consttype != partitionColumn->vartype)
		{
			bool missingOk = false;
			partitionValueConst =
				TransformPartitionRestrictionValue(partitionColumn,
												   partitionValueConst,
												   missingOk);
		}

		if (IsCitusTableTypeCacheEntry(cacheEntry, HASH_DISTRIBUTED) ||
			IsCitusTableTypeCacheEntry(cacheEntry, RANGE_DISTRIBUTED))
		{
			Datum partitionValue = partitionValueConst->constvalue;

			ShardInterval *shardInterval = FindShardInterval(partitionValue, cacheEntry);
			if (shardInterval != NULL)
			{
				prunedShardIntervalList = list_make1(shardInterval);
			}
		}
		else
		{
			Index tableId = 1;
			OpExpr *equalityExpr = MakeOpExpression(partitionColumn,
													BTEqualStrategyNumber);
			Node *rightOp = get_rightop((Expr *) equalityExpr);

			Assert(rightOp != NULL);
			Assert(IsA(rightOp, Const));
			Const *rightConst = (Const *) rightOp;

			rightConst->constvalue = partitionValueConst->constvalue;
			rightConst->constisnull = partitionValueConst->constisnull;
			rightConst->constbyval = partitionValueConst->constbyval;

			List *restrictClauseList = list_make1(equalityExpr);

			prunedShardIntervalList = PruneShards(distributedTableId, tableId,
												  restrictClauseList, NULL);
		}

		int prunedShardIntervalCount = list_length(prunedShardIntervalList);
		if (prunedShardIntervalCount != 1)
		{
			char *partitionKeyString = cacheEntry->partitionKeyString;
			char *partitionColumnName =
				ColumnToColumnName(distributedTableId, stringToNode(partitionKeyString));
			StringInfo errorMessage = makeStringInfo();
			StringInfo errorHint = makeStringInfo();
			const char *targetCountType = NULL;

			if (prunedShardIntervalCount == 0)
			{
				targetCountType = "no";
			}
			else
			{
				targetCountType = "multiple";
			}

			if (prunedShardIntervalCount == 0)
			{
				appendStringInfo(errorHint, "Make sure you have created a shard which "
											"can receive this partition column value.");
			}
			else
			{
				appendStringInfo(errorHint, "Make sure the value for partition column "
											"\"%s\" falls into a single shard.",
								 partitionColumnName);
			}

			appendStringInfo(errorMessage, "cannot run INSERT command which targets %s "
										   "shards", targetCountType);

			(*planningError) = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
											 errorMessage->data, NULL,
											 errorHint->data);

			return NIL;
		}

		ShardInterval *targetShard = (ShardInterval *) linitial(prunedShardIntervalList);
		insertValues->shardId = targetShard->shardId;
	}

	modifyRouteList = GroupInsertValuesByShardId(insertValuesList);

	return modifyRouteList;
}


/*
 * IsMultiRowInsert returns whether the given query is a multi-row INSERT.
 *
 * It does this by determining whether the query is an INSERT that has an
 * RTE_VALUES. Single-row INSERTs will have their RTE_VALUES optimised away
 * in transformInsertStmt, and instead use the target list.
 */
bool
IsMultiRowInsert(Query *query)
{
	return ExtractDistributedInsertValuesRTE(query) != NULL;
}


/*
 * ExtractDistributedInsertValuesRTE does precisely that. If the provided
 * query is not an INSERT, or if the INSERT does not have a VALUES RTE
 * (i.e. it is not a multi-row INSERT), this function returns NULL.
 * If all those conditions are met, an RTE representing the multiple values
 * of a multi-row INSERT is returned.
 */
RangeTblEntry *
ExtractDistributedInsertValuesRTE(Query *query)
{
	ListCell *rteCell = NULL;

	if (query->commandType != CMD_INSERT)
	{
		return NULL;
	}

	foreach(rteCell, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);

		if (rte->rtekind == RTE_VALUES)
		{
			return rte;
		}
	}
	return NULL;
}


/*
 * NormalizeMultiRowInsertTargetList ensures all elements of multi-row INSERT target
 * lists are Vars. In multi-row INSERTs, most target list entries contain a Var
 * expression pointing to a position within the values_lists field of a VALUES
 * RTE, but non-NULL default columns are handled differently. Instead of adding
 * the default expression to each row, a single expression encoding the DEFAULT
 * appears in the target list. For consistency, we move these expressions into
 * values lists and replace them with an appropriately constructed Var.
 */
static void
NormalizeMultiRowInsertTargetList(Query *query)
{
	ListCell *valuesListCell = NULL;
	ListCell *targetEntryCell = NULL;
	int targetEntryNo = 0;

	RangeTblEntry *valuesRTE = ExtractDistributedInsertValuesRTE(query);
	if (valuesRTE == NULL)
	{
		return;
	}

	foreach(valuesListCell, valuesRTE->values_lists)
	{
		List *valuesList = (List *) lfirst(valuesListCell);
		Expr **valuesArray = (Expr **) PointerArrayFromList(valuesList);
		List *expandedValuesList = NIL;

		foreach(targetEntryCell, query->targetList)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
			Expr *targetExpr = targetEntry->expr;

			if (IsA(targetExpr, Var))
			{
				/* expression from the VALUES section */
				Var *targetListVar = (Var *) targetExpr;
				targetExpr = valuesArray[targetListVar->varattno - 1];
			}
			else
			{
				/* copy the column's default expression */
				targetExpr = copyObject(targetExpr);
			}

			expandedValuesList = lappend(expandedValuesList, targetExpr);
		}
		SetListCellPtr(valuesListCell, (void *) expandedValuesList);
	}

	/* reset coltypes, coltypmods, colcollations and rebuild them below */
	valuesRTE->coltypes = NIL;
	valuesRTE->coltypmods = NIL;
	valuesRTE->colcollations = NIL;

	foreach(targetEntryCell, query->targetList)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);
		Node *targetExprNode = (Node *) targetEntry->expr;

		/* RTE_VALUES comes 2nd, after destination table */
		Index valuesVarno = 2;

		targetEntryNo++;

		Oid targetType = exprType(targetExprNode);
		int32 targetTypmod = exprTypmod(targetExprNode);
		Oid targetColl = exprCollation(targetExprNode);

		valuesRTE->coltypes = lappend_oid(valuesRTE->coltypes, targetType);
		valuesRTE->coltypmods = lappend_int(valuesRTE->coltypmods, targetTypmod);
		valuesRTE->colcollations = lappend_oid(valuesRTE->colcollations, targetColl);

		if (IsA(targetExprNode, Var))
		{
			Var *targetVar = (Var *) targetExprNode;
			targetVar->varattno = targetEntryNo;
			continue;
		}

		/* replace the original expression with a Var referencing values_lists */
		Var *syntheticVar = makeVar(valuesVarno, targetEntryNo, targetType, targetTypmod,
									targetColl, 0);
		targetEntry->expr = (Expr *) syntheticVar;

		/*
		 * Postgres appends a dummy column reference into valuesRTE->eref->colnames
		 * list in addRangeTableEntryForValues for each column specified in VALUES
		 * clause. Now that we replaced DEFAULT column with a synthetic Var, we also
		 * need to add a dummy column reference for that column.
		 */
		AppendNextDummyColReference(valuesRTE->eref);
	}
}


/*
 * AppendNextDummyColReference appends a new dummy column reference to colnames
 * list of given Alias object.
 */
static void
AppendNextDummyColReference(Alias *expendedReferenceNames)
{
	int existingColReferences = list_length(expendedReferenceNames->colnames);
	int nextColReferenceId = existingColReferences + 1;
	String *missingColumnString = MakeDummyColumnString(nextColReferenceId);
	expendedReferenceNames->colnames = lappend(expendedReferenceNames->colnames,
											   missingColumnString);
}


/*
 * MakeDummyColumnString returns a String (Value) object by appending given
 * integer to end of the "column" string.
 */
static String *
MakeDummyColumnString(int dummyColumnId)
{
	StringInfo dummyColumnStringInfo = makeStringInfo();
	appendStringInfo(dummyColumnStringInfo, "column%d", dummyColumnId);
	String *dummyColumnString = makeString(dummyColumnStringInfo->data);

	return dummyColumnString;
}


/*
 * IntersectPlacementList performs placement pruning based on matching on
 * nodeName:nodePort fields of shard placement data. We start pruning from all
 * placements of the first relation's shard. Then for each relation's shard, we
 * compute intersection of the new shards placement with existing placement list.
 * This operation could have been done using other methods, but since we do not
 * expect very high replication factor, iterating over a list and making string
 * comparisons should be sufficient.
 */
List *
IntersectPlacementList(List *lhsPlacementList, List *rhsPlacementList)
{
	ListCell *lhsPlacementCell = NULL;
	List *placementList = NIL;

	/* Keep existing placement in the list if it is also present in new placement list */
	foreach(lhsPlacementCell, lhsPlacementList)
	{
		ShardPlacement *lhsPlacement = (ShardPlacement *) lfirst(lhsPlacementCell);
		ListCell *rhsPlacementCell = NULL;
		foreach(rhsPlacementCell, rhsPlacementList)
		{
			ShardPlacement *rhsPlacement = (ShardPlacement *) lfirst(rhsPlacementCell);
			if (rhsPlacement->nodePort == lhsPlacement->nodePort &&
				strncmp(rhsPlacement->nodeName, lhsPlacement->nodeName,
						WORKER_LENGTH) == 0)
			{
				placementList = lappend(placementList, rhsPlacement);

				/*
				 * We don't need to add the same placement over and over again. This
				 * could happen if both placements of a shard appear on the same node.
				 */
				break;
			}
		}
	}

	return placementList;
}


/*
 * GroupInsertValuesByShardId takes care of grouping the rows from a multi-row
 * INSERT by target shard. At this point, all pruning has taken place and we
 * need only to build sets of rows for each destination. This is done by a
 * simple sort (by shard identifier) and gather step. The sort has the side-
 * effect of getting things in ascending order to avoid unnecessary deadlocks
 * during Task execution.
 */
static List *
GroupInsertValuesByShardId(List *insertValuesList)
{
	ModifyRoute *route = NULL;
	ListCell *insertValuesCell = NULL;
	List *modifyRouteList = NIL;

	insertValuesList = SortList(insertValuesList, CompareInsertValuesByShardId);
	foreach(insertValuesCell, insertValuesList)
	{
		InsertValues *insertValues = (InsertValues *) lfirst(insertValuesCell);
		int64 shardId = insertValues->shardId;
		bool foundSameShardId = false;

		if (route != NULL)
		{
			if (route->shardId == shardId)
			{
				foundSameShardId = true;
			}
			else
			{
				/* new shard id seen; current aggregation done; add to list */
				modifyRouteList = lappend(modifyRouteList, route);
			}
		}

		if (foundSameShardId)
		{
			/*
			 * Our current value has the same shard id as our aggregate object,
			 * so append the rowValues.
			 */
			route->rowValuesLists = lappend(route->rowValuesLists,
											insertValues->rowValues);
		}
		else
		{
			/* we encountered a new shard id; build a new aggregate object */
			route = (ModifyRoute *) palloc(sizeof(ModifyRoute));
			route->shardId = insertValues->shardId;
			route->rowValuesLists = list_make1(insertValues->rowValues);
		}
	}

	/* left holding one final aggregate object; add to list */
	modifyRouteList = lappend(modifyRouteList, route);

	return modifyRouteList;
}


/*
 * ExtractInsertValuesList extracts the partition column value for an INSERT
 * command and returns it within an InsertValues struct. For single-row INSERTs
 * this is simply a value extracted from the target list, but multi-row INSERTs
 * will generate a List of InsertValues, each with full row values in addition
 * to the partition value. If a partition value is NULL or missing altogether,
 * this function errors.
 */
static List *
ExtractInsertValuesList(Query *query, Var *partitionColumn)
{
	List *insertValuesList = NIL;
	TargetEntry *targetEntry = get_tle_by_resno(query->targetList,
												partitionColumn->varattno);

	if (targetEntry == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot perform an INSERT without a partition column "
							   "value")));
	}

	/*
	 * We've got a multi-row INSERT. PostgreSQL internally represents such
	 * commands by linking Vars in the target list to lists of values within
	 * a special VALUES range table entry. By extracting the right positional
	 * expression from each list within that RTE, we will extract the partition
	 * values for each row within the multi-row INSERT.
	 */
	if (IsA(targetEntry->expr, Var))
	{
		Var *partitionVar = (Var *) targetEntry->expr;
		ListCell *valuesListCell = NULL;
		Index ivIndex = 0;

		RangeTblEntry *referencedRTE = rt_fetch(partitionVar->varno, query->rtable);
		foreach(valuesListCell, referencedRTE->values_lists)
		{
			InsertValues *insertValues = (InsertValues *) palloc(sizeof(InsertValues));
			insertValues->rowValues = (List *) lfirst(valuesListCell);
			insertValues->partitionValueExpr = list_nth(insertValues->rowValues,
														(partitionVar->varattno - 1));
			insertValues->shardId = INVALID_SHARD_ID;
			insertValues->listIndex = ivIndex;

			insertValuesList = lappend(insertValuesList, insertValues);
			ivIndex++;
		}
	}

	/* nothing's been found yet; this is a simple single-row INSERT */
	if (insertValuesList == NIL)
	{
		InsertValues *insertValues = (InsertValues *) palloc(sizeof(InsertValues));
		insertValues->rowValues = NIL;
		insertValues->partitionValueExpr = targetEntry->expr;
		insertValues->shardId = INVALID_SHARD_ID;

		insertValuesList = lappend(insertValuesList, insertValues);
	}

	return insertValuesList;
}


/*
 * ExtractInsertPartitionKeyValue extracts the partition column value
 * from an INSERT query. If the expression in the partition column is
 * non-constant or it is a multi-row INSERT with multiple different partition
 * column values, the function returns NULL.
 */
Const *
ExtractInsertPartitionKeyValue(Query *query)
{
	Oid distributedTableId = ExtractFirstCitusTableId(query);
	uint32 rangeTableId = 1;
	Const *singlePartitionValueConst = NULL;

	if (IsCitusTableType(distributedTableId, CITUS_TABLE_WITH_NO_DIST_KEY))
	{
		return NULL;
	}

	Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	TargetEntry *targetEntry = get_tle_by_resno(query->targetList,
												partitionColumn->varattno);
	if (targetEntry == NULL)
	{
		/* partition column value not specified */
		return NULL;
	}

	Node *targetExpression = strip_implicit_coercions((Node *) targetEntry->expr);

	/*
	 * Multi-row INSERTs have a Var in the target list that points to
	 * an RTE_VALUES.
	 */
	if (IsA(targetExpression, Var))
	{
		Var *partitionVar = (Var *) targetExpression;
		ListCell *valuesListCell = NULL;

		RangeTblEntry *referencedRTE = rt_fetch(partitionVar->varno, query->rtable);

		foreach(valuesListCell, referencedRTE->values_lists)
		{
			List *rowValues = (List *) lfirst(valuesListCell);
			Node *partitionValueNode = list_nth(rowValues, partitionVar->varattno - 1);
			Expr *partitionValueExpr = (Expr *) strip_implicit_coercions(
				partitionValueNode);

			if (!IsA(partitionValueExpr, Const))
			{
				/* non-constant value in the partition column */
				singlePartitionValueConst = NULL;
				break;
			}

			Const *partitionValueConst = (Const *) partitionValueExpr;

			if (singlePartitionValueConst == NULL)
			{
				/* first row has a constant in the partition column, looks promising! */
				singlePartitionValueConst = partitionValueConst;
			}
			else if (!equal(partitionValueConst, singlePartitionValueConst))
			{
				/* multiple different values in the partition column, too bad */
				singlePartitionValueConst = NULL;
				break;
			}
			else
			{
				/* another row with the same partition column value! */
			}
		}
	}
	else if (IsA(targetExpression, Const))
	{
		/* single-row INSERT with a constant partition column value */
		singlePartitionValueConst = (Const *) targetExpression;
	}
	else
	{
		/* single-row INSERT with a non-constant partition column value */
		singlePartitionValueConst = NULL;
	}

	if (singlePartitionValueConst != NULL)
	{
		singlePartitionValueConst = copyObject(singlePartitionValueConst);
	}

	return singlePartitionValueConst;
}


/*
 * DeferErrorIfUnsupportedRouterPlannableSelectQuery checks if given query is router plannable,
 * SELECT query, setting distributedPlan->planningError if not.
 * The query is router plannable if it is a modify query, or if it is a select
 * query issued on a hash partitioned distributed table. Router plannable checks
 * for select queries can be turned off by setting citus.enable_router_execution
 * flag to false.
 */
static DeferredErrorMessage *
DeferErrorIfUnsupportedRouterPlannableSelectQuery(Query *query)
{
	List *rangeTableRelationList = NIL;
	ListCell *rangeTableRelationCell = NULL;

	if (query->commandType != CMD_SELECT)
	{
		return DeferredError(ERRCODE_ASSERT_FAILURE,
							 "Only SELECT query types are supported in this path",
							 NULL, NULL);
	}

	if (!EnableRouterExecution)
	{
		return DeferredError(ERRCODE_SUCCESSFUL_COMPLETION,
							 "Router planner not enabled.",
							 NULL, NULL);
	}

	if (contain_nextval_expression_walker((Node *) query->targetList, NULL))
	{
		/*
		 * We let queries with nextval in the target list fall through to
		 * the logical planner, which knows how to handle those queries.
		 */
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "Sequences cannot be used in router queries",
							 NULL, NULL);
	}

	bool hasPostgresOrCitusLocalTable = false;
	bool hasDistributedTable = false;

	ExtractRangeTableRelationWalker((Node *) query, &rangeTableRelationList);
	foreach(rangeTableRelationCell, rangeTableRelationList)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rangeTableRelationCell);
		if (rte->rtekind == RTE_RELATION)
		{
			Oid distributedTableId = rte->relid;

			/* local tables are allowed if there are no distributed tables */
			if (!IsCitusTable(distributedTableId))
			{
				hasPostgresOrCitusLocalTable = true;
				continue;
			}
			else if (IsCitusTableType(distributedTableId, CITUS_LOCAL_TABLE))
			{
				hasPostgresOrCitusLocalTable = true;
				elog(DEBUG4, "Router planner finds a local table added to metadata");
				continue;
			}

			if (IsCitusTableType(distributedTableId, APPEND_DISTRIBUTED))
			{
				return DeferredError(
					ERRCODE_FEATURE_NOT_SUPPORTED,
					"Router planner does not support append-partitioned tables.",
					NULL, NULL);
			}

			if (IsCitusTableType(distributedTableId, DISTRIBUTED_TABLE))
			{
				hasDistributedTable = true;
			}

			/*
			 * Currently, we don't support tables with replication factor > 1,
			 * except reference tables with SELECT ... FOR UPDATE queries. It is
			 * also not supported from MX nodes.
			 */
			if (query->hasForUpdate)
			{
				uint32 tableReplicationFactor = TableShardReplicationFactor(
					distributedTableId);

				if (tableReplicationFactor > 1 && IsCitusTableType(distributedTableId,
																   DISTRIBUTED_TABLE))
				{
					return DeferredError(
						ERRCODE_FEATURE_NOT_SUPPORTED,
						"SELECT FOR UPDATE with table replication factor > 1 not supported for non-reference tables.",
						NULL, NULL);
				}
			}
		}
	}

	/* local tables are not allowed if there are distributed tables */
	if (hasPostgresOrCitusLocalTable && hasDistributedTable)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "Local tables cannot be used in distributed queries.",
							 NULL, NULL);
	}

#if PG_VERSION_NUM >= PG_VERSION_14
	DeferredErrorMessage *CTEWithSearchClauseError =
		ErrorIfQueryHasCTEWithSearchClause(query);
	if (CTEWithSearchClauseError != NULL)
	{
		return CTEWithSearchClauseError;
	}
#endif

	return ErrorIfQueryHasUnroutableModifyingCTE(query);
}


/*
 * Copy a RelationRestrictionContext. Note that several subfields are copied
 * shallowly, for lack of copyObject support.
 *
 * Note that CopyRelationRestrictionContext copies the following fields per relation
 * context: index, relationId, distributedRelation, rte, relOptInfo->baserestrictinfo
 * and relOptInfo->joininfo. Also, the function shallowly copies plannerInfo and
 * prunedShardIntervalList which are read-only. All other parts of the relOptInfo
 * is also shallowly copied.
 */
RelationRestrictionContext *
CopyRelationRestrictionContext(RelationRestrictionContext *oldContext)
{
	RelationRestrictionContext *newContext =
		(RelationRestrictionContext *) palloc(sizeof(RelationRestrictionContext));
	ListCell *relationRestrictionCell = NULL;

	newContext->allReferenceTables = oldContext->allReferenceTables;
	newContext->relationRestrictionList = NIL;

	foreach(relationRestrictionCell, oldContext->relationRestrictionList)
	{
		RelationRestriction *oldRestriction =
			(RelationRestriction *) lfirst(relationRestrictionCell);
		RelationRestriction *newRestriction = (RelationRestriction *)
											  palloc0(sizeof(RelationRestriction));

		newRestriction->index = oldRestriction->index;
		newRestriction->relationId = oldRestriction->relationId;
		newRestriction->distributedRelation = oldRestriction->distributedRelation;
		newRestriction->rte = copyObject(oldRestriction->rte);

		/* can't be copied, we copy (flatly) a RelOptInfo, and then decouple baserestrictinfo */
		newRestriction->relOptInfo = palloc(sizeof(RelOptInfo));
		*newRestriction->relOptInfo = *oldRestriction->relOptInfo;

		newRestriction->relOptInfo->baserestrictinfo =
			copyObject(oldRestriction->relOptInfo->baserestrictinfo);

		newRestriction->relOptInfo->joininfo =
			copyObject(oldRestriction->relOptInfo->joininfo);

		/* not copyable, but readonly */
		newRestriction->plannerInfo = oldRestriction->plannerInfo;

		newContext->relationRestrictionList =
			lappend(newContext->relationRestrictionList, newRestriction);
	}

	return newContext;
}


/*
 * ErrorIfQueryHasUnroutableModifyingCTE checks if the query contains modifying common table
 * expressions and errors out if it does.
 */
static DeferredErrorMessage *
ErrorIfQueryHasUnroutableModifyingCTE(Query *queryTree)
{
	Assert(queryTree->commandType == CMD_SELECT);

	if (!queryTree->hasModifyingCTE)
	{
		return NULL;
	}

	/* we can't route conflicting replication models */
	char replicationModel = 0;

	CommonTableExpr *cte = NULL;
	foreach_ptr(cte, queryTree->cteList)
	{
		Query *cteQuery = (Query *) cte->ctequery;

		/*
		 * Here we only check for command type of top level query. Normally there can be
		 * nested CTE, however PostgreSQL dictates that data-modifying statements must
		 * be at top level of CTE. Therefore it is OK to just check for top level.
		 * Similarly, we do not need to check for subqueries.
		 */
		if (cteQuery->commandType != CMD_SELECT &&
			cteQuery->commandType != CMD_UPDATE &&
			cteQuery->commandType != CMD_DELETE)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "only SELECT, UPDATE, or DELETE common table expressions "
								 "may be router planned",
								 NULL, NULL);
		}

		if (cteQuery->commandType != CMD_SELECT)
		{
			Oid distributedTableId = InvalidOid;
			DeferredErrorMessage *cteError =
				ModifyPartialQuerySupported(cteQuery, false, &distributedTableId);
			if (cteError)
			{
				return cteError;
			}

			CitusTableCacheEntry *modificationTableCacheEntry =
				GetCitusTableCacheEntry(distributedTableId);

			if (IsCitusTableTypeCacheEntry(modificationTableCacheEntry,
										   CITUS_TABLE_WITH_NO_DIST_KEY))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "cannot router plan modification of a non-distributed table",
									 NULL, NULL);
			}

			if (replicationModel &&
				modificationTableCacheEntry->replicationModel != replicationModel)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "cannot route mixed replication models",
									 NULL, NULL);
			}

			replicationModel = modificationTableCacheEntry->replicationModel;
		}
	}

	/* everything OK */
	return NULL;
}


#if PG_VERSION_NUM >= PG_VERSION_14

/*
 * ErrorIfQueryHasCTEWithSearchClause checks if the query contains any common table
 * expressions with search clause and errors out if it does.
 */
static DeferredErrorMessage *
ErrorIfQueryHasCTEWithSearchClause(Query *queryTree)
{
	if (ContainsSearchClauseWalker((Node *) queryTree))
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "CTEs with search clauses are not supported",
							 NULL, NULL);
	}
	return NULL;
}


/*
 * ContainsSearchClauseWalker walks over the node and finds if there are any
 * CommonTableExprs with search clause
 */
static bool
ContainsSearchClauseWalker(Node *node)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, CommonTableExpr))
	{
		if (((CommonTableExpr *) node)->search_clause != NULL)
		{
			return true;
		}
	}

	if (IsA(node, Query))
	{
		return query_tree_walker((Query *) node, ContainsSearchClauseWalker, NULL, 0);
	}

	return expression_tree_walker(node, ContainsSearchClauseWalker, NULL);
}


#endif


/*
 * get_all_actual_clauses
 *
 * Returns a list containing the bare clauses from 'restrictinfo_list'.
 *
 * This loses the distinction between regular and pseudoconstant clauses,
 * so be careful what you use it for.
 */
List *
get_all_actual_clauses(List *restrictinfo_list)
{
	List *result = NIL;
	ListCell *l;

	foreach(l, restrictinfo_list)
	{
		RestrictInfo *rinfo = (RestrictInfo *) lfirst(l);

		Assert(IsA(rinfo, RestrictInfo));

		result = lappend(result, rinfo->clause);
	}
	return result;
}


/*
 * CompareInsertValuesByShardId does what it says in the name. Used for sorting
 * InsertValues objects by their shard.
 */
static int
CompareInsertValuesByShardId(const void *leftElement, const void *rightElement)
{
	InsertValues *leftValue = *((InsertValues **) leftElement);
	InsertValues *rightValue = *((InsertValues **) rightElement);
	int64 leftShardId = leftValue->shardId;
	int64 rightShardId = rightValue->shardId;
	Index leftIndex = leftValue->listIndex;
	Index rightIndex = rightValue->listIndex;

	if (leftShardId > rightShardId)
	{
		return 1;
	}
	else if (leftShardId < rightShardId)
	{
		return -1;
	}
	else
	{
		/* shard identifiers are the same, list index is secondary sort key */
		if (leftIndex > rightIndex)
		{
			return 1;
		}
		else if (leftIndex < rightIndex)
		{
			return -1;
		}
		else
		{
			return 0;
		}
	}
}
