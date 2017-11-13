/*-------------------------------------------------------------------------
 *
 * multi_router_planner.c
 *
 * This file contains functions to plan multiple shard queries without any
 * aggregation step including distributed table modifications.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include <stddef.h>

#include "access/stratnum.h"
#include "access/xact.h"
#include "catalog/pg_opfamily.h"
#include "distributed/citus_clauses.h"
#include "catalog/pg_type.h"
#include "distributed/colocation_utils.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_nodefuncs.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distribution_column.h"
#include "distributed/errormessage.h"
#include "distributed/insert_select_planner.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/listutils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/relay_utility.h"
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
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parsetree.h"
#include "parser/parse_oper.h"
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
 * A ModifyRoute encapsulates the the information needed to route modifications
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
static DistributedPlan * CreateSingleTaskRouterPlan(Query *originalQuery,
													Query *query,
													RelationRestrictionContext *
													restrictionContext);
static bool MasterIrreducibleExpression(Node *expression, bool *varArgument,
										bool *badCoalesce);
static bool MasterIrreducibleExpressionWalker(Node *expression, WalkerState *state);
static bool MasterIrreducibleExpressionFunctionChecker(Oid func_id, void *context);
static bool TargetEntryChangesValue(TargetEntry *targetEntry, Var *column,
									FromExpr *joinTree);
static Job * RouterInsertJob(Query *originalQuery, Query *query,
							 DeferredErrorMessage **planningError);
static void ErrorIfNoShardsExist(DistTableCacheEntry *cacheEntry);
static bool CanShardPrune(Oid distributedTableId, Query *query);
static Job * CreateJob(Query *query);
static Task * CreateTask(TaskType taskType);
static Job * RouterJob(Query *originalQuery,
					   RelationRestrictionContext *restrictionContext,
					   DeferredErrorMessage **planningError);
static bool RelationPrunesToMultipleShards(List *relationShardList);
static List * TargetShardIntervalsForRouter(Query *query,
											RelationRestrictionContext *restrictionContext,
											bool *multiShardQuery);
static List * WorkersContainingAllShards(List *prunedShardIntervalsList);
static void NormalizeMultiRowInsertTargetList(Query *query);
static List * BuildRoutesForInsert(Query *query, DeferredErrorMessage **planningError);
static List * GroupInsertValuesByShardId(List *insertValuesList);
static List * ExtractInsertValuesList(Query *query, Var *partitionColumn);
static bool MultiRouterPlannableQuery(Query *query,
									  RelationRestrictionContext *restrictionContext);
static DeferredErrorMessage * ErrorIfQueryHasModifyingCTE(Query *queryTree);
static RangeTblEntry * GetUpdateOrDeleteRTE(List *rangeTableList);
static bool UpdateOrDeleteRTE(RangeTblEntry *rangeTableEntry);
static bool SelectsFromDistributedTable(List *rangeTableList);
#if (PG_VERSION_NUM >= 100000)
static List * get_all_actual_clauses(List *restrictinfo_list);
#endif
static int CompareInsertValuesByShardId(const void *leftElement,
										const void *rightElement);
static uint64 GetInitialShardId(List *relationShardList);
static List * SingleShardSelectTaskList(Query *query, List *relationShardList,
										List *placementList, uint64 shardId);
static List * SingleShardModifyTaskList(Query *query, List *relationShardList,
										List *placementList, uint64 shardId);
static List * MultiShardModifyTaskList(Query *originalQuery, List *relationShardList,
									   bool requiresMasterEvaluation);

/*
 * CreateRouterPlan attempts to create a router executor plan for the given
 * SELECT statement.  If planning fails either NULL is returned, or
 * ->planningError is set to a description of the failure.
 */
DistributedPlan *
CreateRouterPlan(Query *originalQuery, Query *query,
				 RelationRestrictionContext *restrictionContext)
{
	Assert(EnableRouterExecution);

	if (MultiRouterPlannableQuery(query, restrictionContext))
	{
		return CreateSingleTaskRouterPlan(originalQuery, query,
										  restrictionContext);
	}

	/*
	 * TODO: Instead have MultiRouterPlannableQuery set an error describing
	 * why router cannot support the query.
	 */
	return NULL;
}


/*
 * CreateModifyPlan attempts to create a plan the given modification
 * statement.  If planning fails ->planningError is set to a description of
 * the failure.
 */
DistributedPlan *
CreateModifyPlan(Query *originalQuery, Query *query,
				 PlannerRestrictionContext *plannerRestrictionContext)
{
	Job *job = NULL;
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);
	bool multiShardQuery = false;

	distributedPlan->operation = query->commandType;

	distributedPlan->planningError = ModifyQuerySupported(query, multiShardQuery);
	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
	}

	if (UpdateOrDeleteQuery(query))
	{
		RelationRestrictionContext *restrictionContext =
			plannerRestrictionContext->relationRestrictionContext;

		job = RouterJob(originalQuery, restrictionContext,
						&distributedPlan->planningError);
	}
	else
	{
		job = RouterInsertJob(originalQuery, query, &distributedPlan->planningError);
	}

	if (distributedPlan->planningError != NULL)
	{
		return distributedPlan;
	}

	ereport(DEBUG2, (errmsg("Creating router plan")));

	distributedPlan->workerJob = job;
	distributedPlan->masterQuery = NULL;
	distributedPlan->routerExecutable = true;
	distributedPlan->hasReturning = false;

	if (list_length(originalQuery->returningList) > 0)
	{
		distributedPlan->hasReturning = true;
	}

	return distributedPlan;
}


/*
 * CreateSingleTaskRouterPlan creates a physical plan for given query. The created plan is
 * either a modify task that changes a single shard, or a router task that returns
 * query results from a single worker. Supported modify queries (insert/update/delete)
 * are router plannable by default. If query is not router plannable then either NULL is
 * returned, or the returned plan has planningError set to a description of the problem.
 */
static DistributedPlan *
CreateSingleTaskRouterPlan(Query *originalQuery, Query *query,
						   RelationRestrictionContext *restrictionContext)
{
	Job *job = NULL;
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);

	distributedPlan->operation = query->commandType;

	/* FIXME: this should probably rather be inlined into CreateRouterPlan */
	distributedPlan->planningError = ErrorIfQueryHasModifyingCTE(query);
	if (distributedPlan->planningError)
	{
		return distributedPlan;
	}

	/* we cannot have multi shard update/delete query via this code path */
	job = RouterJob(originalQuery, restrictionContext, &distributedPlan->planningError);

	if (distributedPlan->planningError)
	{
		/* query cannot be handled by this planner */
		return NULL;
	}

	ereport(DEBUG2, (errmsg("Creating router plan")));

	distributedPlan->workerJob = job;
	distributedPlan->masterQuery = NULL;
	distributedPlan->routerExecutable = true;
	distributedPlan->hasReturning = false;

	return distributedPlan;
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
	char partitionMethod = PartitionMethod(shardInterval->relationId);
	Var *partitionColumn = NULL;
	Node *baseConstraint = NULL;

	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		partitionColumn = MakeInt4Column();
	}
	else if (partitionMethod == DISTRIBUTE_BY_RANGE || partitionMethod ==
			 DISTRIBUTE_BY_APPEND)
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
	baseConstraint = BuildBaseConstraint(partitionColumn);

	/* walk over shard list and check if shards can be pruned */
	if (shardInterval->minValueExists && shardInterval->maxValueExists)
	{
		UpdateConstraint(baseConstraint, shardInterval);
	}

	return list_make1(baseConstraint);
}


/*
 * AddShardIntervalRestrictionToSelect adds the following range boundaries
 * with the given subquery and shardInterval:
 *
 *    hashfunc(partitionColumn) >= $lower_bound AND
 *    hashfunc(partitionColumn) <= $upper_bound
 *
 * The function expects and asserts that subquery's target list contains a partition
 * column value. Thus, this function should never be called with reference tables.
 */
void
AddShardIntervalRestrictionToSelect(Query *subqery, ShardInterval *shardInterval)
{
	List *targetList = subqery->targetList;
	ListCell *targetEntryCell = NULL;
	Var *targetPartitionColumnVar = NULL;
	Oid integer4GEoperatorId = InvalidOid;
	Oid integer4LEoperatorId = InvalidOid;
	TypeCacheEntry *typeEntry = NULL;
	FuncExpr *hashFunctionExpr = NULL;
	OpExpr *greaterThanAndEqualsBoundExpr = NULL;
	OpExpr *lessThanAndEqualsBoundExpr = NULL;
	List *boundExpressionList = NIL;
	Expr *andedBoundExpressions = NULL;

	/* iterate through the target entries */
	foreach(targetEntryCell, targetList)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);

		if (IsPartitionColumn(targetEntry->expr, subqery) &&
			IsA(targetEntry->expr, Var))
		{
			targetPartitionColumnVar = (Var *) targetEntry->expr;
			break;
		}
	}

	/* we should have found target partition column */
	Assert(targetPartitionColumnVar != NULL);

	integer4GEoperatorId = get_opfamily_member(INTEGER_BTREE_FAM_OID, INT4OID,
											   INT4OID,
											   BTGreaterEqualStrategyNumber);
	integer4LEoperatorId = get_opfamily_member(INTEGER_BTREE_FAM_OID, INT4OID,
											   INT4OID,
											   BTLessEqualStrategyNumber);

	/* ensure that we find the correct operators */
	Assert(integer4GEoperatorId != InvalidOid);
	Assert(integer4LEoperatorId != InvalidOid);

	/* look up the type cache */
	typeEntry = lookup_type_cache(targetPartitionColumnVar->vartype,
								  TYPECACHE_HASH_PROC_FINFO);

	/* probable never possible given that the tables are already hash partitioned */
	if (!OidIsValid(typeEntry->hash_proc_finfo.fn_oid))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_FUNCTION),
						errmsg("could not identify a hash function for type %s",
							   format_type_be(targetPartitionColumnVar->vartype))));
	}

	/* generate hashfunc(partCol) expression */
	hashFunctionExpr = makeNode(FuncExpr);
	hashFunctionExpr->funcid = CitusWorkerHashFunctionId();
	hashFunctionExpr->args = list_make1(targetPartitionColumnVar);

	/* hash functions always return INT4 */
	hashFunctionExpr->funcresulttype = INT4OID;

	/* generate hashfunc(partCol) >= shardMinValue OpExpr */
	greaterThanAndEqualsBoundExpr =
		(OpExpr *) make_opclause(integer4GEoperatorId,
								 InvalidOid, false,
								 (Expr *) hashFunctionExpr,
								 (Expr *) MakeInt4Constant(shardInterval->minValue),
								 targetPartitionColumnVar->varcollid,
								 targetPartitionColumnVar->varcollid);

	/* update the operators with correct operator numbers and function ids */
	greaterThanAndEqualsBoundExpr->opfuncid =
		get_opcode(greaterThanAndEqualsBoundExpr->opno);
	greaterThanAndEqualsBoundExpr->opresulttype =
		get_func_rettype(greaterThanAndEqualsBoundExpr->opfuncid);

	/* generate hashfunc(partCol) <= shardMinValue OpExpr */
	lessThanAndEqualsBoundExpr =
		(OpExpr *) make_opclause(integer4LEoperatorId,
								 InvalidOid, false,
								 (Expr *) hashFunctionExpr,
								 (Expr *) MakeInt4Constant(shardInterval->maxValue),
								 targetPartitionColumnVar->varcollid,
								 targetPartitionColumnVar->varcollid);

	/* update the operators with correct operator numbers and function ids */
	lessThanAndEqualsBoundExpr->opfuncid = get_opcode(lessThanAndEqualsBoundExpr->opno);
	lessThanAndEqualsBoundExpr->opresulttype =
		get_func_rettype(lessThanAndEqualsBoundExpr->opfuncid);

	/* finally add the operators to a list and make them explicitly anded */
	boundExpressionList = lappend(boundExpressionList, greaterThanAndEqualsBoundExpr);
	boundExpressionList = lappend(boundExpressionList, lessThanAndEqualsBoundExpr);

	andedBoundExpressions = make_ands_explicit(boundExpressionList);

	/* finally add the quals */
	if (subqery->jointree->quals == NULL)
	{
		subqery->jointree->quals = (Node *) andedBoundExpressions;
	}
	else
	{
		subqery->jointree->quals = make_and_qual(subqery->jointree->quals,
												 (Node *) andedBoundExpressions);
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
	List *fromList = NULL;
	RangeTblRef *reference = NULL;
	RangeTblEntry *subqueryRte = NULL;

	Assert(InsertSelectIntoDistributedTable(query));

	/*
	 * Since we already asserted InsertSelectIntoDistributedTable() it is safe to access
	 * both lists
	 */
	fromList = query->jointree->fromlist;
	reference = linitial(fromList);
	subqueryRte = rt_fetch(reference->rtindex, query->rtable);

	return subqueryRte;
}


/*
 * ExtractInsertRangeTableEntry returns the INSERT'ed table's range table entry.
 * Note that the function expects and asserts that the input query be
 * an INSERT...SELECT query.
 */
RangeTblEntry *
ExtractInsertRangeTableEntry(Query *query)
{
	int resultRelation = query->resultRelation;
	List *rangeTableList = query->rtable;
	RangeTblEntry *insertRTE = NULL;

	insertRTE = rt_fetch(resultRelation, rangeTableList);

	return insertRTE;
}


/*
 * ModifyQuerySupported returns NULL if the query only contains supported
 * features, otherwise it returns an error description.
 */
DeferredErrorMessage *
ModifyQuerySupported(Query *queryTree, bool multiShardQuery)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(queryTree);
	uint32 rangeTableId = 1;
	Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	bool isCoordinator = IsCoordinator();
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	uint32 queryTableCount = 0;
	bool specifiesPartitionValue = false;
	ListCell *setTargetCell = NULL;
	List *onConflictSet = NIL;
	Node *arbiterWhere = NULL;
	Node *onConflictWhere = NULL;

	CmdType commandType = queryTree->commandType;

	/*
	 * Reject subqueries which are in SELECT or WHERE clause.
	 * Queries which include subqueries in FROM clauses are rejected below.
	 */
	if (queryTree->hasSubLinks == true)
	{
		/*
		 * We support UPDATE and DELETE with subqueries unless they are multi
		 * shard queries.
		 */
		if (!UpdateOrDeleteQuery(queryTree) || multiShardQuery)
		{
			StringInfo errorHint = makeStringInfo();
			DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(
				distributedTableId);
			char *partitionKeyString = cacheEntry->partitionKeyString;
			char *partitionColumnName = ColumnNameToColumn(distributedTableId,
														   partitionKeyString);

			appendStringInfo(errorHint,
							 "Consider using an equality filter on partition column \"%s\" to target a single shard.",
							 partitionColumnName);

			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "subqueries are not supported in modifications across "
								 "multiple shards",
								 errorHint->data, NULL);
		}
	}

	/* reject queries which include CommonTableExpr */
	if (queryTree->cteList != NIL)
	{
		return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
							 "common table expressions are not supported in distributed "
							 "modifications",
							 NULL, NULL);
	}

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		bool referenceTable = false;

		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			/*
			 * We are sure that the table should be distributed, therefore no need to
			 * call IsDistributedTable() here and DistributedTableCacheEntry will
			 * error out if the table is not distributed
			 */
			DistTableCacheEntry *distTableEntry =
				DistributedTableCacheEntry(rangeTableEntry->relid);

			if (distTableEntry->partitionMethod == DISTRIBUTE_BY_NONE)
			{
				referenceTable = true;
			}

			if (referenceTable && !isCoordinator)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "cannot perform distributed planning for the given"
									 " modification",
									 "Modifications to reference tables are "
									 "supported only from the coordinator.",
									 NULL);
			}

			queryTableCount++;

			/* we do not expect to see a view in modify query */
			if (rangeTableEntry->relkind == RELKIND_VIEW)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "cannot modify views over distributed tables",
									 NULL, NULL);
			}
		}
		else if (rangeTableEntry->rtekind == RTE_VALUES)
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
			if (UpdateOrDeleteQuery(queryTree) && !multiShardQuery)
			{
				continue;
			}

			/*
			 * Error out for rangeTableEntries that we do not support.
			 * We do not explicitly specify "in FROM clause" in the error detail
			 * for the features that we do not support at all (SUBQUERY, JOIN).
			 * We do not need to check for RTE_CTE because all common table expressions
			 * are rejected above with queryTree->cteList check.
			 */
			if (rangeTableEntry->rtekind == RTE_SUBQUERY)
			{
				StringInfo errorHint = makeStringInfo();
				DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(
					distributedTableId);
				char *partitionKeyString = cacheEntry->partitionKeyString;
				char *partitionColumnName = ColumnNameToColumn(distributedTableId,
															   partitionKeyString);

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

	/*
	 * Reject queries which involve joins. Note that UPSERTs are exceptional for this case.
	 * Queries like "INSERT INTO table_name ON CONFLICT DO UPDATE (col) SET other_col = ''"
	 * contains two range table entries, and we have to allow them.
	 */
	if (commandType != CMD_INSERT && queryTableCount != 1)
	{
		/*
		 * We support UPDATE and DELETE with joins unless they are multi shard
		 * queries.
		 */
		if (!UpdateOrDeleteQuery(queryTree) || multiShardQuery)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "cannot perform distributed planning for the given "
								 "modification",
								 "Joins are not supported in distributed "
								 "modifications.", NULL);
		}
	}

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
			bool targetEntryPartitionColumn = false;

			/* reference tables do not have partition column */
			if (partitionColumn == NULL)
			{
				targetEntryPartitionColumn = false;
			}
			else if (targetEntry->resno == partitionColumn->varattno)
			{
				targetEntryPartitionColumn = true;
			}

			/* skip resjunk entries: UPDATE adds some for ctid, etc. */
			if (targetEntry->resjunk)
			{
				continue;
			}

			if (commandType == CMD_UPDATE &&
				contain_volatile_functions((Node *) targetEntry->expr))
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
				specifiesPartitionValue = true;
			}

			if (commandType == CMD_UPDATE &&
				MasterIrreducibleExpression((Node *) targetEntry->expr,
											&hasVarArgument, &hasBadCoalesce))
			{
				Assert(hasVarArgument || hasBadCoalesce);
			}
		}

		if (joinTree != NULL)
		{
			if (contain_volatile_functions(joinTree->quals))
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

	if (commandType == CMD_INSERT && queryTree->onConflict != NULL)
	{
		onConflictSet = queryTree->onConflict->onConflictSet;
		arbiterWhere = queryTree->onConflict->arbiterWhere;
		onConflictWhere = queryTree->onConflict->onConflictWhere;
	}

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
		else if (setTargetEntry->resno == partitionColumn->varattno)
		{
			setTargetEntryPartitionColumn = true;
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
 * UpdateOrDeleteQuery checks if the given query is an UPDATE or DELETE command.
 * If it is, it returns true otherwise it returns false.
 */
bool
UpdateOrDeleteQuery(Query *query)
{
	CmdType commandType = query->commandType;

	if (commandType == CMD_UPDATE || commandType == CMD_DELETE)
	{
		return true;
	}

	return false;
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
	bool result;
	WalkerState data;
	data.containsVar = data.varArgument = data.badCoalesce = false;

	result = MasterIrreducibleExpressionWalker(expression, &data);

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
	 * that we either disallow or evaluate on the master anything which has a volatility
	 * category above IMMUTABLE. Newer versions of postgres might add node types which
	 * should be checked in this function.
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

	/* subqueries aren't allowed and should fail before control reaches this point */
	Assert(!IsA(expression, Query));

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

	if (targetEntry->resno != column->varattno)
	{
		/* target entry of the form SET some_other_col = <x> */
		isColumnValueChanged = false;
	}
	else if (IsA(setExpr, Var))
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
		Const *rightConst = (Const *) get_rightop((Expr *) equalityExpr);
		bool predicateIsImplied = false;

		rightConst->constvalue = newValue->constvalue;
		rightConst->constisnull = newValue->constisnull;
		rightConst->constbyval = newValue->constbyval;

#if (PG_VERSION_NUM >= 100000)
		predicateIsImplied = predicate_implied_by(list_make1(equalityExpr),
												  restrictClauseList, false);
#else
		predicateIsImplied = predicate_implied_by(list_make1(equalityExpr),
												  restrictClauseList);
#endif
		if (predicateIsImplied)
		{
			/* target entry of the form SET col = <x> WHERE col = <x> AND ... */
			isColumnValueChanged = false;
		}
	}

	return isColumnValueChanged;
}


/*
 * RouterInsertJob builds a Job to represent an insertion performed by
 * the provided query against the provided shard interval. This task contains
 * shard-extended deparsed SQL to be run during execution.
 */
static Job *
RouterInsertJob(Query *originalQuery, Query *query, DeferredErrorMessage **planningError)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	List *taskList = NIL;
	Job *job = NULL;
	bool requiresMasterEvaluation = false;
	bool deferredPruning = false;

	bool isMultiRowInsert = IsMultiRowInsert(query);
	if (isMultiRowInsert)
	{
		/* add default expressions to RTE_VALUES in multi-row INSERTs */
		NormalizeMultiRowInsertTargetList(originalQuery);
	}

	if (isMultiRowInsert || !CanShardPrune(distributedTableId, query))
	{
		/*
		 * If there is a non-constant (e.g. parameter, function call) in the partition
		 * column of the INSERT then we defer shard pruning until the executor where
		 * these values are known.
		 *
		 * XXX: We also defer pruning for multi-row INSERTs because of some current
		 * limitations with the way multi-row INSERTs are handled. Most notably, we
		 * don't evaluate functions in task->rowValuesList. Therefore we need to
		 * perform function evaluation before we can run RouterInsertTaskList.
		 */
		taskList = NIL;
		deferredPruning = true;

		/* must evaluate the non-constant in the partition column */
		requiresMasterEvaluation = true;
	}
	else
	{
		taskList = RouterInsertTaskList(query, planningError);
		if (*planningError)
		{
			return NULL;
		}

		/* determine whether there are function calls to evaluate */
		requiresMasterEvaluation = RequiresMasterEvaluation(originalQuery);
	}

	if (!requiresMasterEvaluation)
	{
		/* no functions or parameters, build the query strings upfront */
		RebuildQueryStrings(originalQuery, taskList);
	}

	job = CreateJob(originalQuery);
	job->taskList = taskList;
	job->requiresMasterEvaluation = requiresMasterEvaluation;
	job->deferredPruning = deferredPruning;

	return job;
}


/*
 * CreateJob returns a new Job for the given query.
 */
static Job *
CreateJob(Query *query)
{
	Job *job = NULL;

	job = CitusMakeNode(Job);
	job->jobId = INVALID_JOB_ID;
	job->jobQuery = query;
	job->taskList = NIL;
	job->dependedJobList = NIL;
	job->subqueryPushdown = false;
	job->requiresMasterEvaluation = false;
	job->deferredPruning = false;

	return job;
}


/*
 * CanShardPrune determines whether a query is ready for shard pruning
 * by checking whether there is a constant value in the partition column.
 */
static bool
CanShardPrune(Oid distributedTableId, Query *query)
{
	uint32 rangeTableId = 1;
	Var *partitionColumn = NULL;
	List *insertValuesList = NIL;
	ListCell *insertValuesCell = NULL;

	if (query->commandType != CMD_INSERT)
	{
		/* we assume UPDATE/DELETE is always prunable */
		return true;
	}

	partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	if (partitionColumn == NULL)
	{
		/* can always do shard pruning for reference tables */
		return true;
	}

	/* get full list of partition values and ensure they are all Consts */
	insertValuesList = ExtractInsertValuesList(query, partitionColumn);
	foreach(insertValuesCell, insertValuesList)
	{
		InsertValues *insertValues = (InsertValues *) lfirst(insertValuesCell);
		if (!IsA(insertValues->partitionValueExpr, Const))
		{
			/* can't do shard pruning if the partition column is not constant */
			return false;
		}
	}

	return true;
}


/*
 * ErrorIfNoShardsExist throws an error if the given table has no shards.
 */
static void
ErrorIfNoShardsExist(DistTableCacheEntry *cacheEntry)
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
RouterInsertTaskList(Query *query, DeferredErrorMessage **planningError)
{
	List *insertTaskList = NIL;
	List *modifyRouteList = NIL;
	ListCell *modifyRouteCell = NULL;

	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);

	ErrorIfNoShardsExist(cacheEntry);

	Assert(query->commandType == CMD_INSERT);

	modifyRouteList = BuildRoutesForInsert(query, planningError);
	if (*planningError != NULL)
	{
		return NIL;
	}

	foreach(modifyRouteCell, modifyRouteList)
	{
		ModifyRoute *modifyRoute = (ModifyRoute *) lfirst(modifyRouteCell);

		Task *modifyTask = CreateTask(MODIFY_TASK);
		modifyTask->anchorShardId = modifyRoute->shardId;
		modifyTask->replicationModel = cacheEntry->replicationModel;
		modifyTask->rowValuesLists = modifyRoute->rowValuesLists;

		if (query->onConflict != NULL)
		{
			modifyTask->upsertQuery = true;
		}

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
	Task *task = NULL;

	task = CitusMakeNode(Task);
	task->taskType = taskType;
	task->jobId = INVALID_JOB_ID;
	task->taskId = INVALID_TASK_ID;
	task->queryString = NULL;
	task->anchorShardId = INVALID_SHARD_ID;
	task->taskPlacementList = NIL;
	task->dependedTaskList = NIL;

	task->partitionId = 0;
	task->upstreamTaskId = INVALID_TASK_ID;
	task->shardInterval = NULL;
	task->assignmentConstrained = false;
	task->shardId = INVALID_SHARD_ID;
	task->taskExecution = NULL;
	task->upsertQuery = false;
	task->replicationModel = REPLICATION_MODEL_INVALID;

	task->insertSelectQuery = false;
	task->relationShardList = NIL;

	return task;
}


/*
 * ExtractFirstDistributedTableId takes a given query, and finds the relationId
 * for the first distributed table in that query. If the function cannot find a
 * distributed table, it returns InvalidOid.
 */
Oid
ExtractFirstDistributedTableId(Query *query)
{
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	Oid distributedTableId = InvalidOid;

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (IsDistributedTable(rangeTableEntry->relid))
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
RouterJob(Query *originalQuery, RelationRestrictionContext *restrictionContext,
		  DeferredErrorMessage **planningError)
{
	Job *job = NULL;
	uint64 shardId = INVALID_SHARD_ID;
	List *placementList = NIL;
	List *relationShardList = NIL;
	List *rangeTableList = NIL;
	bool replacePrunedQueryWithDummy = false;
	bool requiresMasterEvaluation = false;
	RangeTblEntry *updateOrDeleteRTE = NULL;
	bool isMultiShardModifyQuery = false;

	/* router planner should create task even if it deosn't hit a shard at all */
	replacePrunedQueryWithDummy = true;

	/* check if this query requires master evaluation */
	requiresMasterEvaluation = RequiresMasterEvaluation(originalQuery);

	(*planningError) = PlanRouterQuery(originalQuery, restrictionContext,
									   &placementList, &shardId, &relationShardList,
									   replacePrunedQueryWithDummy,
									   &isMultiShardModifyQuery);
	if (*planningError)
	{
		return NULL;
	}

	job = CreateJob(originalQuery);

	ExtractRangeTableEntryWalker((Node *) originalQuery, &rangeTableList);
	updateOrDeleteRTE = GetUpdateOrDeleteRTE(rangeTableList);

	/*
	 * If all of the shards are pruned, we replace the relation RTE into
	 * subquery RTE that returns no results. However, this is not useful
	 * for UPDATE and DELETE queries. Therefore, if we detect a UPDATE or
	 * DELETE RTE with subquery type, we just set task list to empty and return
	 * the job.
	 */
	if (updateOrDeleteRTE != NULL && updateOrDeleteRTE->rtekind == RTE_SUBQUERY)
	{
		job->taskList = NIL;
		return job;
	}

	if (originalQuery->commandType == CMD_SELECT)
	{
		job->taskList = SingleShardSelectTaskList(originalQuery, relationShardList,
												  placementList, shardId);
	}
	else if (isMultiShardModifyQuery)
	{
		job->taskList = MultiShardModifyTaskList(originalQuery, relationShardList,
												 requiresMasterEvaluation);
	}
	else
	{
		job->taskList = SingleShardModifyTaskList(originalQuery, relationShardList,
												  placementList, shardId);
	}

	job->requiresMasterEvaluation = requiresMasterEvaluation;
	return job;
}


/*
 * SingleShardSelectTaskList generates a task for single shard select query
 * and returns it as a list.
 */
static List *
SingleShardSelectTaskList(Query *query, List *relationShardList, List *placementList,
						  uint64 shardId)
{
	Task *task = CreateTask(ROUTER_TASK);
	StringInfo queryString = makeStringInfo();

	pg_get_query_def(query, queryString);

	task->queryString = queryString->data;
	task->anchorShardId = shardId;
	task->taskPlacementList = placementList;
	task->relationShardList = relationShardList;

	return list_make1(task);
}


/*
 * MultiShardModifyTaskList generates task list for multi shard update/delete
 * queries.
 */
static List *
MultiShardModifyTaskList(Query *originalQuery, List *relationShardList,
						 bool requiresMasterEvaluation)
{
	List *taskList = NIL;
	ListCell *relationShardCell = NULL;
	int taskId = 1;

	foreach(relationShardCell, relationShardList)
	{
		RelationShard *relationShard = (RelationShard *) lfirst(relationShardCell);
		List *relationShardList = list_make1(relationShard);
		Task *task = CreateTask(MODIFY_TASK);

		if (!requiresMasterEvaluation)
		{
			Query *copiedQuery = copyObject(originalQuery);
			StringInfo shardQueryString = makeStringInfo();

			UpdateRelationToShardNames((Node *) copiedQuery, relationShardList);
			pg_get_query_def(copiedQuery, shardQueryString);

			task->queryString = shardQueryString->data;
		}

		task->taskId = taskId++;
		task->anchorShardId = relationShard->shardId;
		task->relationShardList = relationShardList;

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * SingleShardModifyTaskList generates a task for single shard update/delete query
 * and returns it as a list.
 */
static List *
SingleShardModifyTaskList(Query *query, List *relationShardList, List *placementList,
						  uint64 shardId)
{
	Task *task = CreateTask(MODIFY_TASK);
	StringInfo queryString = makeStringInfo();
	DistTableCacheEntry *modificationTableCacheEntry = NULL;
	char modificationPartitionMethod = 0;
	List *rangeTableList = NIL;
	RangeTblEntry *updateOrDeleteRTE = NULL;

	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);
	updateOrDeleteRTE = GetUpdateOrDeleteRTE(rangeTableList);

	modificationTableCacheEntry = DistributedTableCacheEntry(updateOrDeleteRTE->relid);
	modificationPartitionMethod = modificationTableCacheEntry->partitionMethod;

	if (modificationPartitionMethod == DISTRIBUTE_BY_NONE &&
		SelectsFromDistributedTable(rangeTableList))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform select on a distributed table "
							   "and modify a reference table")));
	}

	pg_get_query_def(query, queryString);

	task->queryString = queryString->data;
	task->anchorShardId = shardId;
	task->taskPlacementList = placementList;
	task->relationShardList = relationShardList;
	task->replicationModel = modificationTableCacheEntry->replicationModel;

	return list_make1(task);
}


/*
 * GetUpdateOrDeleteRTE walks over the given range table list, and checks if
 * it has an UPDATE or DELETE RTE. If it finds one, it return it immediately.
 */
static RangeTblEntry *
GetUpdateOrDeleteRTE(List *rangeTableList)
{
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (UpdateOrDeleteRTE(rangeTableEntry))
		{
			return rangeTableEntry;
		}
	}

	return NULL;
}


/*
 * UpdateOrDeleteRTE checks if the given range table entry is an UPDATE or
 * DELETE RTE by checking required permissions on it.
 */
static bool
UpdateOrDeleteRTE(RangeTblEntry *rangeTableEntry)
{
	if ((ACL_UPDATE & rangeTableEntry->requiredPerms) ||
		(ACL_DELETE & rangeTableEntry->requiredPerms))
	{
		return true;
	}
	else
	{
		return false;
	}
}


/*
 * SelectsFromDistributedTable checks if there is a select on a distributed
 * table by looking into range table entries.
 */
static bool
SelectsFromDistributedTable(List *rangeTableList)
{
	ListCell *rangeTableCell = NULL;
	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		DistTableCacheEntry *cacheEntry = NULL;

		if (rangeTableEntry->relid == InvalidOid)
		{
			continue;
		}

		cacheEntry = DistributedTableCacheEntry(rangeTableEntry->relid);
		if (cacheEntry->partitionMethod != DISTRIBUTE_BY_NONE &&
			!UpdateOrDeleteRTE(rangeTableEntry))
		{
			return true;
		}
	}

	return false;
}


/*
 * RouterQuery runs router pruning logic for SELECT, UPDATE and DELETE queries.
 * If there are shards present and query is routable, all RTEs have been updated
 * to point to the relevant shards in the originalQuery. Also, placementList is
 * filled with the list of worker nodes that has all the required shard placements
 * for the query execution. anchorShardId is set to the first pruned shardId of
 * the given query. Finally, relationShardList is filled with the list of
 * relation-to-shard mappings for  the query.
 *
 * If the given query is not routable, it fills planningError with the related
 * DeferredErrorMessage. The caller can check this error message to see if query
 * is routable or not.
 */
DeferredErrorMessage *
PlanRouterQuery(Query *originalQuery, RelationRestrictionContext *restrictionContext,
				List **placementList, uint64 *anchorShardId, List **relationShardList,
				bool replacePrunedQueryWithDummy, bool *multiShardModifyQuery)
{
	static uint32 zeroShardQueryRoundRobin = 0;

	bool isMultiShardQuery = false;
	List *prunedRelationShardList = NIL;
	DeferredErrorMessage *planningError = NULL;
	ListCell *prunedRelationShardListCell = NULL;
	List *workerList = NIL;
	bool shardsPresent = false;
	uint64 shardId = INVALID_SHARD_ID;
	CmdType commandType = originalQuery->commandType;
	bool isMultiShardModifyQuery = false;

	*placementList = NIL;
	prunedRelationShardList = TargetShardIntervalsForRouter(originalQuery,
															restrictionContext,
															&isMultiShardQuery);

	if (isMultiShardQuery)
	{
		/*
		 * If multiShardQuery is true and it is a type of SELECT query, then
		 * return deferred error. We do not support multi-shard SELECT queries
		 * with this code path.
		 */
		if (commandType == CMD_SELECT)
		{
			planningError = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
										  NULL, NULL, NULL);
			return planningError;
		}

		Assert(UpdateOrDeleteQuery(originalQuery));

		planningError = ModifyQuerySupported(originalQuery, isMultiShardQuery);
		if (planningError != NULL)
		{
			return planningError;
		}

		isMultiShardModifyQuery = true;
	}

	foreach(prunedRelationShardListCell, prunedRelationShardList)
	{
		List *prunedShardList = (List *) lfirst(prunedRelationShardListCell);
		ListCell *shardIntervalCell = NULL;

		/* no shard is present or all shards are pruned out case will be handled later */
		if (prunedShardList == NIL)
		{
			continue;
		}

		shardsPresent = true;

		foreach(shardIntervalCell, prunedShardList)
		{
			ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
			RelationShard *relationShard = CitusMakeNode(RelationShard);

			relationShard->relationId = shardInterval->relationId;
			relationShard->shardId = shardInterval->shardId;

			*relationShardList = lappend(*relationShardList, relationShard);
		}
	}

	if (isMultiShardModifyQuery)
	{
		*multiShardModifyQuery = true;
		return planningError;
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
	shardId = GetInitialShardId(prunedRelationShardList);

	/*
	 * Determine the worker that has all shard placements if a shard placement found.
	 * If no shard placement exists and replacePrunedQueryWithDummy flag is set, we will
	 * still run the query but the result will be empty. We create a dummy shard
	 * placement for the first active worker.
	 */
	if (shardsPresent)
	{
		workerList = WorkersContainingAllShards(prunedRelationShardList);
	}
	else if (replacePrunedQueryWithDummy)
	{
		List *workerNodeList = ActiveReadableNodeList();
		if (workerNodeList != NIL)
		{
			int workerNodeCount = list_length(workerNodeList);
			int workerNodeIndex = zeroShardQueryRoundRobin % workerNodeCount;
			WorkerNode *workerNode = (WorkerNode *) list_nth(workerNodeList,
															 workerNodeIndex);
			ShardPlacement *dummyPlacement =
				(ShardPlacement *) CitusMakeNode(ShardPlacement);
			dummyPlacement->nodeName = workerNode->workerName;
			dummyPlacement->nodePort = workerNode->workerPort;
			dummyPlacement->groupId = workerNode->groupId;

			workerList = lappend(workerList, dummyPlacement);

			zeroShardQueryRoundRobin++;
		}
	}
	else
	{
		/*
		 * For INSERT ... SELECT, this query could be still a valid for some other target
		 * shard intervals. Thus, we should return empty list if there aren't any matching
		 * workers, so that the caller can decide what to do with this task.
		 */
		return NULL;
	}

	if (workerList == NIL)
	{
		ereport(DEBUG2, (errmsg("Found no worker with all shard placements")));

		planningError = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									  "found no worker with all shard placements",
									  NULL, NULL);
		return planningError;
	}


	/*
	 * If this is an UPDATE or DELETE query which requires master evaluation,
	 * don't try update shard names, and postpone that to execution phase.
	 */
	if (!(UpdateOrDeleteQuery(originalQuery) && RequiresMasterEvaluation(originalQuery)))
	{
		UpdateRelationToShardNames((Node *) originalQuery, *relationShardList);
	}

	*multiShardModifyQuery = false;
	*placementList = workerList;
	*anchorShardId = shardId;

	return planningError;
}


/*
 * GetInitialShardId returns the initial shard id given relation shard list. If
 * there is no relation shard exist in the list returns INAVLID_SHARD_ID.
 */
static uint64
GetInitialShardId(List *relationShardList)
{
	ListCell *prunedRelationShardListCell = NULL;

	foreach(prunedRelationShardListCell, relationShardList)
	{
		List *prunedShardList = (List *) lfirst(prunedRelationShardListCell);
		ShardInterval *shardInterval = NULL;

		/* no shard is present or all shards are pruned out case will be handled later */
		if (prunedShardList == NIL)
		{
			continue;
		}

		shardInterval = linitial(prunedShardList);
		return shardInterval->shardId;
	}

	return INVALID_SHARD_ID;
}


/*
 * TargetShardIntervalsForRouter performs shard pruning for all referenced relations
 * in the query and returns list of shards per relation. Shard pruning is done based
 * on provided restriction context per relation. The function sets multiShardQuery
 * to true if any of the relations pruned down to more than one active shard. It
 * also records pruned shard intervals in relation restriction context to be used
 * later on. Some queries may have contradiction clauses like 'and false' or
 * 'and 1=0', such queries are treated as if all of the shards of joining
 * relations are pruned out.
 */
static List *
TargetShardIntervalsForRouter(Query *query,
							  RelationRestrictionContext *restrictionContext,
							  bool *multiShardQuery)
{
	List *prunedRelationShardList = NIL;
	ListCell *restrictionCell = NULL;

	Assert(restrictionContext != NULL);

	foreach(restrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(restrictionCell);
		Oid relationId = relationRestriction->relationId;
		Index tableId = relationRestriction->index;
		DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);
		int shardCount = cacheEntry->shardIntervalArrayLength;
		List *baseRestrictionList = relationRestriction->relOptInfo->baserestrictinfo;
		List *restrictClauseList = get_all_actual_clauses(baseRestrictionList);
		List *prunedShardList = NIL;
		List *joinInfoList = relationRestriction->relOptInfo->joininfo;
		List *pseudoRestrictionList = extract_actual_clauses(joinInfoList, true);
		bool whereFalseQuery = false;

		relationRestriction->prunedShardIntervalList = NIL;

		/*
		 * Queries may have contradiction clauses like 'false', or '1=0' in
		 * their filters. Such queries would have pseudo constant 'false'
		 * inside relOptInfo->joininfo list. We treat such cases as if all
		 * shards of the table are pruned out.
		 */
		whereFalseQuery = ContainsFalseClause(pseudoRestrictionList);
		if (!whereFalseQuery && shardCount > 0)
		{
			prunedShardList = PruneShards(relationId, tableId, restrictClauseList);

			if (list_length(prunedShardList) > 1)
			{
				(*multiShardQuery) = true;
			}
		}

		relationRestriction->prunedShardIntervalList = prunedShardList;
		prunedRelationShardList = lappend(prunedRelationShardList, prunedShardList);
	}

	return prunedRelationShardList;
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
 * WorkersContainingSelectShards returns list of shard placements that contain all
 * shard intervals provided to the select query. It returns NIL if no placement
 * exists. The caller should check if there are any shard intervals exist for
 * placement check prior to calling this function.
 */
static List *
WorkersContainingAllShards(List *prunedShardIntervalsList)
{
	ListCell *prunedShardIntervalCell = NULL;
	bool firstShard = true;
	List *currentPlacementList = NIL;

	foreach(prunedShardIntervalCell, prunedShardIntervalsList)
	{
		List *shardIntervalList = (List *) lfirst(prunedShardIntervalCell);
		ShardInterval *shardInterval = NULL;
		uint64 shardId = INVALID_SHARD_ID;
		List *newPlacementList = NIL;

		if (shardIntervalList == NIL)
		{
			continue;
		}

		Assert(list_length(shardIntervalList) == 1);

		shardInterval = (ShardInterval *) linitial(shardIntervalList);
		shardId = shardInterval->shardId;

		/* retrieve all active shard placements for this shard */
		newPlacementList = FinalizedShardPlacementList(shardId);

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
		 * containing all shards referecend by the query, hence we can not forward
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
	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	char partitionMethod = cacheEntry->partitionMethod;
	uint32 rangeTableId = 1;
	Var *partitionColumn = NULL;
	List *insertValuesList = NIL;
	List *modifyRouteList = NIL;
	ListCell *insertValuesCell = NULL;

	Assert(query->commandType == CMD_INSERT);

	/* reference tables can only have one shard */
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		int shardCount = 0;
		List *shardIntervalList = LoadShardIntervalList(distributedTableId);
		RangeTblEntry *valuesRTE = NULL;
		ShardInterval *shardInterval = NULL;
		ModifyRoute *modifyRoute = NULL;

		shardCount = list_length(shardIntervalList);
		if (shardCount != 1)
		{
			ereport(ERROR, (errmsg("reference table cannot have %d shards", shardCount)));
		}

		shardInterval = linitial(shardIntervalList);
		modifyRoute = palloc(sizeof(ModifyRoute));

		modifyRoute->shardId = shardInterval->shardId;

		valuesRTE = ExtractDistributedInsertValuesRTE(query);
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

	partitionColumn = PartitionColumn(distributedTableId, rangeTableId);

	/* get full list of insert values and iterate over them to prune */
	insertValuesList = ExtractInsertValuesList(query, partitionColumn);

	foreach(insertValuesCell, insertValuesList)
	{
		InsertValues *insertValues = (InsertValues *) lfirst(insertValuesCell);
		Const *partitionValueConst = NULL;
		List *prunedShardList = NIL;
		int prunedShardCount = 0;
		ShardInterval *targetShard = NULL;

		if (!IsA(insertValues->partitionValueExpr, Const))
		{
			/* shard pruning not possible right now */
			return NIL;
		}

		partitionValueConst = (Const *) insertValues->partitionValueExpr;
		if (partitionValueConst->constisnull)
		{
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							errmsg("cannot perform an INSERT with NULL in the partition "
								   "column")));
		}

		if (partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod ==
			DISTRIBUTE_BY_RANGE)
		{
			Datum partitionValue = partitionValueConst->constvalue;
			DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(
				distributedTableId);
			ShardInterval *shardInterval = FindShardInterval(partitionValue, cacheEntry);

			if (shardInterval != NULL)
			{
				prunedShardList = list_make1(shardInterval);
			}
		}
		else
		{
			List *restrictClauseList = NIL;
			Index tableId = 1;
			OpExpr *equalityExpr = MakeOpExpression(partitionColumn,
													BTEqualStrategyNumber);
			Node *rightOp = get_rightop((Expr *) equalityExpr);
			Const *rightConst = (Const *) rightOp;

			Assert(IsA(rightOp, Const));

			rightConst->constvalue = partitionValueConst->constvalue;
			rightConst->constisnull = partitionValueConst->constisnull;
			rightConst->constbyval = partitionValueConst->constbyval;

			restrictClauseList = list_make1(equalityExpr);

			prunedShardList = PruneShards(distributedTableId, tableId,
										  restrictClauseList);
		}

		prunedShardCount = list_length(prunedShardList);
		if (prunedShardCount != 1)
		{
			char *partitionKeyString = cacheEntry->partitionKeyString;
			char *partitionColumnName = ColumnNameToColumn(distributedTableId,
														   partitionKeyString);
			StringInfo errorMessage = makeStringInfo();
			StringInfo errorHint = makeStringInfo();
			const char *targetCountType = NULL;

			if (prunedShardCount == 0)
			{
				targetCountType = "no";
			}
			else
			{
				targetCountType = "multiple";
			}

			if (prunedShardCount == 0)
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

		targetShard = (ShardInterval *) linitial(prunedShardList);
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
	RangeTblEntry *valuesRTE = NULL;

	if (query->commandType != CMD_INSERT)
	{
		return NULL;
	}

	foreach(rteCell, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);

		if (rte->rtekind == RTE_VALUES)
		{
			valuesRTE = rte;
			break;
		}
	}

	return valuesRTE;
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

		valuesListCell->data.ptr_value = (void *) expandedValuesList;
	}

#if (PG_VERSION_NUM >= 100000)

	/* reset coltypes, coltypmods, colcollations and rebuild them below */
	valuesRTE->coltypes = NIL;
	valuesRTE->coltypmods = NIL;
	valuesRTE->colcollations = NIL;
#endif

	foreach(targetEntryCell, query->targetList)
	{
		TargetEntry *targetEntry = lfirst(targetEntryCell);
		Node *targetExprNode = (Node *) targetEntry->expr;
		Oid targetType = InvalidOid;
		int32 targetTypmod = -1;
		Oid targetColl = InvalidOid;
		Var *syntheticVar = NULL;

		/* RTE_VALUES comes 2nd, after destination table */
		Index valuesVarno = 2;

		targetEntryNo++;

		targetType = exprType(targetExprNode);
		targetTypmod = exprTypmod(targetExprNode);
		targetColl = exprCollation(targetExprNode);

#if (PG_VERSION_NUM >= 100000)
		valuesRTE->coltypes = lappend_oid(valuesRTE->coltypes, targetType);
		valuesRTE->coltypmods = lappend_int(valuesRTE->coltypmods, targetTypmod);
		valuesRTE->colcollations = lappend_oid(valuesRTE->colcollations, targetColl);
#endif

		if (IsA(targetExprNode, Var))
		{
			Var *targetVar = (Var *) targetExprNode;
			targetVar->varattno = targetEntryNo;
			continue;
		}

		/* replace the original expression with a Var referencing values_lists */
		syntheticVar = makeVar(valuesVarno, targetEntryNo, targetType, targetTypmod,
							   targetColl, 0);
		targetEntry->expr = (Expr *) syntheticVar;
	}
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
		RangeTblEntry *referencedRTE = NULL;
		ListCell *valuesListCell = NULL;
		Index ivIndex = 0;

		referencedRTE = rt_fetch(partitionVar->varno, query->rtable);
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
 * MultiRouterPlannableQuery returns true if given query can be router plannable.
 * The query is router plannable if it is a modify query, or if its is a select
 * query issued on a hash partitioned distributed table. Router plannable checks
 * for select queries can be turned off by setting citus.enable_router_execution
 * flag to false.
 */
static bool
MultiRouterPlannableQuery(Query *query, RelationRestrictionContext *restrictionContext)
{
	CmdType commandType = query->commandType;
	ListCell *relationRestrictionContextCell = NULL;

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		return true;
	}

	Assert(commandType == CMD_SELECT);

	if (!EnableRouterExecution)
	{
		return false;
	}

	if (query->hasForUpdate)
	{
		return false;
	}

	foreach(relationRestrictionContextCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(relationRestrictionContextCell);
		RangeTblEntry *rte = relationRestriction->rte;
		if (rte->rtekind == RTE_RELATION)
		{
			Oid distributedTableId = rte->relid;
			char partitionMethod = 0;

			if (!IsDistributedTable(distributedTableId))
			{
				/* local tables cannot be read from workers */
				return false;
			}

			partitionMethod = PartitionMethod(distributedTableId);
			if (!(partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod ==
				  DISTRIBUTE_BY_NONE || partitionMethod == DISTRIBUTE_BY_RANGE))
			{
				return false;
			}
		}
	}

	return true;
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

	newContext->hasDistributedRelation = oldContext->hasDistributedRelation;
	newContext->hasLocalRelation = oldContext->hasLocalRelation;
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
		memcpy(newRestriction->relOptInfo, oldRestriction->relOptInfo,
			   sizeof(RelOptInfo));

		newRestriction->relOptInfo->baserestrictinfo =
			copyObject(oldRestriction->relOptInfo->baserestrictinfo);

		newRestriction->relOptInfo->joininfo =
			copyObject(oldRestriction->relOptInfo->joininfo);

		/* not copyable, but readonly */
		newRestriction->plannerInfo = oldRestriction->plannerInfo;
		newRestriction->prunedShardIntervalList = oldRestriction->prunedShardIntervalList;

		newContext->relationRestrictionList =
			lappend(newContext->relationRestrictionList, newRestriction);
	}

	return newContext;
}


/*
 * ErrorIfQueryHasModifyingCTE checks if the query contains modifying common table
 * expressions and errors out if it does.
 */
static DeferredErrorMessage *
ErrorIfQueryHasModifyingCTE(Query *queryTree)
{
	ListCell *cteCell = NULL;

	Assert(queryTree->commandType == CMD_SELECT);

	foreach(cteCell, queryTree->cteList)
	{
		CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);
		Query *cteQuery = (Query *) cte->ctequery;

		/*
		 * Here we only check for command type of top level query. Normally there can be
		 * nested CTE, however PostgreSQL dictates that data-modifying statements must
		 * be at top level of CTE. Therefore it is OK to just check for top level.
		 * Similarly, we do not need to check for subqueries.
		 */
		if (cteQuery->commandType != CMD_SELECT)
		{
			return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
								 "data-modifying statements are not supported in "
								 "the WITH clauses of distributed queries",
								 NULL, NULL);
		}
	}

	/* everything OK */
	return NULL;
}


#if (PG_VERSION_NUM >= 100000)

/*
 * get_all_actual_clauses
 *
 * Returns a list containing the bare clauses from 'restrictinfo_list'.
 *
 * This loses the distinction between regular and pseudoconstant clauses,
 * so be careful what you use it for.
 */
static List *
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


#endif


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
