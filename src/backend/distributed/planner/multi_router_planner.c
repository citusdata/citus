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
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/listutils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/query_pushdown_planning.h"
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
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#else
#include "optimizer/var.h"
#include "optimizer/predtest.h"
#endif
#include "optimizer/restrictinfo.h"
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
static void CreateSingleTaskRouterPlan(DistributedPlan *distributedPlan,
									   Query *originalQuery,
									   Query *query,
									   PlannerRestrictionContext *
									   plannerRestrictionContext);
static Oid ResultRelationOidForQuery(Query *query);
static bool IsTidColumn(Node *node);
static DeferredErrorMessage * MultiShardModifyQuerySupported(Query *originalQuery,
															 PlannerRestrictionContext *
															 plannerRestrictionContext);
static bool HasDangerousJoinUsing(List *rtableList, Node *jtnode);
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
					   PlannerRestrictionContext *plannerRestrictionContext,
					   DeferredErrorMessage **planningError);
static bool RelationPrunesToMultipleShards(List *relationShardList);
static void NormalizeMultiRowInsertTargetList(Query *query);
static List * BuildRoutesForInsert(Query *query, DeferredErrorMessage **planningError);
static List * GroupInsertValuesByShardId(List *insertValuesList);
static List * ExtractInsertValuesList(Query *query, Var *partitionColumn);
static DeferredErrorMessage * MultiRouterPlannableQuery(Query *query);
static DeferredErrorMessage * ErrorIfQueryHasModifyingCTE(Query *queryTree);
static RangeTblEntry * GetUpdateOrDeleteRTE(Query *query);
static bool SelectsFromDistributedTable(List *rangeTableList, Query *query);
static List * get_all_actual_clauses(List *restrictinfo_list);
static int CompareInsertValuesByShardId(const void *leftElement,
										const void *rightElement);
static uint64 GetAnchorShardId(List *relationShardList);
static List * TargetShardIntervalForFastPathQuery(Query *query,
												  Const **partitionValueConst,
												  bool *isMultiShardQuery);
static List * SingleShardSelectTaskList(Query *query, uint64 jobId,
										List *relationShardList, List *placementList,
										uint64 shardId);
static bool RowLocksOnRelations(Node *node, List **rtiLockList);
static List * SingleShardModifyTaskList(Query *query, uint64 jobId,
										List *relationShardList, List *placementList,
										uint64 shardId);
static void ReorderTaskPlacementsByTaskAssignmentPolicy(Job *job,
														TaskAssignmentPolicyType
														taskAssignmentPolicy,
														List *placementList);


/*
 * CreateRouterPlan attempts to create a router executor plan for the given
 * SELECT statement. ->planningError is set if planning fails.
 */
DistributedPlan *
CreateRouterPlan(Query *originalQuery, Query *query,
				 PlannerRestrictionContext *plannerRestrictionContext)
{
	DistributedPlan *distributedPlan = CitusMakeNode(DistributedPlan);

	distributedPlan->planningError = MultiRouterPlannableQuery(query);

	if (distributedPlan->planningError == NULL)
	{
		CreateSingleTaskRouterPlan(distributedPlan, originalQuery, query,
								   plannerRestrictionContext);
	}

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
	distributedPlan->targetRelationId = ResultRelationOidForQuery(query);

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
 * are router plannable by default. If query is not router plannable the returned plan
 * has planningError set to a description of the problem.
 */
static void
CreateSingleTaskRouterPlan(DistributedPlan *distributedPlan, Query *originalQuery,
						   Query *query,
						   PlannerRestrictionContext *plannerRestrictionContext)
{
	Job *job = NULL;

	distributedPlan->modLevel = RowModifyLevelForQuery(query);

	/* we cannot have multi shard update/delete query via this code path */
	job = RouterJob(originalQuery, plannerRestrictionContext,
					&distributedPlan->planningError);

	if (distributedPlan->planningError != NULL)
	{
		/* query cannot be handled by this planner */
		return;
	}

	ereport(DEBUG2, (errmsg("Creating router plan")));

	distributedPlan->workerJob = job;
	distributedPlan->masterQuery = NULL;
	distributedPlan->routerExecutable = true;
	distributedPlan->hasReturning = false;
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
	RangeTblEntry *resultRte = NULL;

	/* only modify queries have result relations */
	if (!IsModifyCommand(query))
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("input query is not a modification query")));
	}

	resultRte = ExtractResultRelationRTE(query);
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
 * ExtractResultRelationRTE returns the table's resultRelation range table entry.
 */
RangeTblEntry *
ExtractResultRelationRTE(Query *query)
{
	return rt_fetch(query->resultRelation, query->rtable);
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
	DeferredErrorMessage *deferredError = NULL;
	Oid distributedTableId = ExtractFirstDistributedTableId(queryTree);
	uint32 rangeTableId = 1;
	Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	uint32 queryTableCount = 0;
	CmdType commandType = queryTree->commandType;

	/*
	 * Here, we check if a recursively planned query tries to modify
	 * rows based on the ctid column. This is a bad idea because ctid of
	 * the rows could be changed before the modification part of
	 * the query is executed.
	 */
	if (ContainsReadIntermediateResultFunction((Node *) originalQuery))
	{
		bool hasTidColumn = FindNodeCheck((Node *) originalQuery->jointree, IsTidColumn);
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

		foreach(cteCell, queryTree->cteList)
		{
			CommonTableExpr *cte = (CommonTableExpr *) lfirst(cteCell);
			Query *cteQuery = (Query *) cte->ctequery;
			DeferredErrorMessage *cteError = NULL;

			if (cteQuery->commandType != CMD_SELECT)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "Router planner doesn't support non-select common table expressions.",
									 NULL, NULL);
			}

			if (cteQuery->hasForUpdate)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "Router planner doesn't support SELECT FOR UPDATE"
									 " in common table expressions.",
									 NULL, NULL);
			}

			if (FindNodeCheck((Node *) cteQuery, CitusIsVolatileFunction))
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "Router planner doesn't support VOLATILE functions"
									 " in common table expressions.",
									 NULL, NULL);
			}

			cteError = MultiRouterPlannableQuery(cteQuery);
			if (cteError)
			{
				return cteError;
			}
		}
	}

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			Oid relationId = rangeTableEntry->relid;

			if (!IsDistributedTable(relationId))
			{
				StringInfo errorMessage = makeStringInfo();
				char *relationName = get_rel_name(rangeTableEntry->relid);

				appendStringInfo(errorMessage, "relation %s is not distributed",
								 relationName);

				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 errorMessage->data, NULL, NULL);
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
		else if (rangeTableEntry->rtekind == RTE_VALUES
#if PG_VERSION_NUM >= 120000
				 || rangeTableEntry->rtekind == RTE_RESULT
#endif
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

	/*
	 * We have to allow modify queries with two range table entries, if it is pushdownable.
	 */
	if (commandType != CMD_INSERT)
	{
		/* We can not get restriction context via master_modify_multiple_shards path */
		if (plannerRestrictionContext == NULL)
		{
			if (queryTableCount != 1)
			{
				return DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "cannot run multi shard modify query with master_modify_multiple_shards when the query involves subquery or join",
									 "Execute the query without using master_modify_multiple_shards()",
									 NULL);
			}
		}
		/* If it is a multi-shard modify query with multiple tables */
		else if (multiShardQuery)
		{
			DeferredErrorMessage *errorMessage = MultiShardModifyQuerySupported(
				originalQuery, plannerRestrictionContext);

			if (errorMessage != NULL)
			{
				return errorMessage;
			}
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
				FindNodeCheck((Node *) targetEntry->expr, CitusIsVolatileFunction))
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
		}

		if (joinTree != NULL)
		{
			if (FindNodeCheck((Node *) joinTree->quals, CitusIsVolatileFunction))
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
	Oid distributedTableId = InvalidOid;
	uint32 rangeTableId = 1;
	Var *partitionColumn = NULL;
	List *onConflictSet = NIL;
	Node *arbiterWhere = NULL;
	Node *onConflictWhere = NULL;
	ListCell *setTargetCell = NULL;
	bool specifiesPartitionValue = false;

	CmdType commandType = queryTree->commandType;
	if (commandType != CMD_INSERT || queryTree->onConflict == NULL)
	{
		return NULL;
	}

	distributedTableId = ExtractFirstDistributedTableId(queryTree);
	partitionColumn = PartitionColumn(distributedTableId, rangeTableId);

	onConflictSet = queryTree->onConflict->onConflictSet;
	arbiterWhere = queryTree->onConflict->arbiterWhere;
	onConflictWhere = queryTree->onConflict->onConflictWhere;

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
 * MultiShardModifyQuerySupported returns the error message if the modify query is
 * not pushdownable, otherwise it returns NULL.
 */
static DeferredErrorMessage *
MultiShardModifyQuerySupported(Query *originalQuery,
							   PlannerRestrictionContext *plannerRestrictionContext)
{
	DeferredErrorMessage *errorMessage = NULL;
	RangeTblEntry *resultRangeTable = rt_fetch(originalQuery->resultRelation,
											   originalQuery->rtable);
	Oid resultRelationOid = resultRangeTable->relid;
	char resultPartitionMethod = PartitionMethod(resultRelationOid);

	if (HasDangerousJoinUsing(originalQuery->rtable, (Node *) originalQuery->jointree))
	{
		errorMessage = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "a join with USING causes an internal naming conflict, use "
									 "ON instead",
									 NULL, NULL);
	}
	else if (FindNodeCheck((Node *) originalQuery, CitusIsVolatileFunction))
	{
		errorMessage = DeferredError(ERRCODE_FEATURE_NOT_SUPPORTED,
									 "functions used in UPDATE queries on distributed "
									 "tables must not be VOLATILE",
									 NULL, NULL);
	}
	else if (resultPartitionMethod == DISTRIBUTE_BY_NONE)
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

		predicateIsImplied = predicate_implied_by(list_make1(equalityExpr),
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
	Const *partitionKeyValue = NULL;

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

		/* remember the partition column value */
		partitionKeyValue = ExtractInsertPartitionKeyValue(originalQuery);
	}

	job = CreateJob(originalQuery);
	job->taskList = taskList;
	job->requiresMasterEvaluation = requiresMasterEvaluation;
	job->deferredPruning = deferredPruning;
	job->partitionKeyValue = partitionKeyValue;

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
	job->jobId = UniqueJobId();
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
	task->taskExecution = NULL;
	task->replicationModel = REPLICATION_MODEL_INVALID;
	task->relationRowLockList = NIL;

	task->modifyWithSubquery = false;
	task->relationShardList = NIL;

	return task;
}


/*
 * ExtractFirstDistributedTableId takes a given query, and finds the relationId
 * for the first distributed table in that query. If the function cannot find a
 * distributed table, it returns InvalidOid.
 *
 * We only use this function for modifications and fast path queries, which
 * should have the first distributed table in the top-level rtable.
 */
Oid
ExtractFirstDistributedTableId(Query *query)
{
	List *rangeTableList = query->rtable;
	ListCell *rangeTableCell = NULL;
	Oid distributedTableId = InvalidOid;

	Assert(IsModifyCommand(query) || FastPathRouterQuery(query));

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
RouterJob(Query *originalQuery, PlannerRestrictionContext *plannerRestrictionContext,
		  DeferredErrorMessage **planningError)
{
	Job *job = NULL;
	uint64 shardId = INVALID_SHARD_ID;
	List *placementList = NIL;
	List *relationShardList = NIL;
	List *prunedShardIntervalListList = NIL;
	bool replacePrunedQueryWithDummy = false;
	bool requiresMasterEvaluation = false;
	RangeTblEntry *updateOrDeleteRTE = NULL;
	bool isMultiShardModifyQuery = false;
	Const *partitionKeyValue = NULL;

	/* router planner should create task even if it doesn't hit a shard at all */
	replacePrunedQueryWithDummy = true;

	/* check if this query requires master evaluation */
	requiresMasterEvaluation = RequiresMasterEvaluation(originalQuery);

	(*planningError) = PlanRouterQuery(originalQuery, plannerRestrictionContext,
									   &placementList, &shardId, &relationShardList,
									   &prunedShardIntervalListList,
									   replacePrunedQueryWithDummy,
									   &isMultiShardModifyQuery,
									   &partitionKeyValue);
	if (*planningError)
	{
		return NULL;
	}

	job = CreateJob(originalQuery);
	job->partitionKeyValue = partitionKeyValue;

	updateOrDeleteRTE = GetUpdateOrDeleteRTE(originalQuery);

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
		job->taskList = SingleShardSelectTaskList(originalQuery, job->jobId,
												  relationShardList, placementList,
												  shardId);

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
	else if (isMultiShardModifyQuery)
	{
		job->taskList = QueryPushdownSqlTaskList(originalQuery, job->jobId,
												 plannerRestrictionContext->
												 relationRestrictionContext,
												 prunedShardIntervalListList,
												 MODIFY_TASK,
												 requiresMasterEvaluation);
	}
	else
	{
		job->taskList = SingleShardModifyTaskList(originalQuery, job->jobId,
												  relationShardList, placementList,
												  shardId);
	}

	job->requiresMasterEvaluation = requiresMasterEvaluation;
	return job;
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
		Task *task = NULL;
		List *reorderedPlacementList = NIL;
		ShardPlacement *primaryPlacement = NULL;

		/*
		 * We hit a single shard on router plans, and there should be only
		 * one task in the task list
		 */
		Assert(list_length(job->taskList) == 1);
		task = (Task *) linitial(job->taskList);

		/* reorder the placement list */
		reorderedPlacementList = RoundRobinReorder(task, placementList);
		task->taskPlacementList = reorderedPlacementList;

		primaryPlacement = (ShardPlacement *) linitial(reorderedPlacementList);
		ereport(DEBUG3, (errmsg("assigned task %u to node %s:%u", task->taskId,
								primaryPlacement->nodeName,
								primaryPlacement->nodePort)));
	}
}


/*
 * SingleShardSelectTaskList generates a task for single shard select query
 * and returns it as a list.
 */
static List *
SingleShardSelectTaskList(Query *query, uint64 jobId, List *relationShardList,
						  List *placementList,
						  uint64 shardId)
{
	Task *task = CreateTask(ROUTER_TASK);
	StringInfo queryString = makeStringInfo();
	List *relationRowLockList = NIL;

	RowLocksOnRelations((Node *) query, &relationRowLockList);
	pg_get_query_def(query, queryString);

	task->queryString = queryString->data;
	task->anchorShardId = shardId;
	task->jobId = jobId;
	task->taskPlacementList = placementList;
	task->relationShardList = relationShardList;
	task->relationRowLockList = relationRowLockList;

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

			if (IsDistributedTable(relationId))
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
 * SingleShardModifyTaskList generates a task for single shard update/delete query
 * and returns it as a list.
 */
static List *
SingleShardModifyTaskList(Query *query, uint64 jobId, List *relationShardList,
						  List *placementList, uint64 shardId)
{
	Task *task = CreateTask(MODIFY_TASK);
	StringInfo queryString = makeStringInfo();
	DistTableCacheEntry *modificationTableCacheEntry = NULL;
	char modificationPartitionMethod = 0;
	List *rangeTableList = NIL;
	RangeTblEntry *updateOrDeleteRTE = NULL;

	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);
	updateOrDeleteRTE = GetUpdateOrDeleteRTE(query);

	modificationTableCacheEntry = DistributedTableCacheEntry(updateOrDeleteRTE->relid);
	modificationPartitionMethod = modificationTableCacheEntry->partitionMethod;

	if (modificationPartitionMethod == DISTRIBUTE_BY_NONE &&
		SelectsFromDistributedTable(rangeTableList, query))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform select on a distributed table "
							   "and modify a reference table")));
	}

	pg_get_query_def(query, queryString);

	task->queryString = queryString->data;
	task->anchorShardId = shardId;
	task->jobId = jobId;
	task->taskPlacementList = placementList;
	task->relationShardList = relationShardList;
	task->replicationModel = modificationTableCacheEntry->replicationModel;

	return list_make1(task);
}


/*
 * GetUpdateOrDeleteRTE checks query if it has an UPDATE or DELETE RTE.
 * Returns that RTE if found.
 */
static RangeTblEntry *
GetUpdateOrDeleteRTE(Query *query)
{
	if (query->resultRelation > 0)
	{
		return rt_fetch(query->resultRelation, query->rtable);
	}

	return NULL;
}


/*
 * SelectsFromDistributedTable checks if there is a select on a distributed
 * table by looking into range table entries.
 */
static bool
SelectsFromDistributedTable(List *rangeTableList, Query *query)
{
	ListCell *rangeTableCell = NULL;
	int resultRelation = query->resultRelation;
	RangeTblEntry *resultRangeTableEntry = NULL;

	if (resultRelation > 0)
	{
		resultRangeTableEntry = rt_fetch(resultRelation, query->rtable);
	}

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
			(resultRangeTableEntry == NULL || resultRangeTableEntry->relid !=
			 rangeTableEntry->relid))
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
				Const **partitionValueConst)
{
	static uint32 zeroShardQueryRoundRobin = 0;

	bool isMultiShardQuery = false;
	DeferredErrorMessage *planningError = NULL;
	ListCell *prunedShardIntervalListCell = NULL;
	List *workerList = NIL;
	bool shardsPresent = false;
	uint64 shardId = INVALID_SHARD_ID;
	CmdType commandType = originalQuery->commandType;

	*placementList = NIL;

	/*
	 * When FastPathRouterQuery() returns true, we know that standard_planner() has
	 * not been called. Thus, restriction information is not avaliable and we do the
	 * shard pruning based on the distribution column in the quals of the query.
	 */
	if (FastPathRouterQuery(originalQuery))
	{
		List *shardIntervalList =
			TargetShardIntervalForFastPathQuery(originalQuery, partitionValueConst,
												&isMultiShardQuery);

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

		*prunedShardIntervalListList = list_make1(shardIntervalList);

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

	foreach(prunedShardIntervalListCell, *prunedShardIntervalListList)
	{
		List *prunedShardIntervalList = (List *) lfirst(prunedShardIntervalListCell);
		ListCell *shardIntervalCell = NULL;

		/* no shard is present or all shards are pruned out case will be handled later */
		if (prunedShardIntervalList == NIL)
		{
			continue;
		}

		shardsPresent = true;

		foreach(shardIntervalCell, prunedShardIntervalList)
		{
			ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
			RelationShard *relationShard = CitusMakeNode(RelationShard);

			relationShard->relationId = shardInterval->relationId;
			relationShard->shardId = shardInterval->shardId;

			*relationShardList = lappend(*relationShardList, relationShard);
		}
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
	shardId = GetAnchorShardId(*prunedShardIntervalListList);

	/*
	 * Determine the worker that has all shard placements if a shard placement found.
	 * If no shard placement exists and replacePrunedQueryWithDummy flag is set, we will
	 * still run the query but the result will be empty. We create a dummy shard
	 * placement for the first active worker.
	 */
	if (shardsPresent)
	{
		workerList = WorkersContainingAllShards(*prunedShardIntervalListList);
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
			dummyPlacement->nodeId = workerNode->nodeId;
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
 * GetAnchorShardId returns the anchor shard id given relation shard list.
 * The desired anchor shard is found as follows:
 *
 * - Return the first distributed table shard id in the relationShardList if
 * there is any.
 * - Return a random reference table shard id if all the shards belong to
 * reference tables
 * - Return INVALID_SHARD_ID on empty lists
 */
static uint64
GetAnchorShardId(List *prunedShardIntervalListList)
{
	ListCell *prunedShardIntervalListCell = NULL;
	uint64 referenceShardId = INVALID_SHARD_ID;

	foreach(prunedShardIntervalListCell, prunedShardIntervalListList)
	{
		List *prunedShardIntervalList = (List *) lfirst(prunedShardIntervalListCell);
		ShardInterval *shardInterval = NULL;

		/* no shard is present or all shards are pruned out case will be handled later */
		if (prunedShardIntervalList == NIL)
		{
			continue;
		}

		shardInterval = linitial(prunedShardIntervalList);

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
 * one shard interval (see FastPathRouterQuery() for the detail).
 *
 * Also set the outgoing partition column value if requested via
 * partitionValueConst
 */
static List *
TargetShardIntervalForFastPathQuery(Query *query, Const **partitionValueConst,
									bool *isMultiShardQuery)
{
	Const *queryPartitionValueConst = NULL;

	Oid relationId = ExtractFirstDistributedTableId(query);
	Node *quals = query->jointree->quals;

	int relationIndex = 1;

	List *prunedShardIntervalList =
		PruneShards(relationId, relationIndex, make_ands_implicit((Expr *) quals),
					&queryPartitionValueConst);

	/* we're only expecting single shard from a single table */
	Assert(FastPathRouterQuery(query));

	if (list_length(prunedShardIntervalList) > 1)
	{
		*isMultiShardQuery = true;
	}
	else if (list_length(prunedShardIntervalList) == 1 &&
			 partitionValueConst != NULL)
	{
		/* set the outgoing partition column value if requested */
		*partitionValueConst = queryPartitionValueConst;
	}

	return prunedShardIntervalList;
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
		Index tableId = relationRestriction->index;
		DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(relationId);
		int shardCount = cacheEntry->shardIntervalArrayLength;
		List *baseRestrictionList = relationRestriction->relOptInfo->baserestrictinfo;
		List *restrictClauseList = get_all_actual_clauses(baseRestrictionList);
		List *prunedShardIntervalList = NIL;
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

		relationRestriction->prunedShardIntervalList = prunedShardIntervalList;
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
List *
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
		List *prunedShardIntervalList = NIL;
		int prunedShardIntervalCount = 0;
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
			ShardInterval *shardInterval = NULL;

			cacheEntry = DistributedTableCacheEntry(distributedTableId);
			shardInterval = FindShardInterval(partitionValue, cacheEntry);
			if (shardInterval != NULL)
			{
				prunedShardIntervalList = list_make1(shardInterval);
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

			prunedShardIntervalList = PruneShards(distributedTableId, tableId,
												  restrictClauseList, NULL);
		}

		prunedShardIntervalCount = list_length(prunedShardIntervalList);
		if (prunedShardIntervalCount != 1)
		{
			char *partitionKeyString = cacheEntry->partitionKeyString;
			char *partitionColumnName = ColumnNameToColumn(distributedTableId,
														   partitionKeyString);
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

		targetShard = (ShardInterval *) linitial(prunedShardIntervalList);
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

	/* reset coltypes, coltypmods, colcollations and rebuild them below */
	valuesRTE->coltypes = NIL;
	valuesRTE->coltypmods = NIL;
	valuesRTE->colcollations = NIL;

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
 * ExtractInsertPartitionKeyValue extracts the partition column value
 * from an INSERT query. If the expression in the partition column is
 * non-constant or it is a multi-row INSERT with multiple different partition
 * column values, the function returns NULL.
 */
Const *
ExtractInsertPartitionKeyValue(Query *query)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	uint32 rangeTableId = 1;
	Var *partitionColumn = NULL;
	TargetEntry *targetEntry = NULL;
	Const *singlePartitionValueConst = NULL;
	Node *targetExpression = NULL;

	char partitionMethod = PartitionMethod(distributedTableId);
	if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		return NULL;
	}

	partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	targetEntry = get_tle_by_resno(query->targetList, partitionColumn->varattno);
	if (targetEntry == NULL)
	{
		/* partition column value not specified */
		return NULL;
	}

	targetExpression = strip_implicit_coercions((Node *) targetEntry->expr);

	/*
	 * Multi-row INSERTs have a Var in the target list that points to
	 * an RTE_VALUES.
	 */
	if (IsA(targetExpression, Var))
	{
		Var *partitionVar = (Var *) targetExpression;
		RangeTblEntry *referencedRTE = NULL;
		ListCell *valuesListCell = NULL;

		referencedRTE = rt_fetch(partitionVar->varno, query->rtable);

		foreach(valuesListCell, referencedRTE->values_lists)
		{
			List *rowValues = (List *) lfirst(valuesListCell);
			Node *partitionValueNode = list_nth(rowValues, partitionVar->varattno - 1);
			Expr *partitionValueExpr = (Expr *) strip_implicit_coercions(
				partitionValueNode);
			Const *partitionValueConst = NULL;

			if (!IsA(partitionValueExpr, Const))
			{
				/* non-constant value in the partition column */
				singlePartitionValueConst = NULL;
				break;
			}

			partitionValueConst = (Const *) partitionValueExpr;

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
 * MultiRouterPlannableQuery checks if given select query is router plannable,
 * setting distributedPlan->planningError if not.
 * The query is router plannable if it is a modify query, or if its is a select
 * query issued on a hash partitioned distributed table. Router plannable checks
 * for select queries can be turned off by setting citus.enable_router_execution
 * flag to false.
 */
static DeferredErrorMessage *
MultiRouterPlannableQuery(Query *query)
{
	List *rangeTableRelationList = NIL;
	ListCell *rangeTableRelationCell = NULL;

	Assert(query->commandType == CMD_SELECT);

	if (!EnableRouterExecution)
	{
		return DeferredError(ERRCODE_SUCCESSFUL_COMPLETION,
							 "Router planner not enabled.",
							 NULL, NULL);
	}

	ExtractRangeTableRelationWalker((Node *) query, &rangeTableRelationList);
	foreach(rangeTableRelationCell, rangeTableRelationList)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rangeTableRelationCell);
		if (rte->rtekind == RTE_RELATION)
		{
			/* only hash partitioned tables are supported */
			Oid distributedTableId = rte->relid;
			char partitionMethod = 0;

			if (!IsDistributedTable(distributedTableId))
			{
				/* local tables cannot be read from workers */
				return DeferredError(
					ERRCODE_FEATURE_NOT_SUPPORTED,
					"Local tables cannot be used in distributed queries.",
					NULL, NULL);
			}

			partitionMethod = PartitionMethod(distributedTableId);
			if (!(partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod ==
				  DISTRIBUTE_BY_NONE || partitionMethod == DISTRIBUTE_BY_RANGE))
			{
				return DeferredError(
					ERRCODE_FEATURE_NOT_SUPPORTED,
					"Router planner does not support append-partitioned tables.",
					NULL, NULL);
			}

			/*
			 * Currently, we don't support tables with replication factor > 1,
			 * except reference tables with SELECT ... FOR UDPATE queries. It is
			 * also not supported from MX nodes.
			 */
			if (query->hasForUpdate)
			{
				uint32 tableReplicationFactor = TableShardReplicationFactor(
					distributedTableId);

				if (tableReplicationFactor > 1 && partitionMethod != DISTRIBUTE_BY_NONE)
				{
					return DeferredError(
						ERRCODE_FEATURE_NOT_SUPPORTED,
						"SELECT FOR UPDATE with table replication factor > 1 not supported for non-reference tables.",
						NULL, NULL);
				}
			}
		}
	}

	return ErrorIfQueryHasModifyingCTE(query);
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
