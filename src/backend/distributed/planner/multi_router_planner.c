/*-------------------------------------------------------------------------
 *
 * multi_router_planner.c
 *
 * This file contains functions to plan single shard queries
 * including distributed table modifications.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"

#include <stddef.h>

#if (PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600)
#include "access/stratnum.h"
#else
#include "access/skey.h"
#endif
#include "access/xact.h"
#include "distributed/citus_nodes.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/listutils.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "executor/execdesc.h"
#include "lib/stringinfo.h"
#if (PG_VERSION_NUM >= 90500)
#include "nodes/makefuncs.h"
#endif
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "parser/parsetree.h"
#include "storage/lock.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "catalog/pg_proc.h"
#include "optimizer/planmain.h"


typedef struct {
	bool containsVar;
	bool varArgument;
	bool badCoalesce;
} WalkerState;


/* planner functions forward declarations */
static bool ContainsDisallowedFunctionCalls(Node *expression, bool *varArgument,
											bool *badCoalesce);
static bool ContainsDisallowedFunctionCallsWalker(Node *expression, WalkerState *state);
static char MostPermissiveVolatileFlag(char left, char right);
static Task * RouterModifyTask(Query *query);
#if (PG_VERSION_NUM >= 90500)
static OnConflictExpr * RebuildOnConflict(Oid relationId,
										  OnConflictExpr *originalOnConflict);
#endif
static ShardInterval * TargetShardInterval(Query *query);
static List * QueryRestrictList(Query *query);
static bool FastShardPruningPossible(CmdType commandType, char partitionMethod);
static ShardInterval * FastShardPruning(Oid distributedTableId,
										Const *partionColumnValue);
static Oid ExtractFirstDistributedTableId(Query *query);
static Const * ExtractInsertPartitionValue(Query *query, Var *partitionColumn);
static Task * RouterSelectTask(Query *query);
static Job * RouterQueryJob(Query *query, Task *task);
static bool MultiRouterPlannableQuery(Query *query, MultiExecutorType taskExecutorType);
static bool ColumnMatchExpressionAtTopLevelConjunction(Node *node, Var *column);
static void SetRangeTablesInherited(Query *query);


/*
 * MultiRouterPlanCreate creates a physical plan for given query. The created plan is
 * either a modify task that changes a single shard, or a router task that returns
 * query results from a single shard. Supported modify queries (insert/update/delete)
 * are router plannable by default. If query is not router plannable then the function
 * returns NULL.
 */
MultiPlan *
MultiRouterPlanCreate(Query *query, MultiExecutorType taskExecutorType)
{
	Task *task = NULL;
	Job *job = NULL;
	MultiPlan *multiPlan = NULL;
	CmdType commandType = query->commandType;
	bool modifyTask = false;

	bool routerPlannable = MultiRouterPlannableQuery(query, taskExecutorType);
	if (!routerPlannable)
	{
		return NULL;
	}

	ereport(DEBUG2, (errmsg("Creating router plan")));

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		modifyTask = true;
	}

	if (modifyTask)
	{
		ErrorIfModifyQueryNotSupported(query);
		task = RouterModifyTask(query);
	}
	else
	{
		Assert(commandType == CMD_SELECT);

		task = RouterSelectTask(query);
	}


	job = RouterQueryJob(query, task);

	multiPlan = CitusMakeNode(MultiPlan);
	multiPlan->workerJob = job;
	multiPlan->masterQuery = NULL;
	multiPlan->masterTableName = NULL;

	return multiPlan;
}


/*
 * ErrorIfModifyQueryNotSupported checks if the query contains unsupported features,
 * and errors out if it does.
 */
void
ErrorIfModifyQueryNotSupported(Query *queryTree)
{
	Oid distributedTableId = ExtractFirstDistributedTableId(queryTree);
	uint32 rangeTableId = 1;
	Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	List *rangeTableList = NIL;
	ListCell *rangeTableCell = NULL;
	bool hasValuesScan = false;
	uint32 queryTableCount = 0;
	bool specifiesPartitionValue = false;
#if (PG_VERSION_NUM >= 90500)
	ListCell *setTargetCell = NULL;
	List *onConflictSet = NIL;
	Node *arbiterWhere = NULL;
	Node *onConflictWhere = NULL;
#endif

	CmdType commandType = queryTree->commandType;
	Assert(commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		   commandType == CMD_DELETE);

	/*
	 * Reject subqueries which are in SELECT or WHERE clause.
	 * Queries which include subqueries in FROM clauses are rejected below.
	 */
	if (queryTree->hasSubLinks == true)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Subqueries are not supported in distributed"
								  " modifications.")));
	}

	/* reject queries which include CommonTableExpr */
	if (queryTree->cteList != NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Common table expressions are not supported in"
								  " distributed modifications.")));
	}

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			queryTableCount++;
		}
		else if (rangeTableEntry->rtekind == RTE_VALUES)
		{
			hasValuesScan = true;
		}
		else
		{
			/*
			 * Error out for rangeTableEntries that we do not support.
			 * We do not explicitly specify "in FROM clause" in the error detail
			 * for the features that we do not support at all (SUBQUERY, JOIN).
			 * We do not need to check for RTE_CTE because all common table expressions
			 * are rejected above with queryTree->cteList check.
			 */
			char *rangeTableEntryErrorDetail = NULL;
			if (rangeTableEntry->rtekind == RTE_SUBQUERY)
			{
				rangeTableEntryErrorDetail = "Subqueries are not supported in"
											 " distributed modifications.";
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

			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot perform distributed planning for the given"
								   " modifications"),
							errdetail("%s", rangeTableEntryErrorDetail)));
		}
	}

	/*
	 * Reject queries which involve joins. Note that UPSERTs are exceptional for this case.
	 * Queries like "INSERT INTO table_name ON CONFLICT DO UPDATE (col) SET other_col = ''"
	 * contains two range table entries, and we have to allow them.
	 */
	if (commandType != CMD_INSERT && queryTableCount != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Joins are not supported in distributed "
								  "modifications.")));
	}

	/* reject queries which involve multi-row inserts */
	if (hasValuesScan)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot perform distributed planning for the given"
							   " modification"),
						errdetail("Multi-row INSERTs to distributed tables are not "
								  "supported.")));
	}

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		bool hasVarArgument = false; /* A STABLE function is passed a Var argument */
		bool hasBadCoalesce = false; /* CASE/COALESCE passed a mutable function */
		FromExpr *joinTree = NULL;
		ListCell *targetEntryCell = NULL;

		foreach(targetEntryCell, queryTree->targetList)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

			/* skip resjunk entries: UPDATE adds some for ctid, etc. */
			if (targetEntry->resjunk)
			{
				continue;
			}

			if (commandType == CMD_UPDATE &&
				contain_volatile_functions((Node *) targetEntry->expr))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("functions used in UPDATE queries on distributed "
										 "tables must not be VOLATILE")));
			}

			if (commandType == CMD_UPDATE &&
				targetEntry->resno == partitionColumn->varattno)
			{
				specifiesPartitionValue = true;
			}

			if (targetEntry->resno == partitionColumn->varattno &&
				!IsA(targetEntry->expr, Const))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("values given for the partition column must be"
									   " constants or constant expressions")));
			}

			if (commandType == CMD_UPDATE &&
				ContainsDisallowedFunctionCalls((Node *) targetEntry->expr,
												&hasVarArgument, &hasBadCoalesce))
			{
				Assert(hasVarArgument || hasBadCoalesce);
			}
		}

		joinTree = queryTree->jointree;
		if (joinTree != NULL)
		{
			if (contain_volatile_functions(joinTree->quals))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("functions used in the WHERE clause of modification "
										 "queries on distributed tables must not be VOLATILE")));
			}
			else if (ContainsDisallowedFunctionCalls(joinTree->quals, &hasVarArgument,
													 &hasBadCoalesce))
			{
				Assert(hasVarArgument || hasBadCoalesce);
			}
		}

		if (hasVarArgument)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("STABLE functions used in UPDATE queries"
								   " cannot be called with column references")));
		}

		if (hasBadCoalesce)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("non-IMMUTABLE functions are not allowed in CASE or"
								   " COALESCE statements")));
		}

		if (contain_mutable_functions((Node *) queryTree->returningList))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("non-IMMUTABLE functions are not allowed in the"
								   " RETURNING clause")));
		}
	}

#if (PG_VERSION_NUM >= 90500)
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

		if (setTargetEntry->resno == partitionColumn->varattno)
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
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("functions used in the DO UPDATE SET clause of INSERTs "
										 "on distributed tables must be marked IMMUTABLE")));
			}
		}
	}

	/* error if either arbiter or on conflict WHERE contains a mutable function */
	if (contain_mutable_functions((Node *) arbiterWhere) ||
		contain_mutable_functions((Node *) onConflictWhere))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("functions used in the WHERE clause of the ON CONFLICT "
							   "clause of INSERTs on distributed tables must be marked "
								 "IMMUTABLE")));
	}
#endif

	if (specifiesPartitionValue)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("modifying the partition value of rows is not allowed")));
	}
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
ContainsDisallowedFunctionCalls(Node *expression, bool *varArgument, bool *badCoalesce)
{
	bool result;
	WalkerState data;
	data.containsVar = data.varArgument = data.badCoalesce = false;

	result = ContainsDisallowedFunctionCallsWalker(expression, &data);

	*varArgument |= data.varArgument;
	*badCoalesce |= data.badCoalesce;
	return result;
}


static bool
ContainsDisallowedFunctionCallsWalker(Node *expression, WalkerState *state)
{
	char volatileFlag = 0;
	WalkerState childState;
	bool containsDisallowedFunction = false;

	childState.containsVar = childState.varArgument = childState.badCoalesce = false;

	if (expression == NULL)
	{
		return false;
	}

	if (IsA(expression, CoalesceExpr))
	{
		CoalesceExpr* expr = (CoalesceExpr *) expression;

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
		CaseExpr* expr = (CaseExpr *) expression;
		ListCell *temp;

		/*
		 * contain_mutable_functions doesn't know what to do with CaseWhen so we
		 * have to break it out ourselves
		 */
		foreach(temp, expr->args)
		{
			CaseWhen *when = (CaseWhen *) lfirst(temp);
			Assert(IsA(when, CaseWhen));

			if (contain_mutable_functions((Node *) when->expr) ||
				contain_mutable_functions((Node *) when->result))
			{
				state->badCoalesce = true;
				return true;
			}
		}

		if (contain_mutable_functions((Node *) expr->defresult))
		{
			state->badCoalesce = true;
			return true;
		}

		return ContainsDisallowedFunctionCallsWalker((Node *) (expr->arg), state);
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
	 * node types before bumping this version number to fix compilation.
	 *
	 * Once you've added them to this check, make sure you also evaluate them in the
	 * executor!
	 */
	StaticAssertStmt(PG_VERSION_NUM <= 90503, "When porting to a newer PG this section"
											  " needs to be reviewed.");

	if (IsA(expression, OpExpr))
	{
		OpExpr *expr = (OpExpr *) expression;

		set_opfuncid(expr);
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, FuncExpr))
	{
		FuncExpr *expr = (FuncExpr *) expression;

		volatileFlag = func_volatile(expr->funcid);
	}
	else if (IsA(expression, DistinctExpr))
	{
		/*
		 * to exercise this, you need to create a custom type for which the '=' operator
		 * is STABLE/VOLATILE
		 */
		DistinctExpr *expr = (DistinctExpr *) expression;

		set_opfuncid((OpExpr *) expr);  /* rely on struct equivalence */
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, NullIfExpr))
	{
		/*
		 * same as above, exercising this requires a STABLE/VOLATILE '=' operator
		 */
		NullIfExpr *expr = (NullIfExpr *) expression;

		set_opfuncid((OpExpr *) expr);  /* rely on struct equivalence */
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, ScalarArrayOpExpr))
	{
		/*
		 * to exercise this you need to CREATE OPERATOR with a binary predicate
		 * and use it within an ANY/ALL clause.
		 */
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) expression;

		set_sa_opfuncid(expr);
		volatileFlag = func_volatile(expr->opfuncid);
	}
	else if (IsA(expression, CoerceViaIO))
	{
		/*
		 * to exercise this you need to use a type with a STABLE/VOLATILE intype or
		 * outtype.
		 */
		CoerceViaIO *expr = (CoerceViaIO *) expression;
		Oid iofunc;
		Oid typioparam;
		bool typisvarlena;

		/* check the result type's input function */
		getTypeInputInfo(expr->resulttype,
						 &iofunc, &typioparam);
		volatileFlag = MostPermissiveVolatileFlag(volatileFlag, func_volatile(iofunc));

		/* check the input type's output function */
		getTypeOutputInfo(exprType((Node *) expr->arg),
						  &iofunc, &typisvarlena);
		volatileFlag = MostPermissiveVolatileFlag(volatileFlag, func_volatile(iofunc));
	}
	else if (IsA(expression, ArrayCoerceExpr))
	{
		ArrayCoerceExpr *expr = (ArrayCoerceExpr *) expression;

		if (OidIsValid(expr->elemfuncid))
		{
			volatileFlag = func_volatile(expr->elemfuncid);
		}
	}
	else if (IsA(expression, RowCompareExpr))
	{
		RowCompareExpr *rcexpr = (RowCompareExpr *) expression;
		ListCell *opid;

		foreach(opid, rcexpr->opnos)
		{
			volatileFlag = MostPermissiveVolatileFlag(volatileFlag,
													  op_volatile(lfirst_oid(opid)));
		}
	}
	else if (IsA(expression, Query))
	{
		/* subqueries aren't allowed and fail before control reaches this point */
		Assert(false);
	}

	if (volatileFlag == PROVOLATILE_VOLATILE)
	{
		/* the caller should have already checked for this */
		Assert(false);
	}
	else if (volatileFlag == PROVOLATILE_STABLE)
	{
		containsDisallowedFunction =
			expression_tree_walker(expression,
								   ContainsDisallowedFunctionCallsWalker,
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
								  ContainsDisallowedFunctionCallsWalker,
								  state);
}


/*
 * Return the most-pessimistic volatility flag of the two params.
 *
 * for example: given two flags, if one is stable and one is volatile, an expression
 * involving both is volatile.
 */
char
MostPermissiveVolatileFlag(char left, char right)
{
	if (left == PROVOLATILE_VOLATILE || right == PROVOLATILE_VOLATILE)
	{
		return PROVOLATILE_VOLATILE;
	}
	else if (left == PROVOLATILE_STABLE || right == PROVOLATILE_STABLE)
	{
		return PROVOLATILE_STABLE;
	}
	else
	{
		return PROVOLATILE_IMMUTABLE;
	}
}


/*
 * RouterModifyTask builds a Task to represent a modification performed by
 * the provided query against the provided shard interval. This task contains
 * shard-extended deparsed SQL to be run during execution.
 */
static Task *
RouterModifyTask(Query *query)
{
	ShardInterval *shardInterval = TargetShardInterval(query);
	uint64 shardId = shardInterval->shardId;
	FromExpr *joinTree = NULL;
	StringInfo queryString = makeStringInfo();
	Task *modifyTask = NULL;
	bool upsertQuery = false;

	/* grab shared metadata lock to stop concurrent placement additions */
	LockShardDistributionMetadata(shardId, ShareLock);

	/*
	 * Convert the qualifiers to an explicitly and'd clause, which is needed
	 * before we deparse the query. This applies to SELECT, UPDATE and
	 * DELETE statements.
	 */
	joinTree = query->jointree;
	if ((joinTree != NULL) && (joinTree->quals != NULL))
	{
		Node *whereClause = joinTree->quals;
		if (IsA(whereClause, List))
		{
			joinTree->quals = (Node *) make_ands_explicit((List *) whereClause);
		}
	}

#if (PG_VERSION_NUM >= 90500)
	if (query->onConflict != NULL)
	{
		RangeTblEntry *rangeTableEntry = NULL;
		Oid relationId = shardInterval->relationId;

		/* set the flag */
		upsertQuery = true;

		/* setting an alias simplifies deparsing of UPSERTs */
		rangeTableEntry = linitial(query->rtable);
		if (rangeTableEntry->alias == NULL)
		{
			Alias *alias = makeAlias(UPSERT_ALIAS, NIL);
			rangeTableEntry->alias = alias;
		}

		/* some fields in onConflict expression needs to be updated for deparsing */
		query->onConflict = RebuildOnConflict(relationId, query->onConflict);
	}
#else

	/* always set to false for PG_VERSION_NUM < 90500 */
	upsertQuery = false;
#endif

	/*
	 * We set inh flag of all range tables entries to true so that deparser will not
	 * add ONLY keyword to resulting query string.
	 */
	SetRangeTablesInherited(query);

	deparse_shard_query(query, shardInterval->relationId, shardId, queryString);
	ereport(DEBUG4, (errmsg("distributed statement: %s", queryString->data)));

	modifyTask = CitusMakeNode(Task);
	modifyTask->jobId = INVALID_JOB_ID;
	modifyTask->taskId = INVALID_TASK_ID;
	modifyTask->taskType = MODIFY_TASK;
	modifyTask->queryString = queryString->data;
	modifyTask->anchorShardId = shardId;
	modifyTask->dependedTaskList = NIL;
	modifyTask->upsertQuery = upsertQuery;

	return modifyTask;
}


#if (PG_VERSION_NUM >= 90500)

/*
 * RebuildOnConflict rebuilds OnConflictExpr for correct deparsing. The function
 * makes WHERE clause elements explicit and filters dropped columns
 * from the target list.
 */
static OnConflictExpr *
RebuildOnConflict(Oid relationId, OnConflictExpr *originalOnConflict)
{
	OnConflictExpr *updatedOnConflict = copyObject(originalOnConflict);
	Node *onConflictWhere = updatedOnConflict->onConflictWhere;
	List *onConflictSet = updatedOnConflict->onConflictSet;
	TupleDesc distributedRelationDesc = NULL;
	ListCell *targetEntryCell = NULL;
	List *filteredOnConflictSet = NIL;
	Form_pg_attribute *tableAttributes = NULL;
	Relation distributedRelation = RelationIdGetRelation(relationId);

	/* Convert onConflictWhere qualifiers to an explicitly and'd clause */
	updatedOnConflict->onConflictWhere =
		(Node *) make_ands_explicit((List *) onConflictWhere);

	/*
	 * Here we handle dropped columns on the distributed table. onConflictSet
	 * includes the table attributes even if they are dropped,
	 * since the it is expanded via expand_targetlist() on standard planner.
	 */

	/* get the relation tuple descriptor and table attributes */
	distributedRelationDesc = RelationGetDescr(distributedRelation);
	tableAttributes = distributedRelationDesc->attrs;

	foreach(targetEntryCell, onConflictSet)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		FormData_pg_attribute *tableAttribute = tableAttributes[targetEntry->resno - 1];

		/* skip dropped columns */
		if (tableAttribute->attisdropped)
		{
			continue;
		}

		/* we only want to deparse non-dropped columns */
		filteredOnConflictSet = lappend(filteredOnConflictSet, targetEntry);
	}

	/* close distributedRelation to prevent leaks */
	RelationClose(distributedRelation);

	/* set onConflictSet again with the filtered list */
	updatedOnConflict->onConflictSet = filteredOnConflictSet;

	return updatedOnConflict;
}


#endif


/*
 * TargetShardInterval determines the single shard targeted by a provided command.
 * If no matching shards exist, or if the modification targets more than one one
 * shard, this function raises an error depending on the command type.
 */
static ShardInterval *
TargetShardInterval(Query *query)
{
	CmdType commandType = query->commandType;
	bool selectTask = (commandType == CMD_SELECT);
	List *prunedShardList = NIL;
	int prunedShardCount = 0;


	int shardCount = 0;
	Oid distributedTableId = ExtractFirstDistributedTableId(query);
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	char partitionMethod = cacheEntry->partitionMethod;
	bool fastShardPruningPossible = false;

	/* error out if no shards exist for the table */
	shardCount = cacheEntry->shardIntervalArrayLength;
	if (shardCount == 0)
	{
		char *relationName = get_rel_name(distributedTableId);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("could not find any shards"),
						errdetail("No shards exist for distributed table \"%s\".",
								  relationName),
						errhint("Run master_create_worker_shards to create shards "
								"and try again.")));
	}

	fastShardPruningPossible = FastShardPruningPossible(query->commandType,
														partitionMethod);
	if (fastShardPruningPossible)
	{
		uint32 rangeTableId = 1;
		Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
		Const *partitionValue = ExtractInsertPartitionValue(query, partitionColumn);
		ShardInterval *shardInterval = FastShardPruning(distributedTableId,
														partitionValue);

		if (shardInterval != NULL)
		{
			prunedShardList = lappend(prunedShardList, shardInterval);
		}
	}
	else
	{
		List *restrictClauseList = QueryRestrictList(query);
		Index tableId = 1;
		List *shardIntervalList = LoadShardIntervalList(distributedTableId);

		prunedShardList = PruneShardList(distributedTableId, tableId, restrictClauseList,
										 shardIntervalList);
	}

	prunedShardCount = list_length(prunedShardList);
	if (prunedShardCount != 1)
	{
		if (selectTask)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("router executor queries must target exactly one "
								   "shard")));
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("distributed modifications must target exactly one "
								   "shard")));
		}
	}

	return (ShardInterval *) linitial(prunedShardList);
}


/*
 * UseFastShardPruning returns true if the commandType is INSERT and partition method
 * is hash or range.
 */
static bool
FastShardPruningPossible(CmdType commandType, char partitionMethod)
{
	/* we currently only support INSERTs */
	if (commandType != CMD_INSERT)
	{
		return false;
	}

	/* fast shard pruning is only supported for hash and range partitioned tables */
	if (partitionMethod == DISTRIBUTE_BY_HASH || partitionMethod == DISTRIBUTE_BY_RANGE)
	{
		return true;
	}

	return false;
}


/*
 * FastShardPruning is a higher level API for FindShardInterval function. Given the relationId
 * of the distributed table and partitionValue, FastShardPruning function finds the corresponding
 * shard interval that the partitionValue should be in. FastShardPruning returns NULL if no
 * ShardIntervals exist for the given partitionValue.
 */
static ShardInterval *
FastShardPruning(Oid distributedTableId, Const *partitionValue)
{
	DistTableCacheEntry *cacheEntry = DistributedTableCacheEntry(distributedTableId);
	int shardCount = cacheEntry->shardIntervalArrayLength;
	ShardInterval **sortedShardIntervalArray = cacheEntry->sortedShardIntervalArray;
	bool useBinarySearch = false;
	char partitionMethod = cacheEntry->partitionMethod;
	FmgrInfo *shardIntervalCompareFunction = cacheEntry->shardIntervalCompareFunction;
	bool hasUniformHashDistribution = cacheEntry->hasUniformHashDistribution;
	FmgrInfo *hashFunction = NULL;
	ShardInterval *shardInterval = NULL;

	/* determine whether to use binary search */
	if (partitionMethod != DISTRIBUTE_BY_HASH || !hasUniformHashDistribution)
	{
		useBinarySearch = true;
	}

	/* we only need hash functions for hash distributed tables */
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		hashFunction = cacheEntry->hashFunction;
	}

	/*
	 * Call FindShardInterval to find the corresponding shard interval for the
	 * given partition value.
	 */
	shardInterval = FindShardInterval(partitionValue->constvalue,
									  sortedShardIntervalArray, shardCount,
									  partitionMethod,
									  shardIntervalCompareFunction, hashFunction,
									  useBinarySearch);

	return shardInterval;
}


/*
 * QueryRestrictList returns the restriction clauses for the query. For a SELECT
 * statement these are the where-clause expressions. For INSERT statements we
 * build an equality clause based on the partition-column and its supplied
 * insert value.
 */
static List *
QueryRestrictList(Query *query)
{
	List *queryRestrictList = NIL;
	CmdType commandType = query->commandType;

	if (commandType == CMD_INSERT)
	{
		/* build equality expression based on partition column value for row */
		Oid distributedTableId = ExtractFirstDistributedTableId(query);
		uint32 rangeTableId = 1;
		Var *partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
		Const *partitionValue = ExtractInsertPartitionValue(query, partitionColumn);

		OpExpr *equalityExpr = MakeOpExpression(partitionColumn, BTEqualStrategyNumber);

		Node *rightOp = get_rightop((Expr *) equalityExpr);
		Const *rightConst = (Const *) rightOp;
		Assert(IsA(rightOp, Const));

		rightConst->constvalue = partitionValue->constvalue;
		rightConst->constisnull = partitionValue->constisnull;
		rightConst->constbyval = partitionValue->constbyval;

		queryRestrictList = list_make1(equalityExpr);
	}
	else if (commandType == CMD_UPDATE || commandType == CMD_DELETE ||
			 commandType == CMD_SELECT)
	{
		queryRestrictList = WhereClauseList(query->jointree);
	}

	return queryRestrictList;
}


/*
 * ExtractFirstDistributedTableId takes a given query, and finds the relationId
 * for the first distributed table in that query. If the function cannot find a
 * distributed table, it returns InvalidOid.
 */
static Oid
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
 * ExtractPartitionValue extracts the partition column value from a the target
 * of an INSERT command. If a partition value is missing altogether or is
 * NULL, this function throws an error.
 */
static Const *
ExtractInsertPartitionValue(Query *query, Var *partitionColumn)
{
	Const *partitionValue = NULL;
	TargetEntry *targetEntry = get_tle_by_resno(query->targetList,
												partitionColumn->varattno);
	if (targetEntry != NULL)
	{
		Assert(IsA(targetEntry->expr, Const));

		partitionValue = (Const *) targetEntry->expr;
	}

	if (partitionValue == NULL || partitionValue->constisnull)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("cannot plan INSERT using row with NULL value "
							   "in partition column")));
	}

	return partitionValue;
}


/* RouterSelectTask builds a Task to represent a single shard select query */
static Task *
RouterSelectTask(Query *query)
{
	Task *task = NULL;
	ShardInterval *shardInterval = TargetShardInterval(query);
	StringInfo queryString = makeStringInfo();
	uint64 shardId = INVALID_SHARD_ID;
	bool upsertQuery = false;
	CmdType commandType PG_USED_FOR_ASSERTS_ONLY = query->commandType;
	FromExpr *joinTree = NULL;

	Assert(shardInterval != NULL);
	Assert(commandType == CMD_SELECT);

	shardId = shardInterval->shardId;

	/*
	 * Convert the qualifiers to an explicitly and'd clause, which is needed
	 * before we deparse the query.
	 */
	joinTree = query->jointree;
	if ((joinTree != NULL) && (joinTree->quals != NULL))
	{
		Node *whereClause = (Node *) make_ands_explicit((List *) joinTree->quals);
		joinTree->quals = whereClause;
	}

	/*
	 * We set inh flag of all range tables entries to true so that deparser will not
	 * add ONLY keyword to resulting query string.
	 */
	SetRangeTablesInherited(query);

	deparse_shard_query(query, shardInterval->relationId, shardId, queryString);
	ereport(DEBUG4, (errmsg("distributed statement: %s", queryString->data)));

	task = CitusMakeNode(Task);
	task->jobId = INVALID_JOB_ID;
	task->taskId = INVALID_TASK_ID;
	task->taskType = ROUTER_TASK;
	task->queryString = queryString->data;
	task->anchorShardId = shardId;
	task->dependedTaskList = NIL;
	task->upsertQuery = upsertQuery;

	return task;
}


/*
 * RouterQueryJob creates a Job for the specified query to execute the
 * provided single shard select task.
 */
static Job *
RouterQueryJob(Query *query, Task *task)
{
	Job *job = NULL;
	List *taskList = NIL;
	TaskType taskType = task->taskType;

	/*
	 * We send modify task to the first replica, otherwise we choose the target shard
	 * according to task assignment policy.
	 */
	if (taskType == MODIFY_TASK)
	{
		taskList = FirstReplicaAssignTaskList(list_make1(task));
	}
	else
	{
		taskList = AssignAnchorShardTaskList(list_make1(task));
	}

	job = CitusMakeNode(Job);
	job->dependedJobList = NIL;
	job->jobId = INVALID_JOB_ID;
	job->subqueryPushdown = false;
	job->jobQuery = query;
	job->taskList = taskList;

	return job;
}


/*
 * MultiRouterPlannableQuery returns true if given query can be router plannable.
 * The query is router plannable if it is a select query issued on a hash
 * partitioned distributed table, and it has a exact match comparison on the
 * partition column. This feature is enabled if task executor is set to real-time
 */
bool
MultiRouterPlannableQuery(Query *query, MultiExecutorType taskExecutorType)
{
	uint32 rangeTableId = 1;
	List *rangeTableList = NIL;
	RangeTblEntry *rangeTableEntry = NULL;
	Oid distributedTableId = InvalidOid;
	Var *partitionColumn = NULL;
	char partitionMethod = '\0';
	Node *quals = NULL;
	CmdType commandType PG_USED_FOR_ASSERTS_ONLY = query->commandType;
	FromExpr *joinTree = query->jointree;
	List *varClauseList = NIL;
	ListCell *varClauseCell = NULL;
	bool partitionColumnMatchExpression = false;
	int partitionColumnReferenceCount = 0;
	int shardCount = 0;

	if (commandType == CMD_INSERT || commandType == CMD_UPDATE ||
		commandType == CMD_DELETE)
	{
		return true;
	}

	if (taskExecutorType != MULTI_EXECUTOR_REAL_TIME)
	{
		return false;
	}

	Assert(commandType == CMD_SELECT);

	/*
	 * Reject subqueries which are in SELECT or WHERE clause.
	 * Queries which are recursive, with CommonTableExpr, with locking (hasForUpdate),
	 * or with window functions are also rejected here.
	 * Queries which have subqueries, or tablesamples in FROM clauses are rejected later
	 * during RangeTblEntry checks.
	 */
	if (query->hasSubLinks == true || query->cteList != NIL || query->hasForUpdate ||
		query->hasRecursive)
	{
		return false;
	}

#if (PG_VERSION_NUM >= 90500)
	if (query->groupingSets)
	{
		return false;
	}
#endif

	/* only hash partitioned tables are supported */
	distributedTableId = ExtractFirstDistributedTableId(query);
	partitionColumn = PartitionColumn(distributedTableId, rangeTableId);
	partitionMethod = PartitionMethod(distributedTableId);

	if (partitionMethod != DISTRIBUTE_BY_HASH)
	{
		return false;
	}

	/* extract range table entries */
	ExtractRangeTableEntryWalker((Node *) query, &rangeTableList);

	/* query can have only one range table of type RTE_RELATION */
	if (list_length(rangeTableList) != 1)
	{
		return false;
	}

	rangeTableEntry = (RangeTblEntry *) linitial(rangeTableList);
	if (rangeTableEntry->rtekind != RTE_RELATION)
	{
		return false;
	}

#if (PG_VERSION_NUM >= 90500)
	if (rangeTableEntry->tablesample)
	{
		return false;
	}
#endif

	if (joinTree == NULL)
	{
		return false;
	}

	quals = joinTree->quals;
	if (quals == NULL)
	{
		return false;
	}

	/* convert list of expressions into expression tree */
	if (quals != NULL && IsA(quals, List))
	{
		quals = (Node *) make_ands_explicit((List *) quals);
	}

	/*
	 * Partition column must be used in a simple equality match check and it must be
	 * place at top level conjustion operator.
	 */
	partitionColumnMatchExpression =
		ColumnMatchExpressionAtTopLevelConjunction(quals, partitionColumn);

	if (!partitionColumnMatchExpression)
	{
		return false;
	}

	/* make sure partition column is used only once in the query */
	varClauseList = pull_var_clause_default(quals);
	foreach(varClauseCell, varClauseList)
	{
		Var *column = (Var *) lfirst(varClauseCell);
		if (equal(column, partitionColumn))
		{
			partitionColumnReferenceCount++;
		}
	}

	if (partitionColumnReferenceCount != 1)
	{
		return false;
	}

	/*
	 * We need to make sure there is at least one shard for this hash partitioned
	 * query to be router plannable. We can not prepare a router plan if there
	 * are no shards.
	 */
	shardCount = ShardIntervalCount(distributedTableId);
	if (shardCount == 0)
	{
		return false;
	}

	return true;
}


/*
 * ColumnMatchExpressionAtTopLevelConjunction returns true if the query contains an exact
 * match (equal) expression on the provided column. The function returns true only
 * if the match expression has an AND relation with the rest of the expression tree.
 */
static bool
ColumnMatchExpressionAtTopLevelConjunction(Node *node, Var *column)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, OpExpr))
	{
		OpExpr *opExpr = (OpExpr *) node;
		bool simpleExpression = SimpleOpExpression((Expr *) opExpr);
		bool columnInExpr = false;
		bool usingEqualityOperator = false;

		if (!simpleExpression)
		{
			return false;
		}

		columnInExpr = OpExpressionContainsColumn(opExpr, column);
		if (!columnInExpr)
		{
			return false;
		}

		usingEqualityOperator = OperatorImplementsEquality(opExpr->opno);

		return usingEqualityOperator;
	}
	else if (IsA(node, BoolExpr))
	{
		BoolExpr *boolExpr = (BoolExpr *) node;
		List *argumentList = boolExpr->args;
		ListCell *argumentCell = NULL;

		if (boolExpr->boolop != AND_EXPR)
		{
			return false;
		}

		foreach(argumentCell, argumentList)
		{
			Node *argumentNode = (Node *) lfirst(argumentCell);
			bool columnMatch =
				ColumnMatchExpressionAtTopLevelConjunction(argumentNode, column);
			if (columnMatch)
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * RouterSetRangeTablesInherited sets inh flag of all range table entries to true.
 * We basically iterate over all range table entries and set their inh flag.
 */
static void
SetRangeTablesInherited(Query *query)
{
	List *rangeTableList = query->rtable;
	ListCell *rangeTableCell = NULL;

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);
		if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			rangeTableEntry->inh = true;
		}
	}
}
