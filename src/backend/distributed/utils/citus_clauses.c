/*
 * citus_clauses.c
 *
 * Routines roughly equivalent to postgres' util/clauses.
 *
 * Copyright (c) 2016-2016, Citus Data, Inc.
 */

#include "postgres.h"

#include "distributed/citus_clauses.h"
#include "distributed/insert_select_planner.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_router_planner.h"

#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#include "optimizer/planmain.h"
#include "utils/datum.h"
#include "utils/lsyscache.h"


typedef struct FunctionEvaluationContext
{
	PlanState *planState;
	bool containsVar;
} FunctionEvaluationContext;


/* private function declarations */
static void EvaluateValuesListsItems(List *valuesLists, PlanState *planState);
static Node * EvaluateNodeIfReferencesFunction(Node *expression, PlanState *planState);
static Node * PartiallyEvaluateExpressionMutator(Node *expression,
												 FunctionEvaluationContext *context);
static Expr * citus_evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
								  Oid result_collation, PlanState *planState);
static bool CitusIsVolatileFunctionIdChecker(Oid func_id, void *context);
static bool CitusIsMutableFunctionIdChecker(Oid func_id, void *context);


/*
 * Whether the executor needs to reparse and try to execute this query.
 */
bool
RequiresMasterEvaluation(Query *query)
{
	ListCell *targetEntryCell = NULL;
	ListCell *rteCell = NULL;
	ListCell *cteCell = NULL;

	foreach(targetEntryCell, query->targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

		if (FindNodeCheck((Node *) targetEntry->expr, CitusIsMutableFunction))
		{
			return true;
		}
	}

	foreach(rteCell, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);

		if (rte->rtekind == RTE_SUBQUERY)
		{
			if (RequiresMasterEvaluation(rte->subquery))
			{
				return true;
			}
		}
		else if (rte->rtekind == RTE_VALUES)
		{
			if (FindNodeCheck((Node *) rte->values_lists, CitusIsMutableFunction))
			{
				return true;
			}
		}
	}

	foreach(cteCell, query->cteList)
	{
		CommonTableExpr *expr = (CommonTableExpr *) lfirst(cteCell);

		if (RequiresMasterEvaluation((Query *) expr->ctequery))
		{
			return true;
		}
	}

	if (query->jointree && query->jointree->quals)
	{
		if (FindNodeCheck((Node *) query->jointree->quals, CitusIsMutableFunction))
		{
			return true;
		}
	}

	return false;
}


/*
 * Looks at each TargetEntry of the query and the jointree quals, evaluating
 * any sub-expressions which don't include Vars.
 */
void
ExecuteMasterEvaluableFunctions(Query *query, PlanState *planState)
{
	ListCell *targetEntryCell = NULL;
	ListCell *rteCell = NULL;
	ListCell *cteCell = NULL;
	Node *modifiedNode = NULL;

	if (query->jointree && query->jointree->quals)
	{
		query->jointree->quals = PartiallyEvaluateExpression(query->jointree->quals,
															 planState);
	}

	foreach(targetEntryCell, query->targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

		/* performance optimization for the most common cases */
		if (IsA(targetEntry->expr, Const) || IsA(targetEntry->expr, Var))
		{
			continue;
		}

		modifiedNode = PartiallyEvaluateExpression((Node *) targetEntry->expr,
												   planState);

		targetEntry->expr = (Expr *) modifiedNode;
	}

	foreach(rteCell, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);

		if (rte->rtekind == RTE_SUBQUERY)
		{
			ExecuteMasterEvaluableFunctions(rte->subquery, planState);
		}
		else if (rte->rtekind == RTE_VALUES)
		{
			EvaluateValuesListsItems(rte->values_lists, planState);
		}
	}

	foreach(cteCell, query->cteList)
	{
		CommonTableExpr *expr = (CommonTableExpr *) lfirst(cteCell);

		ExecuteMasterEvaluableFunctions((Query *) expr->ctequery, planState);
	}
}


/*
 * EvaluateValuesListsItems siply does the work of walking over each expression
 * in each value list contained in a multi-row INSERT's VALUES RTE. Basically
 * a nested for loop to perform an in-place replacement of expressions with
 * their ultimate values, should evaluation be necessary.
 */
static void
EvaluateValuesListsItems(List *valuesLists, PlanState *planState)
{
	ListCell *exprListCell = NULL;

	foreach(exprListCell, valuesLists)
	{
		List *exprList = (List *) lfirst(exprListCell);
		ListCell *exprCell = NULL;

		foreach(exprCell, exprList)
		{
			Expr *expr = (Expr *) lfirst(exprCell);
			Node *modifiedNode = NULL;

			modifiedNode = PartiallyEvaluateExpression((Node *) expr, planState);

			exprCell->data.ptr_value = (void *) modifiedNode;
		}
	}
}


/*
 * Walks the expression evaluating any node which invokes a function as long as a Var
 * doesn't show up in the parameter list.
 */
Node *
PartiallyEvaluateExpression(Node *expression, PlanState *planState)
{
	FunctionEvaluationContext globalContext = { planState, false };

	return PartiallyEvaluateExpressionMutator(expression, &globalContext);
}


/*
 * When you find a function call evaluate it, the planner made sure there were no Vars.
 *
 * Tell your parent if either you or one if your children is a Var.
 *
 * A little inefficient. It goes to the bottom of the tree then calls EvaluateExpression
 * on each function on the way back up. Say we had an expression with no Vars, we could
 * only call EvaluateExpression on the top-most level and get the same result.
 */
static Node *
PartiallyEvaluateExpressionMutator(Node *expression, FunctionEvaluationContext *context)
{
	Node *copy = NULL;
	FunctionEvaluationContext localContext = { context->planState, false };

	if (expression == NULL)
	{
		return expression;
	}

	/* pass any argument lists back to the mutator to copy and recurse for us */
	if (IsA(expression, List))
	{
		return expression_tree_mutator(expression,
									   PartiallyEvaluateExpressionMutator,
									   context);
	}

	if (IsA(expression, SubLink))
	{
		return expression;
	}

	if (IsA(expression, Var))
	{
		context->containsVar = true;

		/* makes a copy for us */
		return expression_tree_mutator(expression,
									   PartiallyEvaluateExpressionMutator,
									   context);
	}

	copy = expression_tree_mutator(expression,
								   PartiallyEvaluateExpressionMutator,
								   &localContext);

	if (localContext.containsVar)
	{
		context->containsVar = true;
	}
	else
	{
		copy = EvaluateNodeIfReferencesFunction(copy, context->planState);
	}

	return copy;
}


/*
 * Used to evaluate functions during queries on the master before sending them to workers
 *
 * The idea isn't to evaluate every kind of expression, just the kinds whoes result might
 * change between invocations (the idea is to allow users to use functions but still have
 * consistent shard replicas, since we use statement replication). This means evaluating
 * all nodes which invoke functions which might not be IMMUTABLE.
 */
static Node *
EvaluateNodeIfReferencesFunction(Node *expression, PlanState *planState)
{
	if (expression == NULL || IsA(expression, Const))
	{
		return expression;
	}

	switch (nodeTag(expression))
	{
		case T_FuncExpr:
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
		case T_ScalarArrayOpExpr:
		case T_RowCompareExpr:
		case T_Param:
		case T_RelabelType:
		case T_CoerceToDomain:
		{
			return (Node *) citus_evaluate_expr((Expr *) expression,
												exprType(expression),
												exprTypmod(expression),
												exprCollation(expression),
												planState);
		}

		default:
		{
			break;
		}
	}

	return expression;
}


/*
 * a copy of pg's evaluate_expr, pre-evaluate a constant expression
 *
 * We use the executor's routine ExecEvalExpr() to avoid duplication of
 * code and ensure we get the same result as the executor would get.
 *
 * *INDENT-OFF*
 */
static Expr *
citus_evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
					Oid result_collation, PlanState *planState)
{
	EState     *estate;
	ExprState  *exprstate;
	ExprContext *econtext;
	MemoryContext oldcontext;
	Datum		const_val;
	bool		const_is_null;
	int16		resultTypLen;
	bool		resultTypByVal;

	/*
	 * To use the executor, we need an EState.
	 */
	estate = CreateExecutorState();

	/* We can use the estate's working context to avoid memory leaks. */
	oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

	/* Make sure any opfuncids are filled in. */
	fix_opfuncids((Node *) expr);

	/*
	 * Prepare expr for execution.  (Note: we can't use ExecPrepareExpr
	 * because it'd result in recursively invoking eval_const_expressions.)
	 */
	exprstate = ExecInitExpr(expr, planState);

	if (planState != NULL)
	{
		/* use executor's context to pass down parameters */
		econtext = planState->ps_ExprContext;
	}
	else
	{
		/* when called from a function, use a default context */
		econtext = GetPerTupleExprContext(estate);
	}

	/*
	 * And evaluate it.
	 */
#if (PG_VERSION_NUM >= 100000)
	const_val = ExecEvalExprSwitchContext(exprstate, econtext, &const_is_null);
#else
	const_val = ExecEvalExprSwitchContext(exprstate, econtext, &const_is_null, NULL);
#endif

	/* Get info needed about result datatype */
	get_typlenbyval(result_type, &resultTypLen, &resultTypByVal);

	/* Get back to outer memory context */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Must copy result out of sub-context used by expression eval.
	 *
	 * Also, if it's varlena, forcibly detoast it.  This protects us against
	 * storing TOAST pointers into plans that might outlive the referenced
	 * data.  (makeConst would handle detoasting anyway, but it's worth a few
	 * extra lines here so that we can do the copy and detoast in one step.)
	 */
	if (!const_is_null)
	{
		if (resultTypLen == -1)
			const_val = PointerGetDatum(PG_DETOAST_DATUM_COPY(const_val));
		else
			const_val = datumCopy(const_val, resultTypByVal, resultTypLen);
	}

	/* Release all the junk we just created */
	FreeExecutorState(estate);

	/*
	 * Make the constant result node.
	 */
	return (Expr *) makeConst(result_type, result_typmod, result_collation,
							  resultTypLen,
							  const_val, const_is_null,
							  resultTypByVal);
}


/*
 * CitusIsVolatileFunctionIdChecker checks if the given function id is
 * a volatile function other than read_intermediate_result().
 */
static bool
CitusIsVolatileFunctionIdChecker(Oid func_id, void *context)
{
	if (func_id == CitusReadIntermediateResultFuncId())
	{
		return false;
	}

	return (func_volatile(func_id) == PROVOLATILE_VOLATILE);
}


/*
 * CitusIsVolatileFunction checks if the given node is a volatile function
 * other than Citus's internal functions.
 */
bool
CitusIsVolatileFunction(Node *node)
{
	/* Check for volatile functions in node itself */
	if (check_functions_in_node(node, CitusIsVolatileFunctionIdChecker, NULL))
	{
		return true;
	}

#if (PG_VERSION_NUM >= 100000)
	if (IsA(node, NextValueExpr))
	{
		/* NextValueExpr is volatile */
		return true;
	}
#endif

	return false;
}


/*
 * CitusIsMutableFunctionIdChecker checks if the given function id is
 * a mutable function other than read_intermediate_result().
 */
static bool
CitusIsMutableFunctionIdChecker(Oid func_id, void *context)
{
	if (func_id == CitusReadIntermediateResultFuncId())
	{
		return false;
	}
	else
	{
		return (func_volatile(func_id) != PROVOLATILE_IMMUTABLE);
	}
}


/*
 * CitusIsMutableFunction checks if the given node is a mutable function
 * other than Citus's internal functions.
 */
bool
CitusIsMutableFunction(Node *node)
{
	/* Check for mutable functions in node itself */
	if (check_functions_in_node(node, CitusIsMutableFunctionIdChecker, NULL))
	{
		return true;
	}

#if (PG_VERSION_NUM >= 100000)
	if (IsA(node, SQLValueFunction))
	{
		/* all variants of SQLValueFunction are stable */
		return true;
	}

	if (IsA(node, NextValueExpr))
	{
		/* NextValueExpr is volatile */
		return true;
	}
#endif

	return false;
}


/* *INDENT-ON* */
