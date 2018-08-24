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


/* private function declarations */
static bool IsVarNode(Node *node);
static Expr * citus_evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
								  Oid result_collation, PlanState *planState);
static bool CitusIsVolatileFunctionIdChecker(Oid func_id, void *context);
static bool CitusIsMutableFunctionIdChecker(Oid func_id, void *context);


/*
 * RequiresMastereEvaluation returns the executor needs to reparse and
 * try to execute this query, which is the case if the query contains
 * any stable or volatile function.
 */
bool
RequiresMasterEvaluation(Query *query)
{
	return FindNodeCheck((Node *) query, CitusIsMutableFunction);
}


/*
 * ExecuteMasterEvaluableFunctions evaluates expressions that can be resolved
 * to a constant.
 */
void
ExecuteMasterEvaluableFunctions(Query *query, PlanState *planState)
{
	PartiallyEvaluateExpression((Node *) query, planState);
}


/*
 * PartiallyEvaluateExpression descend into an expression tree to evaluate
 * expressions that can be resolved to a constant on the master. Expressions
 * containing a Var are skipped, since the value of the Var is not known
 * on the master.
 */
Node *
PartiallyEvaluateExpression(Node *expression, PlanState *planState)
{
	if (expression == NULL || IsA(expression, Const))
	{
		return expression;
	}

	switch (nodeTag(expression))
	{
		case T_Param:
		{
			Param *param = (Param *) expression;
			if (param->paramkind == PARAM_SUBLINK)
			{
				/* ExecInitExpr cannot handle PARAM_SUBLINK */
				return expression;
			}
		}

		case T_FuncExpr:
		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
		case T_ScalarArrayOpExpr:
		case T_RowCompareExpr:
		case T_RelabelType:
		case T_CoerceToDomain:
		{
			if (FindNodeCheck(expression, IsVarNode))
			{
				return (Node *) expression_tree_mutator(expression,
														PartiallyEvaluateExpression,
														planState);
			}

			return (Node *) citus_evaluate_expr((Expr *) expression,
												exprType(expression),
												exprTypmod(expression),
												exprCollation(expression),
												planState);
		}

		case T_Query:
		{
			return (Node *) query_tree_mutator((Query *) expression,
											   PartiallyEvaluateExpression,
											   planState, QTW_DONT_COPY_QUERY);
		}

		default:
		{
			return (Node *) expression_tree_mutator(expression,
													PartiallyEvaluateExpression,
													planState);
		}
	}

	return expression;
}


/*
 * IsVarNode returns whether a node is a Var (column reference).
 */
static bool
IsVarNode(Node *node)
{
	return IsA(node, Var);
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
