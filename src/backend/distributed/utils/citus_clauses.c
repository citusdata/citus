/*
 * citus_clauses.c
 *
 * Routines roughly equivalent to postgres' util/clauses.
 *
 * Copyright (c) Citus Data, Inc.
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
static bool IsVariableExpression(Node *node);
static Expr * citus_evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
								  Oid result_collation,
								  MasterEvaluationContext *masterEvaluationContext);
static bool CitusIsVolatileFunctionIdChecker(Oid func_id, void *context);
static bool CitusIsMutableFunctionIdChecker(Oid func_id, void *context);
static bool ShouldEvaluateExpression(Expr *expression);
static bool ShouldEvaluateFunctionWithMasterContext(MasterEvaluationContext *
													evaluationContext);

/*
 * RequiresMastereEvaluation returns the executor needs to reparse and
 * try to execute this query, which is the case if the query contains
 * any stable or volatile function.
 */
bool
RequiresMasterEvaluation(Query *query)
{
	if (query->commandType == CMD_SELECT)
	{
		return false;
	}

	return FindNodeCheck((Node *) query, CitusIsMutableFunction);
}


/*
 * ExecuteMasterEvaluableFunctionsAndParameters evaluates expressions and parameters
 * that can be resolved to a constant.
 */
void
ExecuteMasterEvaluableFunctionsAndParameters(Query *query, PlanState *planState)
{
	MasterEvaluationContext masterEvaluationContext;

	masterEvaluationContext.planState = planState;
	masterEvaluationContext.evaluationMode = EVALUATE_FUNCTIONS_PARAMS;

	PartiallyEvaluateExpression((Node *) query, &masterEvaluationContext);
}


/*
 * ExecuteMasterEvaluableParameters evaluates external parameters that can be
 * resolved to a constant.
 */
void
ExecuteMasterEvaluableParameters(Query *query, PlanState *planState)
{
	MasterEvaluationContext masterEvaluationContext;

	masterEvaluationContext.planState = planState;
	masterEvaluationContext.evaluationMode = EVALUATE_PARAMS;

	PartiallyEvaluateExpression((Node *) query, &masterEvaluationContext);
}


/*
 * PartiallyEvaluateExpression descends into an expression tree to evaluate
 * expressions that can be resolved to a constant on the master. Expressions
 * containing a Var are skipped, since the value of the Var is not known
 * on the master.
 */
Node *
PartiallyEvaluateExpression(Node *expression,
							MasterEvaluationContext *masterEvaluationContext)
{
	if (expression == NULL || IsA(expression, Const))
	{
		return expression;
	}

	NodeTag nodeTag = nodeTag(expression);
	if (nodeTag == T_Param)
	{
		Param *param = (Param *) expression;
		if (param->paramkind == PARAM_SUBLINK)
		{
			/* ExecInitExpr cannot handle PARAM_SUBLINK */
			return expression;
		}

		return (Node *) citus_evaluate_expr((Expr *) expression,
											exprType(expression),
											exprTypmod(expression),
											exprCollation(expression),
											masterEvaluationContext);
	}
	else if (ShouldEvaluateExpression((Expr *) expression) &&
			 ShouldEvaluateFunctionWithMasterContext(masterEvaluationContext))
	{
		if (FindNodeCheck(expression, IsVariableExpression))
		{
			/*
			 * The expression contains a variable expression (e.g. a stable function,
			 * which has a column reference as its input). That means that we cannot
			 * evaluate the expression on the coordinator, since the result depends
			 * on the input.
			 *
			 * Skipping function evaluation for these expressions is safe in most
			 * cases, since the function will always be re-evaluated for every input
			 * value. An exception is function calls that call another stable function
			 * that should not be re-evaluated, such as now().
			 */
			return (Node *) expression_tree_mutator(expression,
													PartiallyEvaluateExpression,
													masterEvaluationContext);
		}

		return (Node *) citus_evaluate_expr((Expr *) expression,
											exprType(expression),
											exprTypmod(expression),
											exprCollation(expression),
											masterEvaluationContext);
	}
	else if (nodeTag == T_Query)
	{
		return (Node *) query_tree_mutator((Query *) expression,
										   PartiallyEvaluateExpression,
										   masterEvaluationContext,
										   QTW_DONT_COPY_QUERY);
	}
	else
	{
		return (Node *) expression_tree_mutator(expression,
												PartiallyEvaluateExpression,
												masterEvaluationContext);
	}

	return expression;
}


/*
 * ShouldEvaluateFunctionWithMasterContext is a helper function which is used to
 * decide whether the function/expression should be evaluated with the input
 * masterEvaluationContext.
 */
static bool
ShouldEvaluateFunctionWithMasterContext(MasterEvaluationContext *evaluationContext)
{
	if (evaluationContext == NULL)
	{
		/* if no context provided, evaluate, which is the default behaviour */
		return true;
	}

	return evaluationContext->evaluationMode == EVALUATE_FUNCTIONS_PARAMS;
}


/*
 * ShouldEvaluateExpression returns true if Citus should evaluate the
 * input node on the coordinator.
 */
static bool
ShouldEvaluateExpression(Expr *expression)
{
	NodeTag nodeTag = nodeTag(expression);

	switch (nodeTag)
	{
		case T_FuncExpr:
		{
			FuncExpr *funcExpr = (FuncExpr *) expression;

			/* we cannot evaluate set returning functions */
			bool isSetReturningFunction = funcExpr->funcretset;
			return !isSetReturningFunction;
		}

		case T_OpExpr:
		case T_DistinctExpr:
		case T_NullIfExpr:
		case T_CoerceViaIO:
		case T_ArrayCoerceExpr:
		case T_ScalarArrayOpExpr:
		case T_RowExpr:
		case T_RowCompareExpr:
		case T_RelabelType:
		case T_CoerceToDomain:
		{
			return true;
		}

		default:
			return false;
	}
}


/*
 * IsVariableExpression returns whether the given node is a variable expression,
 * meaning its result depends on the input data and is not constant for the whole
 * query.
 */
static bool
IsVariableExpression(Node *node)
{
	if (IsA(node, Aggref))
	{
		return true;
	}

	if (IsA(node, WindowFunc))
	{
		return true;
	}

	if (IsA(node, Param))
	{
		/* ExecInitExpr cannot handle PARAM_SUBLINK */
		return ((Param *) node)->paramkind == PARAM_SUBLINK;
	}

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
					Oid result_collation,
					MasterEvaluationContext *masterEvaluationContext)
{
	PlanState *planState = NULL;
	EState     *estate;
	ExprState  *exprstate;
	ExprContext *econtext;
	Datum		const_val;
	bool		const_is_null;
	int16		resultTypLen;
	bool		resultTypByVal;

	if (masterEvaluationContext)
	{
		planState = masterEvaluationContext->planState;

		if (IsA(expr, Param))
		{
			if (masterEvaluationContext->evaluationMode == EVALUATE_NONE)
			{
				/* bail out, the caller doesn't want params to be evaluated  */
				return expr;
			}
		}
		else if (masterEvaluationContext->evaluationMode != EVALUATE_FUNCTIONS_PARAMS)
		{
			/* should only get here for node types we should evaluate */
			Assert(ShouldEvaluateExpression(expr));

			/* bail out, the caller doesn't want functions/expressions to be evaluated */
			return expr;
		}
	}

	/*
	 * To use the executor, we need an EState.
	 */
	estate = CreateExecutorState();

	/* We can use the estate's working context to avoid memory leaks. */
	MemoryContext oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

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
	const_val = ExecEvalExprSwitchContext(exprstate, econtext, &const_is_null);

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
	if (func_id == CitusReadIntermediateResultFuncId() ||
		func_id == CitusReadIntermediateResultArrayFuncId())
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

	if (IsA(node, NextValueExpr))
	{
		/* NextValueExpr is volatile */
		return true;
	}

	return false;
}


/*
 * CitusIsMutableFunctionIdChecker checks if the given function id is
 * a mutable function other than read_intermediate_result().
 */
static bool
CitusIsMutableFunctionIdChecker(Oid func_id, void *context)
{
	if (func_id == CitusReadIntermediateResultFuncId() ||
		func_id == CitusReadIntermediateResultArrayFuncId())
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

	return false;
}


/* *INDENT-ON* */
