/*
 * citus_clauses.c
 *
 * Routines roughly equivalent to postgres' util/clauses.
 *
 * Copyright (c) 2016-2016, Citus Data, Inc.
 */

#include "postgres.h"

#include "distributed/citus_clauses.h"
#include "distributed/multi_router_planner.h"

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
static Node * EvaluateNodeIfReferencesFunction(Node *expression, PlanState *planState);
static Node * PartiallyEvaluateExpressionMutator(Node *expression,
												 FunctionEvaluationContext *context);
static Expr * citus_evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
								  Oid result_collation, PlanState *planState);


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

		if (contain_mutable_functions((Node *) targetEntry->expr))
		{
			return true;
		}
	}

	foreach(rteCell, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);

		if (rte->rtekind != RTE_SUBQUERY)
		{
			continue;
		}

		if (RequiresMasterEvaluation(rte->subquery))
		{
			return true;
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
		return contain_mutable_functions((Node *) query->jointree->quals);
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
	CmdType commandType = query->commandType;
	ListCell *targetEntryCell = NULL;
	ListCell *rteCell = NULL;
	ListCell *cteCell = NULL;
	Node *modifiedNode = NULL;
	bool insertSelectQuery = InsertSelectQuery(query);

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

		if (commandType == CMD_INSERT && !insertSelectQuery)
		{
			modifiedNode = EvaluateNodeIfReferencesFunction((Node *) targetEntry->expr,
															planState);
		}
		else
		{
			modifiedNode = PartiallyEvaluateExpression((Node *) targetEntry->expr,
													   planState);
		}

		targetEntry->expr = (Expr *) modifiedNode;
	}

	foreach(rteCell, query->rtable)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);

		if (rte->rtekind != RTE_SUBQUERY)
		{
			continue;
		}

		ExecuteMasterEvaluableFunctions(rte->subquery, planState);
	}

	foreach(cteCell, query->cteList)
	{
		CommonTableExpr *expr = (CommonTableExpr *) lfirst(cteCell);

		ExecuteMasterEvaluableFunctions((Query *) expr->ctequery, planState);
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
	if (IsA(expression, FuncExpr))
	{
		FuncExpr *expr = (FuncExpr *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr,
											expr->funcresulttype,
											exprTypmod((Node *) expr),
											expr->funccollid,
											planState);
	}

	if (IsA(expression, OpExpr) ||
		IsA(expression, DistinctExpr) ||
		IsA(expression, NullIfExpr))
	{
		/* structural equivalence */
		OpExpr *expr = (OpExpr *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr,
											expr->opresulttype, -1,
											expr->opcollid,
											planState);
	}

	if (IsA(expression, CoerceViaIO))
	{
		CoerceViaIO *expr = (CoerceViaIO *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr,
											expr->resulttype, -1,
											expr->resultcollid,
											planState);
	}

	if (IsA(expression, ArrayCoerceExpr))
	{
		ArrayCoerceExpr *expr = (ArrayCoerceExpr *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr,
											expr->resulttype,
											expr->resulttypmod,
											expr->resultcollid,
											planState);
	}

	if (IsA(expression, ScalarArrayOpExpr))
	{
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr, BOOLOID, -1, InvalidOid,
											planState);
	}

	if (IsA(expression, RowCompareExpr))
	{
		RowCompareExpr *expr = (RowCompareExpr *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr, BOOLOID, -1, InvalidOid,
											planState);
	}

	if (IsA(expression, Param))
	{
		Param *param = (Param *) expression;

		return (Node *) citus_evaluate_expr((Expr *) param,
											param->paramtype,
											param->paramtypmod,
											param->paramcollid,
											planState);
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
	const_val = ExecEvalExprSwitchContext(exprstate, econtext, &const_is_null, NULL);

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

/* *INDENT-ON* */
