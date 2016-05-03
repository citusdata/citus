/*
 * citus_clauses.c
 *
 * Routines roughly equivalent to postgres' util/clauses. 
 *
 * Copyright (c) 2016-2016, Citus Data, Inc.
 */

#include "postgres.h"

#include "distributed/citus_clauses.h"

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

static Node * PartiallyEvaluateExpression(Node *expression);
static Node * EvaluateNodeIfReferencesFunction(Node *expression);
static Node * PartiallyEvaluateExpressionWalker(Node *expression, bool *containsVar);
static Expr * citus_evaluate_expr(Expr *expr, Oid result_type, int32 result_typmod,
								  Oid result_collation);


/*
 * Walks each TargetEntry of the query, evaluates sub-expressions without Vars.
 */
void
ExecuteFunctions(Query *query)
{
	CmdType commandType = query->commandType;
	ListCell *targetEntryCell = NULL;
	Node *modifiedNode = NULL;

	if (query->jointree && query->jointree->quals)
	{
		query->jointree->quals = PartiallyEvaluateExpression(query->jointree->quals);
	}

	foreach(targetEntryCell, query->targetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);

		/* performance optimization for the most common cases */
		if (IsA(targetEntry->expr, Const) || IsA(targetEntry->expr, Var))
		{
			continue;
		}

		if (commandType == CMD_INSERT)
		{
			modifiedNode = EvaluateNodeIfReferencesFunction((Node *) targetEntry->expr);
		}
		else
		{
			modifiedNode = PartiallyEvaluateExpression((Node *) targetEntry->expr);
		}

		targetEntry->expr = (Expr *) modifiedNode;
	}

	if(query->jointree)
	{
		Assert(!contain_mutable_functions((Node *) (query->jointree->quals)));
	}
	Assert(!contain_mutable_functions((Node *) (query->targetList)));
}


/*
 * Walks the expression, evaluating any STABLE or IMMUTABLE functions so long as they
 * don't reference Vars.
 */
static Node *
PartiallyEvaluateExpression(Node *expression)
{
	bool unused;
	return PartiallyEvaluateExpressionWalker(expression, &unused);
}


/*
 * When you find a function call evaluate it, the planner made sure there were no Vars
 *
 * Tell the parent whether you are a Var. If your child was a var tell your parent
 *
 * A little inefficient. It goes to the bottom of the tree then calls EvaluateExpression
 * on each function on the way back up. Say we had an expression with no Vars, we could
 * only call EvaluateExpression on the top-most level and get the same result.
 */
static Node *
PartiallyEvaluateExpressionWalker(Node *expression, bool *containsVar)
{
	bool childContainsVar = false;
	Node *copy = NULL;

	if (expression == NULL)
	{
		return expression;
	}

	/* pass any argument lists back to the mutator to copy and recurse for us */
	if (IsA(expression, List))
	{
		return expression_tree_mutator(expression,
									   PartiallyEvaluateExpressionWalker,
									   containsVar);
	}

	if (IsA(expression, Var))
	{
		*containsVar = true;

		/* makes a copy for us */
		return expression_tree_mutator(expression,
									   PartiallyEvaluateExpressionWalker,
									   containsVar);
	}

	copy = expression_tree_mutator(expression,
								   PartiallyEvaluateExpressionWalker,
								   &childContainsVar);

	if (childContainsVar)
	{
		*containsVar = true;
	}
	else
	{
		copy = EvaluateNodeIfReferencesFunction(copy);
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
EvaluateNodeIfReferencesFunction(Node *expression)
{
	if (IsA(expression, FuncExpr))
	{
		FuncExpr *expr = (FuncExpr *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr,
											expr->funcresulttype,
											exprTypmod((Node *) expr),
											expr->funccollid);
	}

	if (IsA(expression, OpExpr) ||
		IsA(expression, DistinctExpr) ||
		IsA(expression, NullIfExpr))
	{
		/* structural equivalence */
		OpExpr *expr = (OpExpr *) expression;

		/* typemod is always -1? */

		return (Node *) citus_evaluate_expr((Expr *) expr,
											expr->opresulttype, -1,
											expr->opcollid);
	}

	if (IsA(expression, DistinctExpr))
	{
		DistinctExpr *expr = (DistinctExpr *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr,
											expr->opresulttype,
											exprTypmod((Node *) expr),
											expr->opcollid);
	}

	if (IsA(expression, CoerceViaIO))
	{
		CoerceViaIO *expr = (CoerceViaIO *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr,
											expr->resulttype, -1,
											expr->resultcollid);
	}

	if (IsA(expression, ArrayCoerceExpr))
	{
		ArrayCoerceExpr *expr = (ArrayCoerceExpr *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr,
											expr->resulttype,
											expr->resulttypmod,
											expr->resultcollid);
	}

	if (IsA(expression, ScalarArrayOpExpr))
	{
		/* TODO: Test this! */
		ScalarArrayOpExpr *expr = (ScalarArrayOpExpr *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr, BOOLOID, -1, InvalidOid);
	}

	if (IsA(expression, RowCompareExpr))
	{
		/* TODO: Test this! */
		RowCompareExpr *expr = (RowCompareExpr *) expression;

		return (Node *) citus_evaluate_expr((Expr *) expr, BOOLOID, -1, InvalidOid);
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
					Oid result_collation)
{
	EState	   *estate;
	ExprState  *exprstate;
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
	exprstate = ExecInitExpr(expr, NULL);

	/*
	 * And evaluate it.
	 *
	 * It is OK to use a default econtext because none of the ExecEvalExpr()
	 * code used in this situation will use econtext.  That might seem
	 * fortuitous, but it's not so unreasonable --- a constant expression does
	 * not depend on context, by definition, n'est ce pas?
	 */
	const_val = ExecEvalExprSwitchContext(exprstate,
										  GetPerTupleExprContext(estate),
										  &const_is_null, NULL);

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
