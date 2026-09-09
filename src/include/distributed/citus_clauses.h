/*-------------------------------------------------------------------------
 *
 * citus_clauses.h
 *  Routines roughly equivalent to postgres' util/clauses.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_CLAUSES_H
#define CITUS_CLAUSES_H

#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"


/*
 * CoordinatorEvaluationMode is used to signal what expressions in the query
 * should be evaluated on the coordinator.
 */
typedef enum CoordinatorEvaluationMode
{
	/* evaluate nothing */
	EVALUATE_NONE = 0,

	/* evaluate only external parameters */
	EVALUATE_PARAMS,

	/* evaluate both the functions/expressions and the external paramaters */
	EVALUATE_FUNCTIONS_PARAMS
} CoordinatorEvaluationMode;

/*
 * This struct is used to pass information to master
 * evaluation logic.
 */
typedef struct CoordinatorEvaluationContext
{
	PlanState *planState;
	CoordinatorEvaluationMode evaluationMode;
} CoordinatorEvaluationContext;


extern bool RequiresCoordinatorEvaluation(Query *query);
extern void ExecuteCoordinatorEvaluableExpressions(Query *query, PlanState *planState);
extern Node * PartiallyEvaluateExpression(Node *expression,
										  CoordinatorEvaluationContext *
										  coordinatorEvaluationContext);
extern bool CitusIsVolatileFunction(Node *node);
extern bool CitusIsMutableFunction(Node *node);

#endif /* CITUS_CLAUSES_H */
