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
 * MasterEvaluationMode is used to signal what expressions in the query
 * should be evaluated on the coordinator.
 */
typedef enum MasterEvaluationMode
{
	/* evaluate nothing */
	EVALUATE_NONE = 0,

	/* evaluate only external parameters */
	EVALUATE_PARAMS,

	/* evaluate both the functions/expressions and the external paramaters */
	EVALUATE_FUNCTIONS_PARAMS
} MasterEvaluationMode;

/*
 * This struct is used to pass information to master
 * evaluation logic.
 */
typedef struct MasterEvaluationContext
{
	PlanState *planState;
	MasterEvaluationMode evaluationMode;
} MasterEvaluationContext;


extern bool RequiresMasterEvaluation(Query *query);
extern void ExecuteMasterEvaluableFunctionsAndParameters(Query *query,
														 PlanState *planState);
extern void ExecuteMasterEvaluableParameters(Query *query, PlanState *planState);
extern Node * PartiallyEvaluateExpression(Node *expression,
										  MasterEvaluationContext *masterEvaluationContext);
extern bool CitusIsVolatileFunction(Node *node);
extern bool CitusIsMutableFunction(Node *node);

#endif /* CITUS_CLAUSES_H */
