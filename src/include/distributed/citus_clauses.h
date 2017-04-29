/*-------------------------------------------------------------------------
 *
 * citus_clauses.h
 *  Routines roughly equivalent to postgres' util/clauses.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_CLAUSES_H
#define CITUS_CLAUSES_H

#include "nodes/execnodes.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

extern bool RequiresMasterEvaluation(Query *query);
extern void ExecuteMasterEvaluableFunctions(Query *query, PlanState *planState);
extern Node * PartiallyEvaluateExpression(Node *expression, PlanState *planState);

#endif /* CITUS_CLAUSES_H */
