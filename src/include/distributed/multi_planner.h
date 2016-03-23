/*-------------------------------------------------------------------------
 *
 * multi_planner.h
 *	  General Citus planner code.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_PLANNER_H
#define MULTI_PLANNER_H

#include "nodes/plannodes.h"
#include "nodes/relation.h"

extern PlannedStmt * multi_planner(Query *parse, int cursorOptions,
								   ParamListInfo boundParams);

extern bool HasCitusToplevelNode(PlannedStmt *planStatement);
struct MultiPlan;
extern struct MultiPlan * GetMultiPlan(PlannedStmt *planStatement);

#endif /* MULTI_PLANNER_H */
