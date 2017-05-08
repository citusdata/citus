/*-------------------------------------------------------------------------
 *
 * insert_select_planner.h
 *
 * Declarations for public functions and types related to planning
 * INSERT..SELECT commands.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef INSERT_SELECT_PLANNER_H
#define INSERT_SELECT_PLANNER_H


#include "postgres.h"

#include "distributed/multi_physical_planner.h"
#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"


extern MultiPlan * CreateCoordinatorInsertSelectPlan(Query *originalQuery);
extern void CoordinatorInsertSelectExplainScan(CustomScanState *node, List *ancestors,
											   struct ExplainState *es);

#endif /* INSERT_SELECT_PLANNER_H */
