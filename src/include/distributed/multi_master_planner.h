/*-------------------------------------------------------------------------
 *
 * multi_master_planner.h
 *	  Function declarations for building planned statements; these statements
 *	  are then executed on the master node.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_MASTER_PLANNER_H
#define MULTI_MASTER_PLANNER_H

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"


/* Function declarations for building local plans on the master node */
struct DistributedPlan;
struct CustomScan;
extern PlannedStmt * MasterNodeSelectPlan(struct DistributedPlan *distributedPlan,
										  struct CustomScan *dataScan);
extern Unique * make_unique_from_sortclauses(Plan *lefttree, List *distinctList);
extern bool UseStdPlanner;

#endif   /* MULTI_MASTER_PLANNER_H */
