/*-------------------------------------------------------------------------
 *
 * multi_master_planner.h
 *	  Function declarations for building planned statements; these statements
 *	  are then executed on the master node.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
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
struct MultiPlan;
struct CustomScan;
extern PlannedStmt * MasterNodeSelectPlan(struct MultiPlan *multiPlan,
										  struct CustomScan *dataScan);
extern List * MasterTargetList(List *workerTargetList);

#endif   /* MULTI_MASTER_PLANNER_H */
