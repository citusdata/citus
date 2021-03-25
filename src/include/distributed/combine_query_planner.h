/*-------------------------------------------------------------------------
 *
 * combine_query_planner.h
 *	  Function declarations for building planned statements; these statements
 *	  are then executed on the coordinator node.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COMBINE_QUERY_PLANNER_H
#define COMBINE_QUERY_PLANNER_H

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"

#include "nodes/pathnodes.h"


/* Function declarations for building local plans on the coordinator node */
struct DistributedPlan;
struct CustomScan;
extern Path * CreateCitusCustomScanPath(PlannerInfo *root, RelOptInfo *relOptInfo,
										Index restrictionIndex, RangeTblEntry *rte,
										CustomScan *remoteScan);
extern PlannedStmt * PlanCombineQuery(struct DistributedPlan *distributedPlan,
									  struct CustomScan *dataScan);
extern bool ReplaceCitusExtraDataContainer;
extern CustomScan *ReplaceCitusExtraDataContainerWithCustomScan;

#endif   /* COMBINE_QUERY_PLANNER_H */
