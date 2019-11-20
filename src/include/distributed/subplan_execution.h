/*-------------------------------------------------------------------------
 *
 * subplan_execution.h
 *
 * Functions for execution subplans.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef SUBPLAN_EXECUTION_H
#define SUBPLAN_EXECUTION_H


#include "distributed/multi_physical_planner.h"

extern int MaxIntermediateResult;
extern int SubPlanLevel;

extern void ExecuteSubPlans(DistributedPlan *distributedPlan);

/**
 * IntermediateResultsHashEntry is used to store which nodes need to receive
 * intermediate results. Given an intermediate result name, you can lookup
 * the list of nodes that can possibly run a query that will use the
 * intermediate results.
 *
 * The nodeIdList contains a set of unique WorkerNode ids that have placements
 * that can be used in non-colocated subquery joins with the intermediate result
 * given in the key.
 */
typedef struct IntermediateResultsHashEntry
{
	char *key;
	List *nodeIdList;
} IntermediateResultsHashEntry;

#endif /* SUBPLAN_EXECUTION_H */
