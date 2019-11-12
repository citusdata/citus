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

typedef struct IntermediateResultsHashKey
{
	char intermediate_result_id[NAMEDATALEN];
} IntermediateResultsHashKey;

typedef struct IntermediateResultsHashEntry
{
	IntermediateResultsHashKey key;
	List *nodeIdList;
} IntermediateResultsHashEntry;

#endif /* SUBPLAN_EXECUTION_H */
