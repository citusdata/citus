/*-------------------------------------------------------------------------
 *
 * subplan_execution.h
 *
 * Functions for execution subplans.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef SUBPLAN_EXECUTION_H
#define SUBPLAN_EXECUTION_H


#include "distributed/multi_physical_planner.h"

extern int MaxIntermediateResult;
extern uint64 currentIntermediateResult;
extern bool SetResultLimit;

extern void ExecuteSubPlans(DistributedPlan *distributedPlan);


#endif /* SUBPLAN_EXECUTION_H */
