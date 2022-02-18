/*
 * function_call_delegation.h
 *    Declarations for public functions and variables used to delegate
 *    function calls to worker nodes.
 *
 * Copyright (c), Citus Data, Inc.
 */

#ifndef FUNCTION_CALL_DELEGATION_H
#define FUNCTION_CALL_DELEGATION_H

#include "postgres.h"

#include "distributed/distributed_planner.h"
#include "distributed/multi_physical_planner.h"


/*
 * These flags keep track of whether the process is currently in a delegated
 * function or procedure call.
 */
extern bool InTopLevelDelegatedFunctionCall;
extern bool InDelegatedProcedureCall;

PlannedStmt * TryToDelegateFunctionCall(DistributedPlanningContext *planContext);
extern void CheckAndResetAllowedShardKeyValueIfNeeded(void);
extern bool IsShardKeyValueAllowed(Const *shardKey, uint32 colocationId);

#endif /* FUNCTION_CALL_DELEGATION_H */
