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


PlannedStmt * TryToDelegateFunctionCall(DistributedPlanningContext *planContext);


#endif /* FUNCTION_CALL_DELEGATION_H */
