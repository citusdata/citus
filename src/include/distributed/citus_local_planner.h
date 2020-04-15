/*-------------------------------------------------------------------------
 *
 * citus_local_planner.h
 *
 * Declarations for public functions related to planning queries involving
 * citus local tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_LOCAL_PLANNER_H
#define CITUS_LOCAL_PLANNER_H


#include "postgres.h"

#include "distributed/multi_physical_planner.h"

extern DistributedPlan * CreateCitusLocalPlan(Query *querys);

extern bool ShouldUseCitusLocalPlanner(RTEListProperties *rteListProperties);
extern void ErrorIfUnsupportedQueryWithCitusLocalTables(Query *parse);

#endif /* CITUS_LOCAL_PLANNER_H */
