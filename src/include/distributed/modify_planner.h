/*-------------------------------------------------------------------------
 *
 * modify_planner.h
 *
 * Declarations for public functions and types related to modify planning.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MODIFY_PLANNER_H
#define MODIFY_PLANNER_H

#include "c.h"

#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "nodes/parsenodes.h"


/* values used by jobs and tasks which do not require identifiers */
#define INVALID_JOB_ID 0
#define INVALID_TASK_ID 0

#if (PG_VERSION_NUM >= 90500)

/* reserved alias name for UPSERTs */
#define UPSERT_ALIAS "citus_table_alias"
#endif


extern MultiPlan * MultiModifyPlanCreate(Query *query);

#endif /* MODIFY_PLANNER_H */
