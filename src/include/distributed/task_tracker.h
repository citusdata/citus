/*-------------------------------------------------------------------------
 *
 * task_tracker.h
 *
 * Header and type declarations for coordinating execution of tasks and data
 * source transfers on worker nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef TASK_TRACKER_H
#define TASK_TRACKER_H

#include "storage/lwlock.h"
#include "utils/hsearch.h"

/* Config variables managed via guc.c */
extern int TaskTrackerDelay;
extern int MaxTrackedTasksPerNode;
extern int MaxRunningTasksPerNode;
extern int MaxTaskStringSize;

#endif   /* TASK_TRACKER_H */
