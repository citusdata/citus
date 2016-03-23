/*-------------------------------------------------------------------------
 *
 * task_tracker_protocol.h
 *
 * Header and type declarations for assigning tasks to and removing tasks from
 * the task tracker running on this node.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef TASK_TRACKER_PROTOCOL_H
#define TASK_TRACKER_PROTOCOL_H

#include "fmgr.h"


/* Function declarations for distributed task management */
extern Datum task_tracker_assign_task(PG_FUNCTION_ARGS);
extern Datum task_tracker_update_data_fetch_task(PG_FUNCTION_ARGS);
extern Datum task_tracker_task_status(PG_FUNCTION_ARGS);
extern Datum task_tracker_cleanup_job(PG_FUNCTION_ARGS);


#endif   /* TASK_TRACKER_PROTOCOL_H */
