/*-------------------------------------------------------------------------
 *
 * pg_dist_background_task.h
 *	  definition of the relation that holds the tasks metadata
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_PG_DIST_BACKGROUND_TASK_H
#define CITUS_PG_DIST_BACKGROUND_TASK_H

/* ----------------
 *      compiler constants for pg_dist_background_task
 * ----------------
 */
#define Natts_pg_dist_background_task 10
#define Anum_pg_dist_background_task_job_id 1
#define Anum_pg_dist_background_task_task_id 2
#define Anum_pg_dist_background_task_owner 3
#define Anum_pg_dist_background_task_pid 4
#define Anum_pg_dist_background_task_status 5
#define Anum_pg_dist_background_task_command 6
#define Anum_pg_dist_background_task_retry_count 7
#define Anum_pg_dist_background_task_not_before 8
#define Anum_pg_dist_background_task_message 9
#define Anum_pg_dist_background_task_nodes_involved 10

#endif /* CITUS_PG_DIST_BACKGROUND_TASK_H */
