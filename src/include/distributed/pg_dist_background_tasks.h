
#ifndef CITUS_PG_DIST_BACKGROUND_TASKS_H
#define CITUS_PG_DIST_BACKGROUND_TASKS_H

/* ----------------
 *      compiler constants for pg_dist_background_tasks
 * ----------------
 */
#define Natts_pg_dist_background_tasks 6
#define Anum_pg_dist_background_tasks_task_id 1
#define Anum_pg_dist_background_tasks_pid 2
#define Anum_pg_dist_background_tasks_status 3
#define Anum_pg_dist_background_tasks_command 4
#define Anum_pg_dist_background_tasks_retry_count 5
#define Anum_pg_dist_background_tasks_message 6

#define PG_DIST_BACKGROUND_TASK_TASK_ID_SEQUENCE_NAME \
	"pg_catalog.pg_dist_background_tasks_task_id_seq"

#endif /* CITUS_PG_DIST_BACKGROUND_TASKS_H */
