
#ifndef CITUS_PG_DIST_BACKGROUND_TASKS_DEPEND_H
#define CITUS_PG_DIST_BACKGROUND_TASKS_DEPEND_H

typedef struct FormData_pg_dist_background_tasks_depend
{
	int64 task_id;
	int64 depends_on;
#ifdef CATALOG_VARLEN           /* variable-length fields start here */
#endif
} FormData_pg_dist_background_tasks_depend;
typedef FormData_pg_dist_background_tasks_depend *Form_pg_dist_background_tasks_depend;


/* ----------------
 *      compiler constants for pg_dist_background_tasks_depend
 * ----------------
 */
#define Natts_pg_dist_background_tasks_depend 2
#define Anum_pg_dist_background_tasks_depend_task_id 1
#define Anum_pg_dist_background_tasks_depend_depends_on 2

#endif /* CITUS_PG_DIST_BACKGROUND_TASKS_DEPEND_H */
