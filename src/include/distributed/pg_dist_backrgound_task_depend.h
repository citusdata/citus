/*-------------------------------------------------------------------------
 *
 * pg_dist_background_task_depend.h
 *	  definition of the relation that holds which tasks depend on each
 *	  other.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_PG_DIST_BACKGROUND_TASK_DEPEND_H
#define CITUS_PG_DIST_BACKGROUND_TASK_DEPEND_H

typedef struct FormData_pg_dist_background_task_depend
{
	int64 job_id;
	int64 task_id;
	int64 depends_on;
#ifdef CATALOG_VARLEN           /* variable-length fields start here */
#endif
} FormData_pg_dist_background_task_depend;
typedef FormData_pg_dist_background_task_depend *Form_pg_dist_background_task_depend;


/* ----------------
 *      compiler constants for pg_dist_background_task_depend
 * ----------------
 */
#define Natts_pg_dist_background_task_depend 3
#define Anum_pg_dist_background_task_depend_job_id 1
#define Anum_pg_dist_background_task_depend_task_id 2
#define Anum_pg_dist_background_task_depend_depends_on 3

#endif /* CITUS_PG_DIST_BACKGROUND_TASK_DEPEND_H */
