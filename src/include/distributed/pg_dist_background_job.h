/*-------------------------------------------------------------------------
 *
 * pg_dist_background_job.h
 *	  definition of the relation that holds the jobs metadata
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_PG_DIST_BACKGROUND_JOB_H
#define CITUS_PG_DIST_BACKGROUND_JOB_H

/* ----------------
 *      compiler constants for pg_dist_background_job
 * ----------------
 */
#define Natts_pg_dist_background_job 6
#define Anum_pg_dist_background_job_job_id 1
#define Anum_pg_dist_background_job_state 2
#define Anum_pg_dist_background_job_job_type 3
#define Anum_pg_dist_background_job_description 4
#define Anum_pg_dist_background_job_started_at 5
#define Anum_pg_dist_background_job_finished_at 6

#endif /* CITUS_PG_DIST_BACKGROUND_JOB_H */
