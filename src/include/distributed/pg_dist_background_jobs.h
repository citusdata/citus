/*-------------------------------------------------------------------------
 *
 * pg_dist_background_jobs.h
 *	  definition of the relation that holds the jobs metadata
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_PG_DIST_BACKGROUND_JOBS_H
#define CITUS_PG_DIST_BACKGROUND_JOBS_H

/* ----------------
 *      compiler constants for pg_dist_background_jobs
 * ----------------
 */
#define Natts_pg_dist_background_jobs 6
#define Anum_pg_dist_background_jobs_job_id 1
#define Anum_pg_dist_background_jobs_state 2
#define Anum_pg_dist_background_jobs_job_type 3
#define Anum_pg_dist_background_jobs_description 4
#define Anum_pg_dist_background_jobs_started_at 5
#define Anum_pg_dist_background_jobs_finished_at 6

#define PG_DIST_BACKGROUND_JOBS_JOB_ID_SEQUENCE_NAME \
	"pg_catalog.pg_dist_background_jobs_job_id_seq"

#endif /* CITUS_PG_DIST_BACKGROUND_JOBS_H */
