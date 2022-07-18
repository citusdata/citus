
#ifndef CITUS_PG_DIST_REBALANCE_JOBS_H
#define CITUS_PG_DIST_REBALANCE_JOBS_H

/* ----------------
 *		pg_dist_rebalance_job definition.
 * ----------------
 */
typedef struct FormData_pg_dist_rebalance_job
{
	int64 jobid;
	int32 pid;
	Oid status;
#ifdef CATALOG_VARLEN    /* variable-length fields start here */
	text command;
	int32 retry_count;
	text message;
#endif
} FormData_pg_dist_rebalance_job;

/* ----------------
 *      Form_pg_dist_colocation corresponds to a pointer to a tuple with
 *      the format of pg_dist_colocation relation.
 * ----------------
 */
typedef FormData_pg_dist_rebalance_job *Form_pg_dist_rebalance_job;

/* ----------------
 *      compiler constants for pg_dist_rebalance_jobs
 * ----------------
 */
#define Natts_pg_dist_rebalance_jobs 6
#define Anum_pg_dist_rebalance_jobs_jobid 1
#define Anum_pg_dist_rebalance_jobs_pid 2
#define Anum_pg_dist_rebalance_jobs_status 3
#define Anum_pg_dist_rebalance_jobs_command 4
#define Anum_pg_dist_rebalance_jobs_retry_count 5
#define Anum_pg_dist_rebalance_jobs_message 6

#define REBALANCE_JOB_JOBID_SEQUENCE_NAME "pg_catalog.pg_dist_rebalance_jobs_jobid_seq"

#endif /* CITUS_PG_DIST_REBALANCE_JOBS_H */
