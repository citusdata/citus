
#ifndef CITUS_PG_DIST_REBALANCE_JOBS_H
#define CITUS_PG_DIST_REBALANCE_JOBS_H

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
