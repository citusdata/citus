
#ifndef CITUS_PG_DIST_REBALANCE_JOBS_H
#define CITUS_PG_DIST_REBALANCE_JOBS_H

/* ----------------
 *		pg_dist_rebalance_job definition.
 * ----------------
 */
typedef struct FormData_pg_dist_rebalance_job
{
	int64 jobid;
	Oid status;
#ifdef CATALOG_VARLEN    /* variable-length fields start here */
	text citus_move_shard_placement; /* text? we need to understand how to read a variable length stored custon type */
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
#define Natts_pg_dist_rebalance_jobs 3
#define Anum_pg_dist_rebalance_jobs_jobid 1
#define Anum_pg_dist_rebalance_jobs_status 2
#define Anum_pg_dist_rebalance_jobs_citus_move_shard_placement 3

#endif /* CITUS_PG_DIST_REBALANCE_JOBS_H */
