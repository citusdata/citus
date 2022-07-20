
#ifndef CITUS_PG_DIST_REBALANCE_JOBS_DEPEND_H
#define CITUS_PG_DIST_REBALANCE_JOBS_DEPEND_H

typedef struct FormData_pg_dist_rebalance_jobs_depend
{
	int64 jobid;
	int64 depends_on;
#ifdef CATALOG_VARLEN           /* variable-length fields start here */
#endif
} FormData_pg_dist_rebalance_jobs_depend;
typedef FormData_pg_dist_rebalance_jobs_depend *Form_pg_dist_rebalance_jobs_depend;


/* ----------------
 *      compiler constants for pg_dist_rebalance_jobs_depend
 * ----------------
 */
#define Natts_pg_dist_rebalance_jobs_depend 2
#define Anum_pg_dist_rebalance_jobs_depend_jobid 1
#define Anum_pg_dist_rebalance_jobs_depend_depends_on 2

#endif /* CITUS_PG_DIST_REBALANCE_JOBS_DEPEND_H */
