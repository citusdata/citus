

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "utils/wait_event.h"

#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"

/* pg_catalog.citus_job_wait(jobid bigint) */
PG_FUNCTION_INFO_V1(citus_jobs_wait);

Datum
citus_jobs_wait(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	/* int64 jobid = PG_GETARG_INT64(0); */
	ereport(ERROR, (errmsg("not implemented")));
}
