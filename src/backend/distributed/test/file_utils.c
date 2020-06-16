#include "postgres.h"

#include "distributed/worker_protocol.h"
#include "distributed/transmit.h"
#include "distributed/metadata_utility.h"
#include "fmgr.h"
#include "lib/stringinfo.h"

PG_FUNCTION_INFO_V1(citus_rm_job_directory);

/*
 * citus_rm_job_directory removes the job directory for the given job id.
 * Used before beginning multi_query_directory_cleanup.
 */
Datum
citus_rm_job_directory(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	StringInfo jobCacheDirectory = makeStringInfo();

	EnsureSuperUser();

	appendStringInfo(jobCacheDirectory, "base/%s/%s%0*" INT64_MODIFIER "u",
					 PG_JOB_CACHE_DIR, JOB_DIRECTORY_PREFIX,
					 MIN_JOB_DIRNAME_WIDTH, jobId);
	CitusRemoveDirectory(jobCacheDirectory->data);
	FreeStringInfo(jobCacheDirectory);

	PG_RETURN_VOID();
}
