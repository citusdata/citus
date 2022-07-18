

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "utils/wait_event.h"

#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"

/* pg_catalog.citus_wait_for_rebalance_job(jobid bigint) */
PG_FUNCTION_INFO_V1(citus_wait_for_rebalance_job);

Datum
citus_wait_for_rebalance_job(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	int64 jobid = PG_GETARG_INT64(0);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		RebalanceJob *job = GetScheduledRebalanceJobByJobID(jobid);
		if (!job)
		{
			ereport(ERROR, (errmsg("unkown job with jobid: %ld", jobid)));
		}

		if (IsRebalanceJobStatusTerminal(job->status))
		{
			PG_RETURN_VOID();
		}

		/* not finished, sleeping */
		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 1000, /* TODO, what timeout would be good here */
						 WAIT_EVENT_PG_SLEEP);
		ResetLatch(MyLatch);
	}
}
