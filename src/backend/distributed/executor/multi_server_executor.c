/*-------------------------------------------------------------------------
 *
 * multi_server_executor.c
 *
 * Function definitions for distributed task execution for adaptive
 * and task-tracker executors, and routines common to both. The common
 * routines are implement backend-side logic; and they trigger executions
 * on the client-side via function hooks that they load.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include <unistd.h>

#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/subplan_execution.h"
#include "distributed/worker_protocol.h"
#include "utils/lsyscache.h"

int RemoteTaskCheckInterval = 100; /* per cycle sleep interval in millisecs */
int TaskExecutorType = MULTI_EXECUTOR_ADAPTIVE; /* distributed executor type */
bool EnableRepartitionJoins = false;


/*
 * JobExecutorType selects the executor type for the given distributedPlan using the task
 * executor type config value. The function then checks if the given distributedPlan needs
 * more resources than those provided to it by other config values, and issues
 * warnings accordingly. If the selected executor type cannot execute the given
 * distributedPlan, the function errors out.
 */
MultiExecutorType
JobExecutorType(DistributedPlan *distributedPlan)
{
	Job *job = distributedPlan->workerJob;
	MultiExecutorType executorType = TaskExecutorType;
	bool routerExecutablePlan = distributedPlan->routerExecutable;

	/* debug distribution column value */
	if (routerExecutablePlan)
	{
		if (IsLoggableLevel(DEBUG2))
		{
			Const *partitionValueConst = job->partitionKeyValue;

			if (partitionValueConst != NULL && !partitionValueConst->constisnull)
			{
				Datum partitionColumnValue = partitionValueConst->constvalue;
				Oid partitionColumnType = partitionValueConst->consttype;
				char *partitionColumnString = DatumToString(partitionColumnValue,
															partitionColumnType);

				ereport(DEBUG2, (errmsg("Plan is router executable"),
								 errdetail("distribution column value: %s",
										   ApplyLogRedaction(partitionColumnString))));
			}
			else
			{
				ereport(DEBUG2, (errmsg("Plan is router executable")));
			}
		}

		return MULTI_EXECUTOR_ADAPTIVE;
	}

	if (distributedPlan->insertSelectQuery != NULL)
	{
		/*
		 * Even if adaptiveExecutorEnabled, we go through
		 * MULTI_EXECUTOR_NON_PUSHABLE_INSERT_SELECT because
		 * the executor already knows how to handle adaptive
		 * executor when necessary.
		 */
		return MULTI_EXECUTOR_NON_PUSHABLE_INSERT_SELECT;
	}

	Assert(distributedPlan->modLevel == ROW_MODIFY_READONLY);


	if (executorType == MULTI_EXECUTOR_ADAPTIVE)
	{
		/* if we have repartition jobs with adaptive executor and repartition
		 * joins are not enabled, error out. Otherwise, switch to task-tracker
		 */
		int dependentJobCount = list_length(job->dependentJobList);
		if (dependentJobCount > 0)
		{
			if (!EnableRepartitionJoins)
			{
				ereport(ERROR, (errmsg(
									"the query contains a join that requires repartitioning"),
								errhint("Set citus.enable_repartition_joins to on "
										"to enable repartitioning")));
			}
			return MULTI_EXECUTOR_ADAPTIVE;
		}
	}

	return executorType;
}


/*
 * CheckIfSizeLimitIsExceeded checks if the limit is exceeded by intermediate
 * results, if there is any.
 */
bool
CheckIfSizeLimitIsExceeded(DistributedExecutionStats *executionStats)
{
	if (!SubPlanLevel || MaxIntermediateResult < 0)
	{
		return false;
	}

	uint64 maxIntermediateResultInBytes = MaxIntermediateResult * 1024L;
	if (executionStats->totalIntermediateResultSize < maxIntermediateResultInBytes)
	{
		return false;
	}

	return true;
}


/*
 * This function is called when the intermediate result size limitation is
 * exceeded. It basically errors out with a detailed explanation.
 */
void
ErrorSizeLimitIsExceeded()
{
	ereport(ERROR, (errmsg("the intermediate result size exceeds "
						   "citus.max_intermediate_result_size (currently %d kB)",
						   MaxIntermediateResult),
					errdetail("Citus restricts the size of intermediate "
							  "results of complex subqueries and CTEs to "
							  "avoid accidentally pulling large result sets "
							  "into once place."),
					errhint("To run the current query, set "
							"citus.max_intermediate_result_size to a higher"
							" value or -1 to disable.")));
}
