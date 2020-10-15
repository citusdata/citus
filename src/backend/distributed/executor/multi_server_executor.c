/*-------------------------------------------------------------------------
 *
 * multi_server_executor.c
 *
 * Function definitions for distributed task execution for adaptive
 * executor.
 * routines are implemented backend-side logic; and they trigger executions
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
#include "distributed/tuple_destination.h"
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

	if (distributedPlan->insertSelectQuery != NULL)
	{
		/*
		 * We go through
		 * MULTI_EXECUTOR_NON_PUSHABLE_INSERT_SELECT because
		 * the executor already knows how to handle adaptive
		 * executor when necessary.
		 */
		return MULTI_EXECUTOR_NON_PUSHABLE_INSERT_SELECT;
	}

	/*
	 * If we have repartition jobs with adaptive executor and repartition
	 * joins are not enabled, error out.
	 */
	int dependentJobCount = list_length(job->dependentJobList);
	if (!EnableRepartitionJoins && dependentJobCount > 0)
	{
		ereport(ERROR, (errmsg("the query contains a join that requires repartitioning"),
						errhint("Set citus.enable_repartition_joins to on to enable "
								"repartitioning")));
	}

	/*
	 * Debug distribution column value if possible. The distributed planner sometimes
	 * defers creating the tasks, so the task list might be NIL. Still, it sets the
	 * partitionKeyValue and we print it here.
	 */
	if (list_length(job->taskList) <= 1 && IsLoggableLevel(DEBUG2))
	{
		Const *partitionValueConst = job->partitionKeyValue;

		if (partitionValueConst != NULL && !partitionValueConst->constisnull)
		{
			Datum partitionColumnValue = partitionValueConst->constvalue;
			Oid partitionColumnType = partitionValueConst->consttype;
			char *partitionColumnString = DatumToString(partitionColumnValue,
														partitionColumnType);

			ereport(DEBUG2, (errmsg("query has a single distribution column value: "
									"%s", partitionColumnString)));
		}
	}

	return MULTI_EXECUTOR_ADAPTIVE;
}
