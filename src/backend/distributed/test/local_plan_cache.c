/*-------------------------------------------------------------------------
 *
 * local_plan_cache.c
 *
 * This file contains functions to test local plan caching.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/prepare.h"
#include "fmgr.h"
#include "utils/builtins.h"

#include "distributed/citus_custom_scan.h"
#include "distributed/distributed_planner.h"
#include "distributed/listutils.h"
#include "distributed/multi_physical_planner.h"


PG_FUNCTION_INFO_V1(local_plan_cache_entry_count);


/*
 * local_plan_cache_entry_count returns the number of local plans cached by the
 * generic plan of the named prepared statement.
 */
Datum
local_plan_cache_entry_count(PG_FUNCTION_ARGS)
{
	char *statementName = text_to_cstring(PG_GETARG_TEXT_PP(0));
	PreparedStatement *preparedStatement = FetchPreparedStatement(statementName, true);
	CachedPlan *genericPlan = preparedStatement->plansource->gplan;

	if (genericPlan == NULL || !genericPlan->is_valid)
	{
		ereport(ERROR, (errmsg("prepared statement \"%s\" has no valid generic plan",
							   statementName)));
	}

	int entryCount = 0;
	bool foundDistributedPlan = false;
	PlannedStmt *plannedStatement = NULL;
	foreach_declared_ptr(plannedStatement, genericPlan->stmt_list)
	{
		CustomScan *customScan =
			FetchCitusCustomScanIfExists(plannedStatement->planTree);
		if (customScan == NULL)
		{
			continue;
		}

		DistributedPlan *distributedPlan = GetDistributedPlan(customScan);
		if (distributedPlan->workerJob != NULL)
		{
			foundDistributedPlan = true;
			entryCount += list_length(
				distributedPlan->workerJob->localPlannedStatements);
		}
	}

	if (!foundDistributedPlan)
	{
		ereport(ERROR, (errmsg("prepared statement \"%s\" has no distributed plan",
							   statementName)));
	}

	PG_RETURN_INT32(entryCount);
}
