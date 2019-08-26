/*-------------------------------------------------------------------------
 *
 * query_stats.c
 *    Statement-level statistics for distributed queries.
 *
 * Copyright (c) 2012-2019, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "distributed/query_stats.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(citus_stat_statements_reset);
PG_FUNCTION_INFO_V1(citus_query_stats);
PG_FUNCTION_INFO_V1(citus_executor_name);


static char * CitusExecutorName(MultiExecutorType executorType);


/* placeholder for InitializeCitusQueryStats */
void
InitializeCitusQueryStats(void)
{
	/* placeholder for future implementation */
}


/* placeholder for CitusQueryStatsExecutorsEntry */
void
CitusQueryStatsExecutorsEntry(uint64 queryId, MultiExecutorType executorType,
							  char *partitionKey)
{
	/* placeholder for future implementation */
}


/*
 * placeholder function for citus_stat_statements_reset
 */
Datum
citus_stat_statements_reset(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("citus_stat_statements_reset() is only supported on "
						   "Citus Enterprise")));
	PG_RETURN_VOID();
}


/*
 * placeholder function for citus_query_stats
 */
Datum
citus_query_stats(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("citus_query_stats() is only supported on "
						   "Citus Enterprise")));
	PG_RETURN_VOID();
}


/*
 * citus_executor_name is a UDF that returns the name of the executor
 * given the internal enum value.
 */
Datum
citus_executor_name(PG_FUNCTION_ARGS)
{
	MultiExecutorType executorType = PG_GETARG_UINT32(0);

	char *executorName = CitusExecutorName(executorType);

	PG_RETURN_TEXT_P(cstring_to_text(executorName));
}


/*
 * CitusExecutorName returns the name of the executor given the internal
 * enum value.
 */
static char *
CitusExecutorName(MultiExecutorType executorType)
{
	switch (executorType)
	{
		case MULTI_EXECUTOR_ADAPTIVE:
		{
			return "adaptive";
		}

		case MULTI_EXECUTOR_REAL_TIME:
		{
			return "real-time";
		}

		case MULTI_EXECUTOR_TASK_TRACKER:
		{
			return "task-tracker";
		}

		case MULTI_EXECUTOR_ROUTER:
		{
			return "router";
		}

		case MULTI_EXECUTOR_COORDINATOR_INSERT_SELECT:
		{
			return "insert-select";
		}

		default:
		{
			return "unknown";
		}
	}
}
