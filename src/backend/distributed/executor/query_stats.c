/*-------------------------------------------------------------------------
 *
 * query_stats.c
 *    Statement-level statistics for distributed queries.
 *
 * Copyright (c) 2012-2018, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "distributed/query_stats.h"

PG_FUNCTION_INFO_V1(citus_stat_statements_reset);
PG_FUNCTION_INFO_V1(citus_query_stats);


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
