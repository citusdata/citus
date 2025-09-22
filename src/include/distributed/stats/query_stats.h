/*-------------------------------------------------------------------------
 *
 * stats_statements.h
 *    Statement-level statistics for distributed queries.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef QUERY_STATS_H
#define QUERY_STATS_H

#include "distributed/multi_server_executor.h"

#define STATS_SHARED_MEM_NAME "citus_query_stats"

extern Size CitusQueryStatsSharedMemSize(void);
extern void InitializeCitusQueryStats(void);
extern void CitusQueryStatsExecutorsEntry(uint64 queryId, MultiExecutorType executorType,
										  char *partitionKey);
extern void CitusQueryStatsSynchronizeEntries(void);
extern int StatStatementsPurgeInterval;
extern int StatStatementsMax;
extern int StatStatementsTrack;


typedef enum
{
	STAT_STATEMENTS_TRACK_NONE = 0,
	STAT_STATEMENTS_TRACK_ALL = 1
} StatStatementsTrackType;

#endif /* QUERY_STATS_H */
