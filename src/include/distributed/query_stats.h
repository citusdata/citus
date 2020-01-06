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

extern void InitializeCitusQueryStats(void);
extern void CitusQueryStatsExecutorsEntry(uint64 queryId, MultiExecutorType executorType,
										  char *partitionKey);

#endif /* QUERY_STATS_H */
