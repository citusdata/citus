/*-------------------------------------------------------------------------
 *
 * stats_statements.h
 *    Statement-level statistics for distributed queries.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef STATS_STATEMENT_EXECUTORS_H
#define STATS_STATEMENT_EXECUTORS_H

#include "distributed/multi_server_executor.h"

void InitializeStatsStatementExecutors(void);
void StoreStatsStatementExecutorsEntry(uint32 queryId, MultiExecutorType executorType);

#endif /* STATS_STATEMENT_EXECUTORS_H */
