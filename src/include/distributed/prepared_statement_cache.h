/*-------------------------------------------------------------------------
 * prepared_statement_cache.h
 *
 * Declarations for per-connection prepared statement caching on worker
 * connections.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PREPARED_STATEMENT_CACHE_H
#define PREPARED_STATEMENT_CACHE_H

#include "postgres.h"

#include "nodes/execnodes.h"
#include "nodes/parsenodes.h"
#include "utils/hsearch.h"

#include "distributed/connection_management.h"
#include "distributed/multi_physical_planner.h"

struct CitusScanState;

/* compile-time limit for per-connection cached prepared statements */
#define MAX_CACHED_STMTS_PER_CONNECTION 1000

/* maximum length for generated statement names ("__citus_stmt_NNNN") */
#define MAX_STMT_NAME_LENGTH 64


/*
 * PreparedStatementCacheKey uniquely identifies a prepared statement on a
 * given worker connection: planId identifies the cached generic plan,
 * shardId identifies the target shard.
 */
typedef struct PreparedStatementCacheKey
{
	uint64 planId;
	uint64 shardId;
} PreparedStatementCacheKey;


/*
 * PreparedStatementCacheEntry stores the prepared statement handle on a
 * connection, plus metadata needed to re-execute it.
 */
typedef struct PreparedStatementCacheEntry
{
	PreparedStatementCacheKey key;

	char stmtName[MAX_STMT_NAME_LENGTH];
	Oid *paramTypes;
	int paramCount;
	char *parameterizedQueryString;
} PreparedStatementCacheEntry;


/*
 * Outcome of an attempt to dispatch a task through the connection's
 * prepared statement cache.
 */
typedef enum PreparedStatementSendStatus
{
	PREPARED_STMT_NOT_APPLICABLE,   /* caching off or task carries no template */
	PREPARED_STMT_SENT,             /* dispatched on the connection */
	PREPARED_STMT_FAILED,           /* connection lost */
	PREPARED_STMT_FALLBACK          /* cache full; caller sends the returned SQL */
} PreparedStatementSendStatus;


/* GUC variable */
extern bool EnablePreparedStatementCaching;

/* cache lifecycle */
extern HTAB * PreparedStatementCacheCreate(void);
extern void PreparedStatementCacheDestroy(HTAB **cache_ptr);

/* cache operations */
extern PreparedStatementCacheEntry * PreparedStatementCacheLookup(HTAB *cache, uint64
																  planId, uint64 shardId);
extern PreparedStatementCacheEntry * PreparedStatementCacheInsert(HTAB *cache, uint64
																  planId, uint64 shardId);

/* planner-side integration (see citus_custom_scan.c) */
extern bool PreparedStatementCacheTryFastPath(struct CitusScanState *scanState,
											  EState *estate, bool isModify);
extern Query * PreparedStatementCacheSaveTemplate(DistributedPlan *originalPlan);
extern void PreparedStatementCacheAttachToTasks(DistributedPlan *currentPlan,
												Job *workerJob, Query *savedJobQuery);

/* executor-side integration (see adaptive_executor.c) */
extern PreparedStatementSendStatus PreparedStatementCacheSendQuery(MultiConnection *
																   connection, Task *task,
																   ParamListInfo
																   paramListInfo,
																   bool binaryResults,
																   char **
																   fallbackQueryString);

#endif /* PREPARED_STATEMENT_CACHE_H */
