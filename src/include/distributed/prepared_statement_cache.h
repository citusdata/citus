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

#include "utils/hsearch.h"

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
	uint32 planId;
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


/* GUC variable */
extern bool EnablePreparedStatementCaching;

/* cache lifecycle */
extern HTAB * PreparedStatementCacheCreate(void);
extern void PreparedStatementCacheDestroy(HTAB *cache);

/* cache operations */
extern PreparedStatementCacheEntry * PreparedStatementCacheLookup(HTAB *cache, uint32
																  planId, uint64 shardId);
extern PreparedStatementCacheEntry * PreparedStatementCacheInsert(HTAB *cache, uint32
																  planId, uint64 shardId);

#endif /* PREPARED_STATEMENT_CACHE_H */
