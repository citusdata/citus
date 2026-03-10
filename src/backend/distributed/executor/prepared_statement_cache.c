/*-------------------------------------------------------------------------
 *
 * prepared_statement_cache.c
 *   Per-connection cache for prepared statements on worker connections.
 *
 *   When citus.enable_prepared_statement_caching is ON, the coordinator
 *   uses PQprepare/PQsendQueryPrepared on worker connections instead
 *   of PQsendQuery for fast-path prepared statement executions (generic
 *   plan, execution 6+). This module manages the per-connection hash
 *   table that tracks which statements have already been prepared on
 *   each connection.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "distributed/prepared_statement_cache.h"


/* GUC: citus.enable_prepared_statement_caching */
bool EnablePreparedStatementCaching = false;


/*
 * PreparedStatementCacheCreate allocates a new hash table for caching
 * prepared statement entries on a single worker connection. The hash
 * table is allocated in TopMemoryContext so it survives across
 * transactions (matching the connection lifetime).
 */
HTAB *
PreparedStatementCacheCreate(void)
{
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(PreparedStatementCacheKey);
	info.entrysize = sizeof(PreparedStatementCacheEntry);
	info.hcxt = TopMemoryContext;

	HTAB *cache = hash_create("Prepared Statement Cache",
							  32, /* initial size */
							  &info,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	return cache;
}


/*
 * PreparedStatementCacheLookup looks up a cache entry by (planId, shardId).
 * Returns the entry if found, NULL otherwise.
 */
PreparedStatementCacheEntry *
PreparedStatementCacheLookup(HTAB *cache, uint32 planId, uint64 shardId)
{
	PreparedStatementCacheKey key;

	memset(&key, 0, sizeof(key));
	key.planId = planId;
	key.shardId = shardId;

	PreparedStatementCacheEntry *entry =
		(PreparedStatementCacheEntry *) hash_search(cache, &key,
													HASH_FIND, NULL);

	return entry;
}


/*
 * PreparedStatementCacheInsert inserts a new entry for (planId, shardId).
 * Returns the new entry on success, or NULL if the cache has reached
 * MAX_CACHED_STMTS_PER_CONNECTION (caller should fall back to plain SQL).
 *
 * The caller is responsible for filling in the returned entry's fields
 * (stmtName, paramTypes, paramCount, parameterizedQueryString).
 */
PreparedStatementCacheEntry *
PreparedStatementCacheInsert(HTAB *cache, uint32 planId, uint64 shardId)
{
	if (hash_get_num_entries(cache) >= MAX_CACHED_STMTS_PER_CONNECTION)
	{
		return NULL;
	}

	PreparedStatementCacheKey key;

	memset(&key, 0, sizeof(key));
	key.planId = planId;
	key.shardId = shardId;

	bool found = false;
	PreparedStatementCacheEntry *entry =
		(PreparedStatementCacheEntry *) hash_search(cache, &key,
													HASH_ENTER, &found);

	if (found)
	{
		/* already exists — return existing entry */
		return entry;
	}

	/* initialize the new entry with auto-generated statement name */
	snprintf(entry->stmtName, MAX_STMT_NAME_LENGTH,
			 "__citus_stmt_%ld", (long) hash_get_num_entries(cache));
	entry->paramTypes = NULL;
	entry->paramCount = 0;
	entry->parameterizedQueryString = NULL;

	return entry;
}


/*
 * PreparedStatementCacheDestroy frees all memory used by the cache.
 * Safe to call with NULL (no-op).
 */
void
PreparedStatementCacheDestroy(HTAB *cache)
{
	if (cache == NULL)
	{
		return;
	}

	/*
	 * Free dynamically allocated fields in each entry before destroying
	 * the hash table itself.
	 */
	HASH_SEQ_STATUS status;
	PreparedStatementCacheEntry *entry;

	hash_seq_init(&status, cache);
	while ((entry = hash_seq_search(&status)) != NULL)
	{
		if (entry->paramTypes != NULL)
		{
			pfree(entry->paramTypes);
		}
		if (entry->parameterizedQueryString != NULL)
		{
			pfree(entry->parameterizedQueryString);
		}
	}

	hash_destroy(cache);
}
