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
 *   It also owns the two integration points that make the cache usable:
 *   the planner-side fast path that builds a Task straight from
 *   ParamListInfo (skipping replanning), and the executor-side dispatch
 *   that prepares or reuses the named statement on a connection.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "nodes/makefuncs.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "distributed/citus_custom_scan.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_execution_locks.h"
#include "distributed/executor_util.h"
#include "distributed/listutils.h"
#include "distributed/local_plan_cache.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/prepared_statement_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shardinterval_utils.h"


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
PreparedStatementCacheLookup(HTAB *cache, uint64 planId, uint64 shardId)
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
PreparedStatementCacheInsert(HTAB *cache, uint64 planId, uint64 shardId)
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
	SafeSnprintf(entry->stmtName, MAX_STMT_NAME_LENGTH,
				 "__citus_stmt_%ld", (long) hash_get_num_entries(cache));
	entry->paramTypes = NULL;
	entry->paramCount = 0;
	entry->parameterizedQueryString = NULL;

	return entry;
}


/*
 * PreparedStatementCacheDestroy frees all memory used by the cache.
 * Safe to call if the pointed to cache is NULL (NOP)
 * Otherwise, frees all memory used by the cache and
 * sets it to NULL.
 */
void
PreparedStatementCacheDestroy(HTAB **cache_ptr)
{
	Assert(cache_ptr != NULL);
	HTAB *cache = *cache_ptr;

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
	*cache_ptr = NULL;
}


/*
 * DeparseTaskTemplate deparses the task's parameterized job query into SQL
 * targeting the task's shard, leaving Param nodes ($1, ...) intact.
 *
 * The template is shared across executions, so a working copy is deparsed.
 */
static char *
DeparseTaskTemplate(Task *task)
{
	Query *queryCopy = copyObject(task->jobQueryForPrepare);
	StringInfoData buf;
	initStringInfo(&buf);

	if (queryCopy->commandType == CMD_INSERT)
	{
		/* upserts reference the target by name, which becomes the shard name */
		AddInsertAliasIfNeeded(queryCopy);
		deparse_shard_query(queryCopy, task->anchorDistributedTableId,
							task->anchorShardId, &buf);
	}
	else
	{
		UpdateRelationToShardNames((Node *) queryCopy, task->relationShardList);
		pg_get_query_def(queryCopy, &buf);
	}

	return buf.data;
}


/*
 * PreparedStatementCacheSendQuery dispatches a task on a worker connection
 * using a named prepared statement, preparing it first if this connection has
 * not seen it before.
 *
 * Returns PREPARED_STMT_NOT_APPLICABLE when the task carries no template, in
 * which case the caller must use its normal send path. When the connection's
 * cache is full it returns PREPARED_STMT_FALLBACK and sets fallbackQueryString
 * to parameterized SQL for the caller to send instead.
 */
PreparedStatementSendStatus
PreparedStatementCacheSendQuery(MultiConnection *connection, Task *task,
								ParamListInfo paramListInfo, bool binaryResults,
								char **fallbackQueryString)
{
	if (!EnablePreparedStatementCaching || task->jobQueryForPrepare == NULL ||
		paramListInfo == NULL)
	{
		return PREPARED_STMT_NOT_APPLICABLE;
	}

	Oid *parameterTypes = NULL;
	const char **parameterValues = NULL;

	/* force evaluation of bound params */
	paramListInfo = copyParamList(paramListInfo);
	int parameterCount = paramListInfo->numParams;

	ExtractParametersForRemoteExecution(paramListInfo, &parameterTypes,
										&parameterValues);

	if (connection->preparedStatementCache == NULL)
	{
		connection->preparedStatementCache = PreparedStatementCacheCreate();
	}

	PreparedStatementCacheEntry *cacheEntry =
		PreparedStatementCacheLookup(connection->preparedStatementCache,
									 task->preparedStatementPlanId,
									 task->anchorShardId);
	if (cacheEntry == NULL)
	{
		elog(DEBUG2, "prepared statement cache miss: plan " UINT64_FORMAT
			 " shard " UINT64_FORMAT,
			 task->preparedStatementPlanId, task->anchorShardId);

		cacheEntry = PreparedStatementCacheInsert(connection->preparedStatementCache,
												  task->preparedStatementPlanId,
												  task->anchorShardId);
		if (cacheEntry == NULL)
		{
			/*
			 * Cache full. The fast-path task has no query string of its own, so
			 * hand the caller freshly deparsed SQL. Clearing the resolved flag
			 * routes it through the parameterized send path.
			 */
			*fallbackQueryString = DeparseTaskTemplate(task);
			task->parametersInQueryStringResolved = false;
			return PREPARED_STMT_FALLBACK;
		}

		char *queryString = DeparseTaskTemplate(task);

		if (SendRemotePrepare(connection, cacheEntry->stmtName, queryString,
							  parameterCount, parameterTypes) == 0)
		{
			connection->connectionState = MULTI_CONNECTION_LOST;
			return PREPARED_STMT_FAILED;
		}

		Size paramTypesSize = parameterCount * sizeof(Oid);
		cacheEntry->paramTypes = MemoryContextAlloc(TopMemoryContext, paramTypesSize);
		memcpy_s(cacheEntry->paramTypes, paramTypesSize, parameterTypes,
				 paramTypesSize);
		cacheEntry->paramCount = parameterCount;
		cacheEntry->parameterizedQueryString =
			MemoryContextStrdup(TopMemoryContext, queryString);

		pfree(queryString);
	}
	else
	{
		elog(DEBUG2, "prepared statement cache hit: plan " UINT64_FORMAT
			 " shard " UINT64_FORMAT " stmt %s",
			 task->preparedStatementPlanId, task->anchorShardId,
			 cacheEntry->stmtName);
	}

	if (SendRemotePreparedQuery(connection, cacheEntry->stmtName, parameterCount,
								parameterValues, binaryResults) == 0)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return PREPARED_STMT_FAILED;
	}

	if (PQsetSingleRowMode(connection->pgConn) == 0)
	{
		connection->connectionState = MULTI_CONNECTION_LOST;
		return PREPARED_STMT_FAILED;
	}

	return PREPARED_STMT_SENT;
}


/*
 * PreparedStatementCacheSaveTemplate returns the job query template used to
 * build parameterized statements, saving it on the original plan on first use.
 *
 * The template must be captured before coordinator evaluation resolves Param
 * nodes, and is kept on the original (cached) plan so later executions reuse
 * it instead of paying for a copyObject each time.
 *
 * Returns NULL when the plan is not eligible for caching.
 */
Query *
PreparedStatementCacheSaveTemplate(DistributedPlan *originalPlan)
{
	Job *originalJob = originalPlan->workerJob;
	Query *jobQuery = originalJob->jobQuery;

	if (!EnablePreparedStatementCaching)
	{
		return NULL;
	}

	if (jobQuery->commandType != CMD_SELECT)
	{
		/*
		 * Multi-row INSERT can't be cached: each shard's task carries only its
		 * own subset of VALUES rows, but the statement is deparsed once from
		 * the whole job query, so every shard would receive every row.
		 */
		if (!originalJob->deferredPruning ||
			ExtractDistributedInsertValuesRTE(jobQuery) != NULL)
		{
			return NULL;
		}
	}

	if (originalJob->savedJobQueryForCaching == NULL)
	{
		MemoryContext oldContext =
			MemoryContextSwitchTo(GetMemoryChunkContext(originalPlan));
		originalJob->savedJobQueryForCaching = copyObject(jobQuery);
		MemoryContextSwitchTo(oldContext);
	}

	return originalJob->savedJobQueryForCaching;
}


/*
 * PreparedStatementCacheAttachToTasks stamps the cache key and query template
 * onto each task so SendNextQuery can look them up. Does nothing when the plan
 * is not eligible for caching.
 */
void
PreparedStatementCacheAttachToTasks(DistributedPlan *currentPlan, Job *workerJob,
									Query *savedJobQuery)
{
	if (!EnablePreparedStatementCaching || savedJobQuery == NULL)
	{
		return;
	}

	bool isInsert = workerJob->jobQuery->commandType == CMD_INSERT;

	Task *task = NULL;
	foreach_declared_ptr(task, workerJob->taskList)
	{
		task->preparedStatementPlanId = currentPlan->planId;
		task->jobQueryForPrepare = savedJobQuery;

		/* deparse_shard_query needs the target relation for INSERT */
		if (isInsert)
		{
			task->anchorDistributedTableId = linitial_oid(currentPlan->relationIdList);
		}
	}
}


/*
 * FastPathShardInterval returns the shard the distribution key parameter routes
 * to, or NULL if the fast path cannot be used for this execution.
 */
static ShardInterval *
FastPathShardInterval(DistributedPlan *plan, Job *workerJob, EState *estate)
{
	int paramId = workerJob->distributionKeyParamId;
	ParamListInfo paramListInfo = estate->es_param_list_info;

	if (paramId < 1 || paramListInfo == NULL || paramId > paramListInfo->numParams)
	{
		return NULL;
	}

	ParamExternData *param = &paramListInfo->params[paramId - 1];
	if (!OidIsValid(param->ptype) || param->isnull)
	{
		return NULL;
	}

	Oid relationId = linitial_oid(plan->relationIdList);
	CitusTableCacheEntry *tableEntry = GetCitusTableCacheEntry(relationId);

	return FindShardInterval(param->value, tableEntry);
}


/*
 * BuildFastPathTask builds the minimal Task for a single-shard execution,
 * bypassing plan copying, coordinator evaluation and task regeneration.
 */
static Task *
BuildFastPathTask(DistributedPlan *plan, Job *workerJob, EState *estate,
				  ShardInterval *shardInterval, bool isModify)
{
	List *shardIntervalListList = list_make1(list_make1(shardInterval));
	bool shardsPresent = false;
	List *relationShardList =
		RelationShardListForShardIntervalList(shardIntervalListList, &shardsPresent);
	List *placementList =
		CreateTaskPlacementListForShardIntervals(shardIntervalListList, shardsPresent,
												 true, false);

	Task *task = CitusMakeNode(Task);
	task->taskType = isModify ? MODIFY_TASK : READ_TASK;
	task->anchorShardId = shardInterval->shardId;
	task->anchorDistributedTableId = linitial_oid(plan->relationIdList);
	task->taskPlacementList = placementList;
	task->queryCount = 1;
	task->parametersInQueryStringResolved = true;
	task->preparedStatementPlanId = plan->planId;
	task->jobQueryForPrepare = workerJob->savedJobQueryForCaching;
	task->relationShardList = relationShardList;
	task->colocationId = workerJob->colocationId;

	ParamExternData *param =
		&estate->es_param_list_info->params[workerJob->distributionKeyParamId - 1];
	int16 typeLength;
	bool typeByValue;
	get_typlenbyval(param->ptype, &typeLength, &typeByValue);
	task->partitionKeyValue = makeConst(param->ptype, -1, InvalidOid,
										(int) typeLength, param->value, false,
										typeByValue);

	return task;
}


/*
 * PreparedStatementCacheTryFastPath builds the task for this execution directly
 * from the bound parameters, reusing the plan and query template saved on the
 * first execution.
 *
 * Returns false when the fast path does not apply, in which case the caller
 * must fall through to normal planning.
 */
bool
PreparedStatementCacheTryFastPath(struct CitusScanState *scanStateArg, EState *estate,
								  bool isModify)
{
	CitusScanState *scanState = (CitusScanState *) scanStateArg;
	DistributedPlan *originalPlan = scanState->distributedPlan;
	Job *workerJob = originalPlan->workerJob;

	/* the first execution populates the template the fast path depends on */
	if (!EnablePreparedStatementCaching ||
		originalPlan->numberOfTimesExecuted == 0 ||
		workerJob->savedJobQueryForCaching == NULL)
	{
		return false;
	}

	/* volatile functions must be re-evaluated per execution on the coordinator */
	if (isModify &&
		(!workerJob->deferredPruning || workerJob->requiresCoordinatorEvaluation))
	{
		return false;
	}

	ShardInterval *shardInterval = FastPathShardInterval(originalPlan, workerJob,
														 estate);
	if (shardInterval == NULL || (isModify && !ShardExists(shardInterval->shardId)))
	{
		return false;
	}

	Task *task = BuildFastPathTask(originalPlan, workerJob, estate, shardInterval,
								   isModify);

	workerJob->taskList = list_make1(task);
	workerJob->parametersInJobQueryResolved = true;

	elog(DEBUG2, "prepared statement cache-hit fast path%s: plan " UINT64_FORMAT
		 " shard " UINT64_FORMAT,
		 isModify ? " (DML)" : "", originalPlan->planId, shardInterval->shardId);

	/* the executor reads the plan back from the scan state */
	scanState->distributedPlan = originalPlan;

	if (isModify)
	{
		AcquireMetadataLocks(workerJob->taskList);
		EnsureAnchorShardsInJobExist(workerJob);
		workerJob->taskList = FirstReplicaAssignTaskList(workerJob->taskList);
	}

	/*
	 * A fast-path task has no query string, so local execution needs a cached
	 * local plan rather than deparsed SQL.
	 */
	if (IsLocalPlanCachingSupported(workerJob, originalPlan))
	{
		CacheLocalPlanForShardQuery(linitial(workerJob->taskList), originalPlan,
									estate->es_param_list_info);
	}

	return true;
}
