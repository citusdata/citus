/*-------------------------------------------------------------------------
 *
 * citus_stat_tenants.c
 *	  Routines related to the multi tenant monitor.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include <time.h>

#include "postgres.h"

#include "unistd.h"

#include "access/hash.h"
#include "common/pg_prng.h"
#include "executor/execdesc.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "sys/time.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/colocation_utils.h"
#include "distributed/distributed_planner.h"
#include "distributed/jsonbutils.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/tuplestore.h"
#include "distributed/utils/citus_stat_tenants.h"

static void AttributeMetricsIfApplicable(void);

ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

#define ATTRIBUTE_PREFIX "/*{\"cId\":"
#define ATTRIBUTE_STRING_FORMAT "/*{\"cId\":%d,\"tId\":%s}*/"
#define ATTRIBUTE_STRING_FORMAT_WITHOUT_TID "/*{\"cId\":%d}*/"
#define STAT_TENANTS_COLUMNS 9
#define ONE_QUERY_SCORE 1000000000

static char AttributeToTenant[MAX_TENANT_ATTRIBUTE_LENGTH] = "";
static CmdType AttributeToCommandType = CMD_UNKNOWN;
static int AttributeToColocationGroupId = INVALID_COLOCATION_ID;
static clock_t QueryStartClock = { 0 };
static clock_t QueryEndClock = { 0 };

static const char *SharedMemoryNameForMultiTenantMonitor =
	"Shared memory for multi tenant monitor";
static char *MonitorTrancheName = "Multi Tenant Monitor Tranche";

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static int CompareTenantScore(const void *leftElement, const void *rightElement);
static void UpdatePeriodsIfNecessary(TenantStats *tenantStats, TimestampTz queryTime);
static void ReduceScoreIfNecessary(TenantStats *tenantStats, TimestampTz queryTime);
static void EvictTenantsIfNecessary(TimestampTz queryTime);
static void RecordTenantStats(TenantStats *tenantStats, TimestampTz queryTime);
static MultiTenantMonitor * CreateSharedMemoryForMultiTenantMonitor(void);
static MultiTenantMonitor * GetMultiTenantMonitor(void);
static void MultiTenantMonitorSMInit(void);
static TenantStats * CreateTenantStats(MultiTenantMonitor *monitor, TimestampTz
									   queryTime);
static void FillTenantStatsHashKey(TenantStatsHashKey *key, char *tenantAttribute, uint32
								   colocationGroupId);
static TenantStats * FindTenantStats(MultiTenantMonitor *monitor);
static size_t MultiTenantMonitorshmemSize(void);
static char * ExtractTopComment(const char *inputString);
static char * EscapeCommentChars(const char *str);
static char * UnescapeCommentChars(const char *str);

int StatTenantsLogLevel = CITUS_LOG_LEVEL_OFF;
int StatTenantsPeriod = (time_t) 60;
int StatTenantsLimit = 100;
int StatTenantsTrack = STAT_TENANTS_TRACK_NONE;
double StatTenantsSampleRateForNewTenants = 1;

PG_FUNCTION_INFO_V1(citus_stat_tenants_local);
PG_FUNCTION_INFO_V1(citus_stat_tenants_local_reset);


/*
 * citus_stat_tenants_local finds, updates and returns the statistics for tenants.
 */
Datum
citus_stat_tenants_local(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	/*
	 * We keep more than StatTenantsLimit tenants in our monitor.
	 * We do this to not lose data if a tenant falls out of top StatTenantsLimit in case they need to return soon.
	 * Normally we return StatTenantsLimit tenants but if returnAllTenants is true we return all of them.
	 */
	bool returnAllTenants = PG_GETARG_BOOL(0);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	TimestampTz monitoringTime = GetCurrentTimestamp();

	Datum values[STAT_TENANTS_COLUMNS];
	bool isNulls[STAT_TENANTS_COLUMNS];

	MultiTenantMonitor *monitor = GetMultiTenantMonitor();

	if (monitor == NULL)
	{
		PG_RETURN_VOID();
	}

	LWLockAcquire(&monitor->lock, LW_EXCLUSIVE);

	int numberOfRowsToReturn = 0;
	int tenantStatsCount = hash_get_num_entries(monitor->tenants);
	if (returnAllTenants)
	{
		numberOfRowsToReturn = tenantStatsCount;
	}
	else
	{
		numberOfRowsToReturn = Min(tenantStatsCount,
								   StatTenantsLimit);
	}

	/* Allocate an array to hold the tenants. */
	TenantStats **stats = palloc(tenantStatsCount *
								 sizeof(TenantStats *));

	HASH_SEQ_STATUS hash_seq;
	TenantStats *stat;

	/* Get all the tenants from the hash table. */
	int j = 0;
	hash_seq_init(&hash_seq, monitor->tenants);
	while ((stat = hash_seq_search(&hash_seq)) != NULL)
	{
		stats[j++] = stat;
		UpdatePeriodsIfNecessary(stat, monitoringTime);
		ReduceScoreIfNecessary(stat, monitoringTime);
	}

	/* Sort the tenants by their score. */
	SafeQsort(stats, j, sizeof(TenantStats *),
			  CompareTenantScore);

	for (int i = 0; i < numberOfRowsToReturn; i++)
	{
		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		TenantStats *tenantStats = stats[i];

		values[0] = Int32GetDatum(tenantStats->key.colocationGroupId);

		if (tenantStats->key.tenantAttribute[0] == '\0')
		{
			isNulls[1] = true;
		}
		else
		{
			values[1] = PointerGetDatum(cstring_to_text(
											tenantStats->key.tenantAttribute));
		}

		values[2] = Int32GetDatum(tenantStats->readsInThisPeriod);
		values[3] = Int32GetDatum(tenantStats->readsInLastPeriod);
		values[4] = Int32GetDatum(tenantStats->readsInThisPeriod +
								  tenantStats->writesInThisPeriod);
		values[5] = Int32GetDatum(tenantStats->readsInLastPeriod +
								  tenantStats->writesInLastPeriod);
		values[6] = Float8GetDatum(tenantStats->cpuUsageInThisPeriod);
		values[7] = Float8GetDatum(tenantStats->cpuUsageInLastPeriod);
		values[8] = Int64GetDatum(tenantStats->score);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	pfree(stats);

	LWLockRelease(&monitor->lock);

	PG_RETURN_VOID();
}


/*
 * citus_stat_tenants_local_reset resets monitor for tenant statistics
 * on the local node.
 */
Datum
citus_stat_tenants_local_reset(PG_FUNCTION_ARGS)
{
	MultiTenantMonitor *monitor = GetMultiTenantMonitor();

	/* if monitor is not created yet, there is nothing to reset */
	if (monitor == NULL)
	{
		PG_RETURN_VOID();
	}

	HASH_SEQ_STATUS hash_seq;
	TenantStats *stats;

	LWLockAcquire(&monitor->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, monitor->tenants);
	while ((stats = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(monitor->tenants, &stats->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(&monitor->lock);

	PG_RETURN_VOID();
}


/*
 * AttributeQueryIfAnnotated checks the query annotation and if the query is annotated
 * for the tenant statistics monitoring this function records the tenant attributes.
 */
void
AttributeQueryIfAnnotated(const char *query_string, CmdType commandType)
{
	if (StatTenantsTrack == STAT_TENANTS_TRACK_NONE)
	{
		return;
	}

	AttributeToColocationGroupId = INVALID_COLOCATION_ID;

	if (query_string == NULL)
	{
		return;
	}

	if (strncmp(ATTRIBUTE_PREFIX, query_string, strlen(ATTRIBUTE_PREFIX)) == 0)
	{
		char *annotation = ExtractTopComment(query_string);
		if (annotation != NULL)
		{
			Datum jsonbDatum = DirectFunctionCall1(jsonb_in, PointerGetDatum(annotation));

			text *tenantIdTextP = ExtractFieldTextP(jsonbDatum, "tId");
			char *tenantId = NULL;
			if (tenantIdTextP != NULL)
			{
				tenantId = UnescapeCommentChars(text_to_cstring(tenantIdTextP));
			}

			int colocationId = ExtractFieldInt32(jsonbDatum, "cId",
												 INVALID_COLOCATION_ID);

			AttributeTask(tenantId, colocationId, commandType);
		}
	}
}


/*
 * AttributeTask assigns the given attributes of a tenant and starts a timer
 */
void
AttributeTask(char *tenantId, int colocationId, CmdType commandType)
{
	if (StatTenantsTrack == STAT_TENANTS_TRACK_NONE ||
		colocationId == INVALID_COLOCATION_ID)
	{
		return;
	}

	TenantStatsHashKey key = { 0 };
	FillTenantStatsHashKey(&key, tenantId, colocationId);

	MultiTenantMonitor *monitor = GetMultiTenantMonitor();
	bool found = false;

	/* Acquire the lock in shared mode to check if the tenant is already in the hash table. */
	LWLockAcquire(&monitor->lock, LW_SHARED);

	hash_search(monitor->tenants, &key, HASH_FIND, &found);

	LWLockRelease(&monitor->lock);

	/* If the tenant is not found in the hash table, we will track the query with a probability of StatTenantsSampleRateForNewTenants. */
	if (!found)
	{
		double randomValue = pg_prng_double(&pg_global_prng_state);
		bool shouldTrackQuery = randomValue <= StatTenantsSampleRateForNewTenants;
		if (!shouldTrackQuery)
		{
			return;
		}
	}

	/*
	 * if tenantId is NULL, it must be a schema-based tenant and
	 * we try to get the tenantId from the colocationId to lookup schema name and use it as a tenantId
	 */
	if (tenantId == NULL)
	{
		if (!IsTenantSchemaColocationGroup(colocationId))
		{
			return;
		}
	}

	AttributeToColocationGroupId = colocationId;
	if (tenantId != NULL)
	{
		strncpy_s(AttributeToTenant, MAX_TENANT_ATTRIBUTE_LENGTH, tenantId,
				  MAX_TENANT_ATTRIBUTE_LENGTH - 1);
	}
	else
	{
		strcpy_s(AttributeToTenant, sizeof(AttributeToTenant), "");
	}
	AttributeToCommandType = commandType;
	QueryStartClock = clock();
}


/*
 * AnnotateQuery annotates the query with tenant attributes.
 * if the query has a partition key, we annotate it with the partition key value and colocationId
 * if the query doesn't have a partition key and if it's a schema-based tenant, we annotate it with the colocationId only.
 */
char *
AnnotateQuery(char *queryString, Const *partitionKeyValue, int colocationId)
{
	if (StatTenantsTrack == STAT_TENANTS_TRACK_NONE ||
		colocationId == INVALID_COLOCATION_ID)
	{
		return queryString;
	}

	StringInfo newQuery = makeStringInfo();

	/* if the query doesn't have a parititon key value, check if it is a tenant schema */
	if (partitionKeyValue == NULL)
	{
		if (IsTenantSchemaColocationGroup(colocationId))
		{
			/* If it is a schema-based tenant, we only annotate the query with colocationId */
			appendStringInfo(newQuery, ATTRIBUTE_STRING_FORMAT_WITHOUT_TID,
							 colocationId);
		}
		else
		{
			/* If it is not a schema-based tenant query and doesn't have a parititon key,
			 * we don't annotate it
			 */
			return queryString;
		}
	}
	else
	{
		/* if the query has a partition key value, we annotate it with both tenantId and colocationId */
		char *partitionKeyValueString = DatumToString(partitionKeyValue->constvalue,
													  partitionKeyValue->consttype);

		char *commentCharsEscaped = EscapeCommentChars(partitionKeyValueString);
		StringInfo escapedSourceName = makeStringInfo();
		escape_json(escapedSourceName, commentCharsEscaped);

		appendStringInfo(newQuery, ATTRIBUTE_STRING_FORMAT, colocationId,
						 escapedSourceName->data
						 );
	}

	appendStringInfoString(newQuery, queryString);

	return newQuery->data;
}


/*
 * CitusAttributeToEnd keeps the statistics for the tenant and calls the previously installed end hook
 * or the standard executor end function.
 */
void
CitusAttributeToEnd(QueryDesc *queryDesc)
{
	/*
	 * At the end of the Executor is the last moment we have to attribute the previous
	 * attribution to a tenant, if applicable
	 */
	AttributeMetricsIfApplicable();

	/* now call in to the previously installed hook, or the standard implementation */
	if (prev_ExecutorEnd)
	{
		prev_ExecutorEnd(queryDesc);
	}
	else
	{
		standard_ExecutorEnd(queryDesc);
	}
}


/*
 * CompareTenantScore is used to sort the tenant statistics by score
 * in descending order.
 */
static int
CompareTenantScore(const void *leftElement, const void *rightElement)
{
	double l_usage = (*(TenantStats *const *) leftElement)->score;
	double r_usage = (*(TenantStats *const *) rightElement)->score;

	if (l_usage > r_usage)
	{
		return -1;
	}
	else if (l_usage < r_usage)
	{
		return 1;
	}
	return 0;
}


/*
 * AttributeMetricsIfApplicable updates the metrics for current tenant's statistics
 */
static void
AttributeMetricsIfApplicable()
{
	if (StatTenantsTrack == STAT_TENANTS_TRACK_NONE ||
		AttributeToColocationGroupId == INVALID_COLOCATION_ID)
	{
		return;
	}

	/*
	 * return if we are not in the top level to make sure we are not
	 * stopping counting time for a sub-level execution
	 */
	if (ExecutorLevel != 0 || PlannerLevel != 0)
	{
		return;
	}

	QueryEndClock = clock();

	TimestampTz queryTime = GetCurrentTimestamp();

	MultiTenantMonitor *monitor = GetMultiTenantMonitor();

	/*
	 * We need to acquire the monitor lock in shared mode to check if the tenant is
	 * already in the monitor. If it is not, we need to acquire the lock in
	 * exclusive mode to add the tenant to the monitor.
	 *
	 * We need to check again if the tenant is in the monitor after acquiring the
	 * exclusive lock to avoid adding the tenant twice. Some other backend might
	 * have added the tenant while we were waiting for the lock.
	 *
	 * After releasing the exclusive lock, we need to acquire the lock in shared
	 * mode to update the tenant's statistics. We need to check again if the tenant
	 * is in the monitor after acquiring the shared lock because some other backend
	 * might have removed the tenant while we were waiting for the lock.
	 */
	LWLockAcquire(&monitor->lock, LW_SHARED);

	TenantStats *tenantStats = FindTenantStats(monitor);

	if (tenantStats != NULL)
	{
		SpinLockAcquire(&tenantStats->lock);

		UpdatePeriodsIfNecessary(tenantStats, queryTime);
		ReduceScoreIfNecessary(tenantStats, queryTime);
		RecordTenantStats(tenantStats, queryTime);

		SpinLockRelease(&tenantStats->lock);
	}
	else
	{
		LWLockRelease(&monitor->lock);

		LWLockAcquire(&monitor->lock, LW_EXCLUSIVE);
		tenantStats = FindTenantStats(monitor);

		if (tenantStats == NULL)
		{
			tenantStats = CreateTenantStats(monitor, queryTime);
		}

		LWLockRelease(&monitor->lock);

		LWLockAcquire(&monitor->lock, LW_SHARED);
		tenantStats = FindTenantStats(monitor);
		if (tenantStats != NULL)
		{
			SpinLockAcquire(&tenantStats->lock);

			UpdatePeriodsIfNecessary(tenantStats, queryTime);
			ReduceScoreIfNecessary(tenantStats, queryTime);
			RecordTenantStats(tenantStats, queryTime);

			SpinLockRelease(&tenantStats->lock);
		}
	}
	LWLockRelease(&monitor->lock);

	AttributeToColocationGroupId = INVALID_COLOCATION_ID;
}


/*
 * UpdatePeriodsIfNecessary moves the query counts to previous periods if a enough time has passed.
 *
 * If 1 period has passed after the latest query, this function moves this period's counts to the last period
 * and cleans this period's statistics.
 *
 * If 2 or more periods has passed after the last query, this function cleans all both this and last period's
 * statistics.
 */
static void
UpdatePeriodsIfNecessary(TenantStats *tenantStats, TimestampTz queryTime)
{
	long long int periodInMicroSeconds = StatTenantsPeriod * USECS_PER_SEC;
	long long int periodInMilliSeconds = StatTenantsPeriod * 1000;
	TimestampTz periodStart = queryTime - (queryTime % periodInMicroSeconds);

	/*
	 * If the last query in this tenant was before the start of current period
	 * but there are some query count for this period we move them to the last period.
	 */
	if (tenantStats->lastQueryTime < periodStart &&
		(tenantStats->writesInThisPeriod || tenantStats->readsInThisPeriod))
	{
		tenantStats->writesInLastPeriod = tenantStats->writesInThisPeriod;
		tenantStats->writesInThisPeriod = 0;

		tenantStats->readsInLastPeriod = tenantStats->readsInThisPeriod;
		tenantStats->readsInThisPeriod = 0;

		tenantStats->cpuUsageInLastPeriod = tenantStats->cpuUsageInThisPeriod;
		tenantStats->cpuUsageInThisPeriod = 0;
	}

	/*
	 * If the last query is more than two periods ago, we clean the last period counts too.
	 */
	if (TimestampDifferenceExceeds(tenantStats->lastQueryTime, periodStart,
								   periodInMilliSeconds))
	{
		tenantStats->writesInLastPeriod = 0;

		tenantStats->readsInLastPeriod = 0;

		tenantStats->cpuUsageInLastPeriod = 0;
	}
}


/*
 * ReduceScoreIfNecessary reduces the tenant score only if it is necessary.
 *
 * We halve the tenants' scores after each period. This function checks the number of
 * periods that passed after the lsat score reduction and reduces the score accordingly.
 */
static void
ReduceScoreIfNecessary(TenantStats *tenantStats, TimestampTz queryTime)
{
	long long int periodInMicroSeconds = StatTenantsPeriod * USECS_PER_SEC;
	TimestampTz periodStart = queryTime - (queryTime % periodInMicroSeconds);

	/*
	 * With each query we increase the score of tenant by ONE_QUERY_SCORE.
	 * After one period we halve the scores.
	 *
	 * Here we calculate how many periods passed after the last time we did score reduction
	 * If the latest score reduction was in this period this number should be 0,
	 * if it was in the last period this number should be 1 and so on.
	 */
	int periodCountAfterLastScoreReduction = (periodStart -
											  tenantStats->lastScoreReduction +
											  periodInMicroSeconds - 1) /
											 periodInMicroSeconds;

	/*
	 * This should not happen but let's make sure
	 */
	if (periodCountAfterLastScoreReduction < 0)
	{
		periodCountAfterLastScoreReduction = 0;
	}

	/*
	 * If the last score reduction was not in this period we do score reduction now.
	 */
	if (periodCountAfterLastScoreReduction > 0)
	{
		tenantStats->score >>= periodCountAfterLastScoreReduction;
		tenantStats->lastScoreReduction = queryTime;
	}
}


/*
 * EvictTenantsIfNecessary sorts and evicts the tenants if the tenant count is more than or
 * equal to 3 * StatTenantsLimit.
 */
static void
EvictTenantsIfNecessary(TimestampTz queryTime)
{
	MultiTenantMonitor *monitor = GetMultiTenantMonitor();

	/*
	 * We keep up to StatTenantsLimit * 3 tenants instead of StatTenantsLimit,
	 * so we don't lose data immediately after a tenant is out of top StatTenantsLimit
	 *
	 * Every time tenant count hits StatTenantsLimit * 3, we reduce it back to StatTenantsLimit * 2.
	 */
	long tenantStatsCount = hash_get_num_entries(monitor->tenants);
	if (tenantStatsCount >= StatTenantsLimit * 3)
	{
		HASH_SEQ_STATUS hash_seq;
		TenantStats *stat;
		TenantStats **stats = palloc(tenantStatsCount *
									 sizeof(TenantStats *));

		int i = 0;
		hash_seq_init(&hash_seq, monitor->tenants);
		while ((stat = hash_seq_search(&hash_seq)) != NULL)
		{
			stats[i++] = stat;
		}

		SafeQsort(stats, i, sizeof(TenantStats *), CompareTenantScore);

		for (i = StatTenantsLimit * 2; i < tenantStatsCount; i++)
		{
			hash_search(monitor->tenants, &stats[i]->key, HASH_REMOVE, NULL);
		}

		pfree(stats);
	}
}


/*
 * RecordTenantStats records the query statistics for the tenant.
 */
static void
RecordTenantStats(TenantStats *tenantStats, TimestampTz queryTime)
{
	if (tenantStats->score < LLONG_MAX - ONE_QUERY_SCORE)
	{
		tenantStats->score += ONE_QUERY_SCORE;
	}
	else
	{
		tenantStats->score = LLONG_MAX;
	}

	if (AttributeToCommandType == CMD_SELECT)
	{
		tenantStats->readsInThisPeriod++;
	}
	else if (AttributeToCommandType == CMD_UPDATE ||
			 AttributeToCommandType == CMD_INSERT ||
			 AttributeToCommandType == CMD_DELETE)
	{
		tenantStats->writesInThisPeriod++;
	}

	double queryCpuTime = ((double) (QueryEndClock - QueryStartClock)) / CLOCKS_PER_SEC;
	tenantStats->cpuUsageInThisPeriod += queryCpuTime;

	tenantStats->lastQueryTime = queryTime;
}


/*
 * CreateSharedMemoryForMultiTenantMonitor creates a dynamic shared memory segment for multi tenant monitor.
 */
static MultiTenantMonitor *
CreateSharedMemoryForMultiTenantMonitor()
{
	bool found = false;
	MultiTenantMonitor *monitor = ShmemInitStruct(SharedMemoryNameForMultiTenantMonitor,
												  MultiTenantMonitorshmemSize(),
												  &found);
	if (found)
	{
		return monitor;
	}

	monitor->namedLockTranche.trancheId = LWLockNewTrancheId();
	monitor->namedLockTranche.trancheName = MonitorTrancheName;

	LWLockRegisterTranche(monitor->namedLockTranche.trancheId,
						  monitor->namedLockTranche.trancheName);
	LWLockInitialize(&monitor->lock, monitor->namedLockTranche.trancheId);

	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(TenantStatsHashKey);
	info.entrysize = sizeof(TenantStats);

	monitor->tenants = ShmemInitHash("citus_stats_tenants hash",
									 StatTenantsLimit * 3, StatTenantsLimit * 3,
									 &info, HASH_ELEM |
									 HASH_SHARED_MEM | HASH_BLOBS);

	return monitor;
}


/*
 * GetMultiTenantMonitor returns the data structure for multi tenant monitor.
 */
static MultiTenantMonitor *
GetMultiTenantMonitor()
{
	bool found = false;
	MultiTenantMonitor *monitor = ShmemInitStruct(SharedMemoryNameForMultiTenantMonitor,
												  MultiTenantMonitorshmemSize(),
												  &found);

	if (!found)
	{
		elog(WARNING, "monitor not found");
		return NULL;
	}

	return monitor;
}


/*
 * InitializeMultiTenantMonitorSMHandleManagement sets up the shared memory startup hook
 * so that the multi tenant monitor can be initialized and stored in shared memory.
 */
void
InitializeMultiTenantMonitorSMHandleManagement()
{
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = MultiTenantMonitorSMInit;
}


/*
 * MultiTenantMonitorSMInit initializes the shared memory for MultiTenantMonitorSMData.
 */
static void
MultiTenantMonitorSMInit()
{
	CreateSharedMemoryForMultiTenantMonitor();

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * CreateTenantStats creates the data structure for a tenant's statistics.
 *
 * Calling this function should be protected by the monitor->lock in LW_EXCLUSIVE mode.
 */
static TenantStats *
CreateTenantStats(MultiTenantMonitor *monitor, TimestampTz queryTime)
{
	/*
	 * If the tenant count reached 3 * StatTenantsLimit, we evict the tenants
	 * with the lowest score.
	 */
	EvictTenantsIfNecessary(queryTime);

	TenantStatsHashKey key = { 0 };
	FillTenantStatsHashKey(&key, AttributeToTenant, AttributeToColocationGroupId);

	TenantStats *stats = (TenantStats *) hash_search(monitor->tenants, &key,
													 HASH_ENTER, NULL);

	stats->writesInLastPeriod = 0;
	stats->writesInThisPeriod = 0;
	stats->readsInLastPeriod = 0;
	stats->readsInThisPeriod = 0;
	stats->cpuUsageInLastPeriod = 0;
	stats->cpuUsageInThisPeriod = 0;
	stats->score = 0;
	stats->lastScoreReduction = 0;

	SpinLockInit(&stats->lock);

	return stats;
}


/*
 * FindTenantStats finds the current tenant's statistics.
 */
static TenantStats *
FindTenantStats(MultiTenantMonitor *monitor)
{
	TenantStatsHashKey key = { 0 };
	FillTenantStatsHashKey(&key, AttributeToTenant, AttributeToColocationGroupId);

	TenantStats *stats = (TenantStats *) hash_search(monitor->tenants, &key,
													 HASH_FIND, NULL);

	return stats;
}


static void
FillTenantStatsHashKey(TenantStatsHashKey *key, char *tenantAttribute, uint32
					   colocationGroupId)
{
	memset(key->tenantAttribute, 0, MAX_TENANT_ATTRIBUTE_LENGTH);

	if (tenantAttribute != NULL)
	{
		strlcpy(key->tenantAttribute, tenantAttribute, MAX_TENANT_ATTRIBUTE_LENGTH);
	}

	key->colocationGroupId = colocationGroupId;
}


/*
 * MultiTenantMonitorshmemSize calculates the size of the multi tenant monitor using
 * StatTenantsLimit parameter.
 */
static size_t
MultiTenantMonitorshmemSize(void)
{
	Size size = sizeof(MultiTenantMonitor);
	size = add_size(size, mul_size(sizeof(TenantStats), StatTenantsLimit * 3));

	return size;
}


/*
 * ExtractTopComment extracts the top-level multi-line comment from a given input string.
 */
static char *
ExtractTopComment(const char *inputString)
{
	int commentCharsLength = 2;
	int inputStringLen = strlen(inputString);
	if (inputStringLen < commentCharsLength)
	{
		return NULL;
	}

	const char *commentStartChars = "/*";
	const char *commentEndChars = "*/";

	/* If query doesn't start with a comment, return NULL */
	if (strstr(inputString, commentStartChars) != inputString)
	{
		return NULL;
	}

	StringInfo commentData = makeStringInfo();

	/* Skip the comment start characters */
	const char *commentStart = inputString + commentCharsLength;

	/* Find the first comment end character */
	const char *commentEnd = strstr(commentStart, commentEndChars);
	if (commentEnd == NULL)
	{
		return NULL;
	}

	/* Append the comment to the StringInfo buffer */
	int commentLength = commentEnd - commentStart;
	appendStringInfo(commentData, "%.*s", commentLength, commentStart);

	/* Return the extracted comment */
	return commentData->data;
}


/*  EscapeCommentChars adds a backslash before each occurrence of '*' or '/' in the input string */
static char *
EscapeCommentChars(const char *str)
{
	int originalStringLength = strlen(str);
	StringInfo escapedString = makeStringInfo();

	for (int originalStringIndex = 0; originalStringIndex < originalStringLength;
		 originalStringIndex++)
	{
		if (str[originalStringIndex] == '*' || str[originalStringIndex] == '/')
		{
			appendStringInfoChar(escapedString, '\\');
		}

		appendStringInfoChar(escapedString, str[originalStringIndex]);
	}

	return escapedString->data;
}


/*  UnescapeCommentChars removes the backslash that precedes '*' or '/' in the input string. */
static char *
UnescapeCommentChars(const char *str)
{
	int originalStringLength = strlen(str);
	StringInfo unescapedString = makeStringInfo();

	for (int originalStringindex = 0; originalStringindex < originalStringLength;
		 originalStringindex++)
	{
		if (str[originalStringindex] == '\\' &&
			originalStringindex < originalStringLength - 1 &&
			(str[originalStringindex + 1] == '*' ||
			 str[originalStringindex + 1] == '/'))
		{
			originalStringindex++;
		}
		appendStringInfoChar(unescapedString, str[originalStringindex]);
	}

	return unescapedString->data;
}
