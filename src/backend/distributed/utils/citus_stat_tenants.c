/*-------------------------------------------------------------------------
 *
 * citus_stat_tenants.c
 *	  Routines related to the multi tenant monitor.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "unistd.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/log_utils.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/jsonbutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/tuplestore.h"
#include "distributed/colocation_utils.h"
#include "distributed/utils/citus_stat_tenants.h"
#include "executor/execdesc.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "sys/time.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/json.h"


#include <time.h>

static void AttributeMetricsIfApplicable(void);

ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

#define ATTRIBUTE_PREFIX "/*{\"tId\":"
#define ATTRIBUTE_STRING_FORMAT "/*{\"tId\":%s,\"cId\":%d}*/"
#define STAT_TENANTS_COLUMNS 7
#define ONE_QUERY_SCORE 1000000000

static char AttributeToTenant[MAX_TENANT_ATTRIBUTE_LENGTH] = "";
static CmdType AttributeToCommandType = CMD_UNKNOWN;
static int AttributeToColocationGroupId = INVALID_COLOCATION_ID;

static const char *SharedMemoryNameForMultiTenantMonitor =
	"Shared memory for multi tenant monitor";
static char *TenantTrancheName = "Tenant Tranche";
static char *MonitorTrancheName = "Multi Tenant Monitor Tranche";

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static int CompareTenantScore(const void *leftElement, const void *rightElement);
static void UpdatePeriodsIfNecessary(TenantStats *tenantStats, TimestampTz queryTime);
static void ReduceScoreIfNecessary(TenantStats *tenantStats, TimestampTz queryTime);
static void EvictTenantsIfNecessary(TimestampTz queryTime);
static void RecordTenantStats(TenantStats *tenantStats);
static void CreateMultiTenantMonitor(void);
static MultiTenantMonitor * CreateSharedMemoryForMultiTenantMonitor(void);
static MultiTenantMonitor * GetMultiTenantMonitor(void);
static void MultiTenantMonitorSMInit(void);
static int CreateTenantStats(MultiTenantMonitor *monitor, TimestampTz queryTime);
static int FindTenantStats(MultiTenantMonitor *monitor);
static size_t MultiTenantMonitorshmemSize(void);
static char * ExtractTopComment(const char *inputString);
static char * EscapeCommentChars(const char *str);
static char * UnescapeCommentChars(const char *str);

int StatTenantsLogLevel = CITUS_LOG_LEVEL_OFF;
int StatTenantsPeriod = (time_t) 60;
int StatTenantsLimit = 100;
int StatTenantsTrack = STAT_TENANTS_TRACK_NONE;


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
	if (returnAllTenants)
	{
		numberOfRowsToReturn = monitor->tenantCount;
	}
	else
	{
		numberOfRowsToReturn = Min(monitor->tenantCount, StatTenantsLimit);
	}

	for (int tenantIndex = 0; tenantIndex < monitor->tenantCount; tenantIndex++)
	{
		UpdatePeriodsIfNecessary(&monitor->tenants[tenantIndex], monitoringTime);
		ReduceScoreIfNecessary(&monitor->tenants[tenantIndex], monitoringTime);
	}
	SafeQsort(monitor->tenants, monitor->tenantCount, sizeof(TenantStats),
			  CompareTenantScore);

	for (int i = 0; i < numberOfRowsToReturn; i++)
	{
		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		TenantStats *tenantStats = &monitor->tenants[i];

		values[0] = Int32GetDatum(tenantStats->colocationGroupId);
		values[1] = PointerGetDatum(cstring_to_text(tenantStats->tenantAttribute));
		values[2] = Int32GetDatum(tenantStats->readsInThisPeriod);
		values[3] = Int32GetDatum(tenantStats->readsInLastPeriod);
		values[4] = Int32GetDatum(tenantStats->readsInThisPeriod +
								  tenantStats->writesInThisPeriod);
		values[5] = Int32GetDatum(tenantStats->readsInLastPeriod +
								  tenantStats->writesInLastPeriod);
		values[6] = Int64GetDatum(tenantStats->score);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

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
	monitor->tenantCount = 0;

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

	strcpy_s(AttributeToTenant, sizeof(AttributeToTenant), "");

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
		tenantId == NULL || colocationId == INVALID_COLOCATION_ID)
	{
		return;
	}

	AttributeToColocationGroupId = colocationId;
	strncpy_s(AttributeToTenant, MAX_TENANT_ATTRIBUTE_LENGTH, tenantId,
			  MAX_TENANT_ATTRIBUTE_LENGTH - 1);
	AttributeToCommandType = commandType;
}


/*
 * AnnotateQuery annotates the query with tenant attributes.
 */
char *
AnnotateQuery(char *queryString, Const *partitionKeyValue, int colocationId)
{
	if (StatTenantsTrack == STAT_TENANTS_TRACK_NONE || partitionKeyValue == NULL)
	{
		return queryString;
	}

	char *partitionKeyValueString = DatumToString(partitionKeyValue->constvalue,
												  partitionKeyValue->consttype);

	char *commentCharsEscaped = EscapeCommentChars(partitionKeyValueString);
	StringInfo escapedSourceName = makeStringInfo();

	escape_json(escapedSourceName, commentCharsEscaped);

	StringInfo newQuery = makeStringInfo();
	appendStringInfo(newQuery, ATTRIBUTE_STRING_FORMAT, escapedSourceName->data,
					 colocationId);

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
	const TenantStats *leftTenant = (const TenantStats *) leftElement;
	const TenantStats *rightTenant = (const TenantStats *) rightElement;

	if (leftTenant->score > rightTenant->score)
	{
		return -1;
	}
	else if (leftTenant->score < rightTenant->score)
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
		AttributeToTenant[0] == '\0')
	{
		return;
	}

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

	int currentTenantIndex = FindTenantStats(monitor);

	if (currentTenantIndex != -1)
	{
		TenantStats *tenantStats = &monitor->tenants[currentTenantIndex];
		LWLockAcquire(&tenantStats->lock, LW_EXCLUSIVE);

		UpdatePeriodsIfNecessary(tenantStats, queryTime);
		ReduceScoreIfNecessary(tenantStats, queryTime);
		RecordTenantStats(tenantStats);

		LWLockRelease(&tenantStats->lock);
	}
	else
	{
		LWLockRelease(&monitor->lock);

		LWLockAcquire(&monitor->lock, LW_EXCLUSIVE);
		currentTenantIndex = FindTenantStats(monitor);

		if (currentTenantIndex == -1)
		{
			currentTenantIndex = CreateTenantStats(monitor, queryTime);
		}

		LWLockRelease(&monitor->lock);

		LWLockAcquire(&monitor->lock, LW_SHARED);
		currentTenantIndex = FindTenantStats(monitor);
		if (currentTenantIndex != -1)
		{
			TenantStats *tenantStats = &monitor->tenants[currentTenantIndex];
			LWLockAcquire(&tenantStats->lock, LW_EXCLUSIVE);

			UpdatePeriodsIfNecessary(tenantStats, queryTime);
			ReduceScoreIfNecessary(tenantStats, queryTime);
			RecordTenantStats(tenantStats);

			LWLockRelease(&tenantStats->lock);
		}
	}
	LWLockRelease(&monitor->lock);

	strcpy_s(AttributeToTenant, sizeof(AttributeToTenant), "");
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
	}

	/*
	 * If the last query is more than two periods ago, we clean the last period counts too.
	 */
	if (TimestampDifferenceExceeds(tenantStats->lastQueryTime, periodStart,
								   periodInMicroSeconds))
	{
		tenantStats->writesInLastPeriod = 0;

		tenantStats->readsInLastPeriod = 0;
	}

	tenantStats->lastQueryTime = queryTime;
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
	if (monitor->tenantCount >= StatTenantsLimit * 3)
	{
		for (int tenantIndex = 0; tenantIndex < monitor->tenantCount; tenantIndex++)
		{
			ReduceScoreIfNecessary(&monitor->tenants[tenantIndex], queryTime);
		}
		SafeQsort(monitor->tenants, monitor->tenantCount, sizeof(TenantStats),
				  CompareTenantScore);
		monitor->tenantCount = StatTenantsLimit * 2;
	}
}


/*
 * RecordTenantStats records the query statistics for the tenant.
 */
static void
RecordTenantStats(TenantStats *tenantStats)
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
}


/*
 * CreateMultiTenantMonitor creates the data structure for multi tenant monitor.
 */
static void
CreateMultiTenantMonitor()
{
	MultiTenantMonitor *monitor = CreateSharedMemoryForMultiTenantMonitor();
	monitor->tenantCount = 0;
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
	CreateMultiTenantMonitor();

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
static int
CreateTenantStats(MultiTenantMonitor *monitor, TimestampTz queryTime)
{
	/*
	 * If the tenant count reached 3 * StatTenantsLimit, we evict the tenants
	 * with the lowest score.
	 */
	EvictTenantsIfNecessary(queryTime);

	int tenantIndex = monitor->tenantCount;

	memset(&monitor->tenants[tenantIndex], 0, sizeof(monitor->tenants[tenantIndex]));

	strcpy_s(monitor->tenants[tenantIndex].tenantAttribute,
			 sizeof(monitor->tenants[tenantIndex].tenantAttribute), AttributeToTenant);
	monitor->tenants[tenantIndex].colocationGroupId = AttributeToColocationGroupId;

	monitor->tenants[tenantIndex].namedLockTranche.trancheId = LWLockNewTrancheId();
	monitor->tenants[tenantIndex].namedLockTranche.trancheName = TenantTrancheName;

	LWLockRegisterTranche(monitor->tenants[tenantIndex].namedLockTranche.trancheId,
						  monitor->tenants[tenantIndex].namedLockTranche.trancheName);
	LWLockInitialize(&monitor->tenants[tenantIndex].lock,
					 monitor->tenants[tenantIndex].namedLockTranche.trancheId);

	monitor->tenantCount++;

	return tenantIndex;
}


/*
 * FindTenantStats finds the index for the current tenant's statistics.
 */
static int
FindTenantStats(MultiTenantMonitor *monitor)
{
	for (int i = 0; i < monitor->tenantCount; i++)
	{
		TenantStats *tenantStats = &monitor->tenants[i];
		if (strcmp(tenantStats->tenantAttribute, AttributeToTenant) == 0 &&
			tenantStats->colocationGroupId == AttributeToColocationGroupId)
		{
			return i;
		}
	}

	return -1;
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
