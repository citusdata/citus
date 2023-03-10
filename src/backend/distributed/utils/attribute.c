/*-------------------------------------------------------------------------
 *
 * attribute.c
 *	  Routines related to the multi tenant monitor.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "unistd.h"

#include "distributed/log_utils.h"
#include "distributed/listutils.h"
#include "distributed/tuplestore.h"
#include "executor/execdesc.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"

#include "distributed/utils/attribute.h"

#include <time.h>

static void AttributeMetricsIfApplicable(void);

ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

#define ATTRIBUTE_PREFIX "/* attributeTo: "
#define ATTRIBUTE_STRING_FORMAT "/* attributeTo: %s,%d */"
#define CITUS_STATS_TENANTS_COLUMNS 7
#define ONE_QUERY_SCORE 1000000000

/* TODO maybe needs to be a stack */
char attributeToTenant[100] = "";
CmdType attributeCommandType = CMD_UNKNOWN;
int colocationGroupId = -1;
clock_t attributeToTenantStart = { 0 };

const char *SharedMemoryNameForMultiTenantMonitor =
	"Shared memory for multi tenant monitor";

char *tenantTrancheName = "Tenant Tranche";
char *monitorTrancheName = "Multi Tenant Monitor Tranche";

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void UpdatePeriodsIfNecessary(MultiTenantMonitor *monitor,
									 TenantStats *tenantStats);
static void ReduceScoreIfNecessary(MultiTenantMonitor *monitor, TenantStats *tenantStats,
								   time_t updateTime);
static void CreateMultiTenantMonitor(void);
static MultiTenantMonitor * CreateSharedMemoryForMultiTenantMonitor(void);
static MultiTenantMonitor * GetMultiTenantMonitor(void);
static void MultiTenantMonitorSMInit(void);
static int CreateTenantStats(MultiTenantMonitor *monitor);
static int FindTenantStats(MultiTenantMonitor *monitor);
static size_t MultiTenantMonitorshmemSize(void);

int MultiTenantMonitoringLogLevel = CITUS_LOG_LEVEL_OFF;
int CitusStatsTenantsPeriod = (time_t) 60;
int CitusStatsTenantsLimit = 10;


PG_FUNCTION_INFO_V1(citus_stats_tenants);


/*
 * citus_stats_tenants finds, updates and returns the statistics for tenants.
 */
Datum
citus_stats_tenants(PG_FUNCTION_ARGS)
{
	/*CheckCitusVersion(ERROR); */

	/*
	 * We keep more than CitusStatsTenantsLimit tenants in our monitor.
	 * We do this to not lose data if a tenant falls out of top CitusStatsTenantsLimit in case they need to return soon.
	 * Normally we return CitusStatsTenantsLimit tenants but if returnAllTenants is true we return all of them.
	 */
	bool returnAllTenants = PG_GETARG_BOOL(0);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);
	time_t monitoringTime = time(0);

	Datum values[CITUS_STATS_TENANTS_COLUMNS];
	bool isNulls[CITUS_STATS_TENANTS_COLUMNS];

	MultiTenantMonitor *monitor = GetMultiTenantMonitor();

	if (monitor == NULL)
	{
		PG_RETURN_VOID();
	}

	LWLockAcquire(&monitor->lock, LW_EXCLUSIVE);

	monitor->periodStart = monitor->periodStart +
						   ((monitoringTime - monitor->periodStart) /
							CitusStatsTenantsPeriod) *
						   CitusStatsTenantsPeriod;

	int numberOfRowsToReturn = 0;
	if (returnAllTenants)
	{
		numberOfRowsToReturn = monitor->tenantCount;
	}
	else
	{
		numberOfRowsToReturn = Min(monitor->tenantCount, CitusStatsTenantsLimit);
	}

	for (int i = 0; i < numberOfRowsToReturn; i++)
	{
		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		TenantStats *tenantStats = &monitor->tenants[i];

		UpdatePeriodsIfNecessary(monitor, tenantStats);
		ReduceScoreIfNecessary(monitor, tenantStats, monitoringTime);

		values[0] = Int32GetDatum(tenantStats->colocationGroupId);
		values[1] = PointerGetDatum(cstring_to_text(tenantStats->tenantAttribute));
		values[2] = Int32GetDatum(tenantStats->selectsInThisPeriod);
		values[3] = Int32GetDatum(tenantStats->selectsInLastPeriod);
		values[4] = Int32GetDatum(tenantStats->selectsInThisPeriod +
								  tenantStats->insertsInThisPeriod);
		values[5] = Int32GetDatum(tenantStats->selectsInLastPeriod +
								  tenantStats->insertsInLastPeriod);
		values[6] = Int64GetDatum(tenantStats->score);

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	LWLockRelease(&monitor->lock);

	PG_RETURN_VOID();
}


/*
 * AttributeQueryIfAnnotated assigns the attributes of tenant if the query is annotated.
 */
void
AttributeQueryIfAnnotated(const char *query_string, CmdType commandType)
{
/*	attributeToTenant = NULL; */

	attributeCommandType = commandType;

	if (query_string == NULL)
	{
		return;
	}

	if (strncmp(ATTRIBUTE_PREFIX, query_string, strlen(ATTRIBUTE_PREFIX)) == 0)
	{
		/* TODO create a function to safely parse the tenant identifier from the query comment */
		/* query is attributed to a tenant */
		char *tenantId = (char *) query_string + strlen(ATTRIBUTE_PREFIX);
		char *tenantEnd = tenantId;
		while (true && tenantEnd[0] != '\0')
		{
			if (tenantEnd[0] == ' ' && tenantEnd[1] == '*' && tenantEnd[2] == '/')
			{
				break;
			}

			tenantEnd++;
		}
		tenantEnd--;

		colocationGroupId = 0;
		while (*tenantEnd != ',')
		{
			colocationGroupId *= 10;
			colocationGroupId += *tenantEnd - '0';
			tenantEnd--;
		}

		int t = colocationGroupId;
		colocationGroupId = 0;
		while (t)
		{
			colocationGroupId *= 10;
			colocationGroupId += t % 10;
			t /= 10;
		}

		/* hack to get a clean copy of the tenant id string */
		char tenantEndTmp = *tenantEnd;
		*tenantEnd = '\0';
		tenantId = pstrdup(tenantId);
		*tenantEnd = tenantEndTmp;

		if (MultiTenantMonitoringLogLevel != CITUS_LOG_LEVEL_OFF)
		{
			ereport(NOTICE, (errmsg("attributing query to tenant: %s",
									quote_literal_cstr(tenantId))));
		}

		/*	attributeToTenant = (char *) malloc(strlen(tenantId)); */
		strcpy(attributeToTenant, tenantId);
	}
	else
	{
		/*Assert(attributeToTenant == NULL); */
	}

	/*DetachSegment(); */

	attributeToTenantStart = clock();
}


/*
 * AnnotateQuery annotates the query with tenant attributes.
 */
char *
AnnotateQuery(char *queryString, char *partitionColumn, int colocationId)
{
	if (partitionColumn == NULL)
	{
		return queryString;
	}
	StringInfo newQuery = makeStringInfo();
	appendStringInfo(newQuery, ATTRIBUTE_STRING_FORMAT, partitionColumn, colocationId);

	appendStringInfoString(newQuery, queryString);

	return newQuery->data;
}


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
 * AttributeMetricsIfApplicable updates the metrics for current tenant's statistics
 */
static void
AttributeMetricsIfApplicable()
{
	if (strcmp(attributeToTenant, "") != 0)
	{
		clock_t end = { 0 };

		end = clock();
		time_t queryTime = time(0);
		double cpu_time_used = ((double) (end - attributeToTenantStart)) / CLOCKS_PER_SEC;

		if (MultiTenantMonitoringLogLevel != CITUS_LOG_LEVEL_OFF)
		{
			ereport(NOTICE, (errmsg("attribute cpu counter (%f) to tenant: %s",
									cpu_time_used, attributeToTenant)));
		}

		MultiTenantMonitor *monitor = GetMultiTenantMonitor();

		LWLockAcquire(&monitor->lock, LW_SHARED);

		monitor->periodStart = monitor->periodStart +
							   ((queryTime - monitor->periodStart) /
								CitusStatsTenantsPeriod) *
							   CitusStatsTenantsPeriod;

		int tenantIndex = FindTenantStats(monitor);

		if (tenantIndex == -1)
		{
			tenantIndex = CreateTenantStats(monitor);
		}
		TenantStats *tenantStats = &monitor->tenants[tenantIndex];

		LWLockAcquire(&tenantStats->lock, LW_EXCLUSIVE);

		UpdatePeriodsIfNecessary(monitor, tenantStats);
		tenantStats->lastQueryTime = queryTime;

		ReduceScoreIfNecessary(monitor, tenantStats, queryTime);

		/*
		 * We do this after the reducing the scores so the scores in this period are not affected by the reduction.
		 */
		tenantStats->score += ONE_QUERY_SCORE;


		/*
		 * After updating the score we might need to change the rank of the tenant in the monitor
		 */
		while (tenantIndex != 0 &&
			   monitor->tenants[tenantIndex - 1].score <
			   monitor->tenants[tenantIndex].score)
		{
			LWLockAcquire(&monitor->tenants[tenantIndex - 1].lock, LW_EXCLUSIVE);

			ReduceScoreIfNecessary(monitor, &monitor->tenants[tenantIndex - 1],
								   queryTime);

			TenantStats tempTenant = monitor->tenants[tenantIndex];
			monitor->tenants[tenantIndex] = monitor->tenants[tenantIndex - 1];
			monitor->tenants[tenantIndex - 1] = tempTenant;

			LWLockRelease(&monitor->tenants[tenantIndex].lock);

			tenantIndex--;
		}
		tenantStats = &monitor->tenants[tenantIndex];

		if (attributeCommandType == CMD_SELECT)
		{
			tenantStats->selectCount++;
			tenantStats->selectsInThisPeriod++;
			tenantStats->totalSelectTime += cpu_time_used;
		}
		else if (attributeCommandType == CMD_INSERT)
		{
			tenantStats->insertCount++;
			tenantStats->insertsInThisPeriod++;
			tenantStats->totalInsertTime += cpu_time_used;
		}

		LWLockRelease(&monitor->lock);
		LWLockRelease(&tenantStats->lock);

		/*
		 * We keep up to CitusStatsTenantsLimit * 3 tenants instead of CitusStatsTenantsLimit,
		 * so we don't lose data immediately after a tenant is out of top CitusStatsTenantsLimit
		 *
		 * Every time tenant count hits CitusStatsTenantsLimit * 3, we reduce it back to CitusStatsTenantsLimit * 2.
		 */
		if (monitor->tenantCount >= CitusStatsTenantsLimit * 3)
		{
			LWLockAcquire(&monitor->lock, LW_EXCLUSIVE);
			monitor->tenantCount = CitusStatsTenantsLimit * 2;
			LWLockRelease(&monitor->lock);
		}

		if (MultiTenantMonitoringLogLevel != CITUS_LOG_LEVEL_OFF)
		{
			ereport(NOTICE, (errmsg("total select count = %d, total CPU time = %f "
									"to tenant: %s",
									tenantStats->selectCount,
									tenantStats->totalSelectTime,
									tenantStats->tenantAttribute)));
		}
	}

	/*attributeToTenant = NULL; */
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
UpdatePeriodsIfNecessary(MultiTenantMonitor *monitor, TenantStats *tenantStats)
{
	/*
	 * If the last query in this tenant was before the start of current period
	 * but there are some query count for this period we move them to the last period.
	 */
	if (tenantStats->lastQueryTime < monitor->periodStart &&
		(tenantStats->insertsInThisPeriod || tenantStats->selectsInThisPeriod))
	{
		tenantStats->insertsInLastPeriod = tenantStats->insertsInThisPeriod;
		tenantStats->insertsInThisPeriod = 0;

		tenantStats->selectsInLastPeriod = tenantStats->selectsInThisPeriod;
		tenantStats->selectsInThisPeriod = 0;
	}

	/*
	 * If the last query is more than two periods ago, we clean the last period counts too.
	 */
	if (tenantStats->lastQueryTime < monitor->periodStart - CitusStatsTenantsPeriod)
	{
		tenantStats->insertsInLastPeriod = 0;

		tenantStats->selectsInLastPeriod = 0;
	}
}


/*
 * ReduceScoreIfNecessary reduces the tenant score only if it is necessary.
 *
 * We halve the tenants' scores after each period. This function checks the number of
 * periods that passed after the lsat score reduction and reduces the score accordingly.
 */
static void
ReduceScoreIfNecessary(MultiTenantMonitor *monitor, TenantStats *tenantStats,
					   time_t updateTime)
{
	/*
	 * With each query we increase the score of tenant by ONE_QUERY_SCORE.
	 * After one period we halve the scores.
	 *
	 * Here we calculate how many periods passed after the last time we did score reduction
	 * If the latest score reduction was in this period this number should be 0,
	 * if it was in the last period this number should be 1 and so on.
	 */
	int periodCountAfterLastScoreReduction = (monitor->periodStart -
											  tenantStats->lastScoreReduction +
											  CitusStatsTenantsPeriod - 1) /
											 CitusStatsTenantsPeriod;

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
		tenantStats->lastScoreReduction = updateTime;
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
	monitor->periodStart = time(0);
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
	monitor->namedLockTranche.trancheName = monitorTrancheName;

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
 *
 * MultiTenantMonitorSMData only holds the dsm (dynamic shared memory) handle for the actual
 * multi tenant monitor.
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
 */
static int
CreateTenantStats(MultiTenantMonitor *monitor)
{
	int tenantIndex = monitor->tenantCount;

	strcpy(monitor->tenants[tenantIndex].tenantAttribute, attributeToTenant);
	monitor->tenants[tenantIndex].colocationGroupId = colocationGroupId;

	monitor->tenants[tenantIndex].namedLockTranche.trancheId = LWLockNewTrancheId();
	monitor->tenants[tenantIndex].namedLockTranche.trancheName = tenantTrancheName;

	LWLockRegisterTranche(monitor->tenants[tenantIndex].namedLockTranche.trancheId,
						  monitor->tenants[tenantIndex].namedLockTranche.trancheName);
	LWLockInitialize(&monitor->tenants[tenantIndex].lock,
					 monitor->tenants[tenantIndex].namedLockTranche.trancheId);

	monitor->tenantCount++;

	return tenantIndex;
}


/*
 * FindTenantStats finds the dsm (dynamic shared memory) handle for the current tenant's statistics.
 */
static int
FindTenantStats(MultiTenantMonitor *monitor)
{
	for (int i = 0; i < monitor->tenantCount; i++)
	{
		TenantStats *tenantStats = &monitor->tenants[i];
		if (strcmp(tenantStats->tenantAttribute, attributeToTenant) == 0 &&
			tenantStats->colocationGroupId == colocationGroupId)
		{
			return i;
		}
	}

	return -1;
}


/*
 * MultiTenantMonitorshmemSize calculates the size of the multi tenant monitor using
 * CitusStatsTenantsLimit parameter.
 */
static size_t
MultiTenantMonitorshmemSize(void)
{
	Size size = sizeof(MultiTenantMonitor);
	size = add_size(size, mul_size(sizeof(TenantStats), CitusStatsTenantsLimit * 3));

	return size;
}
