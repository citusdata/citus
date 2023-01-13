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
char *attributeToTenant = NULL;
CmdType attributeCommandType = CMD_UNKNOWN;
int colocationGroupId = -1;
clock_t attributeToTenantStart = { 0 };

const char *SharedMemoryNameForMultiTenantMonitorHandleManagement =
	"Shared memory handle for multi tenant monitor";

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void UpdatePeriodsIfNecessary(MultiTenantMonitor *monitor,TenantStats *tenantStats);
static void ReduceScoreIfNecessary(MultiTenantMonitor *monitor, TenantStats *tenantStats, time_t updateTime);
static void CreateMultiTenantMonitor(void);
static dsm_handle CreateSharedMemoryForMultiTenantMonitor(void);
static void StoreMultiTenantMonitorSMHandle(dsm_handle dsmHandle);
static MultiTenantMonitor * GetMultiTenantMonitor(void);
static dsm_handle GetMultiTenantMonitorDSMHandle(void);
static void DetachSegment(void);
static void MultiTenantMonitorSMInit(void);
static dsm_handle CreateTenantStats(MultiTenantMonitor *monitor);
static dsm_handle CreateSharedMemoryForTenantStats(void);
static TenantStats * GetTenantStatsFromDSMHandle(dsm_handle dsmHandle);
static dsm_handle FindTenantStats(MultiTenantMonitor *monitor);

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
	//CheckCitusVersion(ERROR);

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
	
	monitor->periodStart = monitor->periodStart + ((monitoringTime-monitor->periodStart)/CitusStatsTenantsPeriod)*CitusStatsTenantsPeriod;

	int numberOfRowsToReturn = 0;
	if (returnAllTenants)
	{
		numberOfRowsToReturn = monitor->tenantCount;
	}
	else
	{
		numberOfRowsToReturn = min (monitor->tenantCount, CitusStatsTenantsLimit);
	}

	for (int i=0; i<numberOfRowsToReturn; i++)
	{
		memset(values, 0, sizeof(values));
		memset(isNulls, false, sizeof(isNulls));

		TenantStats *tenantStats = GetTenantStatsFromDSMHandle(monitor->tenants[i]);

		UpdatePeriodsIfNecessary(monitor, tenantStats);
		ReduceScoreIfNecessary(monitor, tenantStats, monitoringTime);

		values[0] = Int32GetDatum(tenantStats->colocationGroupId);
		values[1] = PointerGetDatum(cstring_to_text(tenantStats->tenantAttribute));
		values[2] = Int32GetDatum(tenantStats->selectsInThisPeriod);
		values[3] = Int32GetDatum(tenantStats->selectsInLastPeriod);
		values[4] = Int32GetDatum(tenantStats->selectsInThisPeriod + tenantStats->insertsInThisPeriod);
		values[5] = Int32GetDatum(tenantStats->selectsInLastPeriod + tenantStats->insertsInLastPeriod);
		values[6] = Int64GetDatum(monitor->scores[tenantStats->rank]);
		

		tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
	}

	PG_RETURN_VOID();
}


/*
 * AttributeQueryIfAnnotated assigns the attributes of tenant if the query is annotated.
 */
void
AttributeQueryIfAnnotated(const char *query_string, CmdType commandType)
{
	attributeToTenant = NULL;

	attributeCommandType = commandType;

	if (query_string == NULL)
	{
		return;
	}

	if (strncmp(ATTRIBUTE_PREFIX, query_string, strlen(ATTRIBUTE_PREFIX)) == 0)
	{
		/* TODO create a function to safely parse the tenant identifier from the query comment */
		/* query is attributed to a tenant */
		char *tenantId = (char*)query_string + strlen(ATTRIBUTE_PREFIX);
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
		while(*tenantEnd != ',')
		{
			colocationGroupId *= 10;
			colocationGroupId += *tenantEnd - '0';
			tenantEnd--;
		}

		/* hack to get a clean copy of the tenant id string */
		char tenantEndTmp = *tenantEnd;
		*tenantEnd = '\0';
		tenantId = pstrdup(tenantId);
		*tenantEnd = tenantEndTmp;

		if (MultiTenantMonitoringLogLevel != CITUS_LOG_LEVEL_OFF)
		{
			ereport(NOTICE, (errmsg("attributing query to tenant: %s", quote_literal_cstr(tenantId))));
		}

		attributeToTenant=(char *)malloc(strlen(tenantId));
		strcpy(attributeToTenant, tenantId);
	}
	else
	{
		Assert(attributeToTenant == NULL);
	}

	//DetachSegment();

	attributeToTenantStart = clock();
}


/*
 * AnnotateQuery annotates the query with tenant attributes.
 */
char *
AnnotateQuery (char * queryString, char * partitionColumn, int colocationId)
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
	if (attributeToTenant)
	{
		clock_t end = { 0 };
		double cpu_time_used = 0;

		end = clock();
		time_t queryTime = time(0);
		cpu_time_used = ((double) (end - attributeToTenantStart)) / CLOCKS_PER_SEC;

		if (MultiTenantMonitoringLogLevel != CITUS_LOG_LEVEL_OFF)
		{
			ereport(NOTICE, (errmsg("attribute cpu counter (%f) to tenant: %s", cpu_time_used,
									attributeToTenant)));
		}

		if (GetMultiTenantMonitorDSMHandle() == DSM_HANDLE_INVALID)
		{
			CreateMultiTenantMonitor();
		}

		MultiTenantMonitor *monitor = GetMultiTenantMonitor();
		
		monitor->periodStart = monitor->periodStart + ((queryTime-monitor->periodStart)/CitusStatsTenantsPeriod)*CitusStatsTenantsPeriod;

		dsm_handle tenantDSMHandle = FindTenantStats(monitor);

		if (tenantDSMHandle == DSM_HANDLE_INVALID)
		{
			tenantDSMHandle = CreateTenantStats(monitor);
		}
		TenantStats * tenantStats = GetTenantStatsFromDSMHandle(tenantDSMHandle);
		strcpy(tenantStats->tenantAttribute, attributeToTenant);
		tenantStats->colocationGroupId = colocationGroupId;

		UpdatePeriodsIfNecessary(monitor, tenantStats);
		tenantStats->lastQueryTime = queryTime;

		ReduceScoreIfNecessary(monitor, tenantStats, queryTime);

		/*
		 * We do this after the reducing the scores so the scores in this period are not affected by the reduction.
		 */
		monitor->scores[tenantStats->rank] += ONE_QUERY_SCORE;


		/*
		 * After updating the score we might need to change the rank of the tenant in the monitor
		 */
		while(tenantStats->rank != 0 && monitor->scores[tenantStats->rank-1] < monitor->scores[tenantStats->rank])
		{
			// we need to reduce previous tenants score too !!!!!!!!
			TenantStats *previousTenantStats = GetTenantStatsFromDSMHandle(monitor->tenants[tenantStats->rank-1]);
			
			dsm_handle tempTenant = monitor->tenants[tenantStats->rank];
			monitor->tenants[tenantStats->rank] = monitor->tenants[previousTenantStats->rank];
			monitor->tenants[previousTenantStats->rank] = tempTenant;

			long long tempScore = monitor->scores[tenantStats->rank];
			monitor->scores[tenantStats->rank] = monitor->scores[previousTenantStats->rank];
			monitor->scores[previousTenantStats->rank] = tempScore;

			previousTenantStats->rank++;
			tenantStats->rank--;
		}

		/*
		 * We keep up to CitusStatsTenantsLimit * 3 tenants instead of CitusStatsTenantsLimit,
		 * so we don't lose data immediately after a tenant is out of top CitusStatsTenantsLimit
		 * 
		 * Every time tenant count hits CitusStatsTenantsLimit * 3, we reduce it back to CitusStatsTenantsLimit * 2.
		 */
		if (monitor->tenantCount >= CitusStatsTenantsLimit * 3)
		{
			monitor->tenantCount = CitusStatsTenantsLimit * 2;
		}

		if (attributeCommandType == CMD_SELECT)
		{
			tenantStats->selectCount++;
			tenantStats->selectsInThisPeriod++;
			tenantStats->totalSelectTime+=cpu_time_used;
		}
		else if (attributeCommandType == CMD_INSERT)
		{
			tenantStats->insertCount++;
			tenantStats->insertsInThisPeriod++;
			tenantStats->totalInsertTime+=cpu_time_used;
		}

		if (MultiTenantMonitoringLogLevel != CITUS_LOG_LEVEL_OFF)
		{
			ereport(NOTICE, (errmsg("total select count = %d, total CPU time = %f to tenant: %s", tenantStats->selectCount, tenantStats->totalSelectTime,
									tenantStats->tenantAttribute)));
		}
	}
	attributeToTenant = NULL;
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
UpdatePeriodsIfNecessary(MultiTenantMonitor *monitor,TenantStats *tenantStats)
{
	/*
	 * If the last query in this tenant was before the start of current period
	 * but there are some query count for this period we move them to the last period.
	 */
	if (tenantStats->lastQueryTime < monitor->periodStart && (tenantStats->insertsInThisPeriod || tenantStats->selectsInThisPeriod))
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
ReduceScoreIfNecessary(MultiTenantMonitor *monitor, TenantStats *tenantStats, time_t updateTime)
{
	/*
	 * With each query we increase the score of tenant by ONE_QUERY_SCORE.
	 * After one period we halve the scores.
	 * 
	 * Here we calculate how many periods passed after the last time we did score reduction
	 * If the latest score reduction was in this period this number should be 0,
	 * if it was in the last period this number should be 1 and so on.
	 */
	int periodCountAfterLastScoreReduction = (monitor->periodStart - tenantStats->lastScoreReduction + CitusStatsTenantsPeriod -1) / CitusStatsTenantsPeriod;

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
		monitor->scores[tenantStats->rank] >>= periodCountAfterLastScoreReduction;
		tenantStats->lastScoreReduction = updateTime;
	}
}


/*
 * CreateMultiTenantMonitor creates the data structure for multi tenant monitor.
 */
static void
CreateMultiTenantMonitor()
{
	dsm_handle dsmHandle = CreateSharedMemoryForMultiTenantMonitor();
	StoreMultiTenantMonitorSMHandle(dsmHandle);
	MultiTenantMonitor * monitor = GetMultiTenantMonitor();
	monitor->tenantCount = 0;
	monitor->periodStart = time(0);
}


/*
 * CreateSharedMemoryForMultiTenantMonitor creates a dynamic shared memory segment for multi tenant monitor.
 */
static dsm_handle
CreateSharedMemoryForMultiTenantMonitor()
{
	struct dsm_segment *dsmSegment = dsm_create(sizeof(MultiTenantMonitor), DSM_CREATE_NULL_IF_MAXSEGMENTS);
	dsm_pin_segment(dsmSegment);
	dsm_pin_mapping(dsmSegment); // don't know why we do both !!!!!!!!!!!!!!!!!
	return dsm_segment_handle(dsmSegment);
}

/*
 * StoreMultiTenantMonitorSMHandle stores the dsm (dynamic shared memory) handle for multi tenant monitor
 * in a non-dynamic shared memory location, so we don't lose it.
 */
static void
StoreMultiTenantMonitorSMHandle(dsm_handle dsmHandle)
{
	bool found = false;
	MultiTenantMonitorSMData *smData = ShmemInitStruct(SharedMemoryNameForMultiTenantMonitorHandleManagement,
													  sizeof(MultiTenantMonitorSMData),
													  &found);

	smData->dsmHandle = dsmHandle;
}


/*
 * GetMultiTenantMonitor returns the data structure for multi tenant monitor.
 */
static MultiTenantMonitor *
GetMultiTenantMonitor()
{
	dsm_handle dsmHandle = GetMultiTenantMonitorDSMHandle();
	if (dsmHandle == DSM_HANDLE_INVALID)
	{
		return NULL;
	}
	dsm_segment *dsmSegment = dsm_find_mapping(dsmHandle);
	if (dsmSegment == NULL)
	{
		dsmSegment = dsm_attach(dsmHandle);
	}
	MultiTenantMonitor *monitor = (MultiTenantMonitor *) dsm_segment_address(dsmSegment);
	dsm_pin_mapping(dsmSegment);
	return monitor;
}

/*
 * GetMultiTenantMonitorDSMHandle fetches the dsm (dynamic shared memory) handle for multi tenant monitor.
 */
static dsm_handle
GetMultiTenantMonitorDSMHandle()
{
	bool found = false;
	MultiTenantMonitorSMData *smData = ShmemInitStruct(SharedMemoryNameForMultiTenantMonitorHandleManagement,
													  sizeof(MultiTenantMonitorSMData),
													  &found);

	if (!found)
	{
		elog(WARNING, "dsm handle not found");
		return DSM_HANDLE_INVALID;
	}

	dsm_handle dsmHandle = smData->dsmHandle;

	return dsmHandle;
}


static void
DetachSegment()
{
	dsm_handle dsmHandle = GetMultiTenantMonitorDSMHandle();
	dsm_segment *dsmSegment = dsm_find_mapping(dsmHandle);
	if (dsmSegment != NULL)
	{
		dsm_detach(dsmSegment);
	}
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
	bool alreadyInitialized = false;
	MultiTenantMonitorSMData *smData = ShmemInitStruct(SharedMemoryNameForMultiTenantMonitorHandleManagement,
													  sizeof(MultiTenantMonitorSMData),
													  &alreadyInitialized);
	if (!alreadyInitialized)
	{
		smData->dsmHandle = DSM_HANDLE_INVALID;
	}

	if (prev_shmem_startup_hook != NULL)
	{
		prev_shmem_startup_hook();
	}
}


/*
 * CreateTenantStats creates the data structure for a tenant's statistics.
 */
static dsm_handle
CreateTenantStats(MultiTenantMonitor *monitor)
{
	dsm_handle dsmHandle = CreateSharedMemoryForTenantStats();
	monitor->tenants[monitor->tenantCount] = dsmHandle;
	TenantStats *tenantStats = GetTenantStatsFromDSMHandle(dsmHandle);
	tenantStats->rank = monitor->tenantCount;
	monitor->tenantCount++;
	return dsmHandle;
}


/*
 * CreateSharedMemoryForTenantStats creates a dynamic shared memory segment for a tenant's statistics.
 */
static dsm_handle
CreateSharedMemoryForTenantStats()
{
	struct dsm_segment *dsmSegment = dsm_create(sizeof(TenantStats), DSM_CREATE_NULL_IF_MAXSEGMENTS);
	dsm_pin_segment(dsmSegment);
	dsm_pin_mapping(dsmSegment); // don't know why we do both !!!!!!!!!!!!!!!!!
	return dsm_segment_handle(dsmSegment);
}


/*
 * GetTenantStatsFromDSMHandle returns the data structure for a tenant's statistics with the dsm (dynamic shared memory) handle.
 */
static TenantStats *
GetTenantStatsFromDSMHandle(dsm_handle dsmHandle)
{
	dsm_segment *dsmSegment = dsm_find_mapping(dsmHandle);
	if (dsmSegment == NULL)
	{
		dsmSegment = dsm_attach(dsmHandle);
	}
	TenantStats *stats = (TenantStats *) dsm_segment_address(dsmSegment);
	dsm_pin_mapping(dsmSegment);

	return stats;
}


/*
 * FindTenantStats finds the dsm (dynamic shared memory) handle for the current tenant's statistics.
 */
static dsm_handle
FindTenantStats(MultiTenantMonitor *monitor)
{
	for(int i=0; i<monitor->tenantCount; i++)
	{
		TenantStats * tenantStats = GetTenantStatsFromDSMHandle(monitor->tenants[i]);
		if (strcmp(tenantStats->tenantAttribute, attributeToTenant) == 0 && tenantStats->colocationGroupId == colocationGroupId)
		{
			return monitor->tenants[i];
		}
	}

	return DSM_HANDLE_INVALID;
}

