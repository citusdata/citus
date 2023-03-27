/*-------------------------------------------------------------------------
 *
 * attribute.h
 *	  Routines related to the multi tenant monitor.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_ATTRIBUTE_H
#define CITUS_ATTRIBUTE_H

#include "executor/execdesc.h"
#include "executor/executor.h"
#include "storage/lwlock.h"

#define MAX_TENANT_ATTRIBUTE_LENGTH 100

/*
 * TenantStats is the struct that keeps statistics about one tenant.
 */
typedef struct TenantStats
{
	/*
	 * The attribute value, e.g distribution column, and colocation group id
	 * of the tenant.
	 */
	char tenantAttribute[MAX_TENANT_ATTRIBUTE_LENGTH];
	int colocationGroupId;

	/*
	 * Number of SELECT queries this tenant ran in this and last periods.
	 */
	int readsInLastPeriod;
	int readsInThisPeriod;

	/*
	 * Number of INSERT, UPDATE, and DELETE queries this tenant ran in this and last periods.
	 */
	int writesInLastPeriod;
	int writesInThisPeriod;

	/*
	 * The latest time this tenant ran a query. This value is used to update the score later.
	 */
	time_t lastQueryTime;

	/*
	 * The tenant monitoring score of this tenant. This value is increased by ONE_QUERY_SCORE at every query
	 * and halved after every period.
	 */
	long long score;

	/*
	 * The latest time the score of this tenant is halved. This value is used to correctly calculate the reduction later.
	 */
	time_t lastScoreReduction;

	/*
	 * Locks needed to update this tenant's statistics.
	 */
	NamedLWLockTranche namedLockTranche;
	LWLock lock;
} TenantStats;

/*
 * MultiTenantMonitor is the struct for keeping the statistics
 * of the tenants
 */
typedef struct MultiTenantMonitor
{
	/*
	 * Lock mechanism for the monitor.
	 * Each tenant update acquires the lock in shared mode and
	 * the tenant number reduction and monitor view acquires in exclusive mode.
	 */
	NamedLWLockTranche namedLockTranche;
	LWLock lock;

	/*
	 * tenantCount is the number of items in the tenants array.
	 * The total length of tenants array is set up at CreateSharedMemoryForMultiTenantMonitor
	 * and is 3 * citus.stats_tenants_limit
	 */
	int tenantCount;
	TenantStats tenants[FLEXIBLE_ARRAY_MEMBER];
} MultiTenantMonitor;


extern void CitusAttributeToEnd(QueryDesc *queryDesc);
extern void AttributeQueryIfAnnotated(const char *queryString, CmdType commandType);
extern char * AnnotateQuery(char *queryString, char *partitionColumn, int colocationId);
extern void InitializeMultiTenantMonitorSMHandleManagement(void);

extern ExecutorEnd_hook_type prev_ExecutorEnd;

extern int MultiTenantMonitoringLogLevel;
extern int CitusStatsTenantsPeriod;
extern int CitusStatsTenantsLimit;

#endif /*CITUS_ATTRIBUTE_H */
