/*-------------------------------------------------------------------------
 *
 * citus_stat_tenants.h
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
#include "utils/datetime.h"
#include "utils/hsearch.h"

#include "distributed/hash_helpers.h"

#define MAX_TENANT_ATTRIBUTE_LENGTH 100

/*
 * Hashtable key that defines the identity of a hashtable entry.
 * The key is the attribute value, e.g distribution column and the colocation group id of the tenant.
 */
typedef struct TenantStatsHashKey
{
	char tenantAttribute[MAX_TENANT_ATTRIBUTE_LENGTH];
	int colocationGroupId;
} TenantStatsHashKey;
assert_valid_hash_key2(TenantStatsHashKey, tenantAttribute, colocationGroupId);

/*
 * TenantStats is the struct that keeps statistics about one tenant.
 */
typedef struct TenantStats
{
	TenantStatsHashKey key;   /* hash key of entry - MUST BE FIRST */


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
	 * CPU time usage of this tenant in this and last periods.
	 */
	double cpuUsageInLastPeriod;
	double cpuUsageInThisPeriod;

	/*
	 * The latest time this tenant ran a query. This value is used to update the score later.
	 */
	TimestampTz lastQueryTime;

	/*
	 * The tenant monitoring score of this tenant. This value is increased by ONE_QUERY_SCORE at every query
	 * and halved after every period. This custom scoring mechanism is used to rank the tenants based on
	 * the recency and frequency of their activity. The score is used to rank the tenants and decide which
	 * tenants should be removed from the monitor.
	 */
	long long score;

	/*
	 * The latest time the score of this tenant is halved. This value is used to correctly calculate the reduction later.
	 */
	TimestampTz lastScoreReduction;

	/*
	 * Locks needed to update this tenant's statistics.
	 */
	slock_t lock;
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
	 * The max length of tenants hashtable is 3 * citus.stat_tenants_limit
	 */
	HTAB *tenants;
} MultiTenantMonitor;

typedef enum
{
	STAT_TENANTS_TRACK_NONE = 0,
	STAT_TENANTS_TRACK_ALL = 1
} StatTenantsTrackType;

extern void CitusAttributeToEnd(QueryDesc *queryDesc);
extern void AttributeQueryIfAnnotated(const char *queryString, CmdType commandType);
extern char * AnnotateQuery(char *queryString, Const *partitionKeyValue,
							int colocationId);
extern void InitializeMultiTenantMonitorSMHandleManagement(void);
extern void AttributeTask(char *tenantId, int colocationGroupId, CmdType commandType);

extern ExecutorEnd_hook_type prev_ExecutorEnd;

extern int StatTenantsLogLevel;
extern int StatTenantsPeriod;
extern int StatTenantsLimit;
extern int StatTenantsTrack;
extern double StatTenantsSampleRateForNewTenants;

#endif /*CITUS_ATTRIBUTE_H */
