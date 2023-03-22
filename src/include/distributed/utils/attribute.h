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

typedef struct TenantStats
{
	char tenantAttribute[100];

	int colocationGroupId;

	int readCount;
	double totalReadTime;
	int readsInLastPeriod;
	int readsInThisPeriod;

	int writeCount;
	double totalWriteTime;
	int writesInLastPeriod;
	int writesInThisPeriod;

	time_t lastQueryTime;

	long long score;
	time_t lastScoreReduction;

	NamedLWLockTranche namedLockTranche;
	LWLock lock;
} TenantStats;

typedef struct MultiTenantMonitor
{
	time_t periodStart;

	NamedLWLockTranche namedLockTranche;
	LWLock lock;

	int tenantCount;
	TenantStats tenants[FLEXIBLE_ARRAY_MEMBER];
} MultiTenantMonitor;


extern void CitusAttributeToEnd(QueryDesc *queryDesc);
extern void AttributeQueryIfAnnotated(const char *queryString, CmdType commandType);
extern char * AnnotateQuery(char *queryString, char *partitionColumn, int colocationId);
extern void InitializeMultiTenantMonitorSMHandleManagement(void);
extern void AttributeTask(char *tenantId, int colocationGroupId, CmdType commandType);

extern ExecutorEnd_hook_type prev_ExecutorEnd;

extern int MultiTenantMonitoringLogLevel;
extern int CitusStatsTenantsPeriod;
extern int CitusStatsTenantsLimit;

#endif /*CITUS_ATTRIBUTE_H */
