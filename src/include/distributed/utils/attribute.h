//
// Created by Nils Dijk on 02/12/2022.
//

#ifndef CITUS_ATTRIBUTE_H
#define CITUS_ATTRIBUTE_H

#include "executor/execdesc.h"
#include "executor/executor.h"
#include "storage/lwlock.h"

typedef struct TenantStats
{
	char tenantAttribute[100];

	int colocationGroupId;

	int selectCount;
	double totalSelectTime;
	int selectsInLastPeriod;
	int selectsInThisPeriod;

	int insertCount;
	double totalInsertTime;
	int insertsInLastPeriod;
	int insertsInThisPeriod;

	time_t lastQueryTime;

	long long score;
	time_t lastScoreReduction;
	int rank;

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
extern char * AnnotateQuery(char *queryString, char * partitionColumn, int colocationId);
extern void InitializeMultiTenantMonitorSMHandleManagement(void);

extern ExecutorEnd_hook_type prev_ExecutorEnd;

extern int MultiTenantMonitoringLogLevel;
extern int CitusStatsTenantsPeriod;
extern int CitusStatsTenantsLimit;

#endif //CITUS_ATTRIBUTE_H
