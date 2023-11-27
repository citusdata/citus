/*-------------------------------------------------------------------------
 *
 * citus_stat_tenants.c
 *
 * This file contains functions to test citus_stat_tenants.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"

#include "sys/time.h"

#include "distributed/utils/citus_stat_tenants.h"

PG_FUNCTION_INFO_V1(sleep_until_next_period);

/*
 * sleep_until_next_period sleeps until the next monitoring period starts.
 */
Datum
sleep_until_next_period(PG_FUNCTION_ARGS)
{
	struct timeval currentTime;
	gettimeofday(&currentTime, NULL);

	long int nextPeriodStart = currentTime.tv_sec -
							   (currentTime.tv_sec % StatTenantsPeriod) +
							   StatTenantsPeriod;

	long int sleepTime = (nextPeriodStart - currentTime.tv_sec) * 1000000 -
						 currentTime.tv_usec + 100000;
	pg_usleep(sleepTime);

	PG_RETURN_VOID();
}
