/*-------------------------------------------------------------------------
 *
 * multi_server_executor.h
 *	  Type and function declarations for executing remote jobs from a backend;
 *	  the ensemble of these jobs form the distributed execution plan.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_SERVER_EXECUTOR_H
#define MULTI_SERVER_EXECUTOR_H

#include "distributed/multi_physical_planner.h"
#include "distributed/worker_manager.h"

/* Adaptive executor repartioning related defines */
#define WORKER_CREATE_SCHEMA_QUERY "SELECT worker_create_schema (" UINT64_FORMAT ", %s);"
#define WORKER_REPARTITION_CLEANUP_QUERY "SELECT worker_repartition_cleanup (" \
	UINT64_FORMAT \
	");"


/* Enumeration that represents distributed executor types */
typedef enum
{
	MULTI_EXECUTOR_INVALID_FIRST = 0,
	MULTI_EXECUTOR_ADAPTIVE = 1,
	MULTI_EXECUTOR_NON_PUSHABLE_INSERT_SELECT = 2
} MultiExecutorType;


/* Config variable managed via guc.c */
extern int RemoteTaskCheckInterval;
extern int TaskExecutorType;
extern bool EnableRepartitionJoins;
extern int MultiTaskQueryLogLevel;


/* Function declarations common to more than one executor */
extern MultiExecutorType JobExecutorType(DistributedPlan *distributedPlan);

#endif /* MULTI_SERVER_EXECUTOR_H */
