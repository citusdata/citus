/*-------------------------------------------------------------------------
 *
 * background_worker_utils.h
 *    Common utilities for initializing PostgreSQL background workers
 *    used by Citus distributed infrastructure.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef BACKGROUND_WORKER_UTILS_H
#define BACKGROUND_WORKER_UTILS_H

#include "postgres.h"
#include "postmaster/bgworker.h"

/*
 * Background worker configuration parameters
 */
typedef struct CitusBackgroundWorkerConfig
{
	/* Worker identification */
	const char *workerName;
	const char *functionName;

	/* Worker parameters */
	Datum mainArg;
	Oid extensionOwner;

	/* Worker behavior flags */
	bool needsNotification;
	bool waitForStartup;
	int restartTime;

	/* Optional extra data */
	const void *extraData;
	size_t extraDataSize;
} CitusBackgroundWorkerConfig;

/* Default configuration values */
#define CITUS_BGW_DEFAULT_RESTART_TIME 5
#define CITUS_BGW_NEVER_RESTART BGW_NEVER_RESTART

/* Function declarations */
extern BackgroundWorkerHandle * RegisterCitusBackgroundWorker(const
															  CitusBackgroundWorkerConfig
															  *config);

extern void InitializeCitusBackgroundWorker(BackgroundWorker *worker,
											const CitusBackgroundWorkerConfig *config);

#endif /* BACKGROUND_WORKER_UTILS_H */
