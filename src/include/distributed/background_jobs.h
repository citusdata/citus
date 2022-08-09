#ifndef CITUS_BACKGROUND_JOBS_H
#define CITUS_BACKGROUND_JOBS_H

#include "postgres.h"

#include "postmaster/bgworker.h"

extern BackgroundWorkerHandle * StartCitusBackgroundTaskMonitorWorker(Oid database, Oid
																	  extensionOwner);
extern void CitusBackgroundTaskMonitorMain(Datum arg);
extern void CitusBackgroundJobExecuter(Datum main_arg);

extern bool BackgroundTaskMonitorDebugDelay;

#endif /*CITUS_BACKGROUND_JOBS_H */
