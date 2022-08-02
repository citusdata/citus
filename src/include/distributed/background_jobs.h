#ifndef CITUS_BACKGROUND_JOBS_H
#define CITUS_BACKGROUND_JOBS_H

#import "postgres.h"

#import "postmaster/bgworker.h"

extern BackgroundWorkerHandle * StartCitusBackgroundJobWorker(Oid database, Oid
															  extensionOwner);
extern void CitusBackgroundJobMain(Datum arg);
extern void CitusBackgroundJobExecuter(Datum main_arg);

extern bool RebalanceJobDebugDelay;

#endif /*CITUS_BACKGROUND_JOBS_H */
