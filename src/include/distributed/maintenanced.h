/*-------------------------------------------------------------------------
 *
 * maintenanced.h
 *	  Background worker run for each citus using database in a postgres
 *    cluster.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MAINTENANCED_H
#define MAINTENANCED_H

/* collect statistics every 24 hours */
#define STATS_COLLECTION_TIMEOUT_MILLIS (24 * 60 * 60 * 1000)

/* if statistics collection fails, retry in 1 minute */
#define STATS_COLLECTION_RETRY_TIMEOUT_MILLIS (60 * 1000)

/* config variable for */
extern double DistributedDeadlockDetectionTimeoutFactor;

extern void StopMaintenanceDaemon(Oid databaseId);
extern void TriggerNodeMetadataSync(Oid databaseId);
extern void InitializeMaintenanceDaemon(void);
extern void InitializeMaintenanceDaemonBackend(void);
extern bool LockCitusExtension(void);

extern void CitusMaintenanceDaemonMain(Datum main_arg);
extern bool IsMaintainanceDaemonProcess(void);

#endif /* MAINTENANCED_H */
