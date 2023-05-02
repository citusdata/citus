/*-------------------------------------------------------------------------
 *
 * migration_worker.h
 *	  definition of migration worker functions
 *
 * Copyright (c), Microsoft, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_MIGRATION_MONITOR_H
#define PG_MIGRATION_MONITOR_H


pid_t StartMigrationMonitor(Oid databaseId, Oid subscriptionId);


#endif
