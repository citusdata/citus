/*-------------------------------------------------------------------------
 *
 * distributed/utils/restore_interval.h
 *	  Subroutines related to the save checkpoint by time.
 *
 *-------------------------------------------------------------------------
 */

#ifndef RESTORE_POINT_INTERVAL_H
#define RESTORE_POINT_INTERVAL_H

#include "utils/guc.h"


typedef struct citus_tm
{
	int8			ss;
	int8			mm;
	int8			hh;
} citus_tm;

typedef enum RestorePointIntervalMode {
	RESTOREPOINT_INTERVAL_NEVER = 0,
	RESTOREPOINT_INTERVAL_HOURLY,
	RESTOREPOINT_INTERVAL_DAILY
} RestorePointIntervalMode;


extern char *RestorePointInterval;
extern char *RestorePointIntervalName;
extern RestorePointIntervalMode restorePointIntervalMode;


void InitializeRecoveryDaemon(void);
bool GucCheckInterval(char **newval, void **extra, GucSource source);
void CheckRestoreInterval(Oid DatabaseId, Oid UserOid);
#endif	/*  RESTORE_POINT_INTERVAL_H */