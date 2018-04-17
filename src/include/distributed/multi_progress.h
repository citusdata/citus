/*-------------------------------------------------------------------------
 *
 * multi_progress.h
 *    Declarations for public functions and variables used in progress
 *    tracking functions in Citus.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_PROGRESS_H
#define MULTI_PROGRESS_H


#include "fmgr.h"
#include "nodes/pg_list.h"


#if (PG_VERSION_NUM < 100000)

/* define symbols that are undefined in PostgreSQL <= 9.6 */
#define DSM_HANDLE_INVALID 0
extern PGDLLEXPORT Datum pg_stat_get_progress_info(PG_FUNCTION_ARGS);
#endif

typedef struct ProgressMonitorData
{
	uint64 processId;
	int stepCount;
	void *steps;
} ProgressMonitorData;


extern ProgressMonitorData * CreateProgressMonitor(uint64 progressTypeMagicNumber,
												   int stepCount, Size stepSize,
												   Oid relationId);
extern ProgressMonitorData * GetCurrentProgressMonitor(void);
extern void FinalizeCurrentProgressMonitor(void);
extern List * ProgressMonitorList(uint64 commandTypeMagicNumber,
								  List **attachedDSMSegmentList);
extern void DetachFromDSMSegments(List *dsmSegmentList);


#endif /* MULTI_PROGRESS_H */
