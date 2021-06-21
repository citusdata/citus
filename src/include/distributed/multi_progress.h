/*-------------------------------------------------------------------------
 *
 * multi_progress.h
 *    Declarations for public functions and variables used in progress
 *    tracking functions in Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_PROGRESS_H
#define MULTI_PROGRESS_H


#include "postgres.h"

#include "fmgr.h"
#include "nodes/pg_list.h"
#include "storage/dsm.h"


typedef struct ProgressMonitorData
{
	uint64 processId;
	int stepCount;
} ProgressMonitorData;


extern ProgressMonitorData * CreateProgressMonitor(int stepCount, Size stepSize,
												   dsm_handle *dsmHandle);
extern void RegisterProgressMonitor(uint64 progressTypeMagicNumber,
									Oid relationId,
									dsm_handle dsmHandle);
extern ProgressMonitorData * GetCurrentProgressMonitor(void);
extern void FinalizeCurrentProgressMonitor(void);
extern List * ProgressMonitorList(uint64 commandTypeMagicNumber,
								  List **attachedDSMSegmentList);
extern void DetachFromDSMSegments(List *dsmSegmentList);
extern void * ProgressMonitorSteps(ProgressMonitorData *monitor);


#endif /* MULTI_PROGRESS_H */
