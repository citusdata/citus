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


#include "fmgr.h"
#include "nodes/pg_list.h"


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
