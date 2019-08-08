/*-------------------------------------------------------------------------
 *
 * multi_progress.c
 *	  Routines for tracking long-running jobs and seeing their progress.
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "distributed/function_utils.h"
#include "distributed/multi_progress.h"
#include "distributed/version_compat.h"
#include "storage/dsm.h"
#include "utils/builtins.h"


/* dynamic shared memory handle of the current progress */
static uint64 currentProgressDSMHandle = DSM_HANDLE_INVALID;

static ProgressMonitorData * MonitorDataFromDSMHandle(dsm_handle dsmHandle,
													  dsm_segment **attachedSegment);


/*
 * CreateProgressMonitor is used to create a place to store progress information related
 * to long running processes. The function creates a dynamic shared memory segment
 * consisting of a header regarding to the process and an array of "steps" that the long
 * running "operations" consists of. The handle of the dynamic shared memory is stored in
 * pg_stat_get_progress_info output, to be parsed by a progress retrieval command
 * later on. This behavior may cause unrelated (but hopefully harmless) rows in
 * pg_stat_progress_vacuum output. The caller of this function should provide a magic
 * number, a unique 64 bit unsigned integer, to distinguish different types of commands.
 */
ProgressMonitorData *
CreateProgressMonitor(uint64 progressTypeMagicNumber, int stepCount, Size stepSize,
					  Oid relationId)
{
	dsm_segment *dsmSegment = NULL;
	dsm_handle dsmHandle = 0;
	ProgressMonitorData *monitor = NULL;
	Size monitorSize = 0;

	if (stepSize <= 0 || stepCount <= 0)
	{
		ereport(ERROR,
				(errmsg("number of steps and size of each step should be "
						"positive values")));
	}

	monitorSize = sizeof(ProgressMonitorData) + stepSize * stepCount;
	dsmSegment = dsm_create(monitorSize, DSM_CREATE_NULL_IF_MAXSEGMENTS);

	if (dsmSegment == NULL)
	{
		ereport(WARNING,
				(errmsg("could not create a dynamic shared memory segment to "
						"keep track of progress of the current command")));
		return NULL;
	}

	dsmHandle = dsm_segment_handle(dsmSegment);

	monitor = MonitorDataFromDSMHandle(dsmHandle, &dsmSegment);

	monitor->stepCount = stepCount;
	monitor->processId = MyProcPid;

	pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM, relationId);
	pgstat_progress_update_param(1, dsmHandle);
	pgstat_progress_update_param(0, progressTypeMagicNumber);

	currentProgressDSMHandle = dsmHandle;

	return monitor;
}


/*
 * GetCurrentProgressMonitor function returns the header and steps array related to the
 * current progress. A progress monitor should be created by calling
 * CreateProgressMonitor, before calling this function.
 */
ProgressMonitorData *
GetCurrentProgressMonitor(void)
{
	dsm_segment *dsmSegment = NULL;
	ProgressMonitorData *monitor = MonitorDataFromDSMHandle(currentProgressDSMHandle,
															&dsmSegment);

	return monitor;
}


/*
 * FinalizeCurrentProgressMonitor releases the dynamic memory segment of the current
 * progress monitoring data structure and removes the process from
 * pg_stat_get_progress_info() output.
 */
void
FinalizeCurrentProgressMonitor(void)
{
	dsm_segment *dsmSegment = dsm_find_mapping(currentProgressDSMHandle);

	if (dsmSegment != NULL)
	{
		dsm_detach(dsmSegment);
	}

	pgstat_progress_end_command();

	currentProgressDSMHandle = DSM_HANDLE_INVALID;
}


/*
 * ProgressMonitorList returns the addresses of monitors of ongoing commands, associated
 * with the given identifier magic number. The function takes a pass in
 * pg_stat_get_progress_info output, filters the rows according to the given magic number,
 * and returns the list of addresses of dynamic shared memory segments. Notice that the
 * caller detach from the attached segments with a call to DetachFromDSMSegments function.
 */
List *
ProgressMonitorList(uint64 commandTypeMagicNumber, List **attachedDSMSegments)
{
	/*
	 * The expected magic number should reside in the first progress field and the
	 * actual segment handle in the second but the slot ordering is 1-indexed in the
	 * tuple table slot and there are 3 other fields before the progress fields in the
	 * pg_stat_get_progress_info output.
	 */
	const int magicNumberIndex = 0 + 1 + 3;
	const int dsmHandleIndex = 1 + 1 + 3;

	/*
	 * Currently, Postgres' progress logging mechanism supports only the VACUUM
	 * operations. Therefore, we identify ourselves as a VACUUM command but only fill
	 * a couple of the available fields. Therefore the commands that use Citus' progress
	 * monitoring API will appear in pg_stat_progress_vacuum output.
	 */
	text *commandTypeText = cstring_to_text("VACUUM");
	Datum commandTypeDatum = PointerGetDatum(commandTypeText);
	Oid getProgressInfoFunctionOid = InvalidOid;
	TupleTableSlot *tupleTableSlot = NULL;
	ReturnSetInfo *progressResultSet = NULL;
	List *monitorList = NIL;

	getProgressInfoFunctionOid = FunctionOid("pg_catalog",
											 "pg_stat_get_progress_info",
											 1);

	progressResultSet = FunctionCallGetTupleStore1(pg_stat_get_progress_info,
												   getProgressInfoFunctionOid,
												   commandTypeDatum);

	tupleTableSlot = MakeSingleTupleTableSlotCompat(progressResultSet->setDesc,
													&TTSOpsMinimalTuple);

	/* iterate over tuples in tuple store, and send them to destination */
	for (;;)
	{
		bool nextTuple = false;
		bool isNull = false;
		Datum magicNumberDatum = 0;
		uint64 magicNumber = 0;

		nextTuple = tuplestore_gettupleslot(progressResultSet->setResult,
											true,
											false,
											tupleTableSlot);

		if (!nextTuple)
		{
			break;
		}

		magicNumberDatum = slot_getattr(tupleTableSlot, magicNumberIndex, &isNull);
		magicNumber = DatumGetUInt64(magicNumberDatum);

		if (!isNull && magicNumber == commandTypeMagicNumber)
		{
			Datum dsmHandleDatum = slot_getattr(tupleTableSlot, dsmHandleIndex, &isNull);
			dsm_handle dsmHandle = DatumGetUInt64(dsmHandleDatum);
			dsm_segment *attachedSegment = NULL;
			ProgressMonitorData *monitor = MonitorDataFromDSMHandle(dsmHandle,
																	&attachedSegment);

			if (monitor != NULL)
			{
				*attachedDSMSegments = lappend(*attachedDSMSegments, attachedSegment);
				monitorList = lappend(monitorList, monitor);
			}
		}

		ExecClearTuple(tupleTableSlot);
	}

	ExecDropSingleTupleTableSlot(tupleTableSlot);

	return monitorList;
}


/*
 * MonitorDataFromDSMHandle returns the progress monitoring data structure at the
 * given segment
 */
ProgressMonitorData *
MonitorDataFromDSMHandle(dsm_handle dsmHandle, dsm_segment **attachedSegment)
{
	dsm_segment *dsmSegment = dsm_find_mapping(dsmHandle);
	ProgressMonitorData *monitor = NULL;

	if (dsmSegment == NULL)
	{
		dsmSegment = dsm_attach(dsmHandle);
	}

	if (dsmSegment != NULL)
	{
		monitor = (ProgressMonitorData *) dsm_segment_address(dsmSegment);
		monitor->steps = (void *) (monitor + 1);
		*attachedSegment = dsmSegment;
	}

	return monitor;
}


/*
 * DetachFromDSMSegments ensures that the process is detached from all of the segments in
 * the given list.
 */
void
DetachFromDSMSegments(List *dsmSegmentList)
{
	ListCell *dsmSegmentCell = NULL;

	foreach(dsmSegmentCell, dsmSegmentList)
	{
		dsm_segment *dsmSegment = (dsm_segment *) lfirst(dsmSegmentCell);

		dsm_detach(dsmSegment);
	}
}
