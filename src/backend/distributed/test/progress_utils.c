/*-------------------------------------------------------------------------
 *
 * progress_utils.c
 *
 * This file contains functions to exercise progress monitoring functionality
 * within Citus.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include <unistd.h>

#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "nodes/execnodes.h"
#include "utils/tuplestore.h"

#include "distributed/listutils.h"
#include "distributed/multi_progress.h"
#include "distributed/tuplestore.h"


PG_FUNCTION_INFO_V1(create_progress);
PG_FUNCTION_INFO_V1(update_progress);
PG_FUNCTION_INFO_V1(finish_progress);
PG_FUNCTION_INFO_V1(show_progress);


Datum
create_progress(PG_FUNCTION_ARGS)
{
	uint64 magicNumber = PG_GETARG_INT64(0);
	int stepCount = PG_GETARG_INT32(1);
	dsm_handle dsmHandle;
	ProgressMonitorData *monitor = CreateProgressMonitor(stepCount,
														 sizeof(uint64), &dsmHandle);

	if (monitor != NULL)
	{
		uint64 *steps = (uint64 *) ProgressMonitorSteps(monitor);

		int i = 0;
		for (; i < stepCount; i++)
		{
			steps[i] = 0;
		}
	}

	RegisterProgressMonitor(magicNumber, 0, dsmHandle);
	PG_RETURN_VOID();
}


Datum
update_progress(PG_FUNCTION_ARGS)
{
	uint64 step = PG_GETARG_INT64(0);
	uint64 newValue = PG_GETARG_INT64(1);

	ProgressMonitorData *monitor = GetCurrentProgressMonitor();

	if (monitor != NULL && step < monitor->stepCount)
	{
		uint64 *steps = (uint64 *) ProgressMonitorSteps(monitor);
		steps[step] = newValue;
	}

	PG_RETURN_VOID();
}


Datum
finish_progress(PG_FUNCTION_ARGS)
{
	FinalizeCurrentProgressMonitor();

	PG_RETURN_VOID();
}


Datum
show_progress(PG_FUNCTION_ARGS)
{
	uint64 magicNumber = PG_GETARG_INT64(0);
	List *attachedDSMSegments = NIL;
	List *monitorList = ProgressMonitorList(magicNumber, &attachedDSMSegments);
	TupleDesc tupdesc;
	Tuplestorestate *tupstore = SetupTuplestore(fcinfo, &tupdesc);

	ProgressMonitorData *monitor = NULL;
	foreach_declared_ptr(monitor, monitorList)
	{
		uint64 *steps = ProgressMonitorSteps(monitor);

		for (int stepIndex = 0; stepIndex < monitor->stepCount; stepIndex++)
		{
			uint64 step = steps[stepIndex];

			Datum values[2];
			bool nulls[2];

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));

			values[0] = Int32GetDatum(stepIndex);
			values[1] = UInt64GetDatum(step);

			tuplestore_putvalues(tupstore, tupdesc, values, nulls);
		}
	}

	DetachFromDSMSegments(attachedDSMSegments);

	return (Datum) 0;
}
