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


#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "funcapi.h"

#include <unistd.h>

#include "distributed/multi_progress.h"
#include "distributed/tuplestore.h"
#include "nodes/execnodes.h"
#include "utils/tuplestore.h"


PG_FUNCTION_INFO_V1(create_progress);
PG_FUNCTION_INFO_V1(update_progress);
PG_FUNCTION_INFO_V1(finish_progress);
PG_FUNCTION_INFO_V1(show_progress);


Datum
create_progress(PG_FUNCTION_ARGS)
{
	uint64 magicNumber = PG_GETARG_INT64(0);
	int stepCount = PG_GETARG_INT32(1);
	ProgressMonitorData *monitor = CreateProgressMonitor(magicNumber, stepCount,
														 sizeof(uint64), 0);

	if (monitor != NULL)
	{
		uint64 *steps = (uint64 *) monitor->steps;

		int i = 0;
		for (; i < stepCount; i++)
		{
			steps[i] = 0;
		}
	}

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
		uint64 *steps = (uint64 *) monitor->steps;
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
	ListCell *monitorCell = NULL;
	TupleDesc tupdesc;
	Tuplestorestate *tupstore = SetupTuplestore(fcinfo, &tupdesc);

	foreach(monitorCell, monitorList)
	{
		ProgressMonitorData *monitor = lfirst(monitorCell);
		uint64 *steps = monitor->steps;

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

	tuplestore_donestoring(tupstore);

	DetachFromDSMSegments(attachedDSMSegments);

	return (Datum) 0;
}
