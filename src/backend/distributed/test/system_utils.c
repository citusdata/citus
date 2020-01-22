/*-------------------------------------------------------------------------
 *
 * system_utils.c
 *	  Methods for managing databases during regression tests.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "libpq-fe.h"
#include "pgstat.h"
#include "port.h"
#include "storage/fd.h"

#include "distributed/master_metadata_utility.h"
#include "distributed/version_compat.h"

PG_FUNCTION_INFO_V1(citus_stop_test_worker);
PG_FUNCTION_INFO_V1(citus_start_test_worker);


/*
 * Invokes pg_ctl stop for the worker using the given port.
 */
Datum
citus_stop_test_worker(PG_FUNCTION_ARGS)
{
	EnsureSuperUser();

	int port = PG_GETARG_INT32(0);

	StringInfoData commandString;
	initStringInfo(&commandString);
	appendStringInfo(&commandString, "pg_ctl stop -D ../../worker.%d/data", port);

	PG_RETURN_INT32(system(commandString.data));
}


/*
 * Starts worker using the given port.
 */
Datum
citus_start_test_worker(PG_FUNCTION_ARGS)
{
	EnsureSuperUser();

	int port = PG_GETARG_INT32(0);
	const int fileFlags = O_RDONLY;
	const int fileMode = S_IRUSR;
	StringInfoData optionsPath;
	initStringInfo(&optionsPath);
	appendStringInfo(&optionsPath, "../../worker.%d/data/postmaster.opts", port);
	File file =
		PathNameOpenFilePerm(optionsPath.data, fileFlags, fileMode);
	FileCompat fileCompat = FileCompatFromFileStart(file);

	StringInfoData optionsString;
	initStringInfo(&optionsString);

	while (true)
	{
		char chunk[1025];
		int amount = FileReadCompat(&fileCompat, chunk, 1024, PG_WAIT_IO);
		if (amount <= 0)
		{
			break;
		}

		chunk[amount] = 0;
		appendStringInfoString(&optionsString, chunk);
	}

	FileClose(file);

	char *tmpCheckPtr = strstr(optionsString.data, " \"tmp_check/");
	if (tmpCheckPtr == NULL)
	{
		/* If we've already been through this logic previously, the fix will've persisted. */
		char *tmpCheckAlreadyCorrectPtr = strstr(optionsString.data,
												 " \"../../../tmp_check");
		if (tmpCheckAlreadyCorrectPtr == NULL)
		{
			elog(ERROR, "Could not find directory in command string: %s",
				 optionsString.data);
		}
	}
	else
	{
		/* replace " with NUL */
		tmpCheckPtr[1] = 0;
	}

	StringInfoData commandString;
	initStringInfo(&commandString);

	appendStringInfoString(&commandString, optionsString.data);
	if (tmpCheckPtr != NULL)
	{
		appendStringInfoString(&commandString, "\"../../../");
		appendStringInfoString(&commandString, tmpCheckPtr + 2);
	}
	Assert(commandString.data[commandString.len - 1] == '\n');
	commandString.data[commandString.len - 1] = '&';

	int result = system(commandString.data);
	if (result != 0)
	{
		elog(ERROR, "Failed to start worker, exit code: %d command: %s", result,
			 commandString.data);
	}

	StringInfoData connectionString;
	initStringInfo(&connectionString);
	appendStringInfo(&connectionString, "host=localhost port=%d dbname=regression", port);

	/* poll for at least 10 seconds */
	int i = 10000;
	while (PQping(connectionString.data) != PQPING_OK)
	{
		pg_usleep(100);
		if (!i--)
		{
			elog(ERROR, "could not connect");
		}
	}

	PG_RETURN_VOID();
}
