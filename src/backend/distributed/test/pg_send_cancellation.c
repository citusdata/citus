/*-------------------------------------------------------------------------
 *
 * pg_send_cancellation.c
 *
 * This file contains functions to test setting pg_send_cancellation.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "port.h"

#include "postmaster/postmaster.h"


#define PG_SEND_CANCELLATION_VERSION \
	"pg_send_cancellation (PostgreSQL) " PG_VERSION "\n"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(get_cancellation_key);
PG_FUNCTION_INFO_V1(run_pg_send_cancellation);


/*
 * get_cancellation_key returns the cancellation key of the current process
 * as an integer.
 */
Datum
get_cancellation_key(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(MyCancelKey);
}


/*
 * run_pg_send_cancellation runs the pg_send_cancellation program with
 * the specified arguments
 */
Datum
run_pg_send_cancellation(PG_FUNCTION_ARGS)
{
	int pid = PG_GETARG_INT32(0);
	int cancelKey = PG_GETARG_INT32(1);

	char sendCancellationPath[MAXPGPATH];
	char command[1024];

	/* Locate executable backend before we change working directory */
	if (find_other_exec(my_exec_path, "pg_send_cancellation",
						PG_SEND_CANCELLATION_VERSION,
						sendCancellationPath) < 0)
	{
		ereport(ERROR, (errmsg("could not locate pg_send_cancellation")));
	}

	pg_snprintf(command, sizeof(command), "%s %d %d %s %d",
				sendCancellationPath, pid, cancelKey, "localhost", PostPortNumber);

	if (system(command) != 0)
	{
		ereport(ERROR, (errmsg("failed to run command: %s", command)));
	}

	PG_RETURN_VOID();
}
