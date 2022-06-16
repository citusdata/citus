/*-------------------------------------------------------------------------
 *
 * log_utils.c
 *	  Utilities regarding logs
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "utils/guc.h"
#include "distributed/log_utils.h"
#include "distributed/errormessage.h"
#include "common/sha2.h"

#include "utils/builtins.h"

#if PG_VERSION_NUM >= PG_VERSION_14
#include "common/cryptohash.h"
#endif


/*
 * IsLoggableLevel returns true if either of client or server log guc is configured to
 * log the given log level.
 * In postgres, log can be configured differently for clients and servers.
 */
bool
IsLoggableLevel(int logLevel)
{
	return log_min_messages <= logLevel || client_min_messages <= logLevel;
}


/*
 * HashLogMessage is deprecated and doesn't do anything anymore. Its indirect
 * usage will be removed later.
 */
char *
HashLogMessage(const char *logText)
{
	return (char *) logText;
}
