/*-------------------------------------------------------------------------
 *
 * log_utils.c
 *	  Utilities regarding logs
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/guc.h"
#include "distributed/log_utils.h"


bool
IsLoggableLevel(int logLevel)
{
	return log_min_messages <= logLevel || client_min_messages <= logLevel;
}
