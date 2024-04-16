/*-------------------------------------------------------------------------
 *
 * log_utils.c
 *	  Utilities regarding logs
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/cryptohash.h"
#include "common/sha2.h"
#include "utils/builtins.h"
#include "utils/guc.h"

#include "pg_version_constants.h"

#include "distributed/errormessage.h"
#include "distributed/log_utils.h"


/*
 * GUC controls showing of some of the unwanted citus messages, it is intended to be set false
 * before vanilla tests to not break postgres test logs.
 */
bool EnableUnsupportedFeatureMessages = true;

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
