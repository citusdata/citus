/*-------------------------------------------------------------------------
 *
 * citus_version.c
 *
 * This file contains functions for displaying the Citus version string
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "citus_version.h"
#include "utils/builtins.h"
#include "distributed/version_compat.h"


/* exports for SQL callable functions */
CITUS_FUNCTION(citus_version);


Datum
citus_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(CITUS_VERSION_STR));
}
