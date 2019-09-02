/*-------------------------------------------------------------------------
 *
 * citus_version.c
 *
 * This file contains functions for displaying the Citus version string
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "citus_version.h"
#include "utils/builtins.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_version);


Datum
citus_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(CITUS_VERSION_STR));
}
