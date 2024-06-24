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

#include "utils/builtins.h"

#include "citus_version.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_version);

/* GIT_VERSION is passed in as a compiler flag during builds that have git installed */
#ifdef GIT_VERSION
#define GIT_REF " gitref: " GIT_VERSION
#else
#define GIT_REF
#endif

Datum
citus_version(PG_FUNCTION_ARGS)
{
	PG_RETURN_TEXT_P(cstring_to_text(CITUS_VERSION_STR GIT_REF));
}
