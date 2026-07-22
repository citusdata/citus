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
PG_FUNCTION_INFO_V1(citus_version_num);

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


/*
 * citus_version_num returns the Citus version of the loaded library as a single
 * comparable integer, encoded as major * 10000 + minor * 100 + patch (e.g. 14.0.3
 * becomes 140003). This is the value that each node reports for itself so that the
 * cluster-wide minimum version can be computed.
 */
Datum
citus_version_num(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(CITUS_VERSION_NUM);
}
