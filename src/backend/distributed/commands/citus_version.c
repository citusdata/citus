/*-------------------------------------------------------------------------
 *
 * create_distributed_relation.c
 *	  Routines relation to the creation of distributed relations.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"

#include "utils/builtins.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_running_version);

#if !defined(CITUS_VERSION)
#error Something went wrong, CITUS_VERSION is not set!
#endif

#define STRINGIFY(x) #x
#define MACRO(x) STRINGIFY(x)

/*
 * citus_running_version returns the version string the currently running code was built
 * with.
 */
Datum
citus_running_version(PG_FUNCTION_ARGS)
{
	const char *versionStr = MACRO(CITUS_VERSION);
	text *versionText = cstring_to_text(versionStr);

	PG_RETURN_TEXT_P(versionText);
}
