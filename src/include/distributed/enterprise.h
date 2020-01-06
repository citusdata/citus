/*-------------------------------------------------------------------------
 *
 * enterprise.h
 *
 * Utilities related to enterprise code in the community version.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_ENTERPRISE_H
#define CITUS_ENTERPRISE_H

#include "postgres.h"
#include "fmgr.h"


#define NOT_SUPPORTED_IN_COMMUNITY(name) \
	PG_FUNCTION_INFO_V1(name); \
	Datum name(PG_FUNCTION_ARGS) { \
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), \
						errmsg(# name "() is only supported on Citus Enterprise"))); \
	}

#endif
