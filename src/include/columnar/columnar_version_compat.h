/*-------------------------------------------------------------------------
 *
 * columnar_version_compat.h
 *
 *  Compatibility macros for writing code agnostic to PostgreSQL versions
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_COMPAT_H
#define COLUMNAR_COMPAT_H

#include "pg_version_constants.h"

#if PG_VERSION_NUM >= PG_VERSION_15
#define ExecARDeleteTriggers_compat(a, b, c, d, e, f) \
	ExecARDeleteTriggers(a, b, c, d, e, f)
#else
#define ExecARDeleteTriggers_compat(a, b, c, d, e, f) \
	ExecARDeleteTriggers(a, b, c, d, e)
#endif

#define ACLCHECK_OBJECT_TABLE OBJECT_TABLE

#define ExplainPropertyLong(qlabel, value, es) \
	ExplainPropertyInteger(qlabel, NULL, value, es)

#endif /* COLUMNAR_COMPAT_H */
