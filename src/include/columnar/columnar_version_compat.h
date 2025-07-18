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

#ifndef COLUMNAR_VERSION_COMPAT_H
#define COLUMNAR_VERSION_COMPAT_H

#include "pg_version_constants.h"

/* for PG_VERSION_NUM and TupleDescAttr() */
#include "postgres.h"

#include "access/htup_details.h"


#define ACLCHECK_OBJECT_TABLE OBJECT_TABLE

#define ExplainPropertyLong(qlabel, value, es) \
	ExplainPropertyInteger(qlabel, NULL, value, es)


/* tuple-descriptor attributes moved in PostgreSQL 18: */
#if PG_VERSION_NUM >= PG_VERSION_18
#define Attr(tupdesc, colno) TupleDescAttr((tupdesc), (colno))
#else
#define Attr(tupdesc, colno) (&((tupdesc)->attrs[(colno)]))
#endif

#endif /* COLUMNAR_COMPAT_H */
