/*-------------------------------------------------------------------------
 *
 * version_compat.h
 *	  Compatibility macros for writing code agnostic to PostgreSQL versions
 *
 * Copyright (c) 2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef VERSION_COMPAT_H
#define VERSION_COMPAT_H

#include "postgres.h"

#define CITUS_FUNCTION(funcname) \
	PGDLLEXPORT Datum funcname(PG_FUNCTION_ARGS); \
	PG_FUNCTION_INFO_V1(funcname)

#if (PG_VERSION_NUM >= 90600 && PG_VERSION_NUM < 90700)

/* Backports from PostgreSQL 10 */
/* Accessor for the i'th attribute of tupdesc. */
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])

#endif

#if (PG_VERSION_NUM >= 90600 && PG_VERSION_NUM < 110000)

#include "access/hash.h"
#include "storage/fd.h"

/* PostgreSQL 11 splits hash procs into "standard" and "extended" */
#define HASHSTANDARD_PROC HASHPROC


static inline int
BasicOpenFilePerm(FileName fileName, int fileFlags, int fileMode)
{
	return BasicOpenFile(fileName, fileFlags, fileMode);
}


static inline File
PathNameOpenFilePerm(FileName fileName, int fileFlags, int fileMode)
{
	return PathNameOpenFile(fileName, fileFlags, fileMode);
}


#endif

#endif   /* VERSION_COMPAT_H */
