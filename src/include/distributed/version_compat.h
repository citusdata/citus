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

#if (PG_VERSION_NUM >= 90600 && PG_VERSION_NUM < 90700)

/* Backports from PostgreSQL 10 */
/* Accessor for the i'th attribute of tupdesc. */
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])

#endif

#if (PG_VERSION_NUM >= 90600 && PG_VERSION_NUM < 110000)

#include "access/hash.h"

/* PostgreSQL 11 splits hash procs into "standard" and "extended" */
#define HASHSTANDARD_PROC HASHPROC

#endif

#endif   /* VERSION_COMPAT_H */
