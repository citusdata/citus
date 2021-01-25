/*-------------------------------------------------------------------------
 *
 * columnar_version_compat.h
 *
 *  Compatibility macros for writing code agnostic to PostgreSQL versions
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef COLUMNAR_COMPAT_H
#define COLUMNAR_COMPAT_H

#if PG_VERSION_NUM < 100000

/* Accessor for the i'th attribute of tupdesc. */
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])

#endif

#if PG_VERSION_NUM < 110000
#define ALLOCSET_DEFAULT_SIZES ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, \
	ALLOCSET_DEFAULT_MAXSIZE
#define ACLCHECK_OBJECT_TABLE ACL_KIND_CLASS
#else
#define ACLCHECK_OBJECT_TABLE OBJECT_TABLE

#define ExplainPropertyLong(qlabel, value, es) \
	ExplainPropertyInteger(qlabel, NULL, value, es)
#endif

#if PG_VERSION_NUM < 120000
#define TTS_EMPTY(slot) ((slot)->tts_isempty)
#define ExecForceStoreHeapTuple(tuple, slot, shouldFree) \
	ExecStoreTuple(newTuple, tupleSlot, InvalidBuffer, shouldFree);
#define table_open(r, l) heap_open(r, l)
#define table_close(r, l) heap_close(r, l)
#define TableScanDesc HeapScanDesc
#define table_beginscan heap_beginscan
#define table_endscan heap_endscan
#endif

#if PG_VERSION_NUM < 130000
#define detoast_attr(X) heap_tuple_untoast_attr(X)
#endif

#endif /* COLUMNAR_COMPAT_H */
