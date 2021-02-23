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

#define ACLCHECK_OBJECT_TABLE OBJECT_TABLE

#define ExplainPropertyLong(qlabel, value, es) \
	ExplainPropertyInteger(qlabel, NULL, value, es)

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
