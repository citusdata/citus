/*-------------------------------------------------------------------------
 *
 * version_compat.h
 *	  Compatibility macros for writing code agnostic to PostgreSQL versions
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef VERSION_COMPAT_H
#define VERSION_COMPAT_H

#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "commands/explain.h"
#include "catalog/namespace.h"
#include "distributed/citus_safe_lib.h"
#include "nodes/parsenodes.h"
#include "parser/parse_func.h"
#if (PG_VERSION_NUM >= PG_VERSION_12)
#include "optimizer/optimizer.h"
#endif

#if (PG_VERSION_NUM >= PG_VERSION_13)
#include "tcop/tcopprot.h"
#endif

#if PG_VERSION_NUM >= PG_VERSION_13
#define lnext_compat(l, r) lnext(l, r)
#define list_delete_cell_compat(l, c, p) list_delete_cell(l, c)
#define pg_plan_query_compat(p, q, c, b) pg_plan_query(p, q, c, b)
#define planner_compat(p, c, b) planner(p, NULL, c, b)
#define standard_planner_compat(a, c, d) standard_planner(a, NULL, c, d)
#define getOwnedSequencesCompat(a, b) getOwnedSequences(a)
#define CMDTAG_SELECT_COMPAT CMDTAG_SELECT
#define ExplainOnePlanCompat(a, b, c, d, e, f, g) ExplainOnePlan(a, b, c, d, e, f, g, \
																 NULL)
#define varoattno varattnosyn
#define varnoold varnosyn
#define Set_ptr_value(a, b) ((a)->ptr_value = (b))
#define RangeTableEntryFromNSItem(a) ((a)->p_rte)
#define QueryCompletionCompat QueryCompletion
#else /* pre PG13 */
#define lnext_compat(l, r) lnext(r)
#define list_delete_cell_compat(l, c, p) list_delete_cell(l, c, p)
#define pg_plan_query_compat(p, q, c, b) pg_plan_query(p, c, b)
#define planner_compat(p, c, b) planner(p, c, b)
#define standard_planner_compat(a, c, d) standard_planner(a, c, d)
#define CMDTAG_SELECT_COMPAT "SELECT"
#define getOwnedSequencesCompat(a, b) getOwnedSequences(a, b)
#define ExplainOnePlanCompat(a, b, c, d, e, f, g) ExplainOnePlan(a, b, c, d, e, f, g)
#define Set_ptr_value(a, b) ((a)->data.ptr_value = (b))
#define RangeTableEntryFromNSItem(a) (a)
#define QueryCompletionCompat char
#endif
#if PG_VERSION_NUM >= PG_VERSION_12

#define MakeSingleTupleTableSlotCompat MakeSingleTupleTableSlot
#define AllocSetContextCreateExtended AllocSetContextCreateInternal
#define NextCopyFromCompat NextCopyFrom
#define ArrayRef SubscriptingRef
#define T_ArrayRef T_SubscriptingRef
#define or_clause is_orclause
#define GetSysCacheOid1Compat GetSysCacheOid1
#define GetSysCacheOid2Compat GetSysCacheOid2
#define GetSysCacheOid3Compat GetSysCacheOid3
#define GetSysCacheOid4Compat GetSysCacheOid4

#define fcGetArgValue(fc, n) ((fc)->args[n].value)
#define fcGetArgNull(fc, n) ((fc)->args[n].isnull)
#define fcSetArgExt(fc, n, val, is_null) \
	(((fc)->args[n].isnull = (is_null)), ((fc)->args[n].value = (val)))

typedef struct
{
	File fd;
	off_t offset;
} FileCompat;

static inline int
FileWriteCompat(FileCompat *file, char *buffer, int amount, uint32 wait_event_info)
{
	int count = FileWrite(file->fd, buffer, amount, file->offset, wait_event_info);
	if (count > 0)
	{
		file->offset += count;
	}
	return count;
}


static inline int
FileReadCompat(FileCompat *file, char *buffer, int amount, uint32 wait_event_info)
{
	int count = FileRead(file->fd, buffer, amount, file->offset, wait_event_info);
	if (count > 0)
	{
		file->offset += count;
	}
	return count;
}


static inline FileCompat
FileCompatFromFileStart(File fileDesc)
{
	FileCompat fc;

	/* ensure uninitialized padding doesn't escape the function */
	memset_struct_0(fc);
	fc.fd = fileDesc;
	fc.offset = 0;

	return fc;
}


#else /* pre PG12 */
#define table_open(r, l) heap_open(r, l)
#define table_openrv(r, l) heap_openrv(r, l)
#define table_openrv_extended(r, l, m) heap_openrv_extended(r, l, m)
#define table_close(r, l) heap_close(r, l)
#define QTW_EXAMINE_RTES_BEFORE QTW_EXAMINE_RTES
#define MakeSingleTupleTableSlotCompat(tupleDesc, tts_opts) \
	MakeSingleTupleTableSlot(tupleDesc)
#define NextCopyFromCompat(cstate, econtext, values, nulls) \
	NextCopyFrom(cstate, econtext, values, nulls, NULL)

/*
 * In PG12 GetSysCacheOid requires an oid column,
 * whereas beforehand the oid column was implicit with WITH OIDS
 */
#define GetSysCacheOid1Compat(cacheId, oidcol, key1) \
	GetSysCacheOid1(cacheId, key1)
#define GetSysCacheOid2Compat(cacheId, oidcol, key1, key2) \
	GetSysCacheOid2(cacheId, key1, key2)
#define GetSysCacheOid3Compat(cacheId, oidcol, key1, key2, key3) \
	GetSysCacheOid3(cacheId, key1, key2, key3)
#define GetSysCacheOid4Compat(cacheId, oidcol, key1, key2, key3, key4) \
	GetSysCacheOid4(cacheId, key1, key2, key3, key4)

#define LOCAL_FCINFO(name, nargs) \
	FunctionCallInfoData name ## data; \
	FunctionCallInfoData *name = &name ## data

#define fcGetArgValue(fc, n) ((fc)->arg[n])
#define fcGetArgNull(fc, n) ((fc)->argnull[n])
#define fcSetArgExt(fc, n, val, is_null) \
	(((fc)->argnull[n] = (is_null)), ((fc)->arg[n] = (val)))

typedef struct
{
	File fd;
} FileCompat;

static inline int
FileWriteCompat(FileCompat *file, char *buffer, int amount, uint32 wait_event_info)
{
	return FileWrite(file->fd, buffer, amount, wait_event_info);
}


static inline int
FileReadCompat(FileCompat *file, char *buffer, int amount, uint32 wait_event_info)
{
	return FileRead(file->fd, buffer, amount, wait_event_info);
}


static inline FileCompat
FileCompatFromFileStart(File fileDesc)
{
	FileCompat fc = {
		.fd = fileDesc,
	};

	return fc;
}


#endif /* PG12 */

#define fcSetArg(fc, n, value) fcSetArgExt(fc, n, value, false)
#define fcSetArgNull(fc, n) fcSetArgExt(fc, n, (Datum) 0, true)

#endif   /* VERSION_COMPAT_H */
