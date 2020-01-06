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
#include "commands/explain.h"
#include "catalog/namespace.h"
#include "nodes/parsenodes.h"
#include "parser/parse_func.h"
#if (PG_VERSION_NUM >= 120000)
#include "optimizer/optimizer.h"
#endif

#if PG_VERSION_NUM >= 120000

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
	FileCompat fc = {
		.fd = fileDesc,
		.offset = 0
	};

	return fc;
}


#else /* pre PG12 */
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
