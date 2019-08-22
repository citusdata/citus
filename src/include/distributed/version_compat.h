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
#include "commands/explain.h"
#include "catalog/namespace.h"
#include "nodes/parsenodes.h"

#if (PG_VERSION_NUM >= 120000)
#include "optimizer/optimizer.h"
#endif

#if (PG_VERSION_NUM < 110000)

#include "access/hash.h"
#include "storage/fd.h"
#include "optimizer/prep.h"
#include "postmaster/bgworker.h"
#include "utils/memutils.h"
#include "funcapi.h"

/* PostgreSQL 11 splits hash procs into "standard" and "extended" */
#define HASHSTANDARD_PROC HASHPROC

/* following functions are renamed in PG11 */
#define PreventInTransactionBlock PreventTransactionChain
#define DatumGetJsonbP(d) DatumGetJsonb(d)
#define RequireTransactionBlock RequireTransactionChain

/* following defines also exist for PG11 */
#define RELATION_OBJECT_TYPE ACL_OBJECT_RELATION
#define IndexInfoAttributeNumberArray(indexinfo) (indexinfo->ii_KeyAttrNumbers)

/* CreateTrigger api is changed in PG11 */
#define CreateTriggerInternal(stmt, queryString, relOid, refRelOid, constraintOid, \
							  indexOid, funcoid, parentTriggerOid, whenClause, isInternal, \
							  in_partition) \
	CreateTrigger(stmt, queryString, relOid, refRelOid, constraintOid, indexOid, \
				  isInternal)

#define get_attname_internal(relationId, columnNumber, false) \
	get_attname(relationId, columnNumber)

#define BackgroundWorkerInitializeConnectionByOid(dboid, useroid, flags) \
	BackgroundWorkerInitializeConnectionByOid(dboid, useroid)

#define AtEOXact_Files(isCommit) \
	AtEOXact_Files()

#define ACLCHECK_OBJECT_TABLE ACL_KIND_CLASS
#define ACLCHECK_OBJECT_SCHEMA ACL_KIND_NAMESPACE
#define ACLCHECK_OBJECT_INDEX ACL_KIND_CLASS
#define ACLCHECK_OBJECT_SEQUENCE ACL_KIND_CLASS


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


static inline MemoryContext
AllocSetContextCreateExtended(MemoryContext parent, const char *name, Size minContextSize,
							  Size initBlockSize, Size maxBlockSize)
{
	return AllocSetContextCreate(parent, name, minContextSize, initBlockSize,
								 maxBlockSize);
}


static inline void
ExplainPropertyIntegerInternal(const char *qlabel, const char *unit, int64 value,
							   ExplainState *es)
{
	return ExplainPropertyInteger(qlabel, value, es);
}


static inline List *
ExtractVacuumTargetRels(VacuumStmt *vacuumStmt)
{
	List *vacuumList = NIL;

	if (vacuumStmt->relation != NULL)
	{
		vacuumList = list_make1(vacuumStmt->relation);
	}

	return vacuumList;
}


static inline List *
VacuumColumnList(VacuumStmt *vacuumStmt, int relationIndex)
{
	Assert(relationIndex == 0);

	return vacuumStmt->va_cols;
}


#define RVR_MISSING_OK 1
#define RVR_NOWAIT 2

static inline Oid
RangeVarGetRelidInternal(const RangeVar *relation, LOCKMODE lockmode, uint32 flags,
						 RangeVarGetRelidCallback callback, void *callback_arg)
{
	bool missingOK = ((flags & RVR_MISSING_OK) != 0);
	bool noWait = ((flags & RVR_NOWAIT) != 0);

	return RangeVarGetRelidExtended(relation, lockmode, missingOK, noWait,
									callback, callback_arg);
}


static inline Expr *
canonicalize_qual_compat(Expr *qual, bool is_check)
{
	return canonicalize_qual(qual);
}


/*
 * A convenient wrapper around get_expr_result_type() that is added on PG11
 *
 * Note that this function ignores the second parameter and behaves
 * slightly differently than the PG11 version.
 *
 * 1. The original function throws errors if noError flag is not set, we ignore
 * this flag here and return NULL in that case
 * 2. TYPEFUNC_COMPOSITE_DOMAIN is introduced in PG11, and references to this
 * macro is removed
 * */
static inline TupleDesc
get_expr_result_tupdesc(Node *expr, bool noError)
{
	TupleDesc tupleDesc;
	TypeFuncClass functypclass;

	functypclass = get_expr_result_type(expr, NULL, &tupleDesc);

	if (functypclass == TYPEFUNC_COMPOSITE)
	{
		return tupleDesc;
	}
	return NULL;
}


#endif

#if (PG_VERSION_NUM >= 110000)
#include "optimizer/prep.h"

/* following macros should be removed when we drop support for PG10 and below */
#define RELATION_OBJECT_TYPE OBJECT_TABLE
#define IndexInfoAttributeNumberArray(indexinfo) (indexinfo->ii_IndexAttrNumbers)
#define CreateTriggerInternal CreateTrigger
#define get_attname_internal get_attname

#define ACLCHECK_OBJECT_TABLE OBJECT_TABLE
#define ACLCHECK_OBJECT_SCHEMA OBJECT_SCHEMA
#define ACLCHECK_OBJECT_INDEX OBJECT_INDEX
#define ACLCHECK_OBJECT_SEQUENCE OBJECT_SEQUENCE


#define ConstraintRelidIndexId ConstraintRelidTypidNameIndexId

static inline void
ExplainPropertyIntegerInternal(const char *qlabel, const char *unit, int64 value,
							   ExplainState *es)
{
	return ExplainPropertyInteger(qlabel, unit, value, es);
}


static inline Expr *
canonicalize_qual_compat(Expr *qual, bool is_check)
{
	return canonicalize_qual(qual, is_check);
}


/*
 * ExtractVacuumTargetRels returns list of target
 * relations from vacuum statement.
 */
static inline List *
ExtractVacuumTargetRels(VacuumStmt *vacuumStmt)
{
	List *vacuumList = NIL;

	ListCell *vacuumRelationCell = NULL;
	foreach(vacuumRelationCell, vacuumStmt->rels)
	{
		VacuumRelation *vacuumRelation = (VacuumRelation *) lfirst(vacuumRelationCell);
		vacuumList = lappend(vacuumList, vacuumRelation->relation);
	}

	return vacuumList;
}


/*
 * VacuumColumnList returns list of columns from relation
 * in the vacuum statement at specified relationIndex.
 */
static inline List *
VacuumColumnList(VacuumStmt *vacuumStmt, int relationIndex)
{
	VacuumRelation *vacuumRelation = (VacuumRelation *) list_nth(vacuumStmt->rels,
																 relationIndex);

	return vacuumRelation->va_cols;
}


static inline Oid
RangeVarGetRelidInternal(const RangeVar *relation, LOCKMODE lockmode, uint32 flags,
						 RangeVarGetRelidCallback callback, void *callback_arg)
{
	return RangeVarGetRelidExtended(relation, lockmode, flags, callback, callback_arg);
}


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

#define fcSetArg(fc, n, argval) \
	(((fc)->args[n].isnull = false), ((fc)->args[n].value = (argval)))
#define fcSetArgNull(fc, n) \
	(((fc)->args[n].isnull = true), ((fc)->args[n].value = (Datum) 0))

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

#define fcSetArg(fc, n, value) \
	(((fc)->argnull[n] = false), ((fc)->arg[n] = (value)))
#define fcSetArgNull(fc, n) \
	(((fc)->argnull[n] = true), ((fc)->arg[n] = (Datum) 0))

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

#endif   /* VERSION_COMPAT_H */
