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

#if (PG_VERSION_NUM >= 90600 && PG_VERSION_NUM < 90700)

/* Backports from PostgreSQL 10 */
/* Accessor for the i'th attribute of tupdesc. */
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])

#endif

#if (PG_VERSION_NUM >= 90600 && PG_VERSION_NUM < 110000)

#include "access/hash.h"
#include "storage/fd.h"
#include "optimizer/prep.h"
#include "utils/memutils.h"

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


#endif   /* VERSION_COMPAT_H */
