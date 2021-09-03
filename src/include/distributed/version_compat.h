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

#include "access/sdir.h"
#include "access/heapam.h"
#include "commands/explain.h"
#include "catalog/namespace.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/citus_safe_lib.h"
#include "executor/tuptable.h"
#include "nodes/parsenodes.h"
#include "parser/parse_func.h"
#include "optimizer/optimizer.h"

#if (PG_VERSION_NUM >= PG_VERSION_13)
#include "tcop/tcopprot.h"
#endif

#if PG_VERSION_NUM >= PG_VERSION_14
#define AlterTableStmtObjType_compat(a) ((a)->objtype)
#define getObjectTypeDescription_compat(a, b) getObjectTypeDescription(a, b)
#define getObjectIdentity_compat(a, b) getObjectIdentity(a, b)

/* for MemoryContextMethods->stats */
#define stats_compat(a, b, c, d, e) stats(a, b, c, d, e)
#define FuncnameGetCandidates_compat(a, b, c, d, e, f, g) \
	FuncnameGetCandidates(a, b, c, d, e, f, g)
#define expand_function_arguments_compat(a, b, c, d) expand_function_arguments(a, b, c, d)
#define BeginCopyFrom_compat(a, b, c, d, e, f, g, h) BeginCopyFrom(a, b, c, d, e, f, g, h)
#define standard_ProcessUtility_compat(a, b, c, d, e, f, g, h) \
	standard_ProcessUtility(a, b, c, d, e, f, g, h)
#define ProcessUtility_compat(a, b, c, d, e, f, g, h) \
	ProcessUtility(a, b, c, d, e, f, g, h)
#define SetTuplestoreDestReceiverParams_compat(a, b, c, d, e, f) \
	SetTuplestoreDestReceiverParams(a, b, c, d, e, f)
#define pgproc_statusflags_compat(pgproc) ((pgproc)->statusFlags)
#define get_partition_parent_compat(a, b) get_partition_parent(a, b)
#define RelationGetPartitionDesc_compat(a, b) RelationGetPartitionDesc(a, b)
#define make_simple_restrictinfo_compat(a, b) make_simple_restrictinfo(a, b)
#define pull_varnos_compat(a, b) pull_varnos(a, b)
#define pg_get_statisticsobj_worker_compat(a, b, c) pg_get_statisticsobj_worker(a, b, c)
#else
#define AlterTableStmtObjType_compat(a) ((a)->relkind)
#define F_NEXTVAL F_NEXTVAL_OID
#define ROLE_PG_MONITOR DEFAULT_ROLE_MONITOR
#define PROC_WAIT_STATUS_WAITING STATUS_WAITING
#define getObjectTypeDescription_compat(a, b) getObjectTypeDescription(a)
#define getObjectIdentity_compat(a, b) getObjectIdentity(a)

/* for MemoryContextMethods->stats */
#define stats_compat(a, b, c, d, e) stats(a, b, c, d)
#define FuncnameGetCandidates_compat(a, b, c, d, e, f, g) \
	FuncnameGetCandidates(a, b, c, d, e, g)
#define expand_function_arguments_compat(a, b, c, d) expand_function_arguments(a, c, d)
#define VacOptValue VacOptTernaryValue
#define VACOPTVALUE_UNSPECIFIED VACOPT_TERNARY_DEFAULT
#define VACOPTVALUE_DISABLED VACOPT_TERNARY_DISABLED
#define VACOPTVALUE_ENABLED VACOPT_TERNARY_ENABLED
#define CopyFromState CopyState
#define BeginCopyFrom_compat(a, b, c, d, e, f, g, h) BeginCopyFrom(a, b, d, e, f, g, h)
#define standard_ProcessUtility_compat(a, b, c, d, e, f, g, h) \
	standard_ProcessUtility(a, b, d, e, f, g, h)
#define ProcessUtility_compat(a, b, c, d, e, f, g, h) ProcessUtility(a, b, d, e, f, g, h)
#define COPY_FRONTEND COPY_NEW_FE
#define SetTuplestoreDestReceiverParams_compat(a, b, c, d, e, f) \
	SetTuplestoreDestReceiverParams(a, b, c, d)
#define pgproc_statusflags_compat(pgproc) \
	((&ProcGlobal->allPgXact[(pgproc)->pgprocno])->vacuumFlags)
#define get_partition_parent_compat(a, b) get_partition_parent(a)
#define RelationGetPartitionDesc_compat(a, b) RelationGetPartitionDesc(a)
#define PQ_LARGE_MESSAGE_LIMIT 0
#define make_simple_restrictinfo_compat(a, b) make_simple_restrictinfo(b)
#define pull_varnos_compat(a, b) pull_varnos(b)
#define pg_get_statisticsobj_worker_compat(a, b, c) pg_get_statisticsobj_worker(a, c)
#endif

#if PG_VERSION_NUM >= PG_VERSION_13
#define lnext_compat(l, r) lnext(l, r)
#define list_delete_cell_compat(l, c, p) list_delete_cell(l, c)
#define pg_plan_query_compat(p, q, c, b) pg_plan_query(p, q, c, b)
#define planner_compat(p, c, b) planner(p, NULL, c, b)
#define standard_planner_compat(a, c, d) standard_planner(a, NULL, c, d)
#define GetSequencesOwnedByRelation(a) getOwnedSequences(a)
#define GetSequencesOwnedByColumn(a, b) getOwnedSequences_internal(a, b, 0)
#define CMDTAG_SELECT_COMPAT CMDTAG_SELECT
#define ExplainOnePlanCompat(a, b, c, d, e, f, g, h) \
	ExplainOnePlan(a, b, c, d, e, f, g, h)
#define SetListCellPtr(a, b) ((a)->ptr_value = (b))
#define RangeTableEntryFromNSItem(a) ((a)->p_rte)
#define QueryCompletionCompat QueryCompletion
#else /* pre PG13 */
#define lnext_compat(l, r) lnext(r)
#define list_delete_cell_compat(l, c, p) list_delete_cell(l, c, p)
#define pg_plan_query_compat(p, q, c, b) pg_plan_query(p, c, b)
#define planner_compat(p, c, b) planner(p, c, b)
#define standard_planner_compat(a, c, d) standard_planner(a, c, d)
#define CMDTAG_SELECT_COMPAT "SELECT"
#define GetSequencesOwnedByRelation(a) getOwnedSequences(a, InvalidAttrNumber)
#define GetSequencesOwnedByColumn(a, b) getOwnedSequences(a, b)
#define ExplainOnePlanCompat(a, b, c, d, e, f, g, h) ExplainOnePlan(a, b, c, d, e, f, g)
#define SetListCellPtr(a, b) ((a)->data.ptr_value = (b))
#define RangeTableEntryFromNSItem(a) (a)
#define QueryCompletionCompat char
#define varattnosyn varoattno
#define varnosyn varnoold
#endif
#if PG_VERSION_NUM >= PG_VERSION_12

#define CreateTableSlotForRel(rel) table_slot_create(rel, NULL)
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


#endif /* PG12 */

#define fcSetArg(fc, n, value) fcSetArgExt(fc, n, value, false)
#define fcSetArgNull(fc, n) fcSetArgExt(fc, n, (Datum) 0, true)

#endif   /* VERSION_COMPAT_H */
