/*-------------------------------------------------------------------------
 *
 * pg_version_compat.h
 *	  Compatibility macros for writing code agnostic to PostgreSQL versions
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_VERSION_COMPAT_H
#define PG_VERSION_COMPAT_H

#include "distributed/pg_version_constants.h"

#if PG_VERSION_NUM >= PG_VERSION_15
#define ProcessCompletedNotifies()
#define RelationCreateStorage_compat(a, b, c) RelationCreateStorage(a, b, c)
#define parse_analyze_varparams_compat(a, b, c, d, e) parse_analyze_varparams(a, b, c, d, \
																			  e)
#else

#include "nodes/value.h"
#include "storage/smgr.h"
#include "utils/int8.h"
#include "utils/rel.h"

typedef Value String;

#ifdef HAVE_LONG_INT_64
#define strtoi64(str, endptr, base) ((int64) strtol(str, endptr, base))
#define strtou64(str, endptr, base) ((uint64) strtoul(str, endptr, base))
#else
#define strtoi64(str, endptr, base) ((int64) strtoll(str, endptr, base))
#define strtou64(str, endptr, base) ((uint64) strtoull(str, endptr, base))
#endif
#define RelationCreateStorage_compat(a, b, c) RelationCreateStorage(a, b)
#define parse_analyze_varparams_compat(a, b, c, d, e) parse_analyze_varparams(a, b, c, d)
#define pgstat_init_relation(r) pgstat_initstats(r)
#define pg_analyze_and_rewrite_fixedparams(a, b, c, d, e) pg_analyze_and_rewrite(a, b, c, \
																				 d, e)

static inline int64
pg_strtoint64(char *s)
{
	int64 result;
	(void) scanint8(s, false, &result);
	return result;
}


static inline SMgrRelation
RelationGetSmgr(Relation rel)
{
	if (unlikely(rel->rd_smgr == NULL))
	{
		smgrsetowner(&(rel->rd_smgr), smgropen(rel->rd_node, rel->rd_backend));
	}
	return rel->rd_smgr;
}


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
#define PrevProcessUtility_compat(a, b, c, d, e, f, g, h) \
	PrevProcessUtility(a, b, c, d, e, f, g, h)
#define SetTuplestoreDestReceiverParams_compat(a, b, c, d, e, f) \
	SetTuplestoreDestReceiverParams(a, b, c, d, e, f)
#define pgproc_statusflags_compat(pgproc) ((pgproc)->statusFlags)
#define get_partition_parent_compat(a, b) get_partition_parent(a, b)
#define RelationGetPartitionDesc_compat(a, b) RelationGetPartitionDesc(a, b)
#define make_simple_restrictinfo_compat(a, b) make_simple_restrictinfo(a, b)
#define pull_varnos_compat(a, b) pull_varnos(a, b)
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
#define PrevProcessUtility_compat(a, b, c, d, e, f, g, h) \
	PrevProcessUtility(a, b, d, e, f, g, h)
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
#define ROLE_PG_READ_ALL_STATS DEFAULT_ROLE_READ_ALL_STATS
#endif

#define SetListCellPtr(a, b) ((a)->ptr_value = (b))
#define RangeTableEntryFromNSItem(a) ((a)->p_rte)
#define fcGetArgValue(fc, n) ((fc)->args[n].value)
#define fcGetArgNull(fc, n) ((fc)->args[n].isnull)
#define fcSetArgExt(fc, n, val, is_null) \
	(((fc)->args[n].isnull = (is_null)), ((fc)->args[n].value = (val)))
#define fcSetArg(fc, n, value) fcSetArgExt(fc, n, value, false)
#define fcSetArgNull(fc, n) fcSetArgExt(fc, n, (Datum) 0, true)

#endif   /* PG_VERSION_COMPAT_H */
