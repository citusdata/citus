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

#include "lib/stringinfo.h"
#include "storage/lwlock.h"

#include "pg_version_constants.h"

#if PG_VERSION_NUM >= PG_VERSION_19

/*
 * PG19 cleaned up its transitive includes.  These headers used to come
 * in via various other PG headers; pull them in centrally so distributed
 * .c files that include pg_version_compat.h (typically through
 * version_compat.h) keep finding the symbols they expect.
 */
#include "access/tableam.h"
#include "catalog/namespace.h"
#include "executor/executor.h"
#include "executor/instrument.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "replication/origin.h"
#include "storage/condition_variable.h"
#include "storage/lwlock.h"
#include "storage/lwlocknames.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/tuplesort.h"
#include "utils/tuplestore.h"
#include "utils/wait_event_types.h"

/*
 * PageSetChecksumInplace was renamed to PageSetChecksum in PG19; the
 * signature (Page, BlockNumber) is unchanged.
 */
#define PageSetChecksumInplace(page, blkno) PageSetChecksum(page, blkno)

/*
 * PG19 removed the legacy bits16 typedef from c.h.  Citus still uses it
 * in a few deparser signatures; preserve the historical width here so
 * the callers don't need per-call-site edits.
 */
typedef uint16 bits16;

/*
 * PG19 dropped the fmStringInfo typedef that was historically used in
 * fmgr.h to denote a binary-receive buffer.  The underlying type is
 * still StringInfo.
 */
typedef StringInfo fmStringInfo;

/*
 * PG19 renamed RepOriginId to ReplOriginId (replication/origin.h).
 */
#define RepOriginId ReplOriginId
#ifndef InvalidRepOriginId
#define InvalidRepOriginId InvalidReplOriginId
#endif

/*
 * PG19 removed the global `replorigin_session_origin` in favour of a
 * `replorigin_xact_state` struct.  Provide an lvalue alias so existing
 * Citus call sites that read and write the session origin keep working.
 */
#define replorigin_session_origin (replorigin_xact_state.origin)

/*
 * PG19 renamed two publication accessors.
 */
#define GetPublicationRelations(pubid, part) GetIncludedPublicationRelations(pubid, part)
#define GetRelationPublications(relid) GetRelationIncludedPublications(relid)

/*
 * PG19 renamed QueryDesc->totaltime to QueryDesc->query_instr (still an
 * Instrumentation*).  Expose a scoped accessor instead of a blanket
 * `#define totaltime query_instr`, which would silently rewrite every
 * unrelated identifier named "totaltime" in the codebase (e.g. the local
 * variable in multi_explain.c).
 */
#define QueryDescTotalTime(qd) ((qd)->query_instr)

/*
 * PG19 added an ExplainState * parameter to the planner entry points.
 * Wrap each of them so existing Citus call sites continue to compile.
 * Parenthesising the function name in the rhs prevents macro recursion.
 */
#define pg_plan_query(q, s, co, b) ((pg_plan_query) (q, s, co, b, NULL))
#define standard_planner(p, s, co, b) ((standard_planner) (p, s, co, b, NULL))
#define planner(p, s, co, b) ((planner) (p, s, co, b, NULL))

/*
 * PG19 added an `int *fgc_flags` out-parameter to FuncnameGetCandidates,
 * which the callee writes to unconditionally — passing NULL crashes.
 * Wrap so callers continue to use the old 7-arg signature.
 */
#define FuncnameGetCandidates(names, nargs, argnames, ev, ed, io, ok) \
		((FuncnameGetCandidates) (names, nargs, argnames, ev, ed, io, ok, \
								  &(int) { 0 }))

/*
 * PG19 added trailing `uint16 flags` to ExecInitScanTupleSlot and
 * `uint32 flags` to table_beginscan. Provide 4-arg wrappers passing 0.
 */
#define ExecInitScanTupleSlot(estate, scanstate, td, ops) \
		((ExecInitScanTupleSlot) ((estate), (scanstate), (td), (ops), 0))
#define table_beginscan(rel, snap, nkeys, key) \
		((table_beginscan) ((rel), (snap), (nkeys), (key), 0))

/*
 * PG19 removed the init_size parameter from ShmemInitHash: the old
 * (init_size, max_size) long pair became a single int64 nelems argument.
 * Drop init_size and pass max_size (cast to int64) as nelems.
 */
#define ShmemInitHash(name, init, max, info, flags) \
		((ShmemInitHash) ((name), (int64) (max), (info), (flags)))

/*
 * PG19 changed LWLockNewTrancheId() to take the tranche name directly and
 * removed the public LWLockRegisterTranche(); tranche names now live in
 * shared memory and are visible to every backend.  Expose a one-call helper
 * that forwards the real name.  The PG<=18 counterpart (further below) keeps
 * the historical allocate-then-register two-step behind the same interface.
 */
#define LWLockNewTrancheIdCompat(name) LWLockNewTrancheId(name)

/*
 * PG19 dropped the public NamedLWLockTranche struct from lwlock.h.
 * Citus stores its own instances of this layout in shared memory headers;
 * provide a local-compatible typedef so those usages keep their layout.
 */
typedef struct CitusNamedLWLockTrancheCompat
{
	int trancheId;
	char *trancheName;
} NamedLWLockTranche;

/*
 * PG19 made the standard_conforming_strings GUC variable file-local
 * (defined static in guc_tables.c), so extensions can no longer link
 * against it.  Provide a tiny accessor that fetches the current value
 * via GetConfigOption() and expose it under the original identifier
 * via a macro so existing Citus deparser code keeps compiling.
 */
static inline bool
citus_pg19_standard_conforming_strings(void)
{
	const char *val = GetConfigOption("standard_conforming_strings", true, false);
	return (val == NULL) || (strcmp(val, "on") == 0);
}


#define standard_conforming_strings (citus_pg19_standard_conforming_strings())

/*
 * PG19 removed the replication_origin_filter_cb typedef from
 * output_plugin.h.  Citus's shardsplit_decoder defines a function of the
 * same name as a callback; no typedef shim is needed in PG19.
 */

/*
 * PG19 replaced ClusterStmt with the unified RepackStmt (which also
 * subsumes VACUUM FULL).  Provide a typedef so type names compile.
 * Member access to ->relation / ->indexname and command discrimination
 * (CLUSTER vs REPACK vs VACUUM FULL, now sharing one node tag) is tracked
 * as follow-up work in #8613 (see cluster.c and relay_event_utility.c).
 */
typedef struct RepackStmt ClusterStmt;
#define T_ClusterStmt T_RepackStmt

/*
 * PG19 added TupleDesc->firstNonCachedOffsetAttr (an offset cache populated
 * by TupleDescFinalize()).  BlessTupleDesc() and slot_deform_heap_tuple()
 * now assert that the cache is initialised.  Citus builds many TupleDescs
 * by hand (CreateTemplateTupleDesc + TupleDescInitEntry...) and historically
 * relied on BlessTupleDesc as the "finalise + register" step.  Wrap
 * BlessTupleDesc so it finalises first; pre-PG19 builds get a no-op
 * TupleDescFinalize().  Sites that build a TupleDesc and use it with a
 * slot WITHOUT going through BlessTupleDesc must call TupleDescFinalize()
 * explicitly (handled at those call sites).
 */
#include "funcapi.h"

#include "access/tupdesc.h"

/*
 * Single-evaluation wrapper: defined before the macro so the call below
 * resolves to the real BlessTupleDesc(), and so the macro argument is only
 * evaluated once (a plain comma-expression macro would evaluate it twice,
 * duplicating any side effects such as BlessTupleDesc(CreateTemplateTupleDesc(...))).
 */
static inline TupleDesc
citus_BlessTupleDesc(TupleDesc td)
{
	TupleDescFinalize(td);
	return BlessTupleDesc(td);
}


#define BlessTupleDesc(td) citus_BlessTupleDesc(td)

#endif /* PG_VERSION_NUM >= PG_VERSION_19 */

#if PG_VERSION_NUM < PG_VERSION_19

/*
 * PG19 added the pg_fallthrough macro (c.h) for annotating intentional
 * switch fall-throughs, which is required by -Wimplicit-fallthrough=5.
 * Provide the same macro on older PG majors so Citus can annotate
 * fall-throughs uniformly across all supported versions.
 */
#ifndef pg_fallthrough
#if defined(__has_attribute) && __has_attribute(fallthrough)
#define pg_fallthrough __attribute__((fallthrough))
#else
#define pg_fallthrough
#endif
#endif

/*
 * Pre-PG19 the query-level instrumentation lived in QueryDesc->totaltime.
 * Provide the same scoped accessor used on PG19 so call sites are uniform.
 */
#define QueryDescTotalTime(qd) ((qd)->totaltime)

/*
 * PG19 builds rely on TupleDescFinalize() to populate the offset cache; on
 * older majors there is no such cache, so provide a no-op for the explicit
 * call sites that build TupleDescs without going through BlessTupleDesc.
 */
#define TupleDescFinalize(td) ((void) 0)

/*
 * PG<=18 registers tranche names in two steps: allocate the id with
 * LWLockNewTrancheId(), then associate the name via LWLockRegisterTranche().
 * Wrap that dance so call sites use the same one-call LWLockNewTrancheIdCompat()
 * helper as PG19.
 */
static inline int
LWLockNewTrancheIdCompat(const char *name)
{
	int trancheId = LWLockNewTrancheId();

	LWLockRegisterTranche(trancheId, name);
	return trancheId;
}

#endif /* PG_VERSION_NUM < PG_VERSION_19 */

#if PG_VERSION_NUM >= PG_VERSION_18
#define create_foreignscan_path_compat(a, b, c, d, e, f, g, h, i, j, k) \
		create_foreignscan_path( \
			(a),        /* root            */ \
			(b),        /* rel             */ \
			(c),        /* target          */ \
			(d),        /* rows            */ \
			0,          /* disabled_nodes  */ \
			(e),        /* startup_cost    */ \
			(f),        /* total_cost      */ \
			(g),        /* pathkeys        */ \
			(h),        /* required_outer  */ \
			(i),        /* fdw_outerpath   */ \
			(j),        /* fdw_restrictinfo*/ \
			(k)         /* fdw_private     */ \
			)

/* PG-18 introduced get_op_index_interpretation, old name was get_op_btree_interpretation */
#define get_op_btree_interpretation(opno) get_op_index_interpretation(opno)

/* PG-18 unified row-compare operator codes under COMPARE_* */
#define ROWCOMPARE_NE COMPARE_NE

#elif PG_VERSION_NUM >= PG_VERSION_17
#define create_foreignscan_path_compat(a, b, c, d, e, f, g, h, i, j, k) \
		create_foreignscan_path( \
			(a), (b), (c), (d), \
			(e), (f), \
			(g), (h), (i), (j), (k) \
			)

#endif

#if PG_VERSION_NUM >= PG_VERSION_17

#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_conversion.h"
#include "catalog/pg_database.h"
#include "catalog/pg_default_acl.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_init_privs.h"
#include "catalog/pg_language.h"
#include "catalog/pg_largeobject.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_parameter_acl.h"
#include "catalog/pg_policy.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_namespace.h"
#include "catalog/pg_publication_rel.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_subscription.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_transform.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_parser.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"

/*
 * This enum covers all system catalogs whose OIDs can appear in
 * pg_depend.classId or pg_shdepend.classId.
 */
typedef enum ObjectClass
{
	OCLASS_CLASS,               /* pg_class */
	OCLASS_PROC,                /* pg_proc */
	OCLASS_TYPE,                /* pg_type */
	OCLASS_CAST,                /* pg_cast */
	OCLASS_COLLATION,           /* pg_collation */
	OCLASS_CONSTRAINT,          /* pg_constraint */
	OCLASS_CONVERSION,          /* pg_conversion */
	OCLASS_DEFAULT,             /* pg_attrdef */
	OCLASS_LANGUAGE,            /* pg_language */
	OCLASS_LARGEOBJECT,         /* pg_largeobject */
	OCLASS_OPERATOR,            /* pg_operator */
	OCLASS_OPCLASS,             /* pg_opclass */
	OCLASS_OPFAMILY,            /* pg_opfamily */
	OCLASS_AM,                  /* pg_am */
	OCLASS_AMOP,                /* pg_amop */
	OCLASS_AMPROC,              /* pg_amproc */
	OCLASS_REWRITE,             /* pg_rewrite */
	OCLASS_TRIGGER,             /* pg_trigger */
	OCLASS_SCHEMA,              /* pg_namespace */
	OCLASS_STATISTIC_EXT,       /* pg_statistic_ext */
	OCLASS_TSPARSER,            /* pg_ts_parser */
	OCLASS_TSDICT,              /* pg_ts_dict */
	OCLASS_TSTEMPLATE,          /* pg_ts_template */
	OCLASS_TSCONFIG,            /* pg_ts_config */
	OCLASS_ROLE,                /* pg_authid */
	OCLASS_ROLE_MEMBERSHIP,     /* pg_auth_members */
	OCLASS_DATABASE,            /* pg_database */
	OCLASS_TBLSPACE,            /* pg_tablespace */
	OCLASS_FDW,                 /* pg_foreign_data_wrapper */
	OCLASS_FOREIGN_SERVER,      /* pg_foreign_server */
	OCLASS_USER_MAPPING,        /* pg_user_mapping */
	OCLASS_DEFACL,              /* pg_default_acl */
	OCLASS_EXTENSION,           /* pg_extension */
	OCLASS_EVENT_TRIGGER,       /* pg_event_trigger */
	OCLASS_PARAMETER_ACL,       /* pg_parameter_acl */
	OCLASS_POLICY,              /* pg_policy */
	OCLASS_PUBLICATION,         /* pg_publication */
	OCLASS_PUBLICATION_NAMESPACE,   /* pg_publication_namespace */
	OCLASS_PUBLICATION_REL,     /* pg_publication_rel */
	OCLASS_SUBSCRIPTION,        /* pg_subscription */
	OCLASS_TRANSFORM,           /* pg_transform */
} ObjectClass;

#define LAST_OCLASS OCLASS_TRANSFORM

/*
 * Determine the class of a given object identified by objectAddress.
 *
 * We implement it as a function instead of an array because the OIDs aren't
 * consecutive.
 */
static inline ObjectClass
getObjectClass(const ObjectAddress *object)
{
	/* only pg_class entries can have nonzero objectSubId */
	if (object->classId != RelationRelationId &&
		object->objectSubId != 0)
	{
		elog(ERROR, "invalid non-zero objectSubId for object class %u",
			 object->classId);
	}

	switch (object->classId)
	{
		case RelationRelationId:
		{
			/* caller must check objectSubId */
			return OCLASS_CLASS;
		}

		case ProcedureRelationId:
		{
			return OCLASS_PROC;
		}

		case TypeRelationId:
		{
			return OCLASS_TYPE;
		}

		case CastRelationId:
		{
			return OCLASS_CAST;
		}

		case CollationRelationId:
		{
			return OCLASS_COLLATION;
		}

		case ConstraintRelationId:
		{
			return OCLASS_CONSTRAINT;
		}

		case ConversionRelationId:
		{
			return OCLASS_CONVERSION;
		}

		case AttrDefaultRelationId:
		{
			return OCLASS_DEFAULT;
		}

		case LanguageRelationId:
		{
			return OCLASS_LANGUAGE;
		}

		case LargeObjectRelationId:
		{
			return OCLASS_LARGEOBJECT;
		}

		case OperatorRelationId:
		{
			return OCLASS_OPERATOR;
		}

		case OperatorClassRelationId:
		{
			return OCLASS_OPCLASS;
		}

		case OperatorFamilyRelationId:
		{
			return OCLASS_OPFAMILY;
		}

		case AccessMethodRelationId:
		{
			return OCLASS_AM;
		}

		case AccessMethodOperatorRelationId:
		{
			return OCLASS_AMOP;
		}

		case AccessMethodProcedureRelationId:
		{
			return OCLASS_AMPROC;
		}

		case RewriteRelationId:
		{
			return OCLASS_REWRITE;
		}

		case TriggerRelationId:
		{
			return OCLASS_TRIGGER;
		}

		case NamespaceRelationId:
		{
			return OCLASS_SCHEMA;
		}

		case StatisticExtRelationId:
		{
			return OCLASS_STATISTIC_EXT;
		}

		case TSParserRelationId:
		{
			return OCLASS_TSPARSER;
		}

		case TSDictionaryRelationId:
		{
			return OCLASS_TSDICT;
		}

		case TSTemplateRelationId:
		{
			return OCLASS_TSTEMPLATE;
		}

		case TSConfigRelationId:
		{
			return OCLASS_TSCONFIG;
		}

		case AuthIdRelationId:
		{
			return OCLASS_ROLE;
		}

		case AuthMemRelationId:
		{
			return OCLASS_ROLE_MEMBERSHIP;
		}

		case DatabaseRelationId:
		{
			return OCLASS_DATABASE;
		}

		case TableSpaceRelationId:
		{
			return OCLASS_TBLSPACE;
		}

		case ForeignDataWrapperRelationId:
		{
			return OCLASS_FDW;
		}

		case ForeignServerRelationId:
		{
			return OCLASS_FOREIGN_SERVER;
		}

		case UserMappingRelationId:
		{
			return OCLASS_USER_MAPPING;
		}

		case DefaultAclRelationId:
		{
			return OCLASS_DEFACL;
		}

		case ExtensionRelationId:
		{
			return OCLASS_EXTENSION;
		}

		case EventTriggerRelationId:
		{
			return OCLASS_EVENT_TRIGGER;
		}

		case ParameterAclRelationId:
		{
			return OCLASS_PARAMETER_ACL;
		}

		case PolicyRelationId:
		{
			return OCLASS_POLICY;
		}

		case PublicationNamespaceRelationId:
		{
			return OCLASS_PUBLICATION_NAMESPACE;
		}

		case PublicationRelationId:
		{
			return OCLASS_PUBLICATION;
		}

		case PublicationRelRelationId:
		{
			return OCLASS_PUBLICATION_REL;
		}

		case SubscriptionRelationId:
		{
			return OCLASS_SUBSCRIPTION;
		}

		case TransformRelationId:
		{
			return OCLASS_TRANSFORM;
		}
	}

	/* shouldn't get here */
	elog(ERROR, "unrecognized object class: %u", object->classId);
	return OCLASS_CLASS;        /* keep compiler quiet */
}


#include "commands/tablecmds.h"

static inline void
RangeVarCallbackOwnsTable(const RangeVar *relation,
						  Oid relId, Oid oldRelId, void *arg)
{
	return RangeVarCallbackMaintainsTable(relation, relId, oldRelId, arg);
}


#include "catalog/pg_attribute.h"
#include "utils/syscache.h"

static inline int
getAttstattarget_compat(HeapTuple attTuple)
{
	bool isnull;
	Datum dat = SysCacheGetAttr(ATTNUM, attTuple,
								Anum_pg_attribute_attstattarget, &isnull);
	return (isnull ? -1 : DatumGetInt16(dat));
}


#include "catalog/pg_statistic_ext.h"

static inline int
getStxstattarget_compat(HeapTuple tup)
{
	bool isnull;
	Datum dat = SysCacheGetAttr(STATEXTOID, tup,
								Anum_pg_statistic_ext_stxstattarget, &isnull);
	return (isnull ? -1 : DatumGetInt16(dat));
}


#define getAlterStatsStxstattarget_compat(a) ((Node *) makeInteger(a))
#define getIntStxstattarget_compat(a) (intVal(a))

#define WaitEventSetTracker_compat CurrentResourceOwner

#define identitySequenceRelation_compat(a) (a)

#define matched_compat(a) (a->matchKind == MERGE_WHEN_MATCHED)

#define getProcNo_compat(a) (a->vxid.procNumber)
#define getLxid_compat(a) (a->vxid.lxid)

#else

#define Anum_pg_collation_colllocale Anum_pg_collation_colliculocale
#define Anum_pg_database_datlocale Anum_pg_database_daticulocale

#include "access/htup_details.h"
static inline int
getAttstattarget_compat(HeapTuple attTuple)
{
	return ((Form_pg_attribute) GETSTRUCT(attTuple))->attstattarget;
}


#include "catalog/pg_statistic_ext.h"
static inline int
getStxstattarget_compat(HeapTuple tup)
{
	return ((Form_pg_statistic_ext) GETSTRUCT(tup))->stxstattarget;
}


#define getAlterStatsStxstattarget_compat(a) (a)
#define getIntStxstattarget_compat(a) (a)

#define WaitEventSetTracker_compat CurrentMemoryContext

#define identitySequenceRelation_compat(a) (RelationGetRelid(a))

#define matched_compat(a) (a->matched)

#define create_foreignscan_path_compat(a, b, c, d, e, f, g, h, i, j, \
									   k) create_foreignscan_path(a, b, c, d, e, f, g, h, \
																  i, k)

#define getProcNo_compat(a) (a->pgprocno)
#define getLxid_compat(a) (a->lxid)

#define COLLPROVIDER_BUILTIN 'b'

#endif

#define SetListCellPtr(a, b) ((a)->ptr_value = (b))
#define RangeTableEntryFromNSItem(a) ((a)->p_rte)
#define fcGetArgValue(fc, n) ((fc)->args[n].value)
#define fcGetArgNull(fc, n) ((fc)->args[n].isnull)
#define fcSetArgExt(fc, n, val, is_null) \
		(((fc)->args[n].isnull = (is_null)), ((fc)->args[n].value = (val)))
#define fcSetArg(fc, n, value) fcSetArgExt(fc, n, value, false)
#define fcSetArgNull(fc, n) fcSetArgExt(fc, n, (Datum) 0, true)

#define CREATE_SEQUENCE_COMMAND \
		"CREATE %sSEQUENCE IF NOT EXISTS %s AS %s INCREMENT BY " INT64_FORMAT \
		" MINVALUE " INT64_FORMAT " MAXVALUE " INT64_FORMAT \
		" START WITH " INT64_FORMAT " CACHE " INT64_FORMAT " %sCYCLE"

#endif   /* PG_VERSION_COMPAT_H */
