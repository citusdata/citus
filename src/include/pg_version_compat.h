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

#include "pg_version_constants.h"

#if PG_VERSION_NUM >= PG_VERSION_17

#include "catalog/pg_am.h"
#include "catalog/pg_auth_members.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_database.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_parameter_acl.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_transform.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"

typedef int ObjectClass;
#define getObjectClass(a) a->classId
#define LAST_OCLASS TransformRelationId
#define OCLASS_ROLE AuthIdRelationId
#define OCLASS_DATABASE DatabaseRelationId
#define OCLASS_TBLSPACE TableSpaceRelationId
#define OCLASS_PARAMETER_ACL ParameterAclRelationId
#define OCLASS_ROLE_MEMBERSHIP AuthMemRelationId
#define OCLASS_CLASS RelationRelationId
#define OCLASS_COLLATION CollationRelationId
#define OCLASS_CONSTRAINT ConstraintRelationId
#define OCLASS_PROC ProcedureRelationId
#define OCLASS_PUBLICATION PublicationRelationId
#define OCLASS_SCHEMA NamespaceRelationId
#define OCLASS_TSCONFIG TSConfigRelationId
#define OCLASS_TSDICT TSDictionaryRelationId
#define OCLASS_TYPE TypeRelationId
#define OCLASS_EXTENSION ExtensionRelationId
#define OCLASS_FOREIGN_SERVER ForeignServerRelationId
#define OCLASS_AM AccessMethodRelationId
#define OCLASS_TSTEMPLATE TSTemplateRelationId

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

#define create_foreignscan_path_compat(a, b, c, d, e, f, g, h, i, j, \
									   k) create_foreignscan_path(a, b, c, d, e, f, g, h, \
																  i, j, k)

#define getProcNo_compat(a) (a->vxid.procNumber)
#define getLxid_compat(a) (a->vxid.lxid)

#else

#define Anum_pg_collation_colllocale Anum_pg_collation_colliculocale

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

#if PG_VERSION_NUM >= PG_VERSION_16

#include "utils/guc_tables.h"

#define pg_clean_ascii_compat(a, b) pg_clean_ascii(a, b)

#define RelationPhysicalIdentifier_compat(a) ((a)->rd_locator)
#define RelationTablespace_compat(a) (a.spcOid)
#define RelationPhysicalIdentifierNumber_compat(a) (a.relNumber)
#define RelationPhysicalIdentifierNumberPtr_compat(a) (a->relNumber)
#define RelationPhysicalIdentifierBackend_compat(a) (a->smgr_rlocator.locator)

#define float_abs(a) fabs(a)

#define tuplesort_getdatum_compat(a, b, c, d, e, f) tuplesort_getdatum(a, b, c, d, e, f)

static inline struct config_generic **
get_guc_variables_compat(int *gucCount)
{
	return get_guc_variables(gucCount);
}


#define PG_FUNCNAME_MACRO __func__

#define stringToQualifiedNameList_compat(a) stringToQualifiedNameList(a, NULL)
#define typeStringToTypeName_compat(a, b) typeStringToTypeName(a, b)

#define get_relids_in_jointree_compat(a, b, c) get_relids_in_jointree(a, b, c)

#define object_ownercheck(a, b, c) object_ownercheck(a, b, c)
#define object_aclcheck(a, b, c, d) object_aclcheck(a, b, c, d)

#define pgstat_fetch_stat_local_beentry(a) pgstat_get_local_beentry_by_index(a)

#else

#include "catalog/pg_class_d.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc_d.h"
#include "storage/relfilenode.h"
#include "utils/guc.h"
#include "utils/guc_tables.h"

#define pg_clean_ascii_compat(a, b) pg_clean_ascii(a)

#define RelationPhysicalIdentifier_compat(a) ((a)->rd_node)
#define RelationTablespace_compat(a) (a.spcNode)
#define RelationPhysicalIdentifierNumber_compat(a) (a.relNode)
#define RelationPhysicalIdentifierNumberPtr_compat(a) (a->relNode)
#define RelationPhysicalIdentifierBackend_compat(a) (a->smgr_rnode.node)
typedef RelFileNode RelFileLocator;
typedef Oid RelFileNumber;
#define RelidByRelfilenumber(a, b) RelidByRelfilenode(a, b)

#define float_abs(a) Abs(a)

#define tuplesort_getdatum_compat(a, b, c, d, e, f) tuplesort_getdatum(a, b, d, e, f)

static inline struct config_generic **
get_guc_variables_compat(int *gucCount)
{
	*gucCount = GetNumConfigOptions();
	return get_guc_variables();
}


#define stringToQualifiedNameList_compat(a) stringToQualifiedNameList(a)
#define typeStringToTypeName_compat(a, b) typeStringToTypeName(a)

#define get_relids_in_jointree_compat(a, b, c) get_relids_in_jointree(a, b)

static inline bool
object_ownercheck(Oid classid, Oid objectid, Oid roleid)
{
	switch (classid)
	{
		case RelationRelationId:
		{
			return pg_class_ownercheck(objectid, roleid);
		}

		case NamespaceRelationId:
		{
			return pg_namespace_ownercheck(objectid, roleid);
		}

		case ProcedureRelationId:
		{
			return pg_proc_ownercheck(objectid, roleid);
		}

		default:
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Missing classid:%d",
																	classid)));
		}
	}
}


static inline AclResult
object_aclcheck(Oid classid, Oid objectid, Oid roleid, AclMode mode)
{
	switch (classid)
	{
		case NamespaceRelationId:
		{
			return pg_namespace_aclcheck(objectid, roleid, mode);
		}

		case ProcedureRelationId:
		{
			return pg_proc_aclcheck(objectid, roleid, mode);
		}

		default:
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmsg("Missing classid:%d",
																	classid)));
		}
	}
}


typedef bool TU_UpdateIndexes;

/*
 * we define RTEPermissionInfo for PG16 compatibility
 * There are some functions that need to include RTEPermissionInfo in their signature
 * for PG14/PG15 we pass a NULL argument in these functions
 */
typedef RangeTblEntry RTEPermissionInfo;

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
