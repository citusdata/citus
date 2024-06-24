/*
 * citus_depended_object.c
 *
 * Implements udf function related to hiding citus depended objects while executing
 * postgres vanilla tests.
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h"

#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_attrdef.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_event_trigger.h"
#include "catalog/pg_language.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_opfamily.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_rewrite.h"
#include "catalog/pg_sequence.h"
#include "catalog/pg_statistic.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_ts_template.h"
#include "catalog/pg_type.h"

#include "distributed/citus_depended_object.h"
#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"

static bool IsCitusDependentObject(ObjectAddress objectAddress);

PG_FUNCTION_INFO_V1(is_citus_depended_object);

/*
 * is_citus_depended_object a wrapper around IsCitusDependentObject, so
 * see the details there.
 *
 * The first parameter expects an oid for
 * a pg meta class, and the second parameter expects an oid for
 * the object which is found in the pg meta class.
 */
Datum
is_citus_depended_object(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
	{
		/* Because we want to return false for null arguments, we donot use strict keyword while creating that function. */
		PG_RETURN_BOOL(false);
	}

	Oid metaTableId = PG_GETARG_OID(0);
	Oid objectId = PG_GETARG_OID(1);

	if (!OidIsValid(metaTableId) || !OidIsValid(objectId))
	{
		/* we cannot continue without valid meta table or object oid */
		PG_RETURN_BOOL(false);
	}

	bool dependsOnCitus = false;

	ObjectAddress objectAdress = { metaTableId, objectId, 0 };

	switch (metaTableId)
	{
		case ProcedureRelationId:
		case AccessMethodRelationId:
		case EventTriggerRelationId:
		case TriggerRelationId:
		case OperatorRelationId:
		case OperatorClassRelationId:
		case OperatorFamilyRelationId:
		case AccessMethodOperatorRelationId:
		case AccessMethodProcedureRelationId:
		case TSConfigRelationId:
		case TSTemplateRelationId:
		case TSDictionaryRelationId:
		case LanguageRelationId:
		case RewriteRelationId:
		case AttrDefaultRelationId:
		case NamespaceRelationId:
		case ConstraintRelationId:
		case TypeRelationId:
		case RelationRelationId:
		{
			/* meta classes that access their own oid */
			dependsOnCitus = IsCitusDependentObject(objectAdress);
			break;
		}

		case EnumRelationId:
		{
			/*
			 * we do not directly access the oid in pg_enum,
			 * because it does not exist in pg_depend, but its type does
			 */
			objectAdress.classId = TypeRelationId;
			dependsOnCitus = IsCitusDependentObject(objectAdress);
			break;
		}

		case IndexRelationId:
		case AttributeRelationId:
		case SequenceRelationId:
		case StatisticRelationId:
		{
			/* meta classes that access their relation's oid */
			objectAdress.classId = RelationRelationId;
			dependsOnCitus = IsCitusDependentObject(objectAdress);
			break;
		}

		case AggregateRelationId:
		{
			/* We access procedure oid for aggregates. */
			objectAdress.classId = ProcedureRelationId;
			dependsOnCitus = IsCitusDependentObject(objectAdress);
			break;
		}

		default:
		{
			break;
		}
	}

	PG_RETURN_BOOL(dependsOnCitus);
}


/*
 * IsCitusDependentObject returns true if the given object depends on the citus extension.
 */
static bool
IsCitusDependentObject(ObjectAddress objectAddress)
{
	if (IsObjectAddressOwnedByCitus(&objectAddress))
	{
		/* object itself is owned by citus */
		return true;
	}

	/* check if object's any dependency is owned by citus. */
	List *citusDependencies = GetAllCitusDependedDependenciesForObject(&objectAddress);
	return list_length(citusDependencies) > 0;
}
