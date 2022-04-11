/*-------------------------------------------------------------------------
 *
 * pg_get_object_address_13_14_15.c
 *
 * Copied functions from Postgres pg_get_object_address with acl/owner check.
 * Since we need to use intermediate data types Relation and Node from
 * the pg_get_object_address, we've copied that function from PG code and
 * added required owner/acl checks for our own purposes.
 *
 * We need to make sure that function works with future PG versions. Update
 * the function name according to supported PG versions as well.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "catalog/objectaddress.h"
#include "catalog/pg_type.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/pg_version_constants.h"
#include "distributed/version_compat.h"
#include "nodes/value.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/varlena.h"
#include "mb/pg_wchar.h"
#include "parser/parse_type.h"

static void ErrorIfCurrentUserCanNotDistributeObject(ObjectType type,
													 ObjectAddress *addr,
													 Node *node,
													 Relation *relation);
static List * textarray_to_strvaluelist(ArrayType *arr);

/*
 * PgGetObjectAddress gets the object address. This function is mostly copied from
 * pg_get_object_address of the PG code. We need to copy that function to use
 * intermediate data types Relation and Node to check acl or ownership.
 *
 * Codes added by Citus are tagged with CITUS CODE BEGIN/END.
 */
ObjectAddress
PgGetObjectAddress(char *ttype, ArrayType *namearr, ArrayType *argsarr)
{
	List *name = NIL;
	TypeName *typename = NULL;
	List *args = NIL;
	Node *objnode = NULL;
	Relation relation;

	/* Decode object type, raise error if unknown */
	int itype = read_objtype_from_string(ttype);
	if (itype < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported object type \"%s\"", ttype)));
	}
	ObjectType type = (ObjectType) itype;

	/*
	 * Convert the text array to the representation appropriate for the given
	 * object type.  Most use a simple string Values list, but there are some
	 * exceptions.
	 */
	if (type == OBJECT_TYPE || type == OBJECT_DOMAIN || type == OBJECT_CAST ||
		type == OBJECT_TRANSFORM || type == OBJECT_DOMCONSTRAINT)
	{
		Datum *elems;
		bool *nulls;
		int nelems;

		deconstruct_array(namearr, TEXTOID, -1, false, TYPALIGN_INT,
						  &elems, &nulls, &nelems);
		if (nelems != 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("name list length must be exactly %d", 1)));
		}
		if (nulls[0])
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("name or argument lists may not contain nulls")));
		}
		typename = typeStringToTypeName(TextDatumGetCString(elems[0]));
	}
	else if (type == OBJECT_LARGEOBJECT)
	{
		Datum *elems;
		bool *nulls;
		int nelems;

		deconstruct_array(namearr, TEXTOID, -1, false, TYPALIGN_INT,
						  &elems, &nulls, &nelems);
		if (nelems != 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("name list length must be exactly %d", 1)));
		}
		if (nulls[0])
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("large object OID may not be null")));
		}
		objnode = (Node *) makeFloat(TextDatumGetCString(elems[0]));
	}
	else
	{
		name = textarray_to_strvaluelist(namearr);
		if (list_length(name) < 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("name list length must be at least %d", 1)));
		}
	}

	/*
	 * If args are given, decode them according to the object type.
	 */
	if (type == OBJECT_AGGREGATE ||
		type == OBJECT_FUNCTION ||
		type == OBJECT_PROCEDURE ||
		type == OBJECT_ROUTINE ||
		type == OBJECT_OPERATOR ||
		type == OBJECT_CAST ||
		type == OBJECT_AMOP ||
		type == OBJECT_AMPROC)
	{
		/* in these cases, the args list must be of TypeName */
		Datum *elems;
		bool *nulls;
		int nelems;
		int i;

		deconstruct_array(argsarr, TEXTOID, -1, false,
						  TYPALIGN_INT,
						  &elems, &nulls, &nelems);

		args = NIL;
		for (i = 0; i < nelems; i++)
		{
			if (nulls[i])
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("name or argument lists may not contain nulls")));
			}
			args = lappend(args,
						   typeStringToTypeName(TextDatumGetCString(elems[i])));
		}
	}
	else
	{
		/* For all other object types, use string Values */
		args = textarray_to_strvaluelist(argsarr);
	}

	/*
	 * get_object_address is pretty sensitive to the length of its input
	 * lists; check that they're what it wants.
	 */
	switch (type)
	{
		case OBJECT_DOMCONSTRAINT:
		case OBJECT_CAST:
		case OBJECT_USER_MAPPING:
		case OBJECT_PUBLICATION_REL:
		case OBJECT_DEFACL:
		case OBJECT_TRANSFORM:
		{
			if (list_length(args) != 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument list length must be exactly %d", 1)));
			}
			break;
		}

		case OBJECT_OPFAMILY:
		case OBJECT_OPCLASS:
		{
			if (list_length(name) < 2)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("name list length must be at least %d", 2)));
			}
			break;
		}

		case OBJECT_AMOP:
		case OBJECT_AMPROC:
		{
			if (list_length(name) < 3)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("name list length must be at least %d", 3)));
			}

			if (list_length(args) != 2)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument list length must be exactly %d", 2)));
			}
			break;
		}

		case OBJECT_OPERATOR:
		{
			if (list_length(args) != 2)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument list length must be exactly %d", 2)));
			}
			break;
		}

		default:
		{
			break;
		}
	}

	/*
	 * Now build the Node type that get_object_address() expects for the given
	 * type.
	 */
	switch (type)
	{
		case OBJECT_TABLE:
		case OBJECT_SEQUENCE:
		case OBJECT_VIEW:
		case OBJECT_MATVIEW:
		case OBJECT_INDEX:
		case OBJECT_FOREIGN_TABLE:
		case OBJECT_COLUMN:
		case OBJECT_ATTRIBUTE:
		case OBJECT_COLLATION:
		case OBJECT_CONVERSION:
		case OBJECT_STATISTIC_EXT:
		case OBJECT_TSPARSER:
		case OBJECT_TSDICTIONARY:
		case OBJECT_TSTEMPLATE:
		case OBJECT_TSCONFIGURATION:
		case OBJECT_DEFAULT:
		case OBJECT_POLICY:
		case OBJECT_RULE:
		case OBJECT_TRIGGER:
		case OBJECT_TABCONSTRAINT:
		case OBJECT_OPCLASS:
		case OBJECT_OPFAMILY:
		{
			objnode = (Node *) name;
			break;
		}

		case OBJECT_ACCESS_METHOD:
		case OBJECT_DATABASE:
		case OBJECT_EVENT_TRIGGER:
		case OBJECT_EXTENSION:
		case OBJECT_FDW:
		case OBJECT_FOREIGN_SERVER:
		case OBJECT_LANGUAGE:
#if PG_VERSION_NUM >= PG_VERSION_15
		case OBJECT_PARAMETER_ACL:
#endif
		case OBJECT_PUBLICATION:
		case OBJECT_ROLE:
		case OBJECT_SCHEMA:
		case OBJECT_SUBSCRIPTION:
		case OBJECT_TABLESPACE:
		{
			if (list_length(name) != 1)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("name list length must be exactly %d", 1)));
			}
			objnode = linitial(name);
			break;
		}

		case OBJECT_TYPE:
		case OBJECT_DOMAIN:
		{
			objnode = (Node *) typename;
			break;
		}

		case OBJECT_CAST:
		case OBJECT_DOMCONSTRAINT:
		case OBJECT_TRANSFORM:
		{
			objnode = (Node *) list_make2(typename, linitial(args));
			break;
		}

		case OBJECT_PUBLICATION_REL:
		{
			objnode = (Node *) list_make2(name, linitial(args));
			break;
		}

#if PG_VERSION_NUM >= PG_VERSION_15
		case OBJECT_PUBLICATION_NAMESPACE:
#endif
		case OBJECT_USER_MAPPING:
		{
			objnode = (Node *) list_make2(linitial(name), linitial(args));
			break;
		}

		case OBJECT_DEFACL:
		{
			objnode = (Node *) lcons(linitial(args), name);
			break;
		}

		case OBJECT_AMOP:
		case OBJECT_AMPROC:
		{
			objnode = (Node *) list_make2(name, args);
			break;
		}

		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		case OBJECT_ROUTINE:
		case OBJECT_AGGREGATE:
		case OBJECT_OPERATOR:
		{
			ObjectWithArgs *owa = makeNode(ObjectWithArgs);

			owa->objname = name;
			owa->objargs = args;
			objnode = (Node *) owa;
			break;
		}

		case OBJECT_LARGEOBJECT:
		{
			/* already handled above */
			break;
		}

			/* no default, to let compiler warn about missing case */
	}

	if (objnode == NULL)
	{
		elog(ERROR, "unrecognized object type: %d", type);
	}

	ObjectAddress addr = get_object_address(type, objnode,
											&relation, AccessShareLock, false);

	/* CITUS CODE BEGIN */
	ErrorIfCurrentUserCanNotDistributeObject(type, &addr, objnode, &relation);

	/* CITUS CODE END */

	/* We don't need the relcache entry, thank you very much */
	if (relation)
	{
		relation_close(relation, AccessShareLock);
	}

	/* CITUS CODE BEGIN */
	return addr;

	/* CITUS CODE END */
}


/*
 * ErrorIfCurrentUserCanNotDistributeObject checks whether current user can
 * distribute object, if not errors out.
 */
static void
ErrorIfCurrentUserCanNotDistributeObject(ObjectType type, ObjectAddress *addr,
										 Node *node, Relation *relation)
{
	Oid userId = GetUserId();

	if (!SupportedDependencyByCitus(addr))
	{
		ereport(ERROR, (errmsg("Object type %d can not be distributed by Citus", type)));
	}

	switch (type)
	{
		case OBJECT_SCHEMA:
		case OBJECT_DATABASE:
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		case OBJECT_AGGREGATE:
		case OBJECT_TSCONFIGURATION:
		case OBJECT_TSDICTIONARY:
		case OBJECT_TYPE:
		case OBJECT_FOREIGN_SERVER:
		case OBJECT_SEQUENCE:
		case OBJECT_FOREIGN_TABLE:
		case OBJECT_TABLE:
		case OBJECT_EXTENSION:
		case OBJECT_COLLATION:
		{
			check_object_ownership(userId, type, *addr, node, *relation);
			break;
		}

		case OBJECT_ROLE:
		{
			/* Support only extension owner role with community */
			if (addr->objectId != CitusExtensionOwner())
			{
				ereport(ERROR, (errmsg("Current user does not have required "
									   "access privileges on role %d with type %d",
									   addr->objectId, type)));
			}
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("%d object type is not supported within "
								   "object propagation", type)));
			break;
		}
	}
}


/*
 * Copied from PG code.
 *
 * Convert an array of TEXT into a List of string Values, as emitted by the
 * parser, which is what get_object_address uses as input.
 */
static List *
textarray_to_strvaluelist(ArrayType *arr)
{
	Datum *elems;
	bool *nulls;
	int nelems;
	List *list = NIL;
	int i;

	deconstruct_array(arr, TEXTOID, -1, false, TYPALIGN_INT,
					  &elems, &nulls, &nelems);

	for (i = 0; i < nelems; i++)
	{
		if (nulls[i])
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("name or argument lists may not contain nulls")));
		}
		list = lappend(list, makeString(TextDatumGetCString(elems[i])));
	}

	return list;
}
