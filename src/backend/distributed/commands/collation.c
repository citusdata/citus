/*-------------------------------------------------------------------------
 * collation.c
 *
 * This file contains functions to create, alter and drop policies on
 * distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "pg_version_compat.h"
#include "pg_version_constants.h"

#include "distributed/citus_safe_lib.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_create_or_replace.h"
#include "distributed/worker_manager.h"


static char * CreateCollationDDLInternal(Oid collationId, Oid *collowner,
										 char **quotedCollationName);

/*
 * GetCreateCollationDDLInternal returns a CREATE COLLATE sql string for the
 * given collationId.
 *
 * It includes 2 out parameters to assist creation of ALTER COLLATION OWNER.
 * quotedCollationName must not be NULL.
 */
static char *
CreateCollationDDLInternal(Oid collationId, Oid *collowner, char **quotedCollationName)
{
	StringInfoData collationNameDef;

	HeapTuple heapTuple = SearchSysCache1(COLLOID, ObjectIdGetDatum(collationId));
	if (!HeapTupleIsValid(heapTuple))
	{
		elog(ERROR, "citus cache lookup failed for collation %u", collationId);
	}

	Form_pg_collation collationForm = (Form_pg_collation) GETSTRUCT(heapTuple);
	char collprovider = collationForm->collprovider;
	Oid collnamespace = collationForm->collnamespace;
	const char *collname = NameStr(collationForm->collname);
	bool collisdeterministic = collationForm->collisdeterministic;

	char *collcollate;
	char *collctype;

	/*
	 * In PG15, there is an added option to use ICU as global locale provider.
	 * pg_collation has three locale-related fields: collcollate and collctype,
	 * which are libc-related fields, and a new one colliculocale, which is the
	 * ICU-related field. Only the libc-related fields or the ICU-related field
	 * is set, never both.
	 */
	char *colllocale;
	bool isnull;

	Datum datum = SysCacheGetAttr(COLLOID, heapTuple, Anum_pg_collation_collcollate,
								  &isnull);
	if (!isnull)
	{
		collcollate = TextDatumGetCString(datum);
	}
	else
	{
		collcollate = NULL;
	}

	datum = SysCacheGetAttr(COLLOID, heapTuple, Anum_pg_collation_collctype, &isnull);
	if (!isnull)
	{
		collctype = TextDatumGetCString(datum);
	}
	else
	{
		collctype = NULL;
	}

	datum = SysCacheGetAttr(COLLOID, heapTuple, Anum_pg_collation_colllocale, &isnull);
	if (!isnull)
	{
		colllocale = TextDatumGetCString(datum);
	}
	else
	{
		colllocale = NULL;
	}

	Assert((collcollate && collctype) || colllocale);

	if (collowner != NULL)
	{
		*collowner = collationForm->collowner;
	}

	ReleaseSysCache(heapTuple);
	char *schemaName = get_namespace_name(collnamespace);
	*quotedCollationName = quote_qualified_identifier(schemaName, collname);
	const char *providerString =
		collprovider == COLLPROVIDER_BUILTIN ? "builtin" :
		collprovider == COLLPROVIDER_DEFAULT ? "default" :
		collprovider == COLLPROVIDER_ICU ? "icu" :
		collprovider == COLLPROVIDER_LIBC ? "libc" : NULL;

	if (providerString == NULL)
	{
		elog(ERROR, "unknown collation provider: %c", collprovider);
	}

	initStringInfo(&collationNameDef);
	appendStringInfo(&collationNameDef,
					 "CREATE COLLATION %s (provider = '%s'",
					 *quotedCollationName, providerString);

	if (colllocale)
	{
		appendStringInfo(&collationNameDef,
						 ", locale = %s",
						 quote_literal_cstr(colllocale));
		pfree(colllocale);
	}
	else
	{
		if (strcmp(collcollate, collctype) == 0)
		{
			appendStringInfo(&collationNameDef,
							 ", locale = %s",
							 quote_literal_cstr(collcollate));
		}
		else
		{
			appendStringInfo(&collationNameDef,
							 ", lc_collate = %s, lc_ctype = %s",
							 quote_literal_cstr(collcollate),
							 quote_literal_cstr(collctype));
		}
		pfree(collcollate);
		pfree(collctype);
	}

#if PG_VERSION_NUM >= PG_VERSION_16
	char *collicurules = NULL;
	datum = SysCacheGetAttr(COLLOID, heapTuple, Anum_pg_collation_collicurules, &isnull);
	if (!isnull)
	{
		collicurules = TextDatumGetCString(datum);
		appendStringInfo(&collationNameDef, ", rules = %s",
						 quote_literal_cstr(collicurules));
	}
#endif
	if (!collisdeterministic)
	{
		appendStringInfoString(&collationNameDef, ", deterministic = false");
	}


	appendStringInfoChar(&collationNameDef, ')');
	return collationNameDef.data;
}


/*
 * CreateCollationDDL wrap CreateCollationDDLInternal to hide the out parameters.
 */
char *
CreateCollationDDL(Oid collationId)
{
	char *quotedCollationName = NULL;
	return CreateCollationDDLInternal(collationId, NULL, &quotedCollationName);
}


/*
 * CreateCollationDDLsIdempotent returns a List of cstrings for creating the collation
 * using create_or_replace_object & includes ALTER COLLATION ROLE.
 */
List *
CreateCollationDDLsIdempotent(Oid collationId)
{
	StringInfoData collationAlterOwnerCommand;
	Oid collowner = InvalidOid;
	char *quotedCollationName = NULL;
	char *createCollationCommand = CreateCollationDDLInternal(collationId, &collowner,
															  &quotedCollationName);

	initStringInfo(&collationAlterOwnerCommand);
	appendStringInfo(&collationAlterOwnerCommand,
					 "ALTER COLLATION %s OWNER TO %s",
					 quotedCollationName,
					 quote_identifier(GetUserNameFromId(collowner, false)));

	return list_make2(WrapCreateOrReplace(createCollationCommand),
					  collationAlterOwnerCommand.data);
}


List *
AlterCollationOwnerObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Relation relation;

	Assert(stmt->objectType == OBJECT_COLLATION);

	ObjectAddress objectAddress = get_object_address(stmt->objectType, stmt->object,
													 &relation, AccessExclusiveLock,
													 missing_ok);

	ObjectAddress *objectAddressCopy = palloc0(sizeof(ObjectAddress));
	*objectAddressCopy = objectAddress;
	return list_make1(objectAddressCopy);
}


/*
 * RenameCollationStmtObjectAddress returns the ObjectAddress of the type that is the object
 * of the RenameStmt. Errors if missing_ok is false.
 */
List *
RenameCollationStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_COLLATION);

	Oid collationOid = get_collation_oid((List *) stmt->object, missing_ok);
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, CollationRelationId, collationOid);

	return list_make1(address);
}


/*
 * AlterCollationSchemaStmtObjectAddress returns the ObjectAddress of the type that is the
 * subject of the AlterObjectSchemaStmt. Errors if missing_ok is false.
 *
 * This could be called both before or after it has been applied locally. It will look in
 * the old schema first, if the type cannot be found in that schema it will look in the
 * new schema. Errors if missing_ok is false and the type cannot be found in either of the
 * schemas.
 */
List *
AlterCollationSchemaStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_COLLATION);

	List *name = (List *) stmt->object;
	Oid collationOid = get_collation_oid(name, true);

	if (collationOid == InvalidOid)
	{
		List *newName = list_make2(makeString(stmt->newschema), lfirst(list_tail(name)));

		collationOid = get_collation_oid(newName, true);

		if (!missing_ok && collationOid == InvalidOid)
		{
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("type \"%s\" does not exist",
								   NameListToString(name))));
		}
	}

	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, CollationRelationId, collationOid);
	return list_make1(address);
}


/*
 * GenerateBackupNameForCollationCollision generates a new collation name for an existing collation.
 * The name is generated in such a way that the new name doesn't overlap with an existing collation
 * by adding a suffix with incrementing number after the new name.
 */
char *
GenerateBackupNameForCollationCollision(const ObjectAddress *address)
{
	char *newName = palloc0(NAMEDATALEN);
	char suffix[NAMEDATALEN] = { 0 };
	int count = 0;
	char *baseName = get_collation_name(address->objectId);
	int baseLength = strlen(baseName);
	HeapTuple collationTuple = SearchSysCache1(COLLOID, address->objectId);

	if (!HeapTupleIsValid(collationTuple))
	{
		elog(ERROR, "citus cache lookup failed");
		return NULL;
	}
	Form_pg_collation collationForm = (Form_pg_collation) GETSTRUCT(collationTuple);
	String *namespace = makeString(get_namespace_name(collationForm->collnamespace));
	ReleaseSysCache(collationTuple);

	while (true)
	{
		int suffixLength = SafeSnprintf(suffix, NAMEDATALEN - 1, "(citus_backup_%d)",
										count);

		/* trim the base name at the end to leave space for the suffix and trailing \0 */
		baseLength = Min(baseLength, NAMEDATALEN - suffixLength - 1);

		/* clear newName before copying the potentially trimmed baseName and suffix */
		memset(newName, 0, NAMEDATALEN);
		strncpy_s(newName, NAMEDATALEN, baseName, baseLength);
		strncpy_s(newName + baseLength, NAMEDATALEN - baseLength, suffix,
				  suffixLength);

		List *newCollationName = list_make2(namespace, makeString(newName));

		/* don't need to rename if the input arguments don't match */
		Oid existingCollationId = get_collation_oid(newCollationName, true);

		if (existingCollationId == InvalidOid)
		{
			return newName;
		}

		count++;
	}
}


List *
DefineCollationStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	DefineStmt *stmt = castNode(DefineStmt, node);
	Assert(stmt->kind == OBJECT_COLLATION);

	Oid collOid = get_collation_oid(stmt->defnames, missing_ok);
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, CollationRelationId, collOid);

	return list_make1(address);
}
