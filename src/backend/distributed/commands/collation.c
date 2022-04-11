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

#include "pg_version_compat.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/pg_collation.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"
#include "distributed/metadata/dependency.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/worker_create_or_replace.h"
#include "distributed/pg_version_constants.h"
#include "distributed/worker_manager.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "miscadmin.h"


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

#if PG_VERSION_NUM >= PG_VERSION_15
	bool isnull;
	Datum datum = SysCacheGetAttr(COLLOID, heapTuple, Anum_pg_collation_collcollate,
								  &isnull);
	Assert(!isnull);
	char *collcollate = TextDatumGetCString(datum);
	datum = SysCacheGetAttr(COLLOID, heapTuple, Anum_pg_collation_collctype, &isnull);
	Assert(!isnull);
	char *collctype = TextDatumGetCString(datum);
#else

	/*
	 * In versions before 15, collcollate and collctype were type "name". Use
	 * pstrdup() to match the interface of 15 so that we consistently free the
	 * result later.
	 */
	char *collcollate = pstrdup(NameStr(collationForm->collcollate));
	char *collctype = pstrdup(NameStr(collationForm->collctype));
#endif

	if (collowner != NULL)
	{
		*collowner = collationForm->collowner;
	}

	ReleaseSysCache(heapTuple);
	char *schemaName = get_namespace_name(collnamespace);
	*quotedCollationName = quote_qualified_identifier(schemaName, collname);
	const char *providerString =
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

	if (strcmp(collcollate, collctype))
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


ObjectAddress
AlterCollationOwnerObjectAddress(Node *node, bool missing_ok)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Relation relation;

	Assert(stmt->objectType == OBJECT_COLLATION);

	return get_object_address(stmt->objectType, stmt->object, &relation,
							  AccessExclusiveLock, missing_ok);
}


/*
 * RenameCollationStmtObjectAddress returns the ObjectAddress of the type that is the object
 * of the RenameStmt. Errors if missing_ok is false.
 */
ObjectAddress
RenameCollationStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_COLLATION);

	Oid collationOid = get_collation_oid((List *) stmt->object, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, CollationRelationId, collationOid);

	return address;
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
ObjectAddress
AlterCollationSchemaStmtObjectAddress(Node *node, bool missing_ok)
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

	ObjectAddress address = { 0 };
	ObjectAddressSet(address, CollationRelationId, collationOid);
	return address;
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


ObjectAddress
DefineCollationStmtObjectAddress(Node *node, bool missing_ok)
{
	DefineStmt *stmt = castNode(DefineStmt, node);
	Assert(stmt->kind == OBJECT_COLLATION);

	Oid collOid = get_collation_oid(stmt->defnames, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, CollationRelationId, collOid);

	return address;
}
