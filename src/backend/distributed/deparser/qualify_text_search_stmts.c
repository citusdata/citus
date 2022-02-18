/*-------------------------------------------------------------------------
 *
 * qualify_text_search_stmts.c
 *	  Functions specialized in fully qualifying all text search statements. These
 *	  functions are dispatched from qualify.c
 *
 *	  Fully qualifying text search statements consists of adding the schema name
 *	  to the subject of the types as well as any other branch of the parsetree.
 *
 *	  Goal would be that the deparser functions for these statements can
 *	  serialize the statement without any external lookups.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/deparser.h"
#include "distributed/listutils.h"

static Oid get_ts_config_namespace(Oid tsconfigOid);
static Oid get_ts_dict_namespace(Oid tsdictOid);


/*
 * QualifyDropTextSearchConfigurationStmt adds any missing schema names to text search
 * configurations being dropped. All configurations are expected to exists before fully
 * qualifying the statement. Errors will be raised for objects not existing. Non-existing
 * objects are expected to not be distributed.
 */
void
QualifyDropTextSearchConfigurationStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	Assert(stmt->removeType == OBJECT_TSCONFIGURATION);

	List *qualifiedObjects = NIL;
	List *objName = NIL;

	foreach_ptr(objName, stmt->objects)
	{
		char *schemaName = NULL;
		char *tsconfigName = NULL;
		DeconstructQualifiedName(objName, &schemaName, &tsconfigName);

		if (!schemaName)
		{
			Oid tsconfigOid = get_ts_config_oid(objName, false);
			Oid namespaceOid = get_ts_config_namespace(tsconfigOid);
			schemaName = get_namespace_name(namespaceOid);

			objName = list_make2(makeString(schemaName),
								 makeString(tsconfigName));
		}

		qualifiedObjects = lappend(qualifiedObjects, objName);
	}

	stmt->objects = qualifiedObjects;
}


/*
 * QualifyAlterTextSearchConfigurationStmt adds the schema name (if missing) to the name
 * of the text search configurations, as well as the dictionaries referenced.
 */
void
QualifyAlterTextSearchConfigurationStmt(Node *node)
{
	AlterTSConfigurationStmt *stmt = castNode(AlterTSConfigurationStmt, node);

	char *schemaName = NULL;
	char *objName = NULL;
	DeconstructQualifiedName(stmt->cfgname, &schemaName, &objName);

	/* fully qualify the cfgname being altered */
	if (!schemaName)
	{
		Oid tsconfigOid = get_ts_config_oid(stmt->cfgname, false);
		Oid namespaceOid = get_ts_config_namespace(tsconfigOid);
		schemaName = get_namespace_name(namespaceOid);

		stmt->cfgname = list_make2(makeString(schemaName),
								   makeString(objName));
	}

	/* fully qualify the dicts */
	bool useNewDicts = false;
	List *dicts = NULL;
	List *dictName = NIL;
	foreach_ptr(dictName, stmt->dicts)
	{
		DeconstructQualifiedName(dictName, &schemaName, &objName);

		/* fully qualify the cfgname being altered */
		if (!schemaName)
		{
			Oid dictOid = get_ts_dict_oid(dictName, false);
			Oid namespaceOid = get_ts_dict_namespace(dictOid);
			schemaName = get_namespace_name(namespaceOid);

			useNewDicts = true;
			dictName = list_make2(makeString(schemaName), makeString(objName));
		}

		dicts = lappend(dicts, dictName);
	}

	if (useNewDicts)
	{
		/* swap original dicts with the new list */
		stmt->dicts = dicts;
	}
	else
	{
		/* we don't use the new list, everything was already qualified, free-ing */
		list_free(dicts);
	}
}


/*
 * QualifyRenameTextSearchConfigurationStmt adds the schema name (if missing) to the
 * configuration being renamed. The new name will kept be without schema name since this
 * command cannot be used to change the schema of a configuration.
 */
void
QualifyRenameTextSearchConfigurationStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_TSCONFIGURATION);

	char *schemaName = NULL;
	char *objName = NULL;
	DeconstructQualifiedName(castNode(List, stmt->object), &schemaName, &objName);

	/* fully qualify the cfgname being altered */
	if (!schemaName)
	{
		Oid tsconfigOid = get_ts_config_oid(castNode(List, stmt->object), false);
		Oid namespaceOid = get_ts_config_namespace(tsconfigOid);
		schemaName = get_namespace_name(namespaceOid);

		stmt->object = (Node *) list_make2(makeString(schemaName),
										   makeString(objName));
	}
}


/*
 * QualifyAlterTextSearchConfigurationSchemaStmt adds the schema name (if missing) for the
 * text search being moved to a new schema.
 */
void
QualifyAlterTextSearchConfigurationSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TSCONFIGURATION);

	char *schemaName = NULL;
	char *objName = NULL;
	DeconstructQualifiedName(castNode(List, stmt->object), &schemaName, &objName);

	if (!schemaName)
	{
		Oid tsconfigOid = get_ts_config_oid(castNode(List, stmt->object), false);
		Oid namespaceOid = get_ts_config_namespace(tsconfigOid);
		schemaName = get_namespace_name(namespaceOid);

		stmt->object = (Node *) list_make2(makeString(schemaName),
										   makeString(objName));
	}
}


/*
 * QualifyTextSearchConfigurationCommentStmt adds the schema name (if missing) to the
 * configuration name on which the comment is created.
 */
void
QualifyTextSearchConfigurationCommentStmt(Node *node)
{
	CommentStmt *stmt = castNode(CommentStmt, node);
	Assert(stmt->objtype == OBJECT_TSCONFIGURATION);

	char *schemaName = NULL;
	char *objName = NULL;
	DeconstructQualifiedName(castNode(List, stmt->object), &schemaName, &objName);

	if (!schemaName)
	{
		Oid tsconfigOid = get_ts_config_oid(castNode(List, stmt->object), false);
		Oid namespaceOid = get_ts_config_namespace(tsconfigOid);
		schemaName = get_namespace_name(namespaceOid);

		stmt->object = (Node *) list_make2(makeString(schemaName),
										   makeString(objName));
	}
}


/*
 * QualifyAlterTextSearchConfigurationOwnerStmt adds the schema name (if missing) to the
 * configuration for which the owner is changing.
 */
void
QualifyAlterTextSearchConfigurationOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_TSCONFIGURATION);

	char *schemaName = NULL;
	char *objName = NULL;
	DeconstructQualifiedName(castNode(List, stmt->object), &schemaName, &objName);

	if (!schemaName)
	{
		Oid tsconfigOid = get_ts_config_oid(castNode(List, stmt->object), false);
		Oid namespaceOid = get_ts_config_namespace(tsconfigOid);
		schemaName = get_namespace_name(namespaceOid);

		stmt->object = (Node *) list_make2(makeString(schemaName),
										   makeString(objName));
	}
}


/*
 * get_ts_config_namespace returns the oid of the namespace which is housing the text
 * search configuration identified by tsconfigOid.
 */
static Oid
get_ts_config_namespace(Oid tsconfigOid)
{
	HeapTuple tup = SearchSysCache1(TSCONFIGOID, ObjectIdGetDatum(tsconfigOid));

	if (HeapTupleIsValid(tup))
	{
		Form_pg_ts_config cfgform = (Form_pg_ts_config) GETSTRUCT(tup);
		Oid namespaceOid = cfgform->cfgnamespace;
		ReleaseSysCache(tup);

		return namespaceOid;
	}

	return InvalidOid;
}


/*
 * get_ts_dict_namespace returns the oid of the namespace which is housing the text
 * search dictionary identified by tsdictOid.
 */
static Oid
get_ts_dict_namespace(Oid tsdictOid)
{
	HeapTuple tup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(tsdictOid));

	if (HeapTupleIsValid(tup))
	{
		Form_pg_ts_dict cfgform = (Form_pg_ts_dict) GETSTRUCT(tup);
		Oid namespaceOid = cfgform->dictnamespace;
		ReleaseSysCache(tup);

		return namespaceOid;
	}

	return InvalidOid;
}
