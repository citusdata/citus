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
static Oid get_ts_dict_namespace(Oid dictOid);

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


static Oid
get_ts_dict_namespace(Oid dictOid)
{
	HeapTuple tup = SearchSysCache1(TSDICTOID, ObjectIdGetDatum(dictOid));

	if (HeapTupleIsValid(tup))
	{
		Form_pg_ts_dict cfgform = (Form_pg_ts_dict) GETSTRUCT(tup);
		Oid namespaceOid = cfgform->dictnamespace;
		ReleaseSysCache(tup);

		return namespaceOid;
	}

	return InvalidOid;
}
