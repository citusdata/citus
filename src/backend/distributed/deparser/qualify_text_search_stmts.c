#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "catalog/pg_ts_config.h"

#include "distributed/deparser.h"
#include "distributed/listutils.h"

static Oid get_ts_config_namespace(Oid tsconfigOid);

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
