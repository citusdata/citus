/*-------------------------------------------------------------------------
 *
 * qualify_type_stmt.c
 *	  Functins specialized in fully qualifying all type statements
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/deparser.h"
#include "parser/parse_type.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "catalog/pg_type.h"
#include "access/htup_details.h"
#include "access/heapam.h"


static Oid
type_get_namespace_oid(Oid typeOid)
{
	Relation catalog = heap_open(TypeRelationId, AccessShareLock);
	HeapTuple typeTuple = get_catalog_object_by_oid(catalog, typeOid);
	heap_close(catalog, AccessShareLock);

	Form_pg_type typeData = (Form_pg_type) GETSTRUCT(typeTuple);

	return typeData->typnamespace;
}


void
qualify_rename_type_stmt(RenameStmt *stmt)
{
	List *names = (List *) stmt->object;

	Assert(stmt->renameType == OBJECT_TYPE);

	if (list_length(names) == 1)
	{
		/* not qualified, lookup name and add namespace name to names */
		TypeName *typeName = makeTypeNameFromNameList(names);
		Oid typeOid = LookupTypeNameOid(NULL, typeName, false);
		Oid namespaceOid = type_get_namespace_oid(typeOid);
		char *nspname = get_namespace_name_or_temp(namespaceOid);

		names = list_make2(makeString(nspname),
						   linitial(names));

		stmt->object = (Node *) names;
	}
}
