/*-------------------------------------------------------------------------
 *
 * schema.c
 *    Commands for creating and altering schemas for distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "distributed/commands.h"
#include <distributed/connection_management.h>
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include <distributed/metadata_sync.h>
#include <distributed/remote_commands.h>
#include <distributed/remote_commands.h>
#include "nodes/parsenodes.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"


static List * FilterDistributedSchemas(List *schemas);


/*
 * PreprocessDropSchemaStmt invalidates the foreign key cache if any table created
 * under dropped schema involved in any foreign key relationship.
 */
List *
PreprocessDropSchemaStmt(Node *node, const char *queryString)
{
	DropStmt *dropStatement = castNode(DropStmt, node);
	Relation pgClass = NULL;
	HeapTuple heapTuple = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Oid scanIndexId = InvalidOid;
	bool useIndex = false;

	if (dropStatement->behavior != DROP_CASCADE)
	{
		return NIL;
	}

	Value *schemaValue = NULL;
	foreach_ptr(schemaValue, dropStatement->objects)
	{
		const char *schemaString = strVal(schemaValue);
		Oid namespaceOid = get_namespace_oid(schemaString, true);

		if (namespaceOid == InvalidOid)
		{
			continue;
		}

		pgClass = heap_open(RelationRelationId, AccessShareLock);

		ScanKeyInit(&scanKey[0], Anum_pg_class_relnamespace, BTEqualStrategyNumber,
					F_OIDEQ, namespaceOid);
		scanDescriptor = systable_beginscan(pgClass, scanIndexId, useIndex, NULL,
											scanKeyCount, scanKey);

		heapTuple = systable_getnext(scanDescriptor);
		while (HeapTupleIsValid(heapTuple))
		{
			Form_pg_class relationForm = (Form_pg_class) GETSTRUCT(heapTuple);
			char *relationName = NameStr(relationForm->relname);
			Oid relationId = get_relname_relid(relationName, namespaceOid);

			/* we're not interested in non-valid, non-distributed relations */
			if (relationId == InvalidOid || !IsCitusTable(relationId))
			{
				heapTuple = systable_getnext(scanDescriptor);
				continue;
			}

			/* invalidate foreign key cache if the table involved in any foreign key */
			if (TableReferenced(relationId) || TableReferencing(relationId))
			{
				MarkInvalidateForeignKeyGraph();

				systable_endscan(scanDescriptor);
				heap_close(pgClass, NoLock);
				return NIL;
			}

			heapTuple = systable_getnext(scanDescriptor);
		}

		systable_endscan(scanDescriptor);
		heap_close(pgClass, NoLock);
	}

	return NIL;
}


/*
 * PreprocessGrantOnSchemaStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to grant
 * on schemas. Only grant statements for distributed schema are propagated.
 */
List *
PreprocessGrantOnSchemaStmt(Node *node, const char *queryString)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_SCHEMA);

	List *distributedSchemas = FilterDistributedSchemas(stmt->objects);

	if (list_length(distributedSchemas) == 0)
	{
		return NIL;
	}

	List *originalObjects = stmt->objects;

	stmt->objects = distributedSchemas;

	char *sql = DeparseTreeNode((Node *) stmt);

	stmt->objects = originalObjects;

	return NodeDDLTaskList(ALL_WORKERS, list_make1(sql));
}


/*
 * FilterDistributedSchemas filters the schema list and returns the distributed ones
 * as a list
 */
static List *
FilterDistributedSchemas(List *schemas)
{
	List *distributedSchemas = NIL;

	Value *schemaValue = NULL;
	foreach_ptr(schemaValue, schemas)
	{
		const char *schemaName = strVal(schemaValue);
		Oid schemaOid = get_namespace_oid(schemaName, true);

		if (!OidIsValid(schemaOid))
		{
			continue;
		}

		ObjectAddress address = { 0 };
		ObjectAddressSet(address, NamespaceRelationId, schemaOid);

		if (!IsObjectDistributed(&address))
		{
			continue;
		}

		distributedSchemas = lappend(distributedSchemas, schemaValue);
	}

	return distributedSchemas;
}
