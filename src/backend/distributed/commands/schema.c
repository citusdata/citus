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
#include "distributed/commands.h"
#include <distributed/connection_management.h>
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata_cache.h"
#include <distributed/metadata_sync.h>
#include <distributed/remote_commands.h>
#include <distributed/remote_commands.h>
#include "nodes/parsenodes.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"


/*
 * ProcessDropSchemaStmt invalidates the foreign key cache if any table created
 * under dropped schema involved in any foreign key relationship.
 */
void
ProcessDropSchemaStmt(DropStmt *dropStatement)
{
	Relation pgClass = NULL;
	HeapTuple heapTuple = NULL;
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	Oid scanIndexId = InvalidOid;
	bool useIndex = false;
	ListCell *dropSchemaCell;

	if (dropStatement->behavior != DROP_CASCADE)
	{
		return;
	}

	foreach(dropSchemaCell, dropStatement->objects)
	{
		Value *schemaValue = (Value *) lfirst(dropSchemaCell);
		char *schemaString = strVal(schemaValue);

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
			if (relationId == InvalidOid || !IsDistributedTable(relationId))
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
				return;
			}

			heapTuple = systable_getnext(scanDescriptor);
		}

		systable_endscan(scanDescriptor);
		heap_close(pgClass, NoLock);
	}
}


/*
 * PlanAlterObjectSchemaStmt is called by citus' utility hook for AlterObjectSchemaStmt
 * parsetrees. It dispatches the statement based on the object type for which the schema
 * is being altered.
 *
 * A (potentially empty) list of DDLJobs is being returned with the jobs on how to
 * distribute the change into the cluster.
 */
List *
PlanAlterObjectSchemaStmt(AlterObjectSchemaStmt *stmt, const char *queryString)
{
	switch (stmt->objectType)
	{
		case OBJECT_TYPE:
		{
			return PlanAlterTypeSchemaStmt(stmt, queryString);
		}

		case OBJECT_PROCEDURE:
		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		{
			return PlanAlterFunctionSchemaStmt(stmt, queryString);
		}

		default:
		{
			/* do nothing for unsupported objects */
			break;
		}
	}

	/*
	 * old behaviour, needs to be reconciled to the above switch statement for all
	 * objectType's relating to tables. Maybe it is as easy to support
	 * ALTER TABLE ... SET SCHEMA
	 */
	return PlanAlterTableSchemaStmt(stmt, queryString);
}


/*
 * PlanAlterTableSchemaStmt determines whether a given ALTER ... SET SCHEMA
 * statement involves a distributed table and issues a warning if so. Because
 * we do not support distributed ALTER ... SET SCHEMA, this function always
 * returns NIL.
 */
List *
PlanAlterTableSchemaStmt(AlterObjectSchemaStmt *stmt, const char *queryString)
{
	Oid relationId = InvalidOid;

	if (stmt->relation == NULL)
	{
		return NIL;
	}

	relationId = RangeVarGetRelid(stmt->relation,
								  AccessExclusiveLock,
								  stmt->missing_ok);

	/* first check whether a distributed relation is affected */
	if (!OidIsValid(relationId) || !IsDistributedTable(relationId))
	{
		return NIL;
	}

	/* emit a warning if a distributed relation is affected */
	ereport(WARNING, (errmsg("not propagating ALTER ... SET SCHEMA commands to "
							 "worker nodes"),
					  errhint("Connect to worker nodes directly to manually "
							  "change schemas of affected objects.")));

	return NIL;
}


/*
 * ProcessAlterObjectSchemaStmt is called by multi_ProcessUtility _after_ the command has
 * been applied to the local postgres. It is useful to create potentially new dependencies
 * of this object (the new schema) on the workers before the command gets applied to the
 * remote objects.
 */
void
ProcessAlterObjectSchemaStmt(AlterObjectSchemaStmt *stmt, const char *queryString)
{
	switch (stmt->objectType)
	{
		case OBJECT_TYPE:
		{
			ProcessAlterTypeSchemaStmt(stmt, queryString);
			return;
		}

		case OBJECT_PROCEDURE:
		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		{
			ProcessAlterFunctionSchemaStmt(stmt, queryString);
			return;
		}

		default:
		{
			/* do nothing for unsupported objects */
			return;
		}
	}
}
