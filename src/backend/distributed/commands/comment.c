/*-------------------------------------------------------------------------
 *
 * comment.c
 *    Commands to interact with the comments for all database
 *    object types.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_shdescription.h"
#include "commands/comment.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"

#include "distributed/comment.h"

static char * GetCommentForObject(Oid classOid, Oid objectOid);


inline List *
GetCommentPropagationCommands(Oid classOid, Oid objOoid, char *objectName, ObjectType
							  objectType)
{
	return GetCommentPropagationCommandsX(classOid, objOoid, objectName, objectType, NULL,
										  0);
}


List *
GetCommentPropagationCommandsX(Oid classOid, Oid objOoid, char *objectName, ObjectType
							   objectType, char *qualifier, int32 subid)
{
	List *commands = NIL;

	StringInfo commentStmt = makeStringInfo();

	char *comment = NULL;

	if ((objectType == OBJECT_DATABASE) || (objectType == OBJECT_ROLE) || (objectType ==
																		   OBJECT_TABLESPACE))
	{
		/* Get the comment for the shared object */
		comment = GetCommentForObject(classOid, objOoid);
	}
	else
	{
		comment = GetComment(classOid, objOoid, subid);
	}

	char const *commentObjectType = ObjectTypeNames[objectType];

	/* Create the SQL command to propagate the comment to other nodes */
	if (comment != NULL)
	{
		appendStringInfo(commentStmt, "COMMENT ON %s %s IS %s;", commentObjectType,
						 quote_qualified_identifier(qualifier, objectName),
						 quote_literal_cstr(comment));
	}


	/* Add the command to the list */
	if (commentStmt->len > 0)
	{
		commands = list_make1(commentStmt->data);
	}

	return commands;
}


static char *
GetCommentForObject(Oid classOid, Oid objectOid)
{
	HeapTuple tuple;
	char *comment = NULL;

	/* Open pg_shdescription catalog */
	Relation shdescRelation = table_open(SharedDescriptionRelationId, AccessShareLock);

	/* Scan the table */
	ScanKeyData scanKey[2];

	ScanKeyInit(&scanKey[0],
				Anum_pg_shdescription_objoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(objectOid));
	ScanKeyInit(&scanKey[1],
				Anum_pg_shdescription_classoid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(classOid));
	bool indexOk = true;
	int scanKeyCount = 2;
	SysScanDesc scan = systable_beginscan(shdescRelation, SharedDescriptionObjIndexId,
										  indexOk, NULL, scanKeyCount,
										  scanKey);
	if ((tuple = systable_getnext(scan)) != NULL)
	{
		bool isNull = false;

		TupleDesc tupdesc = RelationGetDescr(shdescRelation);

		Datum descDatum = heap_getattr(tuple, Anum_pg_shdescription_description, tupdesc,
									   &isNull);


		/* Add the command to the list */
		if (!isNull)
		{
			comment = TextDatumGetCString(descDatum);
		}
		else
		{
			comment = NULL;
		}
	}

	/* End the scan and close the catalog */
	systable_endscan(scan);
	table_close(shdescRelation, AccessShareLock);

	return comment;
}


/*
 * CommentObjectAddress resolves the ObjectAddress for the object
 * on which the comment is placed. Optionally errors if the object does not
 * exist based on the missing_ok flag passed in by the caller.
 */
List *
CommentObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	CommentStmt *stmt = castNode(CommentStmt, node);
	Relation relation;

	ObjectAddress objectAddress = get_object_address(stmt->objtype, stmt->object,
													 &relation, AccessExclusiveLock,
													 missing_ok);

	ObjectAddress *objectAddressCopy = palloc0(sizeof(ObjectAddress));
	*objectAddressCopy = objectAddress;
	return list_make1(objectAddressCopy);
}
