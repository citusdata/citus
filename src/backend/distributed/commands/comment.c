#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/pg_shdescription.h"

#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "distributed/comment.h"


const CommentStmtType commentStmtTypes[] = {
	{ OBJECT_DATABASE, "DATABASE" },
	{ OBJECT_ROLE, "ROLE" }
};


static char * GetCommentObjectType(ObjectType objectType);
static char * GetCommentForObject(Oid oid);


List *
GetCommentPropagationCommands(Oid oid, char *objectName, ObjectType objectType)
{
	List *commands = NIL;

	StringInfo commentStmt = makeStringInfo();

	/* Get the comment for the database */
	char *comment = GetCommentForObject(oid);
	char *commentObjectType = GetCommentObjectType(objectType);

	/* Create the SQL command to propagate the comment to other nodes */
	if (comment != NULL)
	{
		appendStringInfo(commentStmt, "COMMENT ON %s %s IS %s;", commentObjectType,
						 quote_identifier(objectName),
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
GetCommentObjectType(ObjectType objectType)
{
	char *objectName = NULL;
	for (int i = 0; i < sizeof(commentStmtTypes) / sizeof(CommentStmtType); i++)
	{
		if (commentStmtTypes[i].objectType == objectType)
		{
			objectName = commentStmtTypes[i].objectName;
			break;
		}
	}
	return objectName;
}


static char *
GetCommentForObject(Oid oid)
{
	HeapTuple tuple;
	char *comment = NULL;

	/* Open pg_shdescription catalog */
	Relation shdescRelation = table_open(SharedDescriptionRelationId, AccessShareLock);

	/* Scan the table */
	SysScanDesc scan = systable_beginscan(shdescRelation, InvalidOid, false, NULL, 0,
										  NULL);
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		Form_pg_shdescription shdesc = (Form_pg_shdescription) GETSTRUCT(tuple);

		bool isNull = false;

		TupleDesc tupdesc = RelationGetDescr(shdescRelation);

		Datum descDatum = heap_getattr(tuple, Anum_pg_shdescription_description, tupdesc,
									   &isNull);

		/* Check if the objoid matches the databaseOid */
		if (shdesc->objoid == oid)
		{
			/* Add the command to the list */
			if (!isNull)
			{
				comment = TextDatumGetCString(descDatum);
			}
			else
			{
				comment = NULL;
			}
			break;
		}
	}

	/* End the scan and close the catalog */
	systable_endscan(scan);
	table_close(shdescRelation, AccessShareLock);

	return comment;
}
