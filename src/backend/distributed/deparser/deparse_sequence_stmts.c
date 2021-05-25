/*-------------------------------------------------------------------------
 *
 * deparse_sequence_stmts.c
 *
 *	  All routines to deparse sequence statements.
 *	  This file contains all entry points specific for sequence statement
 *    deparsing
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/deparser.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/* forward declaration for deparse functions */
static void AppendDropSequenceStmt(StringInfo buf, DropStmt *stmt);
static void AppendSequenceNameList(StringInfo buf, List *objects, ObjectType objtype);

/*
 * DeparseDropSequenceStmt builds and returns a string representing the DropStmt
 */
char *
DeparseDropSequenceStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->removeType == OBJECT_SEQUENCE);

	AppendDropSequenceStmt(&str, stmt);

	return str.data;
}


/*
 * AppendDropSequenceStmt appends a string representing the DropStmt to a buffer
 */
static void
AppendDropSequenceStmt(StringInfo buf, DropStmt *stmt)
{
	appendStringInfoString(buf, "DROP SEQUENCE ");

	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}

	AppendSequenceNameList(buf, stmt->objects, stmt->removeType);

	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}

	appendStringInfoString(buf, ";");
}


/*
 * AppendSequenceNameList appends a string representing the list of sequence names to a buffer
 */
static void
AppendSequenceNameList(StringInfo buf, List *objects, ObjectType objtype)
{
	ListCell *objectCell = NULL;
	foreach(objectCell, objects)
	{
		if (objectCell != list_head(objects))
		{
			appendStringInfo(buf, ", ");
		}

		RangeVar *seq = makeRangeVarFromNameList((List *) lfirst(objectCell));

		if (seq->schemaname == NULL)
		{
			Oid schemaOid = RangeVarGetCreationNamespace(seq);
			seq->schemaname = get_namespace_name(schemaOid);
		}

		char *qualifiedSequenceName = quote_qualified_identifier(seq->schemaname,
																 seq->relname);
		appendStringInfoString(buf, qualifiedSequenceName);
	}
}
