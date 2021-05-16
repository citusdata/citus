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
#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/* forward declaration for deparse functions */
static void AppendDropSequenceStmt(StringInfo buf, DropStmt *stmt);
static void AppendSequenceNameList(StringInfo buf, List *objects, ObjectType objtype);
static void AppendRenameSequenceStmt(StringInfo buf, RenameStmt *stmt);
static void AppendAlterSequenceSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);
static void AppendAlterSequenceOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);

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


/*
 * DeparseRenameSequenceStmt builds and returns a string representing the RenameStmt
 */
char *
DeparseRenameSequenceStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->renameType == OBJECT_SEQUENCE);

	AppendRenameSequenceStmt(&str, stmt);

	return str.data;
}


/*
 * AppendRenameSequenceStmt appends a string representing the RenameStmt to a buffer
 */
static void
AppendRenameSequenceStmt(StringInfo buf, RenameStmt *stmt)
{
	RangeVar *seq = stmt->relation;

	char *qualifiedSequenceName = quote_qualified_identifier(seq->schemaname,
															 seq->relname);

	appendStringInfoString(buf, "ALTER SEQUENCE ");

	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}

	appendStringInfoString(buf, qualifiedSequenceName);

	appendStringInfo(buf, " RENAME TO %s;", quote_identifier(stmt->newname));
}


/*
 * QualifyRenameSequenceStmt transforms a
 * ALTER SEQUENCE .. RENAME TO ..
 * statement in place and makes the sequence name fully qualified.
 */
void
QualifyRenameSequenceStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_SEQUENCE);

	RangeVar *seq = stmt->relation;

	if (seq->schemaname == NULL)
	{
		Oid schemaOid = RangeVarGetCreationNamespace(seq);
		seq->schemaname = get_namespace_name(schemaOid);
	}
}


/*
 * QualifyAlterSequenceSchemaStmt transforms a
 * ALTER SEQUENCE .. SET SCHEMA ..
 * statement in place and makes the function name fully qualified.
 */
void
QualifyAlterSequenceSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_SEQUENCE);

	RangeVar *seq = stmt->relation;

	if (seq->schemaname == NULL)
	{
		Oid schemaOid = RangeVarGetCreationNamespace(seq);
		seq->schemaname = get_namespace_name(schemaOid);
	}
}


/*
 * DeparseAlterSequenceSchemaStmt builds and returns a string representing the AlterObjectSchemaStmt
 */
char *
DeparseAlterSequenceSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objectType == OBJECT_SEQUENCE);

	AppendAlterSequenceSchemaStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterSequenceSchemaStmt appends a string representing the AlterObjectSchemaStmt to a buffer
 */
static void
AppendAlterSequenceSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt)
{
	RangeVar *seq = stmt->relation;

	char *qualifiedSequenceName = quote_qualified_identifier(seq->schemaname,
															 seq->relname);

	appendStringInfoString(buf, "ALTER SEQUENCE ");

	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}

	appendStringInfoString(buf, qualifiedSequenceName);

	appendStringInfo(buf, " SET SCHEMA %s;", quote_identifier(stmt->newschema));
}


/*
 * QualifyAlterSequenceOwnerStmt transforms a
 * ALTER SEQUENCE .. OWNER TO ..
 * statement in place and makes the function name fully qualified.
 */
void
QualifyAlterSequenceOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_SEQUENCE);

	RangeVar *seq = stmt->relation;

	if (seq->schemaname == NULL)
	{
		Oid schemaOid = RangeVarGetCreationNamespace(seq);
		seq->schemaname = get_namespace_name(schemaOid);
	}
}


/*
 * DeparseAlterSequenceOwnerStmt builds and returns a string representing the AlterOwnerStmt
 */
char *
DeparseAlterSequenceOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objectType == OBJECT_SEQUENCE);

	AppendAlterSequenceOwnerStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterSequenceOwnerStmt appends a string representing the AlterOwnerStmt to a buffer
 */
static void
AppendAlterSequenceOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt)
{
	RangeVar *seq = stmt->relation;

	char *qualifiedSequenceName = quote_qualified_identifier(seq->schemaname,
															 seq->relname);

	appendStringInfoString(buf, "ALTER SEQUENCE ");

	appendStringInfoString(buf, qualifiedSequenceName);

	appendStringInfo(buf, " OWNER TO %s;", RoleSpecString(stmt->newowner, true));
}
