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
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/* forward declaration for deparse functions */
static void AppendDropSequenceStmt(StringInfo buf, DropStmt *stmt);
static void AppendSequenceNameList(StringInfo buf, List *objects, ObjectType objtype);
static void AppendRenameSequenceStmt(StringInfo buf, RenameStmt *stmt);
static void AppendAlterSequenceSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);
static void AppendAlterSequenceOwnerStmt(StringInfo buf, AlterTableStmt *stmt);

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

	appendStringInfo(buf, " RENAME TO %s", quote_identifier(stmt->newname));
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
 * DeparseAlterSequenceOwnerStmt builds and returns a string representing the AlterTableStmt
 * consisting of changing the owner of a sequence
 */
char *
DeparseAlterSequenceOwnerStmt(Node *node)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->relkind == OBJECT_SEQUENCE);

	AppendAlterSequenceOwnerStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterSequenceOwnerStmt appends a string representing the AlterTableStmt to a buffer
 * consisting of changing the owner of a sequence
 */
static void
AppendAlterSequenceOwnerStmt(StringInfo buf, AlterTableStmt *stmt)
{
	Assert(stmt->relkind == OBJECT_SEQUENCE);
	RangeVar *seq = stmt->relation;
	char *qualifiedSequenceName = quote_qualified_identifier(seq->schemaname,
															 seq->relname);
	appendStringInfoString(buf, "ALTER SEQUENCE ");

	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}

	appendStringInfoString(buf, qualifiedSequenceName);

	ListCell *cmdCell = NULL;
	foreach(cmdCell, stmt->cmds)
	{
		if (cmdCell != list_head(stmt->cmds))
		{
			/*
			 * normally we shouldn't ever reach this
			 * because we enter this function after making sure we have only
			 * one subcommand of the type AT_ChangeOwner
			 */
			ereport(ERROR, (errmsg("More than one subcommand is not supported "
								   "for ALTER SEQUENCE")));
		}

		AlterTableCmd *alterTableCmd = castNode(AlterTableCmd, lfirst(cmdCell));
		switch (alterTableCmd->subtype)
		{
			case AT_ChangeOwner:
			{
				appendStringInfo(buf, " OWNER TO %s;", get_rolespec_name(
									 alterTableCmd->newowner));
				break;
			}

			default:
			{
				/*
				 * normally we shouldn't ever reach this
				 * because we enter this function after making sure this stmt is of the form
				 * ALTER SEQUENCE .. OWNER TO ..
				 */
				ereport(ERROR, (errmsg("unsupported subtype for alter sequence command"),
								errdetail("sub command type: %d",
										  alterTableCmd->subtype)));
			}
		}
	}
}
