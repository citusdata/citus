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
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "distributed/deparser.h"
#include "distributed/version_compat.h"


/* forward declaration for deparse functions */
static void AppendDropSequenceStmt(StringInfo buf, DropStmt *stmt);
static void AppendSequenceNameList(StringInfo buf, List *objects, ObjectType objtype);
static void AppendRenameSequenceStmt(StringInfo buf, RenameStmt *stmt);
static void AppendAlterSequenceSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);
static void AppendAlterSequenceOwnerStmt(StringInfo buf, AlterTableStmt *stmt);
static void AppendAlterSequencePersistenceStmt(StringInfo buf, AlterTableStmt *stmt);
static void AppendGrantOnSequenceStmt(StringInfo buf, GrantStmt *stmt);
static void AppendGrantOnSequenceSequences(StringInfo buf, GrantStmt *stmt);

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

	Assert(stmt->objtype == OBJECT_SEQUENCE);

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
	Assert(stmt->objtype == OBJECT_SEQUENCE);
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


/*
 * DeparseAlterSequencePersistenceStmt builds and returns a string representing
 * the AlterTableStmt consisting of changing the persistence of a sequence
 */
char *
DeparseAlterSequencePersistenceStmt(Node *node)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objtype == OBJECT_SEQUENCE);

	AppendAlterSequencePersistenceStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterSequencePersistenceStmt appends a string representing the
 * AlterTableStmt to a buffer consisting of changing the persistence of a sequence
 */
static void
AppendAlterSequencePersistenceStmt(StringInfo buf, AlterTableStmt *stmt)
{
	Assert(stmt->objtype == OBJECT_SEQUENCE);

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
			 * As of PG15, we cannot reach this code because ALTER SEQUENCE
			 * is only supported for a single sequence. Still, let's be
			 * defensive for future PG changes
			 */
			ereport(ERROR, (errmsg("More than one subcommand is not supported "
								   "for ALTER SEQUENCE")));
		}

		AlterTableCmd *alterTableCmd = castNode(AlterTableCmd, lfirst(cmdCell));
		switch (alterTableCmd->subtype)
		{
			case AT_SetLogged:
			{
				appendStringInfoString(buf, " SET LOGGED;");
				break;
			}

			case AT_SetUnLogged:
			{
				appendStringInfoString(buf, " SET UNLOGGED;");
				break;
			}

			default:
			{
				/*
				 * normally we shouldn't ever reach this
				 * because we enter this function after making sure this stmt is of the form
				 * ALTER SEQUENCE .. SET LOGGED/UNLOGGED
				 */
				ereport(ERROR, (errmsg("unsupported subtype for alter sequence command"),
								errdetail("sub command type: %d",
										  alterTableCmd->subtype)));
			}
		}
	}
}


/*
 * DeparseGrantOnSequenceStmt builds and returns a string representing the GrantOnSequenceStmt
 */
char *
DeparseGrantOnSequenceStmt(Node *node)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendGrantOnSequenceStmt(&str, stmt);

	return str.data;
}


/*
 * AppendGrantOnSequenceStmt builds and returns an SQL command representing a
 * GRANT .. ON SEQUENCE command from given GrantStmt object.
 */
static void
AppendGrantOnSequenceStmt(StringInfo buf, GrantStmt *stmt)
{
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	if (stmt->targtype == ACL_TARGET_ALL_IN_SCHEMA)
	{
		/*
		 * Normally we shouldn't reach this
		 * We deparse a GrantStmt with OBJECT_SEQUENCE after setting targtype
		 * to ACL_TARGET_OBJECT
		 */
		elog(ERROR,
			 "GRANT .. ALL SEQUENCES IN SCHEMA is not supported for formatting.");
	}

	AppendGrantSharedPrefix(buf, stmt);

	AppendGrantOnSequenceSequences(buf, stmt);

	AppendGrantSharedSuffix(buf, stmt);
}


/*
 * AppendGrantOnSequenceSequences appends the sequence names along with their arguments
 * to the given StringInfo from the given GrantStmt
 */
static void
AppendGrantOnSequenceSequences(StringInfo buf, GrantStmt *stmt)
{
	Assert(stmt->objtype == OBJECT_SEQUENCE);

	appendStringInfoString(buf, " ON SEQUENCE ");

	ListCell *cell = NULL;
	foreach(cell, stmt->objects)
	{
		/*
		 * GrantOnSequence statement keeps its objects (sequences) as
		 * a list of RangeVar-s
		 */
		RangeVar *sequence = (RangeVar *) lfirst(cell);

		/*
		 * We have qualified the statement beforehand
		 */
		appendStringInfoString(buf, quote_qualified_identifier(sequence->schemaname,
															   sequence->relname));

		if (cell != list_tail(stmt->objects))
		{
			appendStringInfoString(buf, ", ");
		}
	}
}
