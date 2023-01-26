/*-------------------------------------------------------------------------
 *
 * deparse_table_stmts.c
 *	  All routines to deparse table statements.
 *	  This file contains all entry points specific for table statement deparsing as well as
 *	  functions that are currently only used for deparsing of the table statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/version_compat.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_expr.h"
#include "parser/parse_type.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"

#include "distributed/namespace_utils.h"
#include "commands/tablecmds.h"

static void AppendAlterTableSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);
static void AppendAlterTableStmt(StringInfo buf, AlterTableStmt *stmt);
static void AppendAlterTableCmd(StringInfo buf, AlterTableCmd *alterTableCmd,
								AlterTableStmt *stmt);
static void AppendAlterTableCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd);

char *
DeparseAlterTableSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objectType == OBJECT_TABLE || stmt->objectType == OBJECT_FOREIGN_TABLE);

	AppendAlterTableSchemaStmt(&str, stmt);
	return str.data;
}


static void
AppendAlterTableSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt)
{
	Assert(stmt->objectType == OBJECT_TABLE || stmt->objectType == OBJECT_FOREIGN_TABLE);

	bool isForeignTable = stmt->objectType == OBJECT_FOREIGN_TABLE;
	appendStringInfo(buf, "ALTER %sTABLE ", isForeignTable ? "FOREIGN " : "");
	if (stmt->missing_ok)
	{
		appendStringInfo(buf, "IF EXISTS ");
	}
	char *tableName = quote_qualified_identifier(stmt->relation->schemaname,
												 stmt->relation->relname);
	const char *newSchemaName = quote_identifier(stmt->newschema);
	appendStringInfo(buf, "%s SET SCHEMA %s;", tableName, newSchemaName);
}


/*
 * DeparseAlterTableStmt builds and returns a string representing the
 * AlterTableStmt where the object acted upon is of kind OBJECT_TABLE
 */
char *
DeparseAlterTableStmt(Node *node)
{
	AlterTableStmt *stmt = castNode(AlterTableStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(AlterTableStmtObjType_compat(stmt) == OBJECT_TABLE);

	AppendAlterTableStmt(&str, stmt);
	return str.data;
}


/*
 * AppendAlterTableStmt builds and returns an SQL command representing an
 * ALTER TABLE statement from given AlterTableStmt object where the object
 * acted upon is of kind OBJECT_TABLE
 */
static void
AppendAlterTableStmt(StringInfo buf, AlterTableStmt *stmt)
{
	const char *identifier = quote_qualified_identifier(stmt->relation->schemaname,
														stmt->relation->relname);
	ListCell *cmdCell = NULL;

	Assert(AlterTableStmtObjType_compat(stmt) == OBJECT_TABLE);

	appendStringInfo(buf, "ALTER TABLE %s", identifier);
	foreach(cmdCell, stmt->cmds)
	{
		if (cmdCell != list_head(stmt->cmds))
		{
			appendStringInfoString(buf, ", ");
		}

		AlterTableCmd *alterTableCmd = castNode(AlterTableCmd, lfirst(cmdCell));
		AppendAlterTableCmd(buf, alterTableCmd, stmt);
	}

	appendStringInfoString(buf, ";");
}


/*
 * AppendColumnNameList converts a list of columns into comma separated string format
 * (colname_1, colname_2, .., colname_n).
 */
static void
AppendColumnNameList(StringInfo buf, List *columns)
{
	appendStringInfoString(buf, " (");

	ListCell *lc;
	bool firstkey = true;

	foreach(lc, columns)
	{
		if (firstkey == false)
		{
			appendStringInfoString(buf, ", ");
		}

		appendStringInfo(buf, "%s", quote_identifier(strVal(lfirst(lc))));
		firstkey = false;
	}

	appendStringInfoString(buf, " )");
}


/*
 * AppendAlterTableCmdAddConstraint builds the add constraint command for index constraints
 * in the ADD CONSTRAINT <conname> {PRIMARY KEY, UNIQUE, EXCLUSION} form and appends it to the buf.
 */
static void
AppendAlterTableCmdAddConstraint(StringInfo buf, Constraint *constraint,
								 AlterTableStmt *stmt)
{
	/* Need to deparse the alter table constraint command only if we are adding a constraint name.*/
	if (constraint->conname == NULL)
	{
		ereport(ERROR, (errmsg(
							"Constraint name can not be NULL when deparsing the constraint.")));
	}

	appendStringInfoString(buf, " ADD CONSTRAINT ");
	appendStringInfo(buf, "%s ", quote_identifier(constraint->conname));

	/* postgres version >= PG15
	 * UNIQUE [ NULLS [ NOT ] DISTINCT ] ( column_name [, ... ] ) [ INCLUDE ( column_name [, ...]) ]
	 * postgres version < PG15
	 * UNIQUE ( column_name [, ... ] ) [ INCLUDE ( column_name [, ...]) ]
	 * PRIMARY KEY ( column_name [, ... ] ) [ INCLUDE ( column_name [, ...]) ]
	 */
	if (constraint->contype == CONSTR_PRIMARY || constraint->contype == CONSTR_UNIQUE)
	{
		if (constraint->contype == CONSTR_PRIMARY)
		{
			appendStringInfoString(buf,
								   " PRIMARY KEY ");
		}
		else
		{
			appendStringInfoString(buf, " UNIQUE");

#if (PG_VERSION_NUM >= PG_VERSION_15)
			if (constraint->nulls_not_distinct == true)
			{
				appendStringInfoString(buf, " NULLS NOT DISTINCT");
			}
#endif
		}

		AppendColumnNameList(buf, constraint->keys);

		if (constraint->including != NULL)
		{
			appendStringInfoString(buf, " INCLUDE ");

			AppendColumnNameList(buf, constraint->including);
		}
	}
	else if (constraint->contype == CONSTR_EXCLUSION)
	{
		/*
		 * This block constructs the EXCLUDE clause which is in the following form:
		 * EXCLUDE [ USING index_method ] ( exclude_element WITH operator [, ... ] )
		 */
		appendStringInfoString(buf, " EXCLUDE ");

		if (constraint->access_method != NULL)
		{
			appendStringInfoString(buf, "USING ");
			appendStringInfo(buf, "%s ", quote_identifier(
								 constraint->access_method));
		}

		appendStringInfoString(buf, " (");

		ListCell *lc;
		bool firstOp = true;

		foreach(lc, constraint->exclusions)
		{
			List *pair = (List *) lfirst(lc);

			Assert(list_length(pair) == 2);
			IndexElem *elem = linitial_node(IndexElem, pair);
			List *opname = lsecond_node(List, pair);
			if (firstOp == false)
			{
				appendStringInfoString(buf, " ,");
			}

			ListCell *lc2;

			foreach(lc2, opname)
			{
				appendStringInfo(buf, "%s WITH %s", quote_identifier(elem->name),
								 strVal(lfirst(lc2)));
			}

			firstOp = false;
		}

		appendStringInfoString(buf, " )");
	}
	else if (constraint->contype == CONSTR_CHECK)
	{
		LOCKMODE lockmode = AlterTableGetLockLevel(stmt->cmds);
		Oid leftRelationId = AlterTableLookupRelation(stmt, lockmode);

		/* To be able to use deparse_expression function, which creates an expression string,
		 * the expression should be provided in its cooked form. We transform the raw expression
		 * to cooked form.
		 */
		ParseState *pstate = make_parsestate(NULL);
		Relation relation = table_open(leftRelationId, AccessShareLock);

		/* Add table name to the name space in  parse state. Otherwise column names
		 * cannot be found.
		 */
		AddRangeTableEntryToQueryCompat(pstate, relation);

		Node *exprCooked = transformExpr(pstate, constraint->raw_expr,

										 EXPR_KIND_CHECK_CONSTRAINT);

		char *relationName = get_rel_name(leftRelationId);
		List *relationCtx = deparse_context_for(relationName, leftRelationId);

		char *exprSql = deparse_expression(exprCooked, relationCtx, false, false);

		relation_close(relation, NoLock);

		appendStringInfo(buf, " CHECK (%s)", exprSql);

		if (constraint->is_no_inherit)
		{
			appendStringInfo(buf, " NO INHERIT");
		}
	}
	else if (constraint->contype == CONSTR_FOREIGN)
	{
		appendStringInfoString(buf, " FOREIGN KEY");

		AppendColumnNameList(buf, constraint->fk_attrs);

		appendStringInfoString(buf, " REFERENCES");

		appendStringInfo(buf, " %s", quote_identifier(constraint->pktable->relname));

		if (list_length(constraint->pk_attrs) > 0)
		{
			AppendColumnNameList(buf, constraint->pk_attrs);
		}

		/* Append supported options if provided */

		/* FKCONSTR_MATCH_SIMPLE is default. Append matchtype if not default */
		if (constraint->fk_matchtype == FKCONSTR_MATCH_FULL)
		{
			appendStringInfoString(buf, " MATCH FULL");
		}

		switch (constraint->fk_del_action)
		{
			case FKCONSTR_ACTION_SETDEFAULT:
			{
				appendStringInfoString(buf, " ON DELETE SET DEFAULT");
				break;
			}

			case FKCONSTR_ACTION_SETNULL:
			{
				appendStringInfoString(buf, " ON DELETE SET NULL");
				break;
			}

			case FKCONSTR_ACTION_NOACTION:
			{
				appendStringInfoString(buf, " ON DELETE NO ACTION");
				break;
			}

			case FKCONSTR_ACTION_RESTRICT:
			{
				appendStringInfoString(buf, " ON DELETE RESTRICT");
				break;
			}

			case FKCONSTR_ACTION_CASCADE:
			{
				appendStringInfoString(buf, " ON DELETE CASCADE");
				break;
			}

			default:
			{
				elog(ERROR, "unsupported FK delete action type: %d",
					 (int) constraint->fk_del_action);
				break;
			}
		}

		switch (constraint->fk_upd_action)
		{
			case FKCONSTR_ACTION_SETDEFAULT:
			{
				appendStringInfoString(buf, " ON UPDATE SET DEFAULT");
				break;
			}

			case FKCONSTR_ACTION_SETNULL:
			{
				appendStringInfoString(buf, " ON UPDATE SET NULL");
				break;
			}

			case FKCONSTR_ACTION_NOACTION:
			{
				appendStringInfoString(buf, " ON UPDATE NO ACTION");
				break;
			}

			case FKCONSTR_ACTION_RESTRICT:
			{
				appendStringInfoString(buf, " ON UPDATE RESTRICT");
				break;
			}

			case FKCONSTR_ACTION_CASCADE:
			{
				appendStringInfoString(buf, " ON UPDATE CASCADE");
				break;
			}

			default:
			{
				elog(ERROR, "unsupported FK update action type: %d",
					 (int) constraint->fk_upd_action);
				break;
			}
		}
	}

	/* FOREIGN KEY and CHECK constraints migth have NOT VALID option */
	if (constraint->skip_validation)
	{
		appendStringInfoString(buf, " NOT VALID ");
	}

	if (constraint->deferrable)
	{
		appendStringInfoString(buf, " DEFERRABLE");

		if (constraint->initdeferred)
		{
			appendStringInfoString(buf, " INITIALLY DEFERRED");
		}
	}
}


/*
 * AppendAlterTableCmd builds and appends to the given buffer a command
 * from given AlterTableCmd object. Currently supported commands are of type
 * AT_AddColumn, AT_SetNotNull and AT_AddConstraint {PRIMARY KEY, UNIQUE, EXCLUDE}.
 */
static void
AppendAlterTableCmd(StringInfo buf, AlterTableCmd *alterTableCmd, AlterTableStmt *stmt)
{
	switch (alterTableCmd->subtype)
	{
		case AT_AddColumn:
		{
			AppendAlterTableCmdAddColumn(buf, alterTableCmd);
			break;
		}

		case AT_AddConstraint:
		{
			Constraint *constraint = (Constraint *) alterTableCmd->def;

			/* We need to deparse ALTER TABLE ... ADD {PRIMARY KEY, UNIQUE, EXCLUSION} commands into
			 * ALTER TABLE ... ADD CONSTRAINT <conname> {PRIMARY KEY, UNIQUE, EXCLUSION} ... format to be able
			 * add a constraint name.
			 */
			if (ConstrTypeCitusCanDefaultName(constraint->contype))
			{
				AppendAlterTableCmdAddConstraint(buf, constraint, stmt);
				break;
			}
		}

		/* fallthrough */

		default:
		{
			ereport(ERROR, (errmsg("unsupported subtype for alter table command"),
							errdetail("sub command type: %d", alterTableCmd->subtype)));
		}
	}
}


/*
 * AppendAlterTableCmd builds and appends to the given buffer an AT_AddColumn command
 * from given AlterTableCmd object in the form ADD COLUMN ...
 */
static void
AppendAlterTableCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	Assert(alterTableCmd->subtype == AT_AddColumn);

	appendStringInfoString(buf, " ADD COLUMN ");

	ColumnDef *columnDefinition = (ColumnDef *) alterTableCmd->def;

	/*
	 * the way we use the deparser now, constraints are always NULL
	 * adding this check for ColumnDef consistency
	 */
	if (columnDefinition->constraints != NULL)
	{
		ereport(ERROR, (errmsg("Constraints are not supported for AT_AddColumn")));
	}

	if (columnDefinition->colname)
	{
		appendStringInfo(buf, "%s ", quote_identifier(columnDefinition->colname));
	}

	int32 typmod = 0;
	Oid typeOid = InvalidOid;
	bits16 formatFlags = FORMAT_TYPE_TYPEMOD_GIVEN | FORMAT_TYPE_FORCE_QUALIFY;
	typenameTypeIdAndMod(NULL, columnDefinition->typeName, &typeOid, &typmod);
	appendStringInfo(buf, "%s", format_type_extended(typeOid, typmod,
													 formatFlags));
	if (columnDefinition->is_not_null)
	{
		appendStringInfoString(buf, " NOT NULL");
	}

	/*
	 * the way we use the deparser now, collation is never used
	 * since the data type of columns that use sequences for default
	 * are only int,smallint and bigint (never text, varchar, char)
	 * Adding this part only for ColumnDef consistency
	 */
	Oid collationOid = GetColumnDefCollation(NULL, columnDefinition, typeOid);
	if (OidIsValid(collationOid))
	{
		const char *identifier = FormatCollateBEQualified(collationOid);
		appendStringInfo(buf, " COLLATE %s", identifier);
	}
}
