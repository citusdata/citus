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

#include "catalog/heap.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/namespace_utils.h"
#include "distributed/version_compat.h"

static void AppendAlterTableSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);
static void AppendAlterTableStmt(StringInfo buf, AlterTableStmt *stmt);
static void AppendAlterTableCmd(StringInfo buf, AlterTableCmd *alterTableCmd,
								AlterTableStmt *stmt);
static void AppendAlterTableCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd,
										 AlterTableStmt *stmt);
static void AppendAlterTableCmdDropConstraint(StringInfo buf,
											  AlterTableCmd *alterTableCmd);

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

	Assert(stmt->objtype == OBJECT_TABLE);

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

	Assert(stmt->objtype == OBJECT_TABLE);

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
void
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
 * AppendAlterTableCmdConstraint builds a string required to create given
 * constraint as part of an ADD CONSTRAINT or an ADD COLUMN subcommand,
 * and appends it to the buf.
 */
static void
AppendAlterTableCmdConstraint(StringInfo buf, Constraint *constraint,
							  AlterTableStmt *stmt, AlterTableType subtype)
{
	if (subtype != AT_AddConstraint && subtype != AT_AddColumn)
	{
		ereport(ERROR, (errmsg("Unsupported alter table subtype: %d", (int) subtype)));
	}

	/* Need to deparse the alter table constraint command only if we are adding a constraint name.*/
	if (constraint->conname == NULL)
	{
		ereport(ERROR, (errmsg(
							"Constraint name can not be NULL when deparsing the constraint.")));
	}

	if (subtype == AT_AddConstraint)
	{
		appendStringInfoString(buf, " ADD CONSTRAINT ");
	}
	else
	{
		appendStringInfoString(buf, " CONSTRAINT ");
	}

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

		if (subtype == AT_AddConstraint)
		{
			AppendColumnNameList(buf, constraint->keys);
		}

		if (constraint->including != NULL)
		{
			appendStringInfoString(buf, " INCLUDE ");

			AppendColumnNameList(buf, constraint->including);
		}

		if (constraint->options != NIL)
		{
			appendStringInfoString(buf, " WITH(");

			ListCell *defListCell;
			foreach(defListCell, constraint->options)
			{
				DefElem *def = (DefElem *) lfirst(defListCell);

				bool first = (defListCell == list_head(constraint->options));
				appendStringInfo(buf, "%s%s=%s", first ? "" : ",",
								 quote_identifier(def->defname),
								 quote_literal_cstr(defGetString(def)));
			}

			appendStringInfoChar(buf, ')');
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
		if (subtype == AT_AddColumn)
		{
			/*
			 * Preprocess should've rejected deparsing such an ALTER TABLE
			 * command but be on the safe side.
			 */
			ereport(ERROR, (errmsg("cannot add check constraint to column by "
								   "using ADD COLUMN command"),
							errhint("Consider using ALTER TABLE ... ADD CONSTRAINT "
									"... CHECK command after adding the column")));
		}

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
		if (subtype == AT_AddConstraint)
		{
			appendStringInfoString(buf, " FOREIGN KEY");

			AppendColumnNameList(buf, constraint->fk_attrs);
		}

		appendStringInfoString(buf, " REFERENCES");

		appendStringInfo(buf, " %s", quote_qualified_identifier(
							 constraint->pktable->schemaname,
							 constraint->pktable->relname));

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

	/*
	 * For ADD CONSTRAINT subcommand, FOREIGN KEY and CHECK constraints migth
	 * have NOT VALID option.
	 *
	 * Note that skip_validation might be true for an ADD COLUMN too but this
	 * is not because Postgres supports this but because Citus sets this flag
	 * to true for foreign key constraints added via ADD COLUMN. So we don't
	 * check for skip_validation for ADD COLUMN subcommand.
	 */
	if (subtype == AT_AddConstraint && constraint->skip_validation)
	{
		appendStringInfoString(buf, " NOT VALID ");
	}

	if (subtype == AT_AddColumn &&
		(constraint->deferrable || constraint->initdeferred))
	{
		/*
		 * For ADD COLUMN subcommand, the fact that whether given constraint
		 * is deferrable or initially deferred is indicated by another Constraint
		 * object, not via deferrable / initdeferred fields.
		 */
		ereport(ERROR, (errmsg("unexpected value set for deferrable/initdeferred "
							   "field for an ADD COLUMN subcommand")));
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
			AppendAlterTableCmdAddColumn(buf, alterTableCmd, stmt);
			break;
		}

		case AT_DropConstraint:
		{
			AppendAlterTableCmdDropConstraint(buf, alterTableCmd);
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
				AppendAlterTableCmdConstraint(buf, constraint, stmt, AT_AddConstraint);
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
 * GeneratedWhenStr returns the char representation of given generated_when
 * value.
 */
static const char *
GeneratedWhenStr(char generatedWhen)
{
	switch (generatedWhen)
	{
		case 'a':
		{
			return "ALWAYS";
		}

		case 'd':
		{
			return "BY DEFAULT";
		}

		default:
			ereport(ERROR, (errmsg("unrecognized generated_when: %d",
								   generatedWhen)));
	}
}


/*
 * DeparseRawExprForColumnDefault returns string representation of given
 * rawExpr based on given column type information.
 */
static char *
DeparseRawExprForColumnDefault(Oid relationId, Oid columnTypeId, int32 columnTypeMod,
							   char *columnName, char attgenerated, Node *rawExpr)
{
	ParseState *pstate = make_parsestate(NULL);
	Relation relation = RelationIdGetRelation(relationId);
	AddRangeTableEntryToQueryCompat(pstate, relation);

	Node *defaultExpr = cookDefault(pstate, rawExpr,
									columnTypeId, columnTypeMod,
									columnName, attgenerated);

	List *deparseContext = deparse_context_for(get_rel_name(relationId), relationId);

	int saveNestLevel = PushEmptySearchPath();
	char *defaultExprStr = deparse_expression(defaultExpr, deparseContext, false, false);
	PopEmptySearchPath(saveNestLevel);

	RelationClose(relation);

	return defaultExprStr;
}


/*
 * AppendAlterTableCmd builds and appends to the given buffer an AT_AddColumn command
 * from given AlterTableCmd object in the form ADD COLUMN ...
 */
static void
AppendAlterTableCmdAddColumn(StringInfo buf, AlterTableCmd *alterTableCmd,
							 AlterTableStmt *stmt)
{
	Assert(alterTableCmd->subtype == AT_AddColumn);

	Oid relationId = AlterTableLookupRelation(stmt, NoLock);

	appendStringInfoString(buf, " ADD COLUMN ");

	if (alterTableCmd->missing_ok)
	{
		appendStringInfoString(buf, "IF NOT EXISTS ");
	}

	ColumnDef *columnDefinition = (ColumnDef *) alterTableCmd->def;

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

	if (columnDefinition->compression)
	{
		appendStringInfo(buf, " COMPRESSION %s",
						 quote_identifier(columnDefinition->compression));
	}

	Oid collationOid = GetColumnDefCollation(NULL, columnDefinition, typeOid);
	if (OidIsValid(collationOid))
	{
		const char *identifier = FormatCollateBEQualified(collationOid);
		appendStringInfo(buf, " COLLATE %s", identifier);
	}

	ListCell *constraintCell = NULL;
	foreach(constraintCell, columnDefinition->constraints)
	{
		Constraint *constraint = (Constraint *) lfirst(constraintCell);

		if (constraint->contype == CONSTR_NOTNULL)
		{
			appendStringInfoString(buf, " NOT NULL");
		}
		else if (constraint->contype == CONSTR_NULL)
		{
			appendStringInfoString(buf, " NULL");
		}
		else if (constraint->contype == CONSTR_DEFAULT)
		{
			char attgenerated = '\0';
			appendStringInfo(buf, " DEFAULT %s",
							 DeparseRawExprForColumnDefault(relationId, typeOid, typmod,
															columnDefinition->colname,
															attgenerated,
															constraint->raw_expr));
		}
		else if (constraint->contype == CONSTR_IDENTITY)
		{
			/*
			 * Citus doesn't support adding identity columns via ALTER TABLE,
			 * so we don't bother teaching the deparser about them.
			 */
			ereport(ERROR, (errmsg("unexpectedly found identity column "
								   "definition in ALTER TABLE command")));
		}
		else if (constraint->contype == CONSTR_GENERATED)
		{
			char attgenerated = 's';
			appendStringInfo(buf, " GENERATED %s AS (%s) STORED",
							 GeneratedWhenStr(constraint->generated_when),
							 DeparseRawExprForColumnDefault(relationId, typeOid, typmod,
															columnDefinition->colname,
															attgenerated,
															constraint->raw_expr));
		}
		else if (constraint->contype == CONSTR_CHECK ||
				 constraint->contype == CONSTR_PRIMARY ||
				 constraint->contype == CONSTR_UNIQUE ||
				 constraint->contype == CONSTR_EXCLUSION ||
				 constraint->contype == CONSTR_FOREIGN)
		{
			AppendAlterTableCmdConstraint(buf, constraint, stmt, AT_AddColumn);
		}
		else if (constraint->contype == CONSTR_ATTR_DEFERRABLE)
		{
			appendStringInfoString(buf, " DEFERRABLE");
		}
		else if (constraint->contype == CONSTR_ATTR_NOT_DEFERRABLE)
		{
			appendStringInfoString(buf, " NOT DEFERRABLE");
		}
		else if (constraint->contype == CONSTR_ATTR_DEFERRED)
		{
			appendStringInfoString(buf, " INITIALLY DEFERRED");
		}
		else if (constraint->contype == CONSTR_ATTR_IMMEDIATE)
		{
			appendStringInfoString(buf, " INITIALLY IMMEDIATE");
		}
		else
		{
			ereport(ERROR, (errmsg("unsupported constraint type"),
							errdetail("constraint type: %d", constraint->contype)));
		}
	}
}


/*
 * AppendAlterTableCmdDropConstraint builds and appends to the given buffer an
 * AT_DropConstraint command from given AlterTableCmd object in the form
 * DROP CONSTRAINT ...
 */
static void
AppendAlterTableCmdDropConstraint(StringInfo buf, AlterTableCmd *alterTableCmd)
{
	appendStringInfoString(buf, " DROP CONSTRAINT");

	if (alterTableCmd->missing_ok)
	{
		appendStringInfoString(buf, " IF EXISTS");
	}

	appendStringInfo(buf, " %s", quote_identifier(alterTableCmd->name));

	if (alterTableCmd->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
}
