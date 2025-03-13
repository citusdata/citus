/*-------------------------------------------------------------------------
 *
 * deparse_publication_stmts.c
 *	  All routines to deparse publication statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relation.h"
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/value.h"
#include "parser/parse_clause.h"
#include "parser/parse_collate.h"
#include "parser/parse_node.h"
#include "parser/parse_relation.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/namespace_utils.h"


static void AppendCreatePublicationStmt(StringInfo buf, CreatePublicationStmt *stmt,
										bool whereClauseNeedsTransform,
										bool includeLocalTables);
static bool AppendPublicationObjects(StringInfo buf, List *publicationObjects,
									 bool whereClauseNeedsTransform,
									 bool includeLocalTables);
static void AppendWhereClauseExpression(StringInfo buf, RangeVar *tableName,
										Node *whereClause,
										bool whereClauseNeedsTransform);
static void AppendAlterPublicationAction(StringInfo buf, AlterPublicationAction action);
static bool AppendAlterPublicationStmt(StringInfo buf, AlterPublicationStmt *stmt,
									   bool whereClauseNeedsTransform,
									   bool includeLocalTables);
static void AppendDropPublicationStmt(StringInfo buf, DropStmt *stmt);
static void AppendRenamePublicationStmt(StringInfo buf, RenameStmt *stmt);
static void AppendAlterPublicationOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);
static void AppendPublicationOptions(StringInfo stringBuffer, List *optionList);
static void AppendIdentifierList(StringInfo buf, List *objects);


/*
 * DeparseCreatePublicationStmt builds and returns a string representing a
 * CreatePublicationStmt.
 */
char *
DeparseCreatePublicationStmt(Node *node)
{
	/* regular deparsing function takes CREATE PUBLICATION from the parser */
	bool whereClauseNeedsTransform = false;

	/* for regular CREATE PUBLICATION we do not propagate local tables */
	bool includeLocalTables = false;

	return DeparseCreatePublicationStmtExtended(node, whereClauseNeedsTransform,
												includeLocalTables);
}


/*
 * DeparseCreatePublicationStmtExtended builds and returns a string representing a
 * CreatePublicationStmt, which may have already-transformed expressions.
 */
char *
DeparseCreatePublicationStmtExtended(Node *node, bool whereClauseNeedsTransform,
									 bool includeLocalTables)
{
	CreatePublicationStmt *stmt = castNode(CreatePublicationStmt, node);

	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendCreatePublicationStmt(&str, stmt, whereClauseNeedsTransform,
								includeLocalTables);

	return str.data;
}


/*
 * AppendCreatePublicationStmt appends a string representing a
 * CreatePublicationStmt to a buffer.
 */
static void
AppendCreatePublicationStmt(StringInfo buf, CreatePublicationStmt *stmt,
							bool whereClauseNeedsTransform,
							bool includeLocalTables)
{
	appendStringInfo(buf, "CREATE PUBLICATION %s",
					 quote_identifier(stmt->pubname));

	if (stmt->for_all_tables)
	{
		appendStringInfoString(buf, " FOR ALL TABLES");
	}
	else if (stmt->pubobjects != NIL)
	{
		bool hasObjects = false;
		PublicationObjSpec *publicationObject = NULL;

		/*
		 * Check whether there are objects to propagate, mainly to know whether
		 * we should include "FOR".
		 */
		foreach_declared_ptr(publicationObject, stmt->pubobjects)
		{
			if (publicationObject->pubobjtype == PUBLICATIONOBJ_TABLE)
			{
				/* FOR TABLE ... */
				PublicationTable *publicationTable = publicationObject->pubtable;

				if (includeLocalTables ||
					IsCitusTableRangeVar(publicationTable->relation, NoLock, false))
				{
					hasObjects = true;
					break;
				}
			}
			else
			{
				hasObjects = true;
				break;
			}
		}

		if (hasObjects)
		{
			appendStringInfoString(buf, " FOR");
			AppendPublicationObjects(buf, stmt->pubobjects, whereClauseNeedsTransform,
									 includeLocalTables);
		}
	}

	if (stmt->options != NIL)
	{
		appendStringInfoString(buf, " WITH (");
		AppendPublicationOptions(buf, stmt->options);
		appendStringInfoString(buf, ")");
	}
}


/*
 * AppendPublicationObjects appends a string representing a list of publication
 * objects to a buffer.
 *
 * For instance: TABLE users, departments, TABLES IN SCHEMA production
 */
static bool
AppendPublicationObjects(StringInfo buf, List *publicationObjects,
						 bool whereClauseNeedsTransform,
						 bool includeLocalTables)
{
	PublicationObjSpec *publicationObject = NULL;
	bool appendedObject = false;

	foreach_declared_ptr(publicationObject, publicationObjects)
	{
		if (publicationObject->pubobjtype == PUBLICATIONOBJ_TABLE)
		{
			/* FOR TABLE ... */
			PublicationTable *publicationTable = publicationObject->pubtable;
			RangeVar *rangeVar = publicationTable->relation;
			char *schemaName = rangeVar->schemaname;
			char *tableName = rangeVar->relname;

			if (!includeLocalTables && !IsCitusTableRangeVar(rangeVar, NoLock, false))
			{
				/* do not propagate local tables */
				continue;
			}

			if (schemaName != NULL)
			{
				/* qualified table name */
				appendStringInfo(buf, "%s TABLE %s",
								 appendedObject ? "," : "",
								 quote_qualified_identifier(schemaName, tableName));
			}
			else
			{
				/* unqualified table name */
				appendStringInfo(buf, "%s TABLE %s",
								 appendedObject ? "," : "",
								 quote_identifier(tableName));
			}

			if (publicationTable->columns != NIL)
			{
				appendStringInfoString(buf, " (");
				AppendIdentifierList(buf, publicationTable->columns);
				appendStringInfoString(buf, ")");
			}

			if (publicationTable->whereClause != NULL)
			{
				appendStringInfoString(buf, " WHERE (");

				AppendWhereClauseExpression(buf, rangeVar,
											publicationTable->whereClause,
											whereClauseNeedsTransform);

				appendStringInfoString(buf, ")");
			}
		}
		else
		{
			/* FOR TABLES IN SCHEMA */
			char *schemaName = publicationObject->name;

			if (publicationObject->pubobjtype == PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA)
			{
				List *searchPath = fetch_search_path(false);
				if (searchPath == NIL)
				{
					ereport(ERROR, errcode(ERRCODE_UNDEFINED_SCHEMA),
							errmsg("no schema has been selected for "
								   "CURRENT_SCHEMA"));
				}

				schemaName = get_namespace_name(linitial_oid(searchPath));
			}

			appendStringInfo(buf, "%s TABLES IN SCHEMA %s",
							 appendedObject ? "," : "",
							 quote_identifier(schemaName));
		}

		appendedObject = true;
	}

	return appendedObject;
}


/*
 * AppendWhereClauseExpression appends a deparsed expression that can
 * contain a filter on the given table. If whereClauseNeedsTransform is set
 * the expression is first tranformed.
 */
static void
AppendWhereClauseExpression(StringInfo buf, RangeVar *tableName,
							Node *whereClause, bool whereClauseNeedsTransform)
{
	Relation relation = relation_openrv(tableName, AccessShareLock);

	if (whereClauseNeedsTransform)
	{
		ParseState *pstate = make_parsestate(NULL);
		pstate->p_sourcetext = "";
		ParseNamespaceItem *nsitem = addRangeTableEntryForRelation(pstate,
																   relation,
																   AccessShareLock, NULL,
																   false, false);
		addNSItemToQuery(pstate, nsitem, false, true, true);

		whereClause = transformWhereClause(pstate,
										   copyObject(whereClause),
										   EXPR_KIND_WHERE,
										   "PUBLICATION WHERE");

		assign_expr_collations(pstate, whereClause);
	}

	List *relationContext = deparse_context_for(tableName->relname, relation->rd_id);

	int saveNestLevel = PushEmptySearchPath();
	char *whereClauseString = deparse_expression(whereClause,
												 relationContext,
												 true, true);
	PopEmptySearchPath(saveNestLevel);

	appendStringInfoString(buf, whereClauseString);

	relation_close(relation, AccessShareLock);
}


/*
 * DeparseAlterPublicationSchemaStmt builds and returns a string representing
 * an AlterPublicationStmt.
 */
char *
DeparseAlterPublicationStmt(Node *node)
{
	/* regular deparsing function takes ALTER PUBLICATION from the parser */
	bool whereClauseNeedsTransform = true;

	/* for regular ALTER PUBLICATION we do not propagate local tables */
	bool includeLocalTables = false;

	return DeparseAlterPublicationStmtExtended(node, whereClauseNeedsTransform,
											   includeLocalTables);
}


/*
 * DeparseAlterPublicationStmtExtended builds and returns a string representing a
 * AlterPublicationStmt, which may have already-transformed expressions.
 */
char *
DeparseAlterPublicationStmtExtended(Node *node, bool whereClauseNeedsTransform,
									bool includeLocalTables)
{
	AlterPublicationStmt *stmt = castNode(AlterPublicationStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	if (!AppendAlterPublicationStmt(&str, stmt, whereClauseNeedsTransform,
									includeLocalTables))
	{
		Assert(!includeLocalTables);

		/*
		 * When there are no objects to propagate, then there is no
		 * valid ALTER PUBLICATION to construct.
		 */
		return NULL;
	}

	return str.data;
}


/*
 * AppendAlterPublicationStmt appends a string representing an AlterPublicationStmt
 * of the form ALTER PUBLICATION .. ADD/SET/DROP
 */
static bool
AppendAlterPublicationStmt(StringInfo buf, AlterPublicationStmt *stmt,
						   bool whereClauseNeedsTransform,
						   bool includeLocalTables)
{
	appendStringInfo(buf, "ALTER PUBLICATION %s",
					 quote_identifier(stmt->pubname));

	if (stmt->options)
	{
		appendStringInfoString(buf, " SET (");
		AppendPublicationOptions(buf, stmt->options);
		appendStringInfoString(buf, ")");

		/* changing options cannot be combined with other actions */
		return true;
	}

	AppendAlterPublicationAction(buf, stmt->action);
	return AppendPublicationObjects(buf, stmt->pubobjects, whereClauseNeedsTransform,
									includeLocalTables);
}


/*
 * AppendAlterPublicationAction appends a string representing an AlterPublicationAction
 * to a buffer.
 */
static void
AppendAlterPublicationAction(StringInfo buf, AlterPublicationAction action)
{
	switch (action)
	{
		case AP_AddObjects:
		{
			appendStringInfoString(buf, " ADD");
			break;
		}

		case AP_DropObjects:
		{
			appendStringInfoString(buf, " DROP");
			break;
		}

		case AP_SetObjects:
		{
			appendStringInfoString(buf, " SET");
			break;
		}

		default:
		{
			ereport(ERROR, (errmsg("unrecognized publication action: %d", action)));
		}
	}
}


/*
 * DeparseDropPublicationStmt builds and returns a string representing the DropStmt
 */
char *
DeparseDropPublicationStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->removeType == OBJECT_PUBLICATION);

	AppendDropPublicationStmt(&str, stmt);

	return str.data;
}


/*
 * AppendDropPublicationStmt appends a string representing the DropStmt to a buffer
 */
static void
AppendDropPublicationStmt(StringInfo buf, DropStmt *stmt)
{
	appendStringInfoString(buf, "DROP PUBLICATION ");
	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}
	AppendIdentifierList(buf, stmt->objects);
	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
}


/*
 * DeparseRenamePublicationStmt builds and returns a string representing the RenameStmt
 */
char *
DeparseRenamePublicationStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->renameType == OBJECT_PUBLICATION);

	AppendRenamePublicationStmt(&str, stmt);

	return str.data;
}


/*
 * AppendRenamePublicationStmt appends a string representing the RenameStmt to a buffer
 */
static void
AppendRenamePublicationStmt(StringInfo buf, RenameStmt *stmt)
{
	appendStringInfo(buf, "ALTER PUBLICATION %s RENAME TO %s;",
					 quote_identifier(strVal(stmt->object)),
					 quote_identifier(stmt->newname));
}


/*
 * DeparseAlterPublicationOwnerStmt builds and returns a string representing the AlterOwnerStmt
 */
char *
DeparseAlterPublicationOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(stmt->objectType == OBJECT_PUBLICATION);

	AppendAlterPublicationOwnerStmt(&str, stmt);

	return str.data;
}


/*
 * AppendAlterPublicationOwnerStmt appends a string representing the AlterOwnerStmt to a buffer
 */
static void
AppendAlterPublicationOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt)
{
	Assert(stmt->objectType == OBJECT_PUBLICATION);

	appendStringInfo(buf, "ALTER PUBLICATION %s OWNER TO %s;",
					 quote_identifier(strVal(stmt->object)),
					 RoleSpecString(stmt->newowner, true));
}


/*
 * AppendPublicationOptions appends a string representing a list of publication opions.
 */
static void
AppendPublicationOptions(StringInfo stringBuffer, List *optionList)
{
	ListCell *optionCell = NULL;
	bool firstOptionPrinted = false;

	foreach(optionCell, optionList)
	{
		DefElem *option = (DefElem *) lfirst(optionCell);
		char *optionName = option->defname;
		char *optionValue = defGetString(option);
		NodeTag valueType = nodeTag(option->arg);

		if (firstOptionPrinted)
		{
			appendStringInfo(stringBuffer, ", ");
		}
		firstOptionPrinted = true;

		appendStringInfo(stringBuffer, "%s = ",
						 quote_identifier(optionName));

		if (valueType == T_Integer || valueType == T_Float || valueType == T_Boolean)
		{
			/* string escaping is unnecessary for numeric types and can cause issues */
			appendStringInfo(stringBuffer, "%s", optionValue);
		}
		else
		{
			appendStringInfo(stringBuffer, "%s", quote_literal_cstr(optionValue));
		}
	}
}


/*
 * AppendIdentifierList appends a string representing a list of
 * identifiers (of String type).
 */
static void
AppendIdentifierList(StringInfo buf, List *objects)
{
	ListCell *objectCell = NULL;

	foreach(objectCell, objects)
	{
		char *name = strVal(lfirst(objectCell));

		if (objectCell != list_head(objects))
		{
			appendStringInfo(buf, ", ");
		}

		appendStringInfoString(buf, quote_identifier(name));
	}
}
