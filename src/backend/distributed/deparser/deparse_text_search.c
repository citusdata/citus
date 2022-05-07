/*-------------------------------------------------------------------------
 *
 * deparse_text_search.c
 *	  All routines to deparse text search statements.
 *	  This file contains all entry points specific for text search statement deparsing.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "utils/builtins.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"

static void AppendDefElemList(StringInfo buf, List *defelems, char *objectName);

static void AppendStringInfoTokentypeList(StringInfo buf, List *tokentypes);
static void AppendStringInfoDictnames(StringInfo buf, List *dicts);


/*
 * DeparseCreateTextSearchConfigurationStmt returns the sql for a DefineStmt defining a
 * TEXT SEARCH CONFIGURATION
 *
 * Although the syntax is mutually exclusive on the two arguments that can be passed in
 * the deparser will syntactically correct multiple definitions if provided. *
 */
char *
DeparseCreateTextSearchConfigurationStmt(Node *node)
{
	DefineStmt *stmt = castNode(DefineStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	const char *identifier = NameListToQuotedString(stmt->defnames);
	appendStringInfo(&buf, "CREATE TEXT SEARCH CONFIGURATION %s ", identifier);
	appendStringInfoString(&buf, "(");
	AppendDefElemList(&buf, stmt->definition, "CONFIGURATION");
	appendStringInfoString(&buf, ");");

	return buf.data;
}


/*
 * DeparseCreateTextSearchDictionaryStmt returns the sql for a DefineStmt defining a
 * TEXT SEARCH DICTIONARY
 *
 * Although the syntax is mutually exclusive on the two arguments that can be passed in
 * the deparser will syntactically correct multiple definitions if provided. *
 */
char *
DeparseCreateTextSearchDictionaryStmt(Node *node)
{
	DefineStmt *stmt = castNode(DefineStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	const char *identifier = NameListToQuotedString(stmt->defnames);
	appendStringInfo(&buf, "CREATE TEXT SEARCH DICTIONARY %s ", identifier);
	appendStringInfoString(&buf, "(");
	AppendDefElemList(&buf, stmt->definition, "DICTIONARY");
	appendStringInfoString(&buf, ");");

	return buf.data;
}


/*
 * AppendDefElemList is a helper to append a comma separated list of definitions to a
 * define statement.
 *
 * The extra objectName parameter is used to create meaningful error messages.
 */
static void
AppendDefElemList(StringInfo buf, List *defelems, char *objectName)
{
	DefElem *defelem = NULL;
	bool first = true;
	foreach_ptr(defelem, defelems)
	{
		if (!first)
		{
			appendStringInfoString(buf, ", ");
		}
		first = false;

		/*
		 * There are some operations that can omit the argument. In that case, we only use
		 * the defname.
		 *
		 * For example, omitting [ = value ] in the next query results in resetting the
		 * option to defaults:
		 * ALTER TEXT SEARCH DICTIONARY name ( option [ = value ] );
		 */
		if (defelem->arg == NULL)
		{
			appendStringInfo(buf, "%s", defelem->defname);
			continue;
		}

		/* extract value from defelem */
		const char *value = defGetString(defelem);

		/* stringify */
		appendStringInfo(buf, "%s = %s", defelem->defname, value);
	}
}


/*
 * DeparseDropTextSearchConfigurationStmt returns the sql representation for a DROP TEXT
 * SEARCH CONFIGURATION ... statment. Supports dropping multiple configurations at once.
 */
char *
DeparseDropTextSearchConfigurationStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	Assert(stmt->removeType == OBJECT_TSCONFIGURATION);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfoString(&buf, "DROP TEXT SEARCH CONFIGURATION ");
	List *nameList = NIL;
	bool first = true;
	foreach_ptr(nameList, stmt->objects)
	{
		if (!first)
		{
			appendStringInfoString(&buf, ", ");
		}
		first = false;

		appendStringInfoString(&buf, NameListToQuotedString(nameList));
	}

	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(&buf, " CASCADE");
	}

	appendStringInfoString(&buf, ";");

	return buf.data;
}


/*
 * DeparseDropTextSearchDictionaryStmt returns the sql representation for a DROP TEXT SEARCH
 * DICTIONARY ... statment. Supports dropping multiple dictionaries at once.
 */
char *
DeparseDropTextSearchDictionaryStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	Assert(stmt->removeType == OBJECT_TSDICTIONARY);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfoString(&buf, "DROP TEXT SEARCH DICTIONARY ");
	List *nameList = NIL;
	bool first = true;
	foreach_ptr(nameList, stmt->objects)
	{
		if (!first)
		{
			appendStringInfoString(&buf, ", ");
		}
		first = false;

		appendStringInfoString(&buf, NameListToQuotedString(nameList));
	}

	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(&buf, " CASCADE");
	}

	appendStringInfoString(&buf, ";");

	return buf.data;
}


/*
 * DeparseRenameTextSearchConfigurationStmt returns the sql representation of a ALTER TEXT
 * SEARCH CONFIGURATION ... RENAME TO ... statement.
 */
char *
DeparseRenameTextSearchConfigurationStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_TSCONFIGURATION);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	char *identifier = NameListToQuotedString(castNode(List, stmt->object));
	appendStringInfo(&buf, "ALTER TEXT SEARCH CONFIGURATION %s RENAME TO %s;",
					 identifier, quote_identifier(stmt->newname));

	return buf.data;
}


/*
 * DeparseRenameTextSearchDictionaryStmt returns the sql representation of a ALTER TEXT SEARCH
 * DICTIONARY ... RENAME TO ... statement.
 */
char *
DeparseRenameTextSearchDictionaryStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_TSDICTIONARY);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	char *identifier = NameListToQuotedString(castNode(List, stmt->object));
	appendStringInfo(&buf, "ALTER TEXT SEARCH DICTIONARY %s RENAME TO %s;",
					 identifier, quote_identifier(stmt->newname));

	return buf.data;
}


/*
 * DeparseAlterTextSearchConfigurationStmt returns the sql representation of any generic
 * ALTER TEXT SEARCH CONFIGURATION .... statement. The statements supported include:
 *  - ALTER TEXT SEARCH CONFIGURATIONS ... ADD MAPPING FOR [, ...] WITH [, ...]
 *  - ALTER TEXT SEARCH CONFIGURATIONS ... ALTER MAPPING FOR [, ...] WITH [, ...]
 *  - ALTER TEXT SEARCH CONFIGURATIONS ... ALTER MAPPING REPLACE ... WITH ...
 *  - ALTER TEXT SEARCH CONFIGURATIONS ... ALTER MAPPING FOR [, ...] REPLACE ... WITH ...
 *  - ALTER TEXT SEARCH CONFIGURATIONS ... DROP MAPPING [ IF EXISTS ] FOR ...
 */
char *
DeparseAlterTextSearchConfigurationStmt(Node *node)
{
	AlterTSConfigurationStmt *stmt = castNode(AlterTSConfigurationStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	char *identifier = NameListToQuotedString(castNode(List, stmt->cfgname));
	appendStringInfo(&buf, "ALTER TEXT SEARCH CONFIGURATION %s", identifier);

	switch (stmt->kind)
	{
		case ALTER_TSCONFIG_ADD_MAPPING:
		{
			appendStringInfoString(&buf, " ADD MAPPING FOR ");
			AppendStringInfoTokentypeList(&buf, stmt->tokentype);

			appendStringInfoString(&buf, " WITH ");
			AppendStringInfoDictnames(&buf, stmt->dicts);

			break;
		}

		case ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN:
		{
			appendStringInfoString(&buf, " ALTER MAPPING FOR ");
			AppendStringInfoTokentypeList(&buf, stmt->tokentype);

			appendStringInfoString(&buf, " WITH ");
			AppendStringInfoDictnames(&buf, stmt->dicts);

			break;
		}

		case ALTER_TSCONFIG_REPLACE_DICT:
		case ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN:
		{
			appendStringInfoString(&buf, " ALTER MAPPING");
			if (list_length(stmt->tokentype) > 0)
			{
				appendStringInfoString(&buf, " FOR ");
				AppendStringInfoTokentypeList(&buf, stmt->tokentype);
			}

			if (list_length(stmt->dicts) != 2)
			{
				elog(ERROR, "unexpected number of dictionaries while deparsing ALTER "
							"TEXT SEARCH CONFIGURATION ... ALTER MAPPING [FOR ...] REPLACE "
							"statement.");
			}

			appendStringInfo(&buf, " REPLACE %s",
							 NameListToQuotedString(linitial(stmt->dicts)));

			appendStringInfo(&buf, " WITH %s",
							 NameListToQuotedString(lsecond(stmt->dicts)));

			break;
		}

		case ALTER_TSCONFIG_DROP_MAPPING:
		{
			appendStringInfoString(&buf, " DROP MAPPING");

			if (stmt->missing_ok)
			{
				appendStringInfoString(&buf, " IF EXISTS");
			}

			appendStringInfoString(&buf, " FOR ");
			AppendStringInfoTokentypeList(&buf, stmt->tokentype);
			break;
		}

		default:
		{
			elog(ERROR, "unable to deparse unsupported ALTER TEXT SEARCH STATEMENT");
		}
	}

	appendStringInfoString(&buf, ";");

	return buf.data;
}


/*
 * DeparseAlterTextSearchConfigurationStmt returns the sql representation of any generic
 * ALTER TEXT SEARCH DICTIONARY .... statement. The statements supported include
 * - ALTER TEXT SEARCH DICTIONARY name ( option [ = value ] [, ... ] )
 */
char *
DeparseAlterTextSearchDictionaryStmt(Node *node)
{
	AlterTSDictionaryStmt *stmt = castNode(AlterTSDictionaryStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	char *identifier = NameListToQuotedString(castNode(List, stmt->dictname));
	appendStringInfo(&buf, "ALTER TEXT SEARCH DICTIONARY %s ( ", identifier);

	AppendDefElemList(&buf, stmt->options, "DICTIONARY");
	appendStringInfoString(&buf, " );");
	return buf.data;
}


/*
 * DeparseAlterTextSearchConfigurationSchemaStmt returns the sql statement representing
 * ALTER TEXT SEARCH CONFIGURATION ... SET SCHEMA ... statements.
 */
char *
DeparseAlterTextSearchConfigurationSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TSCONFIGURATION);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfo(&buf, "ALTER TEXT SEARCH CONFIGURATION %s SET SCHEMA %s;",
					 NameListToQuotedString(castNode(List, stmt->object)),
					 quote_identifier(stmt->newschema));

	return buf.data;
}


/*
 * DeparseAlterTextSearchDictionarySchemaStmt returns the sql statement representing ALTER TEXT
 * SEARCH DICTIONARY ... SET SCHEMA ... statements.
 */
char *
DeparseAlterTextSearchDictionarySchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_TSDICTIONARY);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfo(&buf, "ALTER TEXT SEARCH DICTIONARY %s SET SCHEMA %s;",
					 NameListToQuotedString(castNode(List, stmt->object)),
					 quote_identifier(stmt->newschema));

	return buf.data;
}


/*
 * DeparseTextSearchConfigurationCommentStmt returns the sql statement representing
 * COMMENT ON TEXT SEARCH CONFIGURATION ... IS ...
 */
char *
DeparseTextSearchConfigurationCommentStmt(Node *node)
{
	CommentStmt *stmt = castNode(CommentStmt, node);
	Assert(stmt->objtype == OBJECT_TSCONFIGURATION);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfo(&buf, "COMMENT ON TEXT SEARCH CONFIGURATION %s IS ",
					 NameListToQuotedString(castNode(List, stmt->object)));

	if (stmt->comment == NULL)
	{
		appendStringInfoString(&buf, "NULL");
	}
	else
	{
		appendStringInfoString(&buf, quote_literal_cstr(stmt->comment));
	}

	appendStringInfoString(&buf, ";");

	return buf.data;
}


/*
 * DeparseTextSearchDictionaryCommentStmt returns the sql statement representing
 * COMMENT ON TEXT SEARCH DICTIONARY ... IS ...
 */
char *
DeparseTextSearchDictionaryCommentStmt(Node *node)
{
	CommentStmt *stmt = castNode(CommentStmt, node);
	Assert(stmt->objtype == OBJECT_TSDICTIONARY);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfo(&buf, "COMMENT ON TEXT SEARCH DICTIONARY %s IS ",
					 NameListToQuotedString(castNode(List, stmt->object)));

	if (stmt->comment == NULL)
	{
		appendStringInfoString(&buf, "NULL");
	}
	else
	{
		appendStringInfoString(&buf, quote_literal_cstr(stmt->comment));
	}

	appendStringInfoString(&buf, ";");

	return buf.data;
}


/*
 * AppendStringInfoTokentypeList specializes in adding a comma separated list of
 * token_tyoe's to TEXT SEARCH CONFIGURATION commands
 */
static void
AppendStringInfoTokentypeList(StringInfo buf, List *tokentypes)
{
	String *tokentype = NULL;
	bool first = true;
	foreach_ptr(tokentype, tokentypes)
	{
		if (nodeTag(tokentype) != T_String)
		{
			elog(ERROR,
				 "unexpected tokentype for deparsing in text search configuration");
		}

		if (!first)
		{
			appendStringInfoString(buf, ", ");
		}
		first = false;

		appendStringInfoString(buf, strVal(tokentype));
	}
}


/*
 * AppendStringInfoDictnames specializes in appending a comma separated list of
 * dictionaries to TEXT SEARCH CONFIGURATION commands.
 */
static void
AppendStringInfoDictnames(StringInfo buf, List *dicts)
{
	List *dictNames = NIL;
	bool first = true;
	foreach_ptr(dictNames, dicts)
	{
		if (!first)
		{
			appendStringInfoString(buf, ", ");
		}
		first = false;

		char *dictIdentifier = NameListToQuotedString(dictNames);
		appendStringInfoString(buf, dictIdentifier);
	}
}


/*
 * DeparseAlterTextSearchConfigurationOwnerStmt returns the sql statement representing
 * ALTER TEXT SEARCH CONFIGURATION ... ONWER TO ... commands.
 */
char *
DeparseAlterTextSearchConfigurationOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_TSCONFIGURATION);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfo(&buf, "ALTER TEXT SEARCH CONFIGURATION %s OWNER TO %s;",
					 NameListToQuotedString(castNode(List, stmt->object)),
					 RoleSpecString(stmt->newowner, true));

	return buf.data;
}


/*
 * DeparseAlterTextSearchDictionaryOwnerStmt returns the sql statement representing ALTER TEXT
 * SEARCH DICTIONARY ... ONWER TO ... commands.
 */
char *
DeparseAlterTextSearchDictionaryOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_TSDICTIONARY);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfo(&buf, "ALTER TEXT SEARCH DICTIONARY %s OWNER TO %s;",
					 NameListToQuotedString(castNode(List, stmt->object)),
					 RoleSpecString(stmt->newowner, true));

	return buf.data;
}
