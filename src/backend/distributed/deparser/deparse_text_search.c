#include "postgres.h"

#include "catalog/namespace.h"
#include "utils/builtins.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"

static void AppendDefElemList(StringInfo buf, List *defelms);

static void AppendStringInfoTokentypeList(StringInfo buf, List *tokentypes);
static void AppendStringInfoDictnames(StringInfo buf, List *dicts);

char *
DeparseCreateTextSearchStmt(Node *node)
{
	DefineStmt *stmt = castNode(DefineStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	const char *identifier = NameListToQuotedString(stmt->defnames);
	appendStringInfo(&buf, "CREATE TEXT SEARCH CONFIGURATION %s ", identifier);
	appendStringInfoString(&buf, "(");
	AppendDefElemList(&buf, stmt->definition);
	appendStringInfoString(&buf, ");");

	return buf.data;
}


static void
AppendDefElemList(StringInfo buf, List *defelems)
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

		/* extract identifier from defelem */
		const char *identifier = NULL;
		switch (nodeTag(defelem->arg))
		{
			case T_String:
			{
				identifier = quote_identifier(strVal(defelem->arg));
				break;
			}

			case T_TypeName:
			{
				TypeName *typeName = castNode(TypeName, defelem->arg);
				identifier = NameListToQuotedString(typeName->names);
				break;
			}

			default:
			{
				ereport(ERROR, (errmsg("unexpected argument during deparsing of "
									   "TEXT SEARCH CONFIGURATION definition")));
			}
		}

		/* stringify */
		appendStringInfo(buf, "%s = %s", defelem->defname, identifier);
	}
}


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


char *
DeparseTextSearchConfigurationCommentStmt(Node *node)
{
	CommentStmt *stmt = castNode(CommentStmt, node);
	Assert(stmt->objtype == OBJECT_TSCONFIGURATION);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfo(&buf, "COMMENT ON TEXT SEARCH CONFIGURATION %s IS %s;",
					 NameListToQuotedString(castNode(List, stmt->object)),
					 quote_literal_cstr(stmt->comment));

	return buf.data;
}


static void
AppendStringInfoTokentypeList(StringInfo buf, List *tokentypes)
{
	Value *tokentype = NULL;
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
