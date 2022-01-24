#include "postgres.h"

#include "catalog/namespace.h"
#include "utils/builtins.h"

#include "distributed/deparser.h"
#include "distributed/listutils.h"

static void AppendDefElemList(StringInfo buf, List *defelms);

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
DeparseRenameTextSearchStmt(Node *node)
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
	if (stmt->kind != ALTER_TSCONFIG_ADD_MAPPING)
	{
		ereport(ERROR, (errmsg("can only deparse ADD MAPPING statements for "
							   "ALTER TEXT SEARCH CONFIGURATION")));
	}

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	char *identifier = NameListToQuotedString(castNode(List, stmt->cfgname));
	appendStringInfo(&buf, "ALTER TEXT SEARCH CONFIGURATION %s ", identifier);

	appendStringInfoString(&buf, " ALTER MAPPING FOR ");
	Value *tokentype = NULL;
	bool first = true;
	foreach_ptr(tokentype, stmt->tokentype)
	{
		Assert(nodeTag(tokentype) == T_String);
		if (!first)
		{
			appendStringInfoString(&buf, ", ");
		}
		first = false;

		appendStringInfoString(&buf, strVal(tokentype));
	}

	appendStringInfoString(&buf, " WITH ");
	List *dictNames = NIL;
	first = true;
	foreach_ptr(dictNames, stmt->dicts)
	{
		if (!first)
		{
			appendStringInfoString(&buf, ", ");
		}
		first = false;

		char *dictIdentifier = NameListToQuotedString(dictNames);
		appendStringInfoString(&buf, dictIdentifier);
	}

	appendStringInfoString(&buf, ";");

	return buf.data;
}
