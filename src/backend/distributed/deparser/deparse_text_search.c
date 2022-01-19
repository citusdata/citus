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
