/*-------------------------------------------------------------------------
 *
 * deparse_database_stmts.c
 * All routines to deparse parameter statements.
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pg_version_constants.h"
#if PG_VERSION_NUM >= PG_VERSION_15

#include "utils/builtins.h"

#include "distributed/deparser.h"
#include "distributed/listutils.h"

static void AppendGrantParameters(StringInfo buf, GrantStmt *stmt);
static void AppendGrantOnParameterStmt(StringInfo buf, GrantStmt *stmt);

static void
AppendGrantParameters(StringInfo buf, GrantStmt *stmt)
{
	appendStringInfo(buf, " ON PARAMETER ");

	DefElem *def = NULL;
	foreach_ptr(def, stmt->objects)
	{
		char *parameter = strVal(def);
		appendStringInfoString(buf, quote_identifier(parameter));
		if (def != (DefElem *) lfirst(list_tail(stmt->objects)))
		{
			appendStringInfo(buf, ", ");
		}
	}
}


static void
AppendGrantOnParameterStmt(StringInfo buf, GrantStmt *stmt)
{
	Assert(stmt->objtype == OBJECT_PARAMETER_ACL);

	AppendGrantSharedPrefix(buf, stmt);

	AppendGrantParameters(buf, stmt);

	AppendGrantSharedSuffix(buf, stmt);
}


char *
DeparseGrantOnParameterStmt(Node *node)
{
	GrantStmt *stmt = castNode(GrantStmt, node);
	Assert(stmt->objtype == OBJECT_PARAMETER_ACL);

	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendGrantOnParameterStmt(&str, stmt);

	return str.data;
}


#endif /* PG_VERSION_NUM >= PG_VERSION_15 */
