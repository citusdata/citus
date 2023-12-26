/*-------------------------------------------------------------------------
 *
 * deparse_coment_stmts.c
 *
 *	  All routines to deparse comment statements.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"

#include "pg_version_compat.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"


const char * const ObjectTypeNames[] =
{
    [OBJECT_DATABASE] = "DATABASE",
    [OBJECT_ROLE] = "ROLE",
    [OBJECT_TSCONFIGURATION]  = "TEXT SEARCH CONFIGURATION",
    [OBJECT_TSDICTIONARY]  = "TEXT SEARCH DICTIONARY",
    /* etc. */
};

char *
DeparseCommentStmt(Node *node)
{
	CommentStmt *stmt = castNode(CommentStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	const char *objectName = quote_identifier(strVal(stmt->object));
	const char  *objectType = ObjectTypeNames[stmt->objtype];

	char *comment = stmt->comment != NULL ? quote_literal_cstr(stmt->comment) : "NULL";


	appendStringInfo(&str, "COMMENT ON %s %s IS %s;",objectType, objectName, comment);

	return str.data;
}
