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
#include "utils/elog.h"

#include "pg_version_compat.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/comment.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/log_utils.h"


const ObjectTypeInfo ObjectTypeInfos[] =
{
	[OBJECT_DATABASE] = { "DATABASE", T_String },
	[OBJECT_ROLE] = { "ROLE", T_String },
	[OBJECT_TSCONFIGURATION] = { "TEXT SEARCH CONFIGURATION", T_List },
	[OBJECT_TSDICTIONARY] = { "TEXT SEARCH DICTIONARY", T_List },

	/* When support for propagating comments to new objects is introduced, an entry for each
	 * statement type should be added to this list. The first element in each entry is the keyword
	 * that will be included in the 'COMMENT ..' statement. The second element is the type of
	 * stmt->object, which represents the name of the propagated object.
	 */
};

char *
DeparseCommentStmt(Node *node)
{
	CommentStmt *stmt = castNode(CommentStmt, node);
	StringInfoData str = { 0 };
	initStringInfo(&str);

	const char *objectName = NULL;
	if (IsA(stmt->object, String))
	{
		objectName = quote_identifier(strVal(stmt->object));
	}
	else if (IsA(stmt->object, List))
	{
		objectName = NameListToQuotedString(castNode(List, stmt->object));
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("unknown object type")));
	}

	const char *objectType = ObjectTypeInfos[stmt->objtype].name;

	char *comment = stmt->comment != NULL ? quote_literal_cstr(stmt->comment) : "NULL";


	appendStringInfo(&str, "COMMENT ON %s %s IS %s;", objectType, objectName, comment);

	return str.data;
}
