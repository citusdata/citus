/*-------------------------------------------------------------------------
 *
 * role.c
 *
 * Utilities for ALTER ROLE statments.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/commands/utility_hook.h"
#include "distributed/worker_protocol.h"
#include "fmgr.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(alter_role_if_exists);

/*
 * alter_role_if_exists checks if the role, whose name is given
 * in the first parameter exists and then runs the query, which is the second
 * parameter. This UDF is particularly used for ALTER ROLE queries, how ever it
 * can run any other query too.
 */
Datum
alter_role_if_exists(PG_FUNCTION_ARGS)
{
	text *rolenameText = PG_GETARG_TEXT_P(0);
	const char *rolename = text_to_cstring(rolenameText);
	text *utilityQueryText = PG_GETARG_TEXT_P(1);
	const char *utilityQuery = text_to_cstring(utilityQueryText);
	Node *parseTree = NULL;

	if (get_role_oid(rolename, true) == InvalidOid)
	{
		PG_RETURN_BOOL(false);
	}

	parseTree = ParseTreeNode(utilityQuery);

	CitusProcessUtility(parseTree, utilityQuery, PROCESS_UTILITY_TOPLEVEL, NULL,
						None_Receiver, NULL);

	PG_RETURN_BOOL(true);
}
