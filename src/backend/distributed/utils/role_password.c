/*-------------------------------------------------------------------------
 *
 * role_password.c
 *
 * Utilities for role passwords.
 *
 * Copyright (c) 2019, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "distributed/commands/utility_hook.h"
#include "distributed/worker_protocol.h"
#include "fmgr.h"
#include "postgres.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(set_password_if_role_exists);

Datum
set_password_if_role_exists(PG_FUNCTION_ARGS)
{
	text *rolenameText = PG_GETARG_TEXT_P(0);
	const char *rolename = text_to_cstring(rolenameText);
	text *newPasswordText = PG_GETARG_TEXT_P(1);
	const char *newPassword = text_to_cstring(newPasswordText);
	const char *setPasswordQuery;
	StringInfoData setPasswordQueryBuffer;
	Node *parseTree;

	if (get_role_oid(rolename, true) == InvalidOid)
	{
		PG_RETURN_BOOL(false);
	}

	initStringInfo(&setPasswordQueryBuffer);
	appendStringInfo(&setPasswordQueryBuffer,
					 "ALTER ROLE %s PASSWORD '%s'",
					 rolename,
					 newPassword);

	setPasswordQuery = pstrdup(setPasswordQueryBuffer.data);

	parseTree = ParseTreeNode(setPasswordQuery);

	CitusProcessUtility(parseTree, setPasswordQuery, PROCESS_UTILITY_TOPLEVEL, NULL,
						None_Receiver, NULL);

	PG_RETURN_BOOL(true);
}
