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

#include "fmgr.h"

#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/builtins.h"

#include "distributed/commands/utility_hook.h"
#include "distributed/worker_protocol.h"

PG_FUNCTION_INFO_V1(alter_role_if_exists);
PG_FUNCTION_INFO_V1(worker_create_or_alter_role);

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

	if (get_role_oid(rolename, true) == InvalidOid)
	{
		PG_RETURN_BOOL(false);
	}

	Node *parseTree = ParseTreeNode(utilityQuery);

	ProcessUtilityParseTree(parseTree, utilityQuery, PROCESS_UTILITY_QUERY, NULL,
							None_Receiver, NULL);

	PG_RETURN_BOOL(true);
}


/*
 * worker_create_or_alter_role(
 *   role_name text,
 *   create_role_utility_query text,
 *   alter_role_utility_query text)
 *
 * This UDF checks if the role, whose name is given in role_name exists.
 *
 * If the role does not exist it will run the query provided in create_role_utility_query
 * which is expected to be a CreateRoleStmt. If a different statement is provided the call
 * will raise an error,
 *
 * If the role does exist it will run the query provided in alter_role_utility_query to
 * change the existing user in such a way that it is compatible with the user on the
 * coordinator. This query is expected to be a AlterRoleStmt, if a different statement is
 * provided the function will raise an error.
 *
 * For both queries a NULL value can be passed to omit the execution of that condition.
 *
 * The function returns true if a command has been successfully executed, false if no
 * command was executed, and raises an error if something went wrong.
 */
Datum
worker_create_or_alter_role(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
	{
		ereport(ERROR, (errmsg("role name cannot be NULL")));
	}

	text *rolenameText = PG_GETARG_TEXT_P(0);
	const char *rolename = text_to_cstring(rolenameText);

	if (get_role_oid(rolename, true) == InvalidOid)
	{
		if (PG_ARGISNULL(1))
		{
			PG_RETURN_BOOL(false);
		}

		text *createRoleUtilityQueryText = PG_GETARG_TEXT_P(1);
		const char *createRoleUtilityQuery = text_to_cstring(createRoleUtilityQueryText);
		Node *parseTree = ParseTreeNode(createRoleUtilityQuery);

		if (nodeTag(parseTree) != T_CreateRoleStmt)
		{
			ereport(ERROR, (errmsg("cannot create role"),
							errdetail("the role %s does not exist "
									  "but %s is not a correct CREATE ROLE query",
									  quote_literal_cstr(rolename),
									  quote_literal_cstr(createRoleUtilityQuery))));
		}

		ProcessUtilityParseTree(parseTree,
								createRoleUtilityQuery,
								PROCESS_UTILITY_QUERY,
								NULL,
								None_Receiver, NULL);

		PG_RETURN_BOOL(true);
	}
	else
	{
		if (PG_ARGISNULL(2))
		{
			PG_RETURN_BOOL(false);
		}

		text *alterRoleUtilityQueryText = PG_GETARG_TEXT_P(2);
		const char *alterRoleUtilityQuery = text_to_cstring(alterRoleUtilityQueryText);
		Node *parseTree = ParseTreeNode(alterRoleUtilityQuery);

		if (nodeTag(parseTree) != T_AlterRoleStmt)
		{
			ereport(ERROR, (errmsg("cannot alter role"),
							errdetail("the role %s exists "
									  "but %s is not a correct alter ROLE query",
									  quote_literal_cstr(rolename),
									  quote_literal_cstr(alterRoleUtilityQuery))));
		}

		ProcessUtilityParseTree(parseTree,
								alterRoleUtilityQuery,
								PROCESS_UTILITY_QUERY,
								NULL,
								None_Receiver, NULL);

		PG_RETURN_BOOL(true);
	}
}
