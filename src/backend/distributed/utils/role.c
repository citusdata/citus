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

	CitusProcessUtility(parseTree, utilityQuery, PROCESS_UTILITY_TOPLEVEL, NULL,
						None_Receiver, NULL);

	PG_RETURN_BOOL(true);
}


/*
 * worker_create_or_alter_role checks if the role, whose name is given
 * in the first parameter exists and then runs the query, which is the second
 * parameter. This UDF is particularly used for ALTER ROLE queries, how ever it
 * can run any other query too.
 */
Datum
worker_create_or_alter_role(PG_FUNCTION_ARGS)
{
	text *rolenameText = PG_GETARG_TEXT_P(0);
	const char *rolename = text_to_cstring(rolenameText);

	if (get_role_oid(rolename, true) == InvalidOid)
	{
		if (PG_ARGISNULL(1))
		{
			PG_RETURN_BOOL(true);
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

		CitusProcessUtility(parseTree,
							createRoleUtilityQuery,
							PROCESS_UTILITY_TOPLEVEL,
							NULL,
							None_Receiver, NULL);
	}
	else
	{
		if (PG_ARGISNULL(2))
		{
			PG_RETURN_BOOL(true);
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

		CitusProcessUtility(parseTree,
							alterRoleUtilityQuery,
							PROCESS_UTILITY_TOPLEVEL,
							NULL,
							None_Receiver, NULL);
	}

	if (PG_ARGISNULL(3))
	{
		PG_RETURN_BOOL(true);
	}

	ArrayType *grantRoleQueriesArray = PG_GETARG_ARRAYTYPE_P(3);
	int grantQueryCount = ArrayObjectCount(grantRoleQueriesArray);
	Datum *grantRoleQueries = DeconstructArrayObject(grantRoleQueriesArray);

	for (int queryIndex = 0; queryIndex < grantQueryCount; queryIndex++)
	{
		text *grantQueryText = DatumGetTextP(grantRoleQueries[queryIndex]);
		char *grantQuery = text_to_cstring(grantQueryText);
		Node *parseTree = ParseTreeNode(grantQuery);
		if (nodeTag(parseTree) != T_GrantRoleStmt)
		{
			ereport(ERROR, (errmsg("cannot grant role"),
							errdetail("%s is not a correct GRANT ROLE query",
									  quote_literal_cstr(grantQuery))));
		}
		CitusProcessUtility(parseTree,
							grantQuery,
							PROCESS_UTILITY_TOPLEVEL,
							NULL,
							None_Receiver,
							NULL);
	}

	PG_RETURN_BOOL(true);
}
