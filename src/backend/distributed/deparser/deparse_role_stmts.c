/*-------------------------------------------------------------------------
 *
 * deparse_role_stmts.c
 *	  All routines to deparse role statements.
 *	  This file contains all entry points specific for ALTER ROLE statement
 *    deparsing as well as functions that are currently only used for deparsing
 *    ALTER ROLE statements.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/defrem.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

#include "pg_version_compat.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"

static void AppendAlterRoleStmt(StringInfo buf, AlterRoleStmt *stmt);
static void AppendAlterRoleSetStmt(StringInfo buf, AlterRoleSetStmt *stmt);
static void AppendCreateRoleStmt(StringInfo buf, CreateRoleStmt *stmt);
static void AppendRoleOption(StringInfo buf, ListCell *optionCell);
static void AppendRoleList(StringInfo buf, List *roleList);
static void AppendDropRoleStmt(StringInfo buf, DropRoleStmt *stmt);
static void AppendGrantRoleStmt(StringInfo buf, GrantRoleStmt *stmt);
static void AppendRevokeAdminOptionFor(StringInfo buf, GrantRoleStmt *stmt);
static void AppendGrantWithAdminOption(StringInfo buf, GrantRoleStmt *stmt);


/*
 * DeparseAlterRoleStmt builds and returns a string representing of the
 * AlterRoleStmt for application on a remote server.
 */
char *
DeparseAlterRoleStmt(Node *node)
{
	AlterRoleStmt *stmt = castNode(AlterRoleStmt, node);
	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	AppendAlterRoleStmt(&buf, stmt);

	return buf.data;
}


/*
 * DeparseAlterRoleSetStmt builds and returns a string representing of the
 * AlterRoleSetStmt for application on a remote server.
 */
char *
DeparseAlterRoleSetStmt(Node *node)
{
	AlterRoleSetStmt *stmt = castNode(AlterRoleSetStmt, node);
	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	AppendAlterRoleSetStmt(&buf, stmt);

	return buf.data;
}


/*
 * AppendAlterRoleStmt generates the string representation of the
 * AlterRoleStmt and appends it to the buffer.
 */
static void
AppendAlterRoleStmt(StringInfo buf, AlterRoleStmt *stmt)
{
	ListCell *optionCell = NULL;
	RoleSpec *role = stmt->role;

	/*
	 * If the role_specification used is CURRENT_USER or SESSION_USER,
	 * it will be converted to thats roles role name.
	 */
	appendStringInfo(buf, "ALTER ROLE %s", RoleSpecString(role, true));

	foreach(optionCell, stmt->options)
	{
		AppendRoleOption(buf, optionCell);
	}
}


/*
 * AppendRoleOption generates the string representation for the role options
 * and appends it to the buffer.
 *
 * This function only generates strings for common role options of ALTER ROLE
 * and CREATE ROLE statements. The extra options for CREATE ROLE are handled
 * seperately.
 */
static void
AppendRoleOption(StringInfo buf, ListCell *optionCell)
{
	DefElem *option = (DefElem *) lfirst(optionCell);

	if (strcmp(option->defname, "superuser") == 0 && boolVal(option->arg))
	{
		appendStringInfo(buf, " SUPERUSER");
	}
	else if (strcmp(option->defname, "superuser") == 0 && !boolVal(option->arg))
	{
		appendStringInfo(buf, " NOSUPERUSER");
	}
	else if (strcmp(option->defname, "createdb") == 0 && boolVal(option->arg))
	{
		appendStringInfo(buf, " CREATEDB");
	}
	else if (strcmp(option->defname, "createdb") == 0 && !boolVal(option->arg))
	{
		appendStringInfo(buf, " NOCREATEDB");
	}
	else if (strcmp(option->defname, "createrole") == 0 && boolVal(option->arg))
	{
		appendStringInfo(buf, " CREATEROLE");
	}
	else if (strcmp(option->defname, "createrole") == 0 && !boolVal(option->arg))
	{
		appendStringInfo(buf, " NOCREATEROLE");
	}
	else if (strcmp(option->defname, "inherit") == 0 && boolVal(option->arg))
	{
		appendStringInfo(buf, " INHERIT");
	}
	else if (strcmp(option->defname, "inherit") == 0 && !boolVal(option->arg))
	{
		appendStringInfo(buf, " NOINHERIT");
	}
	else if (strcmp(option->defname, "canlogin") == 0 && boolVal(option->arg))
	{
		appendStringInfo(buf, " LOGIN");
	}
	else if (strcmp(option->defname, "canlogin") == 0 && !boolVal(option->arg))
	{
		appendStringInfo(buf, " NOLOGIN");
	}
	else if (strcmp(option->defname, "isreplication") == 0 && boolVal(option->arg))
	{
		appendStringInfo(buf, " REPLICATION");
	}
	else if (strcmp(option->defname, "isreplication") == 0 && !boolVal(option->arg))
	{
		appendStringInfo(buf, " NOREPLICATION");
	}
	else if (strcmp(option->defname, "bypassrls") == 0 && boolVal(option->arg))
	{
		appendStringInfo(buf, " BYPASSRLS");
	}
	else if (strcmp(option->defname, "bypassrls") == 0 && !boolVal(option->arg))
	{
		appendStringInfo(buf, " NOBYPASSRLS");
	}
	else if (strcmp(option->defname, "connectionlimit") == 0)
	{
		appendStringInfo(buf, " CONNECTION LIMIT %d", intVal(option->arg));
	}
	else if (strcmp(option->defname, "password") == 0)
	{
		if (option->arg != NULL)
		{
			appendStringInfo(buf, " PASSWORD %s", quote_literal_cstr(strVal(
																		 option->arg)));
		}
		else
		{
			appendStringInfo(buf, " PASSWORD NULL");
		}
	}
	else if (strcmp(option->defname, "validUntil") == 0)
	{
		appendStringInfo(buf, " VALID UNTIL %s", quote_literal_cstr(strVal(option->arg)));
	}
}


/*
 * DeparseCreateRoleStmt builds and returns a string representing of the
 * CreateRoleStmt for application on a remote server.
 */
char *
DeparseCreateRoleStmt(Node *node)
{
	CreateRoleStmt *stmt = castNode(CreateRoleStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	AppendCreateRoleStmt(&buf, stmt);

	return buf.data;
}


/*
 * AppendCreateRoleStmt generates the string representation of the
 * CreateRoleStmt and appends it to the buffer.
 */
static void
AppendCreateRoleStmt(StringInfo buf, CreateRoleStmt *stmt)
{
	ListCell *optionCell = NULL;

	appendStringInfo(buf, "CREATE ");

	switch (stmt->stmt_type)
	{
		case ROLESTMT_ROLE:
		{
			appendStringInfo(buf, "ROLE ");
			break;
		}

		case ROLESTMT_USER:
		{
			appendStringInfo(buf, "USER ");
			break;
		}

		case ROLESTMT_GROUP:
		{
			appendStringInfo(buf, "GROUP ");
			break;
		}
	}

	appendStringInfo(buf, "%s", quote_identifier(stmt->role));

	foreach(optionCell, stmt->options)
	{
		AppendRoleOption(buf, optionCell);

		DefElem *option = (DefElem *) lfirst(optionCell);

		if (strcmp(option->defname, "sysid") == 0)
		{
			appendStringInfo(buf, " SYSID %d", intVal(option->arg));
		}
		else if (strcmp(option->defname, "adminmembers") == 0)
		{
			appendStringInfo(buf, " ADMIN ");
			AppendRoleList(buf, (List *) option->arg);
		}
		else if (strcmp(option->defname, "rolemembers") == 0)
		{
			appendStringInfo(buf, " ROLE ");
			AppendRoleList(buf, (List *) option->arg);
		}
		else if (strcmp(option->defname, "addroleto") == 0)
		{
			appendStringInfo(buf, " IN ROLE ");
			AppendRoleList(buf, (List *) option->arg);
		}
	}
}


/*
 * DeparseDropRoleStmt builds and returns a string representing of the
 * DropRoleStmt for application on a remote server.
 */
char *
DeparseDropRoleStmt(Node *node)
{
	DropRoleStmt *stmt = castNode(DropRoleStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	AppendDropRoleStmt(&buf, stmt);

	return buf.data;
}


/*
 * AppendDropRoleStmt generates the string representation of the
 * DropRoleStmt and appends it to the buffer.
 */
static void
AppendDropRoleStmt(StringInfo buf, DropRoleStmt *stmt)
{
	appendStringInfo(buf, "DROP ROLE ");

	if (stmt->missing_ok)
	{
		appendStringInfo(buf, "IF EXISTS ");
	}

	AppendRoleList(buf, stmt->roles);
}


static void
AppendRoleList(StringInfo buf, List *roleList)
{
	ListCell *cell = NULL;
	foreach(cell, roleList)
	{
		Node *roleNode = (Node *) lfirst(cell);
		Assert(IsA(roleNode, RoleSpec) || IsA(roleNode, AccessPriv));
		char const *rolename = NULL;
		if (IsA(roleNode, RoleSpec))
		{
			rolename = RoleSpecString((RoleSpec *) roleNode, true);
		}
		if (IsA(roleNode, AccessPriv))
		{
			rolename = quote_identifier(((AccessPriv *) roleNode)->priv_name);
		}
		appendStringInfoString(buf, rolename);
		if (cell != list_tail(roleList))
		{
			appendStringInfo(buf, ", ");
		}
	}
}


/*
 * DeparseGrantRoleStmt builds and returns a string representing of the
 * GrantRoleStmt for application on a remote server.
 */
char *
DeparseGrantRoleStmt(Node *node)
{
	GrantRoleStmt *stmt = castNode(GrantRoleStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	AppendGrantRoleStmt(&buf, stmt);

	return buf.data;
}


/*
 * Append the 'RESTRICT' or 'CASCADE' clause to the given buffer if the given
 * statement is a 'REVOKE' statement and the behavior is specified.
 * After PostgreSQL 16, the behavior is specified in the 'opt' field of
 * GrantRoleStmt and may have multiple values.
 * Here, compile time version is checked to support both versions.
 */
static void
AppendRevokeAdminOptionFor(StringInfo buf, GrantRoleStmt *stmt)
{
#if PG_VERSION_NUM >= PG_VERSION_16
	if (!stmt->is_grant)
	{
		DefElem *opt = NULL;
		foreach_declared_ptr(opt, stmt->opt)
		{
			if (strcmp(opt->defname, "admin") == 0)
			{
				appendStringInfo(buf, "ADMIN OPTION FOR ");
				break;
			}
		}
	}
#else
	if (!stmt->is_grant && stmt->admin_opt)
	{
		appendStringInfo(buf, "ADMIN OPTION FOR ");
	}
#endif
}


static void
AppendGrantWithAdminOption(StringInfo buf, GrantRoleStmt *stmt)
{
	if (stmt->is_grant)
	{
#if PG_VERSION_NUM >= PG_VERSION_16
		DefElem *opt = NULL;
		foreach_declared_ptr(opt, stmt->opt)
		{
			bool admin_option = false;
			char *optval = defGetString(opt);
			if (strcmp(opt->defname, "admin") == 0 &&
				parse_bool(optval, &admin_option) && admin_option)
			{
				appendStringInfo(buf, " WITH ADMIN OPTION");
				break;
			}
		}
#else
		if (stmt->admin_opt)
		{
			appendStringInfo(buf, " WITH ADMIN OPTION");
		}
#endif
	}
}


/*
 * AppendGrantRoleStmt generates the string representation of the
 * GrantRoleStmt and appends it to the buffer.
 */
static void
AppendGrantRoleStmt(StringInfo buf, GrantRoleStmt *stmt)
{
	appendStringInfo(buf, "%s ", stmt->is_grant ? "GRANT" : "REVOKE");
	AppendRevokeAdminOptionFor(buf, stmt);
	AppendRoleList(buf, stmt->granted_roles);
	appendStringInfo(buf, "%s ", stmt->is_grant ? " TO " : " FROM ");
	AppendRoleList(buf, stmt->grantee_roles);
	AppendGrantWithAdminOption(buf, stmt);
	AppendGrantedByInGrantForRoleSpec(buf, stmt->grantor, stmt->is_grant);
	AppendGrantRestrictAndCascadeForRoleSpec(buf, stmt->behavior, stmt->is_grant);
	AppendGrantedByInGrantForRoleSpec(buf, stmt->grantor, stmt->is_grant);
	appendStringInfo(buf, ";");
}


/*
 * AppendAlterRoleSetStmt generates the string representation of the
 * AlterRoleSetStmt and appends it to the buffer.
 */
static void
AppendAlterRoleSetStmt(StringInfo buf, AlterRoleSetStmt *stmt)
{
	RoleSpec *role = stmt->role;
	const char *roleSpecStr = NULL;

	if (role == NULL)
	{
		/*
		 * If all roles are be affected, role field is left blank in an
		 * AlterRoleSetStmt.
		 */
		roleSpecStr = "ALL";
	}
	else
	{
		/*
		 * If the role_specification used is CURRENT_USER or SESSION_USER,
		 * it will be converted to thats roles role name.
		 *
		 * We also set withQuoteIdentifier parameter to true. Since the
		 * roleSpecStr will be used in a query, the quotes are needed.
		 */
		roleSpecStr = RoleSpecString(role, true);
	}

	appendStringInfo(buf, "ALTER ROLE %s", roleSpecStr);

	if (stmt->database != NULL)
	{
		appendStringInfo(buf, " IN DATABASE %s", quote_identifier(stmt->database));
	}

	VariableSetStmt *setStmt = castNode(VariableSetStmt, stmt->setstmt);
	AppendVariableSet(buf, setStmt);
}
