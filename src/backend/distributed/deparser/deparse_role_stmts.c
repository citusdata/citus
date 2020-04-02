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

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

static void AppendAlterRoleStmt(StringInfo buf, AlterRoleStmt *stmt);
static void AppendAlterRoleSetStmt(StringInfo buf, AlterRoleSetStmt *stmt);
static void AppendRoleOption(StringInfo buf, ListCell *optionCell);


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

	if (strcmp(option->defname, "superuser") == 0 && intVal(option->arg))
	{
		appendStringInfo(buf, " SUPERUSER");
	}
	else if (strcmp(option->defname, "superuser") == 0 && !intVal(option->arg))
	{
		appendStringInfo(buf, " NOSUPERUSER");
	}
	else if (strcmp(option->defname, "createdb") == 0 && intVal(option->arg))
	{
		appendStringInfo(buf, " CREATEDB");
	}
	else if (strcmp(option->defname, "createdb") == 0 && !intVal(option->arg))
	{
		appendStringInfo(buf, " NOCREATEDB");
	}
	else if (strcmp(option->defname, "createrole") == 0 && intVal(option->arg))
	{
		appendStringInfo(buf, " CREATEROLE");
	}
	else if (strcmp(option->defname, "createrole") == 0 && !intVal(option->arg))
	{
		appendStringInfo(buf, " NOCREATEROLE");
	}
	else if (strcmp(option->defname, "inherit") == 0 && intVal(option->arg))
	{
		appendStringInfo(buf, " INHERIT");
	}
	else if (strcmp(option->defname, "inherit") == 0 && !intVal(option->arg))
	{
		appendStringInfo(buf, " NOINHERIT");
	}
	else if (strcmp(option->defname, "canlogin") == 0 && intVal(option->arg))
	{
		appendStringInfo(buf, " LOGIN");
	}
	else if (strcmp(option->defname, "canlogin") == 0 && !intVal(option->arg))
	{
		appendStringInfo(buf, " NOLOGIN");
	}
	else if (strcmp(option->defname, "isreplication") == 0 && intVal(option->arg))
	{
		appendStringInfo(buf, " REPLICATION");
	}
	else if (strcmp(option->defname, "isreplication") == 0 && !intVal(option->arg))
	{
		appendStringInfo(buf, " NOREPLICATION");
	}
	else if (strcmp(option->defname, "bypassrls") == 0 && intVal(option->arg))
	{
		appendStringInfo(buf, " BYPASSRLS");
	}
	else if (strcmp(option->defname, "bypassrls") == 0 && !intVal(option->arg))
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
 * AppendAlterRoleStmt generates the string representation of the
 * AlterRoleStmt and appends it to the buffer.
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
