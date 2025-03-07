/*-------------------------------------------------------------------------
 *
 * deparse_grant_stmts.c
 *	  All routines to deparse grant statements.
 *
 *	  reference: https://www.postgresql.org/docs/current/sql-grant.html
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
// #include "commands/defrem.h"
// #include "lib/stringinfo.h"
// #include "nodes/nodes.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
// #include "distributed/listutils.h"
// #include "distributed/relay_utility.h"

/* the real deparser */
static void deparseGrantSql(StringInfoData	 *stringInfoData, GrantStmt *grantStmt);

/*
 * deparseGrantSql builds and returns a string representing the GrantOnStmt
 */
void
deparseGrantSql(StringInfoData	 *stringInfoData, GrantStmt *grantStmt)
{
	ObjectType		objectType = grantStmt->objtype;
	StringInfoData	objectsStringInfoData = { 0 };
	ListCell	   *listCell = NULL;
	bool			isFirst = true;
	bool			defaultObjectDeparser = false;

	// elog(WARNING, "grant TREE: %s", nodeToString(grantStmt));

	initStringInfo(&objectsStringInfoData);

	switch(objectType)
	{
		case OBJECT_DATABASE:
			appendStringInfo(&objectsStringInfoData, "ON DATABASE ");
			defaultObjectDeparser = true;
			break;

		case OBJECT_SCHEMA:
			appendStringInfo(&objectsStringInfoData, "ON SCHEMA ");
			defaultObjectDeparser = true;
			break;

		case OBJECT_TABLE:
			appendStringInfo(&objectsStringInfoData, "ON ");
			foreach(listCell, grantStmt->objects)
			{
				RangeVar *relvar = (RangeVar *) lfirst(listCell);
				Oid relationId = RangeVarGetRelid(relvar, NoLock, false);
				if (!isFirst)
					appendStringInfo(&objectsStringInfoData, ", ");

				isFirst = false;
				appendStringInfo(&objectsStringInfoData, "%s",
								 generate_relation_name(relationId, NIL));
			}
			break;

		case OBJECT_SEQUENCE:
			appendStringInfoString(&objectsStringInfoData, "ON SEQUENCE ");
			foreach(listCell, grantStmt->objects)
			{
				/*
				* GrantOnSequence statement keeps its objects (sequences) as
				* a list of RangeVar-s
				*/
				RangeVar *sequence = (RangeVar *) lfirst(listCell);

				if (!isFirst)
					appendStringInfo(&objectsStringInfoData, ", ");

				isFirst = false;
				/*
				* We have qualified the statement beforehand
				*/
				appendStringInfoString(&objectsStringInfoData,
									   quote_qualified_identifier(sequence->schemaname,
																	sequence->relname));
			}
			break;

		/*
		 * The FUNCTION syntax works for plain functions, aggregate functions, and window
		 * functions, but not for procedures; use PROCEDURE for those. Alternatively, use
		 * ROUTINE to refer to a function, aggregate function, window function, or procedure
		 * regardless of its precise type.
		 * https://www.postgresql.org/docs/current/sql-grant.html
		 */
		case OBJECT_AGGREGATE:
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		case OBJECT_ROUTINE:
			appendStringInfoString(&objectsStringInfoData, "ON ROUTINE ");

			if (grantStmt->targtype == ACL_TARGET_ALL_IN_SCHEMA)
			{
				elog(ERROR,
					"%s .. ALL FUNCTIONS/PROCEDURES IN SCHEMA is not supported for formatting.",
					stringInfoData->data);
			}

			foreach(listCell, grantStmt->objects)
			{
				/*
				* GrantOnFunction statement keeps its objects (functions) as
				* a list of ObjectWithArgs
				*/
				ObjectWithArgs *objectWithArgs = (ObjectWithArgs *) lfirst(listCell);

				if (!isFirst)
					appendStringInfo(&objectsStringInfoData, ", ");

				isFirst = false;

				appendStringInfoString(&objectsStringInfoData,
									   NameListToString(objectWithArgs->objname));

				if (!objectWithArgs->args_unspecified)
				{
					/* if args are specified, we should append "(arg1, arg2, ...)" to the function name */
					const char *args = TypeNameListToString(objectWithArgs->objargs);
					appendStringInfo(&objectsStringInfoData, "(%s)", args);
				}
			}
			break;

		case OBJECT_FDW:
			appendStringInfo(&objectsStringInfoData, "ON FOREIGN DATA WRAPPER ");
			defaultObjectDeparser = true;
			break;
		case OBJECT_FOREIGN_SERVER:
			appendStringInfo(&objectsStringInfoData, "ON FOREIGN SERVER ");
			defaultObjectDeparser = true;
			break;

		case OBJECT_COLUMN:
			/* we want to exclude system columns here */

		case OBJECT_VIEW:
		case OBJECT_MATVIEW:
		case OBJECT_FOREIGN_TABLE:
		case OBJECT_DOMAIN:
		case OBJECT_LANGUAGE:
		case OBJECT_LARGEOBJECT:
		case OBJECT_PARAMETER_ACL:
		case OBJECT_TABLESPACE:
		case OBJECT_TYPE:
		case OBJECT_ROLE:
			elog(WARNING, "grant TREE: %s", nodeToString(grantStmt));
			return;
			// elog(ERROR, "GRANT statement not managed by citus YET");
			break;

		/* no GRANT for those: */
		// case OBJECT_ACCESS_METHOD:
		// case OBJECT_AMOP:
		// case OBJECT_AMPROC:
		// case OBJECT_ATTRIBUTE:			/* type's attribute: when distinct from column */
		// case OBJECT_CAST:
		// case OBJECT_COLLATION:
		// case OBJECT_CONVERSION:
		// case OBJECT_DEFAULT:
		// case OBJECT_DEFACL:
		// case OBJECT_DOMCONSTRAINT:
		// case OBJECT_EVENT_TRIGGER:
		// case OBJECT_EXTENSION:
		// case OBJECT_INDEX:
		// case OBJECT_OPCLASS:
		// case OBJECT_OPERATOR:
		// case OBJECT_OPFAMILY:
		// case OBJECT_POLICY:
		// case OBJECT_PUBLICATION:
		// case OBJECT_PUBLICATION_NAMESPACE:
		// case OBJECT_PUBLICATION_REL:
		// case OBJECT_RULE:
		// case OBJECT_SUBSCRIPTION:
		// case OBJECT_STATISTIC_EXT:
		// case OBJECT_TABCONSTRAINT:
		// case OBJECT_TRANSFORM:
		// case OBJECT_TRIGGER:
		// case OBJECT_TSCONFIGURATION:
		// case OBJECT_TSDICTIONARY:
		// case OBJECT_TSPARSER:
		// case OBJECT_TSTEMPLATE:
		// case OBJECT_USER_MAPPING:
		default:
			elog(WARNING, "grant TREE: %s", nodeToString(grantStmt));
			// elog(ERROR, "Cannot deparse unsupported GRANT statement");
			return;
	}

	/* manage the objects string. By default, name is on first cell. */
	if (defaultObjectDeparser)
	{
		isFirst = true;
		foreach(listCell, grantStmt->objects)
		{
			char *objectName = strVal(lfirst(listCell));
			if (!isFirst)
				appendStringInfo(&objectsStringInfoData, ", ");

			isFirst = false;
			appendStringInfoString(&objectsStringInfoData,
									quote_identifier(objectName));
		}
	}

	/* start of the query string with a GRANT or REVOKE */
	appendStringInfo(stringInfoData, "%s ", grantStmt->is_grant ? "GRANT" : "REVOKE");

	/* only revoke the grant option, not the privilege itself */
	if (!grantStmt->is_grant && grantStmt->grant_option)
	{
		appendStringInfo(stringInfoData, "GRANT OPTION FOR ");
	}

	/*
	 * manage privileges, either the defaults or deparsing (including columns when
	 * relevant).
	 */
	if (grantStmt->privileges == NIL)
	{
		appendStringInfo(stringInfoData, "ALL PRIVILEGES ");
	}
	else
	{
		isFirst = true;
		foreach(listCell, grantStmt->privileges)
		{
			AccessPriv *accessPriv = (AccessPriv *) lfirst(listCell);
			if (!isFirst)
			{
				appendStringInfo(stringInfoData, ", ");
			}
			isFirst = false;

			appendStringInfoString(stringInfoData, accessPriv->priv_name);
			// AppendGrantOnColumnColumns(stringInfoData, accessPriv->cols);
		}
	}

	/* manage the objects string */
	appendStringInfo(stringInfoData, " ");
	appendStringInfoString(stringInfoData, objectsStringInfoData.data);
	// resetStringInfo(&objectsStringInfoData);

	/*
	 * manage grantees, we must have at least one.
	 */
	if (grantStmt->grantees == NIL)
	{
		elog(ERROR, "unexpected case");
	}
	else
	{
		isFirst = true;
		appendStringInfo(stringInfoData, " %s ", grantStmt->is_grant ? "TO" : "FROM");
		foreach(listCell, grantStmt->grantees)
		{
			RoleSpec *grantee = (RoleSpec *) lfirst(listCell);
			if (!isFirst)
			{
				appendStringInfo(stringInfoData, ", ");
			}
			isFirst = false;

			appendStringInfoString(stringInfoData, RoleSpecString(grantee, true));
		}
	}
	/* manage grant option */
	if (grantStmt->is_grant && grantStmt->grant_option)
	{
		appendStringInfo(stringInfoData, " WITH GRANT OPTION");
	}

	/*
	 * Append the 'GRANTED BY' clause to the given buffer if the given statement is a
	 * 'GRANT' statement and the grantor is specified.
	 */
	if (grantStmt->grantor)
	{
		appendStringInfo(stringInfoData, " GRANTED BY %s",
						 RoleSpecString(grantStmt->grantor, true));
	}

	if (!grantStmt->is_grant)
	{
		switch(grantStmt->behavior)
		{
			case DROP_RESTRICT:
				appendStringInfo(stringInfoData, " RESTRICT");
				break;

			case DROP_CASCADE:
				appendStringInfo(stringInfoData, " CASCADE");
				break;
			default:
				elog(ERROR, "unexpected case");
		}
	}
	appendStringInfo(stringInfoData, ";");
	// elog(WARNING, "deparsed grant: %s", stringInfoData->data);
}

char *
DeparseGrantStmt(Node *node)
{
	GrantStmt *stmt = castNode(GrantStmt, node);

	StringInfoData str = { 0 };
	initStringInfo(&str);

	deparseGrantSql(&str, stmt);

	return str.data;
}
