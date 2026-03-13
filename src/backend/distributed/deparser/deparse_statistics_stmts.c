/*-------------------------------------------------------------------------
 *
 * deparse_statistics_stmts.c
 *	  All routines to deparse statistics statements.
 *	  This file contains all entry points specific for statistics statement deparsing
 *    as well as functions that are currently only used for deparsing of the statistics
 *    statements.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "parser/parse_expr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"

#include "pg_version_constants.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/relay_utility.h"

static void AppendCreateStatisticsStmt(StringInfo buf, CreateStatsStmt *stmt);
static void AppendDropStatisticsStmt(StringInfo buf, List *nameList, bool ifExists);
static void AppendAlterStatisticsRenameStmt(StringInfo buf, RenameStmt *stmt);
static void AppendAlterStatisticsSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);
static void AppendAlterStatisticsStmt(StringInfo buf, AlterStatsStmt *stmt);
static void AppendAlterStatisticsOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt);
static void AppendStatisticsName(StringInfo buf, CreateStatsStmt *stmt);
static void AppendStatTypes(StringInfo buf, CreateStatsStmt *stmt);
static void AppendColumnNames(StringInfo buf, CreateStatsStmt *stmt);
static void AppendTableName(StringInfo buf, CreateStatsStmt *stmt);

char *
DeparseCreateStatisticsStmt(Node *node)
{
	CreateStatsStmt *stmt = castNode(CreateStatsStmt, node);

	StringInfoData str;
	initStringInfo(&str);

	AppendCreateStatisticsStmt(&str, stmt);

	return str.data;
}


char *
DeparseDropStatisticsStmt(List *nameList, bool ifExists)
{
	StringInfoData str;
	initStringInfo(&str);

	AppendDropStatisticsStmt(&str, nameList, ifExists);

	return str.data;
}


char *
DeparseAlterStatisticsRenameStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);

	StringInfoData str;
	initStringInfo(&str);

	AppendAlterStatisticsRenameStmt(&str, stmt);

	return str.data;
}


char *
DeparseAlterStatisticsSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);

	StringInfoData str;
	initStringInfo(&str);

	AppendAlterStatisticsSchemaStmt(&str, stmt);

	return str.data;
}


char *
DeparseAlterStatisticsStmt(Node *node)
{
	AlterStatsStmt *stmt = castNode(AlterStatsStmt, node);

	StringInfoData str;
	initStringInfo(&str);

	AppendAlterStatisticsStmt(&str, stmt);

	return str.data;
}


char *
DeparseAlterStatisticsOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_STATISTIC_EXT);

	StringInfoData str;
	initStringInfo(&str);

	AppendAlterStatisticsOwnerStmt(&str, stmt);

	return str.data;
}


static void
AppendCreateStatisticsStmt(StringInfo buf, CreateStatsStmt *stmt)
{
	appendStringInfoString(buf, "CREATE STATISTICS ");

	if (stmt->if_not_exists)
	{
		appendStringInfoString(buf, "IF NOT EXISTS ");
	}

	AppendStatisticsName(buf, stmt);

	AppendStatTypes(buf, stmt);

	appendStringInfoString(buf, " ON ");

	AppendColumnNames(buf, stmt);

	appendStringInfoString(buf, " FROM ");

	AppendTableName(buf, stmt);
}


static void
AppendDropStatisticsStmt(StringInfo buf, List *nameList, bool ifExists)
{
	appendStringInfoString(buf, "DROP STATISTICS ");

	if (ifExists)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}

	appendStringInfo(buf, "%s", NameListToQuotedString(nameList));
}


static void
AppendAlterStatisticsRenameStmt(StringInfo buf, RenameStmt *stmt)
{
	appendStringInfo(buf, "ALTER STATISTICS %s RENAME TO %s",
					 NameListToQuotedString((List *) stmt->object), quote_identifier(
						 stmt->newname));
}


static void
AppendAlterStatisticsSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt)
{
	appendStringInfo(buf, "ALTER STATISTICS %s SET SCHEMA %s",
					 NameListToQuotedString((List *) stmt->object), quote_identifier(
						 stmt->newschema));
}


static void
AppendAlterStatisticsStmt(StringInfo buf, AlterStatsStmt *stmt)
{
	appendStringInfo(buf, "ALTER STATISTICS %s SET STATISTICS %d",
					 NameListToQuotedString(stmt->defnames),
					 getIntStxstattarget_compat(stmt->stxstattarget));
}


static void
AppendAlterStatisticsOwnerStmt(StringInfo buf, AlterOwnerStmt *stmt)
{
	List *names = (List *) stmt->object;
	appendStringInfo(buf, "ALTER STATISTICS %s OWNER TO %s",
					 NameListToQuotedString(names),
					 RoleSpecString(stmt->newowner, true));
}


static void
AppendStatisticsName(StringInfo buf, CreateStatsStmt *stmt)
{
	String *schemaNameVal = (String *) linitial(stmt->defnames);
	const char *schemaName = quote_identifier(strVal(schemaNameVal));

	String *statNameVal = (String *) lsecond(stmt->defnames);
	const char *statName = quote_identifier(strVal(statNameVal));

	appendStringInfo(buf, "%s.%s", schemaName, statName);
}


static void
AppendStatTypes(StringInfo buf, CreateStatsStmt *stmt)
{
	if (list_length(stmt->stat_types) == 0)
	{
		return;
	}

	appendStringInfoString(buf, " (");

	String *statType = NULL;
	foreach_declared_ptr(statType, stmt->stat_types)
	{
		appendStringInfoString(buf, strVal(statType));

		if (statType != llast(stmt->stat_types))
		{
			appendStringInfoString(buf, ", ");
		}
	}

	appendStringInfoString(buf, ")");
}


/* See ruleutils.c in postgres for the logic here. */
static bool
looks_like_function(Node *node)
{
	if (node == NULL)
	{
		return false;           /* probably shouldn't happen */
	}
	switch (nodeTag(node))
	{
		case T_FuncExpr:
		{
			/* OK, unless it's going to deparse as a cast */
			return (((FuncExpr *) node)->funcformat == COERCE_EXPLICIT_CALL ||
					((FuncExpr *) node)->funcformat == COERCE_SQL_SYNTAX);
		}

		case T_NullIfExpr:
		case T_CoalesceExpr:
		case T_MinMaxExpr:
		case T_SQLValueFunction:
		case T_XmlExpr:
		{
			/* these are all accepted by func_expr_common_subexpr */
			return true;
		}

		default:
		{
			break;
		}
	}
	return false;
}


static void
AppendColumnNames(StringInfo buf, CreateStatsStmt *stmt)
{
	StatsElem *column = NULL;

	foreach_declared_ptr(column, stmt->exprs)
	{
		if (!column->name)
		{
			/*
			 * Since these expressions are parser statements, we first call
			 * transform to get the transformed Expr tree, and then deparse
			 * the transformed tree. This is similar to the logic found in
			 * deparse_table_stmts for check constraints.
			 */
			if (list_length(stmt->relations) != 1)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg(
									"cannot use expressions in CREATE STATISTICS with multiple relations")));
			}

			RangeVar *rel = (RangeVar *) linitial(stmt->relations);
			bool missingOk = false;
			Oid relOid = RangeVarGetRelid(rel, AccessShareLock, missingOk);

			/* Add table name to the name space in parse state. Otherwise column names
			 * cannot be found.
			 */
			Relation relation = table_open(relOid, AccessShareLock);
			ParseState *pstate = make_parsestate(NULL);
			AddRangeTableEntryToQueryCompat(pstate, relation);
			Node *exprCooked = transformExpr(pstate, column->expr,
											 EXPR_KIND_STATS_EXPRESSION);

			char *relationName = get_rel_name(relOid);
			List *relationCtx = deparse_context_for(relationName, relOid);

			char *exprSql = deparse_expression(exprCooked, relationCtx, false, false);
			relation_close(relation, NoLock);

			/* Need parens if it's not a bare function call */
			if (looks_like_function(exprCooked))
			{
				appendStringInfoString(buf, exprSql);
			}
			else
			{
				appendStringInfo(buf, "(%s)", exprSql);
			}
		}
		else
		{
			const char *columnName = quote_identifier(column->name);

			appendStringInfoString(buf, columnName);
		}

		if (column != llast(stmt->exprs))
		{
			appendStringInfoString(buf, ", ");
		}
	}
}


static void
AppendTableName(StringInfo buf, CreateStatsStmt *stmt)
{
	/* statistics' can be created with only one relation */
	Assert(list_length(stmt->relations) == 1);
	RangeVar *relation = (RangeVar *) linitial(stmt->relations);
	char *relationName = relation->relname;
	char *schemaName = relation->schemaname;

	appendStringInfoString(buf, quote_qualified_identifier(schemaName, relationName));
}
