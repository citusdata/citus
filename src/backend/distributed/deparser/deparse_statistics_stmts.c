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

#include "distributed/pg_version_constants.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/relay_utility.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "utils/builtins.h"

static void AppendCreateStatisticsStmt(StringInfo buf, CreateStatsStmt *stmt);
static void AppendDropStatisticsStmt(StringInfo buf, List *nameList, bool ifExists);
static void AppendAlterStatisticsRenameStmt(StringInfo buf, RenameStmt *stmt);
static void AppendAlterStatisticsSchemaStmt(StringInfo buf, AlterObjectSchemaStmt *stmt);
#if PG_VERSION_NUM >= PG_VERSION_13
static void AppendAlterStatisticsStmt(StringInfo buf, AlterStatsStmt *stmt);
#endif
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


#if PG_VERSION_NUM >= PG_VERSION_13
char *
DeparseAlterStatisticsStmt(Node *node)
{
	AlterStatsStmt *stmt = castNode(AlterStatsStmt, node);

	StringInfoData str;
	initStringInfo(&str);

	AppendAlterStatisticsStmt(&str, stmt);

	return str.data;
}


#endif
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


#if PG_VERSION_NUM >= PG_VERSION_13
static void
AppendAlterStatisticsStmt(StringInfo buf, AlterStatsStmt *stmt)
{
	appendStringInfo(buf, "ALTER STATISTICS %s SET STATISTICS %d", NameListToQuotedString(
						 stmt->defnames), stmt->stxstattarget);
}


#endif
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
	Value *schemaNameVal = (Value *) linitial(stmt->defnames);
	const char *schemaName = quote_identifier(strVal(schemaNameVal));

	Value *statNameVal = (Value *) lsecond(stmt->defnames);
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

	Value *statType = NULL;
	foreach_ptr(statType, stmt->stat_types)
	{
		appendStringInfoString(buf, strVal(statType));

		if (statType != llast(stmt->stat_types))
		{
			appendStringInfoString(buf, ", ");
		}
	}

	appendStringInfoString(buf, ")");
}


#if PG_VERSION_NUM >= PG_VERSION_14
static void
AppendColumnNames(StringInfo buf, CreateStatsStmt *stmt)
{
	StatsElem *column = NULL;

	foreach_ptr(column, stmt->exprs)
	{
		if (!column->name)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg(
						 "only simple column references are allowed in CREATE STATISTICS")));
		}

		const char *columnName = quote_identifier(column->name);

		appendStringInfoString(buf, columnName);

		if (column != llast(stmt->exprs))
		{
			appendStringInfoString(buf, ", ");
		}
	}
}


#else
static void
AppendColumnNames(StringInfo buf, CreateStatsStmt *stmt)
{
	ColumnRef *column = NULL;

	foreach_ptr(column, stmt->exprs)
	{
		if (!IsA(column, ColumnRef) || list_length(column->fields) != 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg(
						 "only simple column references are allowed in CREATE STATISTICS")));
		}

		char *columnName = NameListToQuotedString(column->fields);

		appendStringInfoString(buf, columnName);

		if (column != llast(stmt->exprs))
		{
			appendStringInfoString(buf, ", ");
		}
	}
}


#endif

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
