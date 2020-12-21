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

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/relay_utility.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "utils/builtins.h"

static void AppendCreateStatisticsStmt(StringInfo buf, CreateStatsStmt *stmt);
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

	appendStringInfoString(buf, ";");
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


static void
AppendColumnNames(StringInfo buf, CreateStatsStmt *stmt)
{
	ColumnRef *column = NULL;
	foreach_ptr(column, stmt->exprs)
	{
		Assert(IsA(column, ColumnRef));

		char *columnName = NameListToQuotedString(column->fields);

		appendStringInfoString(buf, columnName);

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
