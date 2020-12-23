/*-------------------------------------------------------------------------
 *
 * qualify_statistics_stmt.c
 *	  Functions specialized in fully qualifying all statistics statements.
 *    These functions are dispatched from qualify.c
 *
 *	  Goal would be that the deparser functions for these statements can
 *	  serialize the statement without any external lookups.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"

void
QualifyCreateStatisticsStmt(Node *node)
{
	CreateStatsStmt *stmt = castNode(CreateStatsStmt, node);

	RangeVar *relation = (RangeVar *) linitial(stmt->relations);

	if (relation->schemaname == NULL)
	{
		Oid tableOid = RelnameGetRelid(relation->relname);
		Oid schemaOid = get_rel_namespace(tableOid);
		relation->schemaname = get_namespace_name(schemaOid);
	}

	RangeVar *stat = makeRangeVarFromNameList(stmt->defnames);

	if (stat->schemaname == NULL)
	{
		Oid schemaOid = RangeVarGetCreationNamespace(stat);
		stat->schemaname = get_namespace_name(schemaOid);

		stmt->defnames = MakeNameListFromRangeVar(stat);
	}
}


/*
 * QualifyDropStatisticsStmt qualifies DropStmt's with schema name for
 * DROP STATISTICS statements.
 */
void
QualifyDropStatisticsStmt(Node *node)
{
	DropStmt *dropStatisticsStmt = castNode(DropStmt, node);
	Assert(dropStatisticsStmt->removeType == OBJECT_STATISTIC_EXT);

	List *objectNameListWithSchema = NIL;
	List *objectNameList = NULL;
	foreach_ptr(objectNameList, dropStatisticsStmt->objects)
	{
		RangeVar *stat = makeRangeVarFromNameList(objectNameList);

		if (stat->schemaname == NULL)
		{
			Oid schemaOid = RangeVarGetCreationNamespace(stat);
			stat->schemaname = get_namespace_name(schemaOid);
		}

		objectNameListWithSchema = lappend(objectNameListWithSchema,
										   MakeNameListFromRangeVar(stat));
	}

	dropStatisticsStmt->objects = objectNameListWithSchema;
}


/*
 * QualifyAlterStatisticsRenameStmt qualifies RenameStmt's with schema name for
 * ALTER STATISTICS RENAME statements.
 */
void
QualifyAlterStatisticsRenameStmt(Node *node)
{
	RenameStmt *renameStmt = castNode(RenameStmt, node);
	Assert(renameStmt->renameType == OBJECT_STATISTIC_EXT);

	List *nameList = (List *) renameStmt->object;
	if (list_length(nameList) == 1)
	{
		RangeVar *stat = makeRangeVarFromNameList(nameList);
		Oid schemaOid = RangeVarGetCreationNamespace(stat);
		stat->schemaname = get_namespace_name(schemaOid);
		renameStmt->object = (Node *) MakeNameListFromRangeVar(stat);
	}
}
