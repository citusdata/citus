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
#include "catalog/pg_statistic_ext.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"

static Oid GetStatsNamespaceOid(Oid statsOid);

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

	if (list_length(stmt->defnames) < 1)
	{
		/* no name to qualify */
		return;
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
	foreach_declared_ptr(objectNameList, dropStatisticsStmt->objects)
	{
		RangeVar *stat = makeRangeVarFromNameList(objectNameList);

		if (stat->schemaname == NULL)
		{
			Oid statsOid = get_statistics_object_oid(objectNameList,
													 dropStatisticsStmt->missing_ok);

			if (OidIsValid(statsOid))
			{
				Oid schemaOid = GetStatsNamespaceOid(statsOid);
				stat->schemaname = get_namespace_name(schemaOid);
			}
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
		Oid statsOid = get_statistics_object_oid(nameList, renameStmt->missing_ok);

		if (!OidIsValid(statsOid))
		{
			return;
		}

		Oid schemaOid = GetStatsNamespaceOid(statsOid);
		stat->schemaname = get_namespace_name(schemaOid);
		renameStmt->object = (Node *) MakeNameListFromRangeVar(stat);
	}
}


/*
 * QualifyAlterStatisticsSchemaStmt qualifies AlterObjectSchemaStmt's with schema name for
 * ALTER STATISTICS RENAME statements.
 */
void
QualifyAlterStatisticsSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_STATISTIC_EXT);

	List *nameList = (List *) stmt->object;
	if (list_length(nameList) == 1)
	{
		RangeVar *stat = makeRangeVarFromNameList(nameList);
		Oid statsOid = get_statistics_object_oid(nameList, stmt->missing_ok);

		if (!OidIsValid(statsOid))
		{
			return;
		}

		Oid schemaOid = GetStatsNamespaceOid(statsOid);
		stat->schemaname = get_namespace_name(schemaOid);
		stmt->object = (Node *) MakeNameListFromRangeVar(stat);
	}
}


/*
 * QualifyAlterStatisticsStmt qualifies AlterStatsStmt's with schema name for
 * ALTER STATISTICS .. SET STATISTICS statements.
 */
void
QualifyAlterStatisticsStmt(Node *node)
{
	AlterStatsStmt *stmt = castNode(AlterStatsStmt, node);

	if (list_length(stmt->defnames) == 1)
	{
		RangeVar *stat = makeRangeVarFromNameList(stmt->defnames);
		Oid statsOid = get_statistics_object_oid(stmt->defnames, stmt->missing_ok);

		if (!OidIsValid(statsOid))
		{
			return;
		}

		Oid schemaOid = GetStatsNamespaceOid(statsOid);
		stat->schemaname = get_namespace_name(schemaOid);
		stmt->defnames = MakeNameListFromRangeVar(stat);
	}
}


/*
 * QualifyAlterStatisticsOwnerStmt qualifies AlterOwnerStmt's with schema
 * name for ALTER STATISTICS .. OWNER TO statements.
 */
void
QualifyAlterStatisticsOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_STATISTIC_EXT);

	List *nameList = (List *) stmt->object;
	if (list_length(nameList) == 1)
	{
		RangeVar *stat = makeRangeVarFromNameList(nameList);
		Oid statsOid = get_statistics_object_oid(nameList, /* missing_ok */ true);

		if (!OidIsValid(statsOid))
		{
			return;
		}

		Oid schemaOid = GetStatsNamespaceOid(statsOid);
		stat->schemaname = get_namespace_name(schemaOid);
		stmt->object = (Node *) MakeNameListFromRangeVar(stat);
	}
}


/*
 * GetStatsNamespaceOid takes the id of a Statistics object and returns
 * the id of the schema that the statistics object belongs to.
 * Errors out if the stats object is not found.
 */
static Oid
GetStatsNamespaceOid(Oid statsOid)
{
	HeapTuple heapTuple = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statsOid));
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("cache lookup failed for statistics "
							   "object with oid %u", statsOid)));
	}
	FormData_pg_statistic_ext *statisticsForm =
		(FormData_pg_statistic_ext *) GETSTRUCT(heapTuple);

	Oid result = statisticsForm->stxnamespace;

	ReleaseSysCache(heapTuple);

	return result;
}
