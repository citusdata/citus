/*-------------------------------------------------------------------------
 *
 * statistics.c
 *    Commands for STATISTICS statements.
 *
 *    We currently support replicating statistics definitions on the
 *    coordinator in all the worker nodes in the form of
 *
 *    CREATE STATISTICS ... queries.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_type.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/commands.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/namespace_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_transaction.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

static List * GetExplicitStatisticsIdList(Oid relationId);


/*
 * PreprocessCreateStatisticsStmt is called during the planning phase for
 * CREATE STATISTICS.
 */
List *
PreprocessCreateStatisticsStmt(Node *node, const char *queryString)
{
	CreateStatsStmt *stmt = castNode(CreateStatsStmt, node);

	RangeVar *relation = (RangeVar *) linitial(stmt->relations);
	Oid relationId = RangeVarGetRelid(relation, ShareUpdateExclusiveLock, false);

	if (!IsCitusTable(relationId) || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);

	char *ddlCommand = DeparseTreeNode((Node *) stmt);

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));

	ddlJob->targetRelationId = relationId;
	ddlJob->concurrentIndexCmd = false;
	ddlJob->startNewTransaction = false;
	ddlJob->commandString = ddlCommand;
	ddlJob->taskList = DDLTaskList(relationId, ddlCommand);

	List *ddlJobs = list_make1(ddlJob);

	return ddlJobs;
}


/*
 * PostprocessCreateStatisticsStmt is called after a CREATE STATISTICS command has
 * been executed by standard process utility.
 */
List *
PostprocessCreateStatisticsStmt(Node *node, const char *queryString)
{
	CreateStatsStmt *stmt = castNode(CreateStatsStmt, node);
	Assert(stmt->type == T_CreateStatsStmt);

	RangeVar *relation = (RangeVar *) linitial(stmt->relations);
	Oid relationId = RangeVarGetRelid(relation, ShareUpdateExclusiveLock, false);

	if (!IsCitusTable(relationId) || !ShouldPropagate())
	{
		return NIL;
	}

	bool missingOk = false;
	ObjectAddress objectAddress = GetObjectAddressFromParseTree((Node *) stmt, missingOk);

	EnsureDependenciesExistOnAllNodes(&objectAddress);

	return NIL;
}


/*
 * CreateStatisticsStmtObjectAddress finds the ObjectAddress for the statistics that
 * is created by given CreateStatsStmt. If missingOk is false and if statistics
 * does not exist, then it errors out.
 *
 * Never returns NULL, but the objid in the address can be invalid if missingOk
 * was set to true.
 */
ObjectAddress
CreateStatisticsStmtObjectAddress(Node *node, bool missingOk)
{
	CreateStatsStmt *stmt = castNode(CreateStatsStmt, node);

	ObjectAddress address = { 0 };
	Oid statsOid = get_statistics_object_oid(stmt->defnames, missingOk);
	ObjectAddressSet(address, StatisticExtRelationId, statsOid);

	return address;
}


/*
 * GetExplicitStatisticsCommandList returns the list of DDL commands to create
 * statistics that are explicitly created for the table with relationId. See
 * comment of GetExplicitStatisticsIdList function.
 */
List *
GetExplicitStatisticsCommandList(Oid relationId)
{
	List *createStatisticsCommandList = NIL;

	PushOverrideEmptySearchPath(CurrentMemoryContext);

	List *statisticsIdList = GetExplicitStatisticsIdList(relationId);

	Oid statisticsId = InvalidOid;
	foreach_oid(statisticsId, statisticsIdList)
	{
		char *createStatisticsCommand = pg_get_statisticsobj_worker(statisticsId, false);

		createStatisticsCommandList = lappend(
			createStatisticsCommandList,
			makeTableDDLCommandString(createStatisticsCommand));
	}

	/* revert back to original search_path */
	PopOverrideSearchPath();

	return createStatisticsCommandList;
}


/*
 * GetExplicitStatisticsSchemaIdList returns the list of schema ids of statistics'
 * which are created on relation with given relation id.
 */
List *
GetExplicitStatisticsSchemaIdList(Oid relationId)
{
	List *schemaIdList = NIL;

	Relation pgStatistics = table_open(StatisticExtRelationId, AccessShareLock);

	int scanKeyCount = 1;
	ScanKeyData scanKey[1];

	ScanKeyInit(&scanKey[0], Anum_pg_statistic_ext_stxrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	bool useIndex = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgStatistics,
													StatisticExtRelidIndexId,
													useIndex, NULL, scanKeyCount,
													scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		FormData_pg_statistic_ext *statisticsForm =
			(FormData_pg_statistic_ext *) GETSTRUCT(heapTuple);

		Oid schemaId = statisticsForm->stxnamespace;
		if (!list_member_oid(schemaIdList, schemaId))
		{
			schemaIdList = lappend_oid(schemaIdList, schemaId);
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgStatistics, NoLock);

	return schemaIdList;
}


/*
 * GetExplicitStatisticsIdList returns a list of OIDs corresponding to the statistics
 * that are explicitly created on the relation with relationId. That means,
 * this function discards internal statistics implicitly created by postgres.
 */
static List *
GetExplicitStatisticsIdList(Oid relationId)
{
	List *statisticsIdList = NIL;

	Relation pgStatistics = table_open(StatisticExtRelationId, AccessShareLock);

	int scanKeyCount = 1;
	ScanKeyData scanKey[1];

	ScanKeyInit(&scanKey[0], Anum_pg_statistic_ext_stxrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);

	bool useIndex = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgStatistics,
													StatisticExtRelidIndexId,
													useIndex, NULL, scanKeyCount,
													scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Oid statisticsId = InvalidOid;
#if PG_VERSION_NUM >= PG_VERSION_12
		FormData_pg_statistic_ext *statisticsForm =
			(FormData_pg_statistic_ext *) GETSTRUCT(heapTuple);
		statisticsId = statisticsForm->oid;
#else
		statisticsId = HeapTupleGetOid(heapTuple);
#endif
		statisticsIdList = lappend_oid(statisticsIdList, statisticsId);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgStatistics, NoLock);

	return statisticsIdList;
}
