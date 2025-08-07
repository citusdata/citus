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
 *    We also support dropping statistics from all the worker nodes in form of
 *
 *    DROP STATISTICS ... queries.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/namespace_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_transaction.h"

#define DEFAULT_STATISTICS_TARGET -1
#define ALTER_INDEX_COLUMN_SET_STATS_COMMAND \
	"ALTER INDEX %s ALTER COLUMN %d SET STATISTICS %d"

static char * GenerateAlterIndexColumnSetStatsCommand(char *indexNameWithSchema,
													  int16 attnum,
													  int32 attstattarget);
static Oid GetRelIdByStatsOid(Oid statsOid);
static char * CreateAlterCommandIfOwnerNotDefault(Oid statsOid);
static char * CreateAlterCommandIfTargetNotDefault(Oid statsOid);

/*
 * PreprocessCreateStatisticsStmt is called during the planning phase for
 * CREATE STATISTICS.
 */
List *
PreprocessCreateStatisticsStmt(Node *node, const char *queryString,
							   ProcessUtilityContext processUtilityContext)
{
	CreateStatsStmt *stmt = castNode(CreateStatsStmt, node);

	RangeVar *relation = (RangeVar *) linitial(stmt->relations);
	Oid relationId = RangeVarGetRelid(relation, ShareUpdateExclusiveLock, false);

	if (!IsCitusTable(relationId) || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	if (!(stmt->defnames))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot create statistics without a name on a "
							   "Citus table"),
						errhint("Consider specifying a name for the statistics")));
	}

	QualifyTreeNode((Node *) stmt);

	Oid statsOid = get_statistics_object_oid(stmt->defnames, true);
	if (statsOid != InvalidOid)
	{
		/* if stats object already exists, don't create DDLJobs */
		return NIL;
	}

	char *ddlCommand = DeparseTreeNode((Node *) stmt);

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));

	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
	ddlJob->startNewTransaction = false;
	ddlJob->metadataSyncCommand = ddlCommand;
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
	List *objectAddresses = GetObjectAddressListFromParseTree((Node *) stmt, missingOk,
															  true);

	/*  the code-path only supports a single object */
	Assert(list_length(objectAddresses) == 1);

	EnsureAllObjectDependenciesExistOnAllNodes(objectAddresses);

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
List *
CreateStatisticsStmtObjectAddress(Node *node, bool missingOk, bool isPostprocess)
{
	CreateStatsStmt *stmt = castNode(CreateStatsStmt, node);

	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	Oid statsOid = get_statistics_object_oid(stmt->defnames, missingOk);
	ObjectAddressSet(*address, StatisticExtRelationId, statsOid);

	return list_make1(address);
}


/*
 * PreprocessDropStatisticsStmt is called during the planning phase for
 * DROP STATISTICS.
 */
List *
PreprocessDropStatisticsStmt(Node *node, const char *queryString,
							 ProcessUtilityContext processUtilityContext)
{
	DropStmt *dropStatisticsStmt = castNode(DropStmt, node);
	Assert(dropStatisticsStmt->removeType == OBJECT_STATISTIC_EXT);

	if (!ShouldPropagate())
	{
		return NIL;
	}

	QualifyTreeNode((Node *) dropStatisticsStmt);

	List *ddlJobs = NIL;
	List *processedStatsOids = NIL;
	List *objectNameList = NULL;
	foreach_declared_ptr(objectNameList, dropStatisticsStmt->objects)
	{
		Oid statsOid = get_statistics_object_oid(objectNameList,
												 dropStatisticsStmt->missing_ok);

		if (list_member_oid(processedStatsOids, statsOid))
		{
			/* skip duplicate entries in DROP STATISTICS */
			continue;
		}

		processedStatsOids = lappend_oid(processedStatsOids, statsOid);

		Oid relationId = GetRelIdByStatsOid(statsOid);

		if (!OidIsValid(relationId) || !IsCitusTable(relationId))
		{
			continue;
		}

		char *ddlCommand = DeparseDropStatisticsStmt(objectNameList,
													 dropStatisticsStmt->missing_ok);

		DDLJob *ddlJob = palloc0(sizeof(DDLJob));

		ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
		ddlJob->startNewTransaction = false;
		ddlJob->metadataSyncCommand = ddlCommand;
		ddlJob->taskList = DDLTaskList(relationId, ddlCommand);

		ddlJobs = lappend(ddlJobs, ddlJob);
	}

	return ddlJobs;
}


/*
 * DropStatisticsObjectAddress returns list of object addresses in the drop statistics
 * statement.
 */
List *
DropStatisticsObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	DropStmt *dropStatisticsStmt = castNode(DropStmt, node);
	Assert(dropStatisticsStmt->removeType == OBJECT_STATISTIC_EXT);

	List *objectAddresses = NIL;

	List *objectNameList = NULL;
	foreach_declared_ptr(objectNameList, dropStatisticsStmt->objects)
	{
		Oid statsOid = get_statistics_object_oid(objectNameList,
												 dropStatisticsStmt->missing_ok);

		ObjectAddress *objectAddress = palloc0(sizeof(ObjectAddress));
		ObjectAddressSet(*objectAddress, StatisticExtRelationId, statsOid);
		objectAddresses = lappend(objectAddresses, objectAddress);
	}

	return objectAddresses;
}


/*
 * PreprocessAlterStatisticsRenameStmt is called during the planning phase for
 * ALTER STATISTICS RENAME.
 */
List *
PreprocessAlterStatisticsRenameStmt(Node *node, const char *queryString,
									ProcessUtilityContext processUtilityContext)
{
	RenameStmt *renameStmt = castNode(RenameStmt, node);
	Assert(renameStmt->renameType == OBJECT_STATISTIC_EXT);

	Oid statsOid = get_statistics_object_oid((List *) renameStmt->object, false);
	Oid relationId = GetRelIdByStatsOid(statsOid);

	if (!IsCitusTable(relationId) || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) renameStmt);

	char *ddlCommand = DeparseTreeNode((Node *) renameStmt);

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));

	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
	ddlJob->startNewTransaction = false;
	ddlJob->metadataSyncCommand = ddlCommand;
	ddlJob->taskList = DDLTaskList(relationId, ddlCommand);

	List *ddlJobs = list_make1(ddlJob);

	return ddlJobs;
}


/*
 * PreprocessAlterStatisticsSchemaStmt is called during the planning phase for
 * ALTER STATISTICS SET SCHEMA.
 */
List *
PreprocessAlterStatisticsSchemaStmt(Node *node, const char *queryString,
									ProcessUtilityContext processUtilityContext)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_STATISTIC_EXT);

	Oid statsOid = get_statistics_object_oid((List *) stmt->object, false);
	Oid relationId = GetRelIdByStatsOid(statsOid);

	if (!IsCitusTable(relationId) || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);

	char *ddlCommand = DeparseTreeNode((Node *) stmt);

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));

	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
	ddlJob->startNewTransaction = false;
	ddlJob->metadataSyncCommand = ddlCommand;
	ddlJob->taskList = DDLTaskList(relationId, ddlCommand);

	List *ddlJobs = list_make1(ddlJob);

	return ddlJobs;
}


/*
 * PostprocessAlterStatisticsSchemaStmt is called after a ALTER STATISTICS SCHEMA
 * command has been executed by standard process utility.
 */
List *
PostprocessAlterStatisticsSchemaStmt(Node *node, const char *queryString)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_STATISTIC_EXT);

	String *statName = llast((List *) stmt->object);
	Oid statsOid = get_statistics_object_oid(list_make2(makeString(stmt->newschema),
														statName), false);
	Oid relationId = GetRelIdByStatsOid(statsOid);

	if (!IsCitusTable(relationId) || !ShouldPropagate())
	{
		return NIL;
	}

	bool missingOk = false;
	List *objectAddresses = GetObjectAddressListFromParseTree((Node *) stmt, missingOk,
															  true);

	/*  the code-path only supports a single object */
	Assert(list_length(objectAddresses) == 1);

	EnsureAllObjectDependenciesExistOnAllNodes(objectAddresses);

	return NIL;
}


/*
 * AlterStatisticsSchemaStmtObjectAddress finds the ObjectAddress for the statistics
 * that is altered by given AlterObjectSchemaStmt. If missingOk is false and if
 * the statistics does not exist, then it errors out.
 *
 * Never returns NULL, but the objid in the address can be invalid if missingOk
 * was set to true.
 */
List *
AlterStatisticsSchemaStmtObjectAddress(Node *node, bool missingOk, bool isPostprocess)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);

	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	Oid statsOid = InvalidOid;

	List *statName = (List *) stmt->object;

	if (isPostprocess)
	{
		/*
		 * we should search the object in the new schema because the method is
		 * called during postprocess, standard_utility should have already moved
		 * the stat into new schema.
		 */
		List *newStatName = list_make2(makeString(stmt->newschema), llast(statName));
		statsOid = get_statistics_object_oid(newStatName, missingOk);
	}
	else
	{
		statsOid = get_statistics_object_oid(statName, missingOk);
	}

	ObjectAddressSet(*address, StatisticExtRelationId, statsOid);

	return list_make1(address);
}


/*
 * PreprocessAlterStatisticsStmt is called during the planning phase for
 * ALTER STATISTICS .. SET STATISTICS.
 */
List *
PreprocessAlterStatisticsStmt(Node *node, const char *queryString,
							  ProcessUtilityContext processUtilityContext)
{
	AlterStatsStmt *stmt = castNode(AlterStatsStmt, node);

	Oid statsOid = get_statistics_object_oid(stmt->defnames, stmt->missing_ok);

	if (!OidIsValid(statsOid))
	{
		/*
		 * If statsOid is invalid, here we can assume that the query includes
		 * IF EXISTS clause, since get_statistics_object_oid would error out otherwise.
		 * So here we can safely return NIL here without checking stmt->missing_ok.
		 */
		return NIL;
	}

	Oid relationId = GetRelIdByStatsOid(statsOid);

	if (!IsCitusTable(relationId) || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);

	char *ddlCommand = DeparseTreeNode((Node *) stmt);

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));

	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
	ddlJob->startNewTransaction = false;
	ddlJob->metadataSyncCommand = ddlCommand;
	ddlJob->taskList = DDLTaskList(relationId, ddlCommand);

	List *ddlJobs = list_make1(ddlJob);

	return ddlJobs;
}


/*
 * PreprocessAlterStatisticsOwnerStmt is called during the planning phase for
 * ALTER STATISTICS .. OWNER TO.
 */
List *
PreprocessAlterStatisticsOwnerStmt(Node *node, const char *queryString,
								   ProcessUtilityContext processUtilityContext)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_STATISTIC_EXT);

	Oid statsOid = get_statistics_object_oid((List *) stmt->object, false);
	Oid relationId = GetRelIdByStatsOid(statsOid);

	if (!IsCitusTable(relationId) || !ShouldPropagate())
	{
		return NIL;
	}

	EnsureCoordinator();

	QualifyTreeNode((Node *) stmt);

	char *ddlCommand = DeparseTreeNode((Node *) stmt);

	DDLJob *ddlJob = palloc0(sizeof(DDLJob));

	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
	ddlJob->startNewTransaction = false;
	ddlJob->metadataSyncCommand = ddlCommand;
	ddlJob->taskList = DDLTaskList(relationId, ddlCommand);

	List *ddlJobs = list_make1(ddlJob);

	return ddlJobs;
}


/*
 * PostprocessAlterStatisticsOwnerStmt is invoked after the owner has been changed locally.
 * Since changing the owner could result in new dependencies being found for this object
 * we re-ensure all the dependencies for the statistics do exist.
 *
 * This is solely to propagate the new owner (and all its dependencies) if it was not
 * already distributed in the cluster.
 */
List *
PostprocessAlterStatisticsOwnerStmt(Node *node, const char *queryString)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_STATISTIC_EXT);

	Oid statsOid = get_statistics_object_oid((List *) stmt->object, false);
	Oid relationId = GetRelIdByStatsOid(statsOid);

	if (!IsCitusTable(relationId) || !ShouldPropagate())
	{
		return NIL;
	}

	ObjectAddress *statisticsAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*statisticsAddress, StatisticExtRelationId, statsOid);
	EnsureAllObjectDependenciesExistOnAllNodes(list_make1(statisticsAddress));

	return NIL;
}


/*
 * GetExplicitStatisticsCommandList returns the list of DDL commands to create
 * or alter statistics that are explicitly created for the table with relationId.
 * This function gets called when distributing the table with relationId.
 */
List *
GetExplicitStatisticsCommandList(Oid relationId)
{
	List *explicitStatisticsCommandList = NIL;

	Relation relation = RelationIdGetRelation(relationId);
	if (!RelationIsValid(relation))
	{
		ereport(ERROR, (errmsg("could not open relation with OID %u", relationId)));
	}

	List *statisticsIdList = RelationGetStatExtList(relation);
	RelationClose(relation);

	/* generate fully-qualified names */
	int saveNestLevel = PushEmptySearchPath();

	Oid statisticsId = InvalidOid;
	foreach_declared_oid(statisticsId, statisticsIdList)
	{
		/* we need create commands for already created stats before distribution */
		Datum commandText = DirectFunctionCall1(pg_get_statisticsobjdef,
												ObjectIdGetDatum(statisticsId));

		/*
		 * pg_get_statisticsobjdef doesn't throw an error if there is no such
		 * statistics object, be on the safe side.
		 */
		if (DatumGetPointer(commandText) == NULL)
		{
			ereport(ERROR, (errmsg("statistics with oid %u does not exist",
								   statisticsId)));
		}

		char *createStatisticsCommand = TextDatumGetCString(commandText);

		explicitStatisticsCommandList =
			lappend(explicitStatisticsCommandList,
					makeTableDDLCommandString(createStatisticsCommand));

		/* we need to alter stats' target if it's getting distributed after creation */
		char *alterStatisticsTargetCommand =
			CreateAlterCommandIfTargetNotDefault(statisticsId);

		if (alterStatisticsTargetCommand != NULL)
		{
			explicitStatisticsCommandList =
				lappend(explicitStatisticsCommandList,
						makeTableDDLCommandString(alterStatisticsTargetCommand));
		}

		/* we need to alter stats' owner if it's getting distributed after creation */
		char *alterStatisticsOwnerCommand =
			CreateAlterCommandIfOwnerNotDefault(statisticsId);

		if (alterStatisticsOwnerCommand != NULL)
		{
			explicitStatisticsCommandList =
				lappend(explicitStatisticsCommandList,
						makeTableDDLCommandString(alterStatisticsOwnerCommand));
		}
	}

	/* revert back to original search_path */
	PopEmptySearchPath(saveNestLevel);

	return explicitStatisticsCommandList;
}


/*
 * GetExplicitStatisticsSchemaIdList returns the list of schema ids of statistics'
 * which are created on relation with given relation id.
 */
List *
GetExplicitStatisticsSchemaIdList(Oid relationId)
{
	List *schemaIdList = NIL;

	Relation relation = RelationIdGetRelation(relationId);
	if (!RelationIsValid(relation))
	{
		ereport(ERROR, (errmsg("could not open relation with OID %u", relationId)));
	}

	List *statsIdList = RelationGetStatExtList(relation);
	RelationClose(relation);

	Oid statsId = InvalidOid;
	foreach_declared_oid(statsId, statsIdList)
	{
		HeapTuple heapTuple = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statsId));
		if (!HeapTupleIsValid(heapTuple))
		{
			ereport(ERROR, (errmsg("cache lookup failed for statistics "
								   "object with oid %u", statsId)));
		}
		FormData_pg_statistic_ext *statisticsForm =
			(FormData_pg_statistic_ext *) GETSTRUCT(heapTuple);

		Oid schemaId = statisticsForm->stxnamespace;
		if (!list_member_oid(schemaIdList, schemaId))
		{
			schemaIdList = lappend_oid(schemaIdList, schemaId);
		}
		ReleaseSysCache(heapTuple);
	}


	return schemaIdList;
}


/*
 * GetAlterIndexStatisticsCommands returns the list of ALTER INDEX .. ALTER COLUMN ..
 * SET STATISTICS commands, based on non default targets of the index with given id.
 * Note that this function only looks for expression indexes, since this command is
 * valid for only expression indexes.
 */
List *
GetAlterIndexStatisticsCommands(Oid indexOid)
{
	List *alterIndexStatisticsCommandList = NIL;
	int16 exprCount = 1;
	while (true)
	{
		HeapTuple attTuple = SearchSysCacheAttNum(indexOid, exprCount);

		if (!HeapTupleIsValid(attTuple))
		{
			break;
		}

		Form_pg_attribute targetAttr = (Form_pg_attribute) GETSTRUCT(attTuple);
		int32 targetAttstattarget = getAttstattarget_compat(attTuple);
		if (targetAttstattarget != DEFAULT_STATISTICS_TARGET)
		{
			char *indexNameWithSchema = generate_qualified_relation_name(indexOid);

			char *command =
				GenerateAlterIndexColumnSetStatsCommand(indexNameWithSchema,
														targetAttr->attnum,
														targetAttstattarget);

			alterIndexStatisticsCommandList =
				lappend(alterIndexStatisticsCommandList,
						makeTableDDLCommandString(command));
		}

		ReleaseSysCache(attTuple);
		exprCount++;
	}

	return alterIndexStatisticsCommandList;
}


/*
 * GenerateAlterIndexColumnSetStatsCommand returns a string in form of 'ALTER INDEX ..
 * ALTER COLUMN .. SET STATISTICS ..' which will be used to create a DDL command to
 * send to workers.
 */
static char *
GenerateAlterIndexColumnSetStatsCommand(char *indexNameWithSchema,
										int16 attnum,
										int32 attstattarget)
{
	StringInfoData command;
	initStringInfo(&command);
	appendStringInfo(&command,
					 ALTER_INDEX_COLUMN_SET_STATS_COMMAND,
					 indexNameWithSchema,
					 attnum,
					 attstattarget);

	return command.data;
}


/*
 * GetRelIdByStatsOid returns the relation id for given statistics oid.
 * If statistics doesn't exist, returns InvalidOid.
 */
static Oid
GetRelIdByStatsOid(Oid statsOid)
{
	HeapTuple tup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statsOid));

	if (!HeapTupleIsValid(tup))
	{
		return InvalidOid;
	}

	Form_pg_statistic_ext statisticsForm = (Form_pg_statistic_ext) GETSTRUCT(tup);
	ReleaseSysCache(tup);

	return statisticsForm->stxrelid;
}


/*
 * CreateAlterCommandIfOwnerNotDefault returns an ALTER STATISTICS .. OWNER TO
 * command if the stats object with given id has an owner different than the default one.
 * Returns NULL otherwise.
 */
static char *
CreateAlterCommandIfOwnerNotDefault(Oid statsOid)
{
	HeapTuple tup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statsOid));

	if (!HeapTupleIsValid(tup))
	{
		ereport(WARNING, (errmsg("No stats object found with id: %u", statsOid)));
		return NULL;
	}

	Form_pg_statistic_ext statisticsForm = (Form_pg_statistic_ext) GETSTRUCT(tup);
	ReleaseSysCache(tup);

	if (statisticsForm->stxowner == GetUserId())
	{
		return NULL;
	}

	char *schemaName = get_namespace_name(statisticsForm->stxnamespace);
	char *statName = NameStr(statisticsForm->stxname);
	char *ownerName = GetUserNameFromId(statisticsForm->stxowner, false);

	StringInfoData str;
	initStringInfo(&str);

	appendStringInfo(&str, "ALTER STATISTICS %s OWNER TO %s",
					 NameListToQuotedString(list_make2(makeString(schemaName),
													   makeString(statName))),
					 quote_identifier(ownerName));

	return str.data;
}


/*
 * CreateAlterCommandIfTargetNotDefault returns an ALTER STATISTICS .. SET STATISTICS
 * command if the stats object with given id has a target different than the default one.
 * Returns NULL otherwise.
 */
static char *
CreateAlterCommandIfTargetNotDefault(Oid statsOid)
{
	HeapTuple tup = SearchSysCache1(STATEXTOID, ObjectIdGetDatum(statsOid));

	if (!HeapTupleIsValid(tup))
	{
		ereport(WARNING, (errmsg("No stats object found with id: %u", statsOid)));
		return NULL;
	}

	Form_pg_statistic_ext statisticsForm = (Form_pg_statistic_ext) GETSTRUCT(tup);
	int16 currentStxstattarget = getStxstattarget_compat(tup);
	ReleaseSysCache(tup);

	if (currentStxstattarget == -1)
	{
		return NULL;
	}

	AlterStatsStmt *alterStatsStmt = makeNode(AlterStatsStmt);

	char *schemaName = get_namespace_name(statisticsForm->stxnamespace);
	char *statName = NameStr(statisticsForm->stxname);

	alterStatsStmt->stxstattarget = getAlterStatsStxstattarget_compat(
		currentStxstattarget);
	alterStatsStmt->defnames = list_make2(makeString(schemaName), makeString(statName));

	return DeparseAlterStatisticsStmt((Node *) alterStatsStmt);
}
