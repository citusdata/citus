/*-------------------------------------------------------------------------
 *
 * index.c
 *    Commands for creating and altering indices on distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/pg_version_constants.h"
#include "access/genam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#if PG_VERSION_NUM >= PG_VERSION_16
#include "catalog/pg_namespace.h"
#endif
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/deparser.h"
#include "distributed/distributed_planner.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/namespace_utils.h"
#include "distributed/resource_lock.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/relation_utils.h"
#include "distributed/version_compat.h"
#include "distributed/worker_manager.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "parser/parse_utilcmd.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/* Local functions forward declarations for helper functions */
static void ErrorIfCreateIndexHasTooManyColumns(IndexStmt *createIndexStatement);
static int GetNumberOfIndexParameters(IndexStmt *createIndexStatement);
static bool IndexAlreadyExists(IndexStmt *createIndexStatement);
static Oid CreateIndexStmtGetIndexId(IndexStmt *createIndexStatement);
static Oid CreateIndexStmtGetSchemaId(IndexStmt *createIndexStatement);
static void SwitchToSequentialAndLocalExecutionIfIndexNameTooLong(
	IndexStmt *createIndexStatement);
static char * GenerateLongestShardPartitionIndexName(IndexStmt *createIndexStatement);
static char * GenerateDefaultIndexName(IndexStmt *createIndexStatement);
static List * GenerateIndexParameters(IndexStmt *createIndexStatement);
static DDLJob * GenerateCreateIndexDDLJob(IndexStmt *createIndexStatement,
										  const char *createIndexCommand);
static Oid CreateIndexStmtGetRelationId(IndexStmt *createIndexStatement);
static List * CreateIndexTaskList(IndexStmt *indexStmt);
static List * CreateReindexTaskList(Oid relationId, ReindexStmt *reindexStmt);
static void RangeVarCallbackForDropIndex(const RangeVar *rel, Oid relOid, Oid oldRelOid,
										 void *arg);
static void RangeVarCallbackForReindexIndex(const RangeVar *rel, Oid relOid, Oid
											oldRelOid,
											void *arg);
static void ErrorIfUnsupportedIndexStmt(IndexStmt *createIndexStatement);
static void ErrorIfUnsupportedDropIndexStmt(DropStmt *dropIndexStatement);
static List * DropIndexTaskList(Oid relationId, Oid indexId, DropStmt *dropStmt);
static Oid ReindexStmtFindRelationOid(ReindexStmt *reindexStmt, bool missingOk);

/*
 * This struct defines the state for the callback for drop statements.
 * It is copied as it is from commands/tablecmds.c in Postgres source.
 */
struct DropRelationCallbackState
{
	char relkind;
	Oid heapOid;
	bool concurrent;
};

/*
 * This struct defines the state for the callback for reindex statements.
 * It is copied as it is from commands/indexcmds.c in Postgres source.
 */
struct ReindexIndexCallbackState
{
	bool concurrent;
	Oid locked_table_oid;
};


/*
 * IsIndexRenameStmt returns whether the passed-in RenameStmt is the following
 * form:
 *
 *   - ALTER INDEX RENAME
 */
bool
IsIndexRenameStmt(RenameStmt *renameStmt)
{
	bool isIndexRenameStmt = false;

	if (renameStmt->renameType == OBJECT_INDEX)
	{
		isIndexRenameStmt = true;
	}

	return isIndexRenameStmt;
}


/*
 * PreprocessIndexStmt determines whether a given CREATE INDEX statement involves
 * a distributed table. If so (and if the statement does not use unsupported
 * options), it modifies the input statement to ensure proper execution against
 * the coordinator node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PreprocessIndexStmt(Node *node, const char *createIndexCommand,
					ProcessUtilityContext processUtilityContext)
{
	IndexStmt *createIndexStatement = castNode(IndexStmt, node);

	RangeVar *relationRangeVar = createIndexStatement->relation;
	if (relationRangeVar == NULL)
	{
		/* let's be on the safe side */
		return NIL;
	}

	/*
	 * We first check whether a distributed relation is affected. For that,
	 * we need to open the relation. To prevent race conditions with later
	 * lookups, lock the table.
	 *
	 * XXX: Consider using RangeVarGetRelidExtended with a permission
	 * checking callback. Right now we'll acquire the lock before having
	 * checked permissions, and will only fail when executing the actual
	 * index statements.
	 */
	LOCKMODE lockMode = GetCreateIndexRelationLockMode(createIndexStatement);
	Relation relation = table_openrv(relationRangeVar, lockMode);

	/*
	 * Before we do any further processing, fix the schema name to make sure
	 * that a (distributed) table with the same name does not appear on the
	 * search_path in front of the current schema. We do this even if the
	 * table is not distributed, since a distributed table may appear on the
	 * search_path by the time postgres starts processing this command.
	 */
	if (relationRangeVar->schemaname == NULL)
	{
		/* ensure we copy string into proper context */
		MemoryContext relationContext = GetMemoryChunkContext(relationRangeVar);
		char *namespaceName = RelationGetNamespaceName(relation);
		relationRangeVar->schemaname = MemoryContextStrdup(relationContext,
														   namespaceName);
	}

	table_close(relation, NoLock);

	Oid relationId = CreateIndexStmtGetRelationId(createIndexStatement);
	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	if (createIndexStatement->idxname == NULL)
	{
		/*
		 * Postgres does not support indexes with over INDEX_MAX_KEYS columns
		 * and we should not attempt to generate an index name for such cases.
		 */
		ErrorIfCreateIndexHasTooManyColumns(createIndexStatement);

		/*
		 * If there are expressions on the index, we should first transform
		 * the statement as the default index name depends on that. We do
		 * it on a copy not to interfere with standard process utility.
		 */
		IndexStmt *copyCreateIndexStatement =
			transformIndexStmt(relation->rd_id, copyObject(createIndexStatement),
							   createIndexCommand);

		/* ensure we copy string into proper context */
		MemoryContext relationContext = GetMemoryChunkContext(relationRangeVar);
		char *defaultIndexName = GenerateDefaultIndexName(copyCreateIndexStatement);
		createIndexStatement->idxname = MemoryContextStrdup(relationContext,
															defaultIndexName);
	}

	if (IndexAlreadyExists(createIndexStatement))
	{
		/*
		 * Let standard_processUtility to error out or skip if command has
		 * IF NOT EXISTS.
		 */
		return NIL;
	}

	ErrorIfUnsupportedIndexStmt(createIndexStatement);

	/*
	 * Citus has the logic to truncate the long shard names to prevent
	 * various issues, including self-deadlocks. However, for partitioned
	 * tables, when index is created on the parent table, the index names
	 * on the partitions are auto-generated by Postgres. We use the same
	 * Postgres function to generate the index names on the shards of the
	 * partitions. If the length exceeds the limit, we switch to sequential
	 * execution mode.
	 *
	 * The root cause of the problem is that postgres truncates the
	 * table/index names if they are longer than "NAMEDATALEN - 1".
	 * From Citus' perspective, running commands in parallel on the
	 * shards could mean these table/index names are truncated to be
	 * the same, and thus forming a self-deadlock as these tables/
	 * indexes are inserted into postgres' metadata tables, like pg_class.
	 */
	SwitchToSequentialAndLocalExecutionIfIndexNameTooLong(createIndexStatement);

	DDLJob *ddlJob = GenerateCreateIndexDDLJob(createIndexStatement, createIndexCommand);
	return list_make1(ddlJob);
}


/*
 * ErrorIfCreateIndexHasTooManyColumns errors out if given CREATE INDEX command
 * would use more than INDEX_MAX_KEYS columns.
 */
static void
ErrorIfCreateIndexHasTooManyColumns(IndexStmt *createIndexStatement)
{
	int numberOfIndexParameters = GetNumberOfIndexParameters(createIndexStatement);
	if (numberOfIndexParameters <= INDEX_MAX_KEYS)
	{
		return;
	}

	ereport(ERROR, (errcode(ERRCODE_TOO_MANY_COLUMNS),
					errmsg("cannot use more than %d columns in an index",
						   INDEX_MAX_KEYS)));
}


/*
 * GetNumberOfIndexParameters returns number of parameters to be used when
 * creating the index to be defined by given CREATE INDEX command.
 */
static int
GetNumberOfIndexParameters(IndexStmt *createIndexStatement)
{
	List *indexParams = createIndexStatement->indexParams;
	List *indexIncludingParams = createIndexStatement->indexIncludingParams;
	return list_length(indexParams) + list_length(indexIncludingParams);
}


/*
 * IndexAlreadyExists returns true if index to be created by given CREATE INDEX
 * command already exists.
 */
static bool
IndexAlreadyExists(IndexStmt *createIndexStatement)
{
	Oid indexRelationId = CreateIndexStmtGetIndexId(createIndexStatement);
	return OidIsValid(indexRelationId);
}


/*
 * CreateIndexStmtGetIndexId returns OID of the index that given CREATE INDEX
 * command attempts to create if it's already created before. Otherwise, returns
 * InvalidOid.
 */
static Oid
CreateIndexStmtGetIndexId(IndexStmt *createIndexStatement)
{
	char *indexName = createIndexStatement->idxname;
	Oid namespaceId = CreateIndexStmtGetSchemaId(createIndexStatement);
	Oid indexRelationId = get_relname_relid(indexName, namespaceId);
	return indexRelationId;
}


/*
 * CreateIndexStmtGetSchemaId returns schemaId of the schema that given
 * CREATE INDEX command operates on.
 */
static Oid
CreateIndexStmtGetSchemaId(IndexStmt *createIndexStatement)
{
	RangeVar *relationRangeVar = createIndexStatement->relation;
	char *schemaName = relationRangeVar->schemaname;
	bool missingOk = false;
	Oid namespaceId = get_namespace_oid(schemaName, missingOk);
	return namespaceId;
}


/*
 * ExecuteFunctionOnEachTableIndex executes the given pgIndexProcessor function on each
 * index of the given relation.
 * It returns a list that is filled by the pgIndexProcessor.
 */
List *
ExecuteFunctionOnEachTableIndex(Oid relationId, PGIndexProcessor pgIndexProcessor,
								int indexFlags)
{
	List *result = NIL;

	Relation relation = RelationIdGetRelation(relationId);
	if (!RelationIsValid(relation))
	{
		ereport(ERROR, (errmsg("could not open relation with OID %u", relationId)));
	}

	List *indexIdList = RelationGetIndexList(relation);
	Oid indexId = InvalidOid;
	foreach_oid(indexId, indexIdList)
	{
		HeapTuple indexTuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));
		if (!HeapTupleIsValid(indexTuple))
		{
			ereport(ERROR, (errmsg("cache lookup failed for index with oid %u",
								   indexId)));
		}

		Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
		pgIndexProcessor(indexForm, &result, indexFlags);
		ReleaseSysCache(indexTuple);
	}

	RelationClose(relation);
	return result;
}


/*
 * SwitchToSequentialAndLocalExecutionIfIndexNameTooLong generates the longest index name
 * on the shards of the partitions, and if exceeds the limit switches to sequential and
 * local execution to prevent self-deadlocks.
 */
static void
SwitchToSequentialAndLocalExecutionIfIndexNameTooLong(IndexStmt *createIndexStatement)
{
	Oid relationId = CreateIndexStmtGetRelationId(createIndexStatement);
	if (!PartitionedTable(relationId))
	{
		/* Citus already handles long names for regular tables */
		return;
	}

	if (ShardIntervalCount(relationId) == 0)
	{
		/*
		 * Relation has no shards, so we cannot run into "long shard index
		 * name" issue.
		 */
		return;
	}

	char *indexName = GenerateLongestShardPartitionIndexName(createIndexStatement);
	if (indexName &&
		strnlen(indexName, NAMEDATALEN) >= NAMEDATALEN - 1)
	{
		if (ParallelQueryExecutedInTransaction())
		{
			/*
			 * If there has already been a parallel query executed, the sequential mode
			 * would still use the already opened parallel connections to the workers,
			 * thus contradicting our purpose of using sequential mode.
			 */
			ereport(ERROR, (errmsg(
								"The index name (%s) on a shard is too long and could lead "
								"to deadlocks when executed in a transaction "
								"block after a parallel query", indexName),
							errhint("Try re-running the transaction with "
									"\"SET LOCAL citus.multi_shard_modify_mode TO "
									"\'sequential\';\"")));
		}
		else
		{
			elog(DEBUG1, "the index name on the shards of the partition "
						 "is too long, switching to sequential and local execution "
						 "mode to prevent self deadlocks: %s", indexName);

			SetLocalMultiShardModifyModeToSequential();
			SetLocalExecutionStatus(LOCAL_EXECUTION_REQUIRED);
		}
	}
}


/*
 * GenerateLongestShardPartitionIndexName emulates Postgres index name
 * generation for partitions on the shards. It returns the longest
 * possible index name.
 */
static char *
GenerateLongestShardPartitionIndexName(IndexStmt *createIndexStatement)
{
	Oid relationId = CreateIndexStmtGetRelationId(createIndexStatement);
	Oid longestNamePartitionId = PartitionWithLongestNameRelationId(relationId);
	if (!OidIsValid(longestNamePartitionId))
	{
		/* no partitions have been created yet */
		return NULL;
	}

	char *longestPartitionShardName = get_rel_name(longestNamePartitionId);
	ShardInterval *shardInterval = LoadShardIntervalWithLongestShardName(
		longestNamePartitionId);
	AppendShardIdToName(&longestPartitionShardName, shardInterval->shardId);

	IndexStmt *createLongestShardIndexStmt = copyObject(createIndexStatement);
	createLongestShardIndexStmt->relation->relname = longestPartitionShardName;

	char *choosenIndexName = GenerateDefaultIndexName(createLongestShardIndexStmt);
	return choosenIndexName;
}


/*
 * GenerateDefaultIndexName is a wrapper around postgres function ChooseIndexName
 * that generates default index name for the index to be created by given CREATE
 * INDEX statement as postgres would do.
 *
 * (See DefineIndex at postgres/src/backend/commands/indexcmds.c)
 */
static char *
GenerateDefaultIndexName(IndexStmt *createIndexStatement)
{
	char *relationName = createIndexStatement->relation->relname;
	Oid namespaceId = CreateIndexStmtGetSchemaId(createIndexStatement);
	List *indexParams = GenerateIndexParameters(createIndexStatement);
	List *indexColNames = ChooseIndexColumnNames(indexParams);
	char *indexName = ChooseIndexName(relationName, namespaceId, indexColNames,
									  createIndexStatement->excludeOpNames,
									  createIndexStatement->primary,
									  createIndexStatement->isconstraint);

	return indexName;
}


/*
 * GenerateIndexParameters is a helper function that creates a list of parameters
 * required to assign a default index name for the index to be created by given
 * CREATE INDEX command.
 */
static List *
GenerateIndexParameters(IndexStmt *createIndexStatement)
{
	List *indexParams = createIndexStatement->indexParams;
	List *indexIncludingParams = createIndexStatement->indexIncludingParams;
	List *allIndexParams = list_concat(list_copy(indexParams),
									   list_copy(indexIncludingParams));
	return allIndexParams;
}


/*
 * GenerateCreateIndexDDLJob returns DDLJob for given CREATE INDEX command.
 */
static DDLJob *
GenerateCreateIndexDDLJob(IndexStmt *createIndexStatement, const char *createIndexCommand)
{
	DDLJob *ddlJob = palloc0(sizeof(DDLJob));

	ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId,
					 CreateIndexStmtGetRelationId(createIndexStatement));
	ddlJob->startNewTransaction = createIndexStatement->concurrent;
	ddlJob->metadataSyncCommand = createIndexCommand;
	ddlJob->taskList = CreateIndexTaskList(createIndexStatement);

	return ddlJob;
}


/*
 * CreateIndexStmtGetRelationId returns relationId for relation that given
 * CREATE INDEX command operates on.
 */
static Oid
CreateIndexStmtGetRelationId(IndexStmt *createIndexStatement)
{
	RangeVar *relationRangeVar = createIndexStatement->relation;
	LOCKMODE lockMode = GetCreateIndexRelationLockMode(createIndexStatement);
	bool missingOk = false;
	Oid relationId = RangeVarGetRelid(relationRangeVar, lockMode, missingOk);
	return relationId;
}


/*
 * GetCreateIndexRelationLockMode returns required lock mode to open the
 * relation that given CREATE INDEX command operates on.
 */
LOCKMODE
GetCreateIndexRelationLockMode(IndexStmt *createIndexStatement)
{
	if (createIndexStatement->concurrent)
	{
		return ShareUpdateExclusiveLock;
	}
	else
	{
		return ShareLock;
	}
}


/*
 * ReindexStmtFindRelationOid returns the oid of the relation on which the index exist
 * if the object is an index in the reindex stmt. It returns the oid of the relation
 * if the object is a table in the reindex stmt. It also acquires the relevant lock
 * for the statement.
 */
static Oid
ReindexStmtFindRelationOid(ReindexStmt *reindexStmt, bool missingOk)
{
	Assert(reindexStmt->relation != NULL);

	Assert(reindexStmt->kind == REINDEX_OBJECT_INDEX ||
		   reindexStmt->kind == REINDEX_OBJECT_TABLE);

	Oid relationId = InvalidOid;

	LOCKMODE lockmode = IsReindexWithParam_compat(reindexStmt, "concurrently") ?
						ShareUpdateExclusiveLock : AccessExclusiveLock;

	if (reindexStmt->kind == REINDEX_OBJECT_INDEX)
	{
		struct ReindexIndexCallbackState state;
		state.concurrent = IsReindexWithParam_compat(reindexStmt,
													 "concurrently");
		state.locked_table_oid = InvalidOid;

		Oid indOid = RangeVarGetRelidExtended(reindexStmt->relation, lockmode,
											  (missingOk) ? RVR_MISSING_OK : 0,
											  RangeVarCallbackForReindexIndex,
											  &state);
		relationId = IndexGetRelation(indOid, missingOk);
	}
	else
	{
		relationId = RangeVarGetRelidExtended(reindexStmt->relation, lockmode,
											  (missingOk) ? RVR_MISSING_OK : 0,
											  RangeVarCallbackOwnsTable, NULL);
	}

	return relationId;
}


/*
 * PreprocessReindexStmt determines whether a given REINDEX statement involves
 * a distributed table. If so (and if the statement does not use unsupported
 * options), it modifies the input statement to ensure proper execution against
 * the coordinator node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PreprocessReindexStmt(Node *node, const char *reindexCommand,
					  ProcessUtilityContext processUtilityContext)
{
	ReindexStmt *reindexStatement = castNode(ReindexStmt, node);
	List *ddlJobs = NIL;

	/*
	 * We first check whether a distributed relation is affected. For that, we need to
	 * open the relation. To prevent race conditions with later lookups, lock the table,
	 * and modify the rangevar to include the schema.
	 */
	if (reindexStatement->relation != NULL)
	{
		Oid relationId = ReindexStmtFindRelationOid(reindexStatement, false);
		MemoryContext relationContext = NULL;
		Relation relation = NULL;
		if (reindexStatement->kind == REINDEX_OBJECT_INDEX)
		{
			Oid indOid = RangeVarGetRelid(reindexStatement->relation, NoLock, 0);
			relation = index_open(indOid, NoLock);
		}
		else
		{
			relation = table_openrv(reindexStatement->relation, NoLock);
		}

		bool isCitusRelation = IsCitusTable(relationId);

		if (reindexStatement->relation->schemaname == NULL)
		{
			/*
			 * Before we do any further processing, fix the schema name to make sure
			 * that a (distributed) table with the same name does not appear on the
			 * search path in front of the current schema. We do this even if the
			 * table is not distributed, since a distributed table may appear on the
			 * search path by the time postgres starts processing this statement.
			 */
			char *namespaceName = get_namespace_name(RelationGetNamespace(relation));

			/* ensure we copy string into proper context */
			relationContext = GetMemoryChunkContext(reindexStatement->relation);
			reindexStatement->relation->schemaname = MemoryContextStrdup(relationContext,
																		 namespaceName);
		}

		if (reindexStatement->kind == REINDEX_OBJECT_INDEX)
		{
			index_close(relation, NoLock);
		}
		else
		{
			table_close(relation, NoLock);
		}

		if (isCitusRelation)
		{
			if (PartitionedTable(relationId))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("REINDEX TABLE queries on distributed partitioned "
									   "tables are not supported")));
			}

			DDLJob *ddlJob = palloc0(sizeof(DDLJob));
			ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId, relationId);
			ddlJob->startNewTransaction = IsReindexWithParam_compat(reindexStatement,
																	"concurrently");
			ddlJob->metadataSyncCommand = reindexCommand;
			ddlJob->taskList = CreateReindexTaskList(relationId, reindexStatement);

			ddlJobs = list_make1(ddlJob);
		}
	}

	return ddlJobs;
}


/*
 * ReindexStmtObjectAddress returns list of object addresses in the reindex
 * statement. We add the address if the object is either index or table;
 * else, we add invalid address.
 */
List *
ReindexStmtObjectAddress(Node *stmt, bool missing_ok, bool isPostprocess)
{
	ReindexStmt *reindexStatement = castNode(ReindexStmt, stmt);

	Oid relationId = InvalidOid;
	if (reindexStatement->relation != NULL)
	{
		/* we currently only support reindex commands on tables */
		relationId = ReindexStmtFindRelationOid(reindexStatement, missing_ok);
	}

	ObjectAddress *objectAddress = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*objectAddress, RelationRelationId, relationId);

	return list_make1(objectAddress);
}


/*
 * PreprocessDropIndexStmt determines whether a given DROP INDEX statement involves
 * a distributed table. If so (and if the statement does not use unsupported
 * options), it modifies the input statement to ensure proper execution against
 * the coordinator node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PreprocessDropIndexStmt(Node *node, const char *dropIndexCommand,
						ProcessUtilityContext processUtilityContext)
{
	DropStmt *dropIndexStatement = castNode(DropStmt, node);
	List *ddlJobs = NIL;
	Oid distributedIndexId = InvalidOid;
	Oid distributedRelationId = InvalidOid;

	Assert(dropIndexStatement->removeType == OBJECT_INDEX);

	/* check if any of the indexes being dropped belong to a distributed table */
	List *objectNameList = NULL;
	foreach_ptr(objectNameList, dropIndexStatement->objects)
	{
		struct DropRelationCallbackState state;
		uint32 rvrFlags = RVR_MISSING_OK;
		LOCKMODE lockmode = AccessExclusiveLock;

		RangeVar *rangeVar = makeRangeVarFromNameList(objectNameList);

		/*
		 * We don't support concurrently dropping indexes for distributed
		 * tables, but till this point, we don't know if it is a regular or a
		 * distributed table.
		 */
		if (dropIndexStatement->concurrent)
		{
			lockmode = ShareUpdateExclusiveLock;
		}

		/*
		 * The next few statements are based on RemoveRelations() in
		 * commands/tablecmds.c in Postgres source.
		 */
		AcceptInvalidationMessages();

		state.relkind = RELKIND_INDEX;
		state.heapOid = InvalidOid;
		state.concurrent = dropIndexStatement->concurrent;

		Oid indexId = RangeVarGetRelidExtended(rangeVar, lockmode, rvrFlags,
											   RangeVarCallbackForDropIndex,
											   (void *) &state);

		/*
		 * If the index does not exist, we don't do anything here, and allow
		 * postgres to throw appropriate error or notice message later.
		 */
		if (!OidIsValid(indexId))
		{
			continue;
		}

		Oid relationId = IndexGetRelation(indexId, false);
		bool isCitusRelation = IsCitusTable(relationId);
		if (isCitusRelation)
		{
			distributedIndexId = indexId;
			distributedRelationId = relationId;
			break;
		}
	}

	if (OidIsValid(distributedIndexId))
	{
		DDLJob *ddlJob = palloc0(sizeof(DDLJob));

		ErrorIfUnsupportedDropIndexStmt(dropIndexStatement);

		if (AnyForeignKeyDependsOnIndex(distributedIndexId))
		{
			MarkInvalidateForeignKeyGraph();
		}

		ObjectAddressSet(ddlJob->targetObjectAddress, RelationRelationId,
						 distributedRelationId);

		/*
		 * We do not want DROP INDEX CONCURRENTLY to commit locally before
		 * sending commands, because if there is a failure we would like to
		 * to be able to repeat the DROP INDEX later.
		 */
		ddlJob->startNewTransaction = false;
		ddlJob->metadataSyncCommand = dropIndexCommand;
		ddlJob->taskList = DropIndexTaskList(distributedRelationId, distributedIndexId,
											 dropIndexStatement);

		ddlJobs = list_make1(ddlJob);
	}

	return ddlJobs;
}


/*
 * PostprocessIndexStmt marks new indexes invalid if they were created using the
 * CONCURRENTLY flag. This (non-transactional) change provides the fallback
 * state if an error is raised, otherwise a sub-sequent change to valid will be
 * committed.
 */
List *
PostprocessIndexStmt(Node *node, const char *queryString)
{
	IndexStmt *indexStmt = castNode(IndexStmt, node);

	/* this logic only applies to the coordinator */
	if (!IsCoordinator())
	{
		return NIL;
	}

	/*
	 * We make sure schema name is not null in the PreprocessIndexStmt
	 */
	Oid schemaId = get_namespace_oid(indexStmt->relation->schemaname, true);
	Oid relationId = get_relname_relid(indexStmt->relation->relname, schemaId);
	if (!IsCitusTable(relationId))
	{
		return NIL;
	}

	Oid indexRelationId = get_relname_relid(indexStmt->idxname, schemaId);

	/* ensure dependencies of index exist on all nodes */
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*address, RelationRelationId, indexRelationId);
	EnsureAllObjectDependenciesExistOnAllNodes(list_make1(address));

	/* furtheron we are only processing CONCURRENT index statements */
	if (!indexStmt->concurrent)
	{
		return NIL;
	}

	/*
	 * EnsureAllObjectDependenciesExistOnAllNodes could have distributed objects that are required
	 * by this index. During the propagation process an active snapshout might be left as
	 * a side effect of inserting the local tuples via SPI. To not leak a snapshot like
	 * that we will pop any snapshot if we have any right before we commit.
	 */
	if (ActiveSnapshotSet())
	{
		PopActiveSnapshot();
	}

	/* commit the current transaction and start anew */
	CommitTransactionCommand();
	StartTransactionCommand();

	/* get the affected relation and index */
	Relation relation = table_openrv(indexStmt->relation, ShareUpdateExclusiveLock);
	Relation indexRelation = index_open(indexRelationId, RowExclusiveLock);

	/* close relations but retain locks */
	table_close(relation, NoLock);
	index_close(indexRelation, NoLock);

	/* mark index as invalid, in-place (cannot be rolled back) */
	index_set_state_flags(indexRelationId, INDEX_DROP_CLEAR_VALID);

	/* re-open a transaction command from here on out */
	CommitTransactionCommand();
	StartTransactionCommand();

	return NIL;
}


/*
 * ErrorIfUnsupportedAlterIndexStmt checks if the corresponding alter index
 * statement is supported for distributed tables and errors out if it is not.
 * Currently, only the following commands are supported.
 *
 * ALTER INDEX SET ()
 * ALTER INDEX RESET ()
 * ALTER INDEX ALTER COLUMN SET STATISTICS
 */
void
ErrorIfUnsupportedAlterIndexStmt(AlterTableStmt *alterTableStatement)
{
	/* error out if any of the subcommands are unsupported */
	List *commandList = alterTableStatement->cmds;
	AlterTableCmd *command = NULL;
	foreach_ptr(command, commandList)
	{
		AlterTableType alterTableType = command->subtype;

		switch (alterTableType)
		{
			case AT_SetRelOptions:  /* SET (...) */
			case AT_ResetRelOptions:    /* RESET (...) */
			case AT_ReplaceRelOptions:  /* replace entire option list */
			case AT_SetStatistics:  /* SET STATISTICS */
			case AT_AttachPartition: /* ATTACH PARTITION */
			{
				/* this command is supported by Citus */
				break;
			}

			/* unsupported create index statements */
			case AT_SetTableSpace:
			default:
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("alter index ... set tablespace ... "
								"is currently unsupported"),
						 errdetail("Only RENAME TO, SET (), RESET (), ATTACH PARTITION "
								   "and SET STATISTICS are supported.")));
				return; /* keep compiler happy */
			}
		}
	}
}


/*
 * CreateIndexTaskList builds a list of tasks to execute a CREATE INDEX command.
 */
static List *
CreateIndexTaskList(IndexStmt *indexStmt)
{
	List *taskList = NIL;
	Oid relationId = CreateIndexStmtGetRelationId(indexStmt);
	List *shardIntervalList = LoadShardIntervalList(relationId);
	StringInfoData ddlString;
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	initStringInfo(&ddlString);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;

		deparse_shard_index_statement(indexStmt, relationId, shardId, &ddlString);

		Task *task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, pstrdup(ddlString.data));
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependentTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);
		task->cannotBeExecutedInTransction = indexStmt->concurrent;

		taskList = lappend(taskList, task);

		resetStringInfo(&ddlString);
	}

	return taskList;
}


/*
 * CreateReindexTaskList builds a list of tasks to execute a REINDEX command
 * against a specified distributed table.
 */
static List *
CreateReindexTaskList(Oid relationId, ReindexStmt *reindexStmt)
{
	List *taskList = NIL;
	List *shardIntervalList = LoadShardIntervalList(relationId);
	StringInfoData ddlString;
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	initStringInfo(&ddlString);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;

		deparse_shard_reindex_statement(reindexStmt, relationId, shardId, &ddlString);

		Task *task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, pstrdup(ddlString.data));
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependentTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);
		task->cannotBeExecutedInTransction =
			IsReindexWithParam_compat(reindexStmt, "concurrently");

		taskList = lappend(taskList, task);

		resetStringInfo(&ddlString);
	}

	return taskList;
}


/*
 * Before acquiring a table lock, check whether we have sufficient rights.
 * In the case of DROP INDEX, also try to lock the table before the index.
 *
 * This code is heavily borrowed from RangeVarCallbackForDropRelation() in
 * commands/tablecmds.c in Postgres source. We need this to ensure the right
 * order of locking while dealing with DROP INDEX statements. Because we are
 * exclusively using this callback for INDEX processing, the PARTITION-related
 * logic from PostgreSQL's similar callback has been omitted as unneeded.
 */
static void
RangeVarCallbackForDropIndex(const RangeVar *rel, Oid relOid, Oid oldRelOid, void *arg)
{
	/* *INDENT-OFF* */
	HeapTuple	tuple;
	char		relkind;
	char		expected_relkind;
	LOCKMODE	heap_lockmode;

	struct DropRelationCallbackState *state = (struct DropRelationCallbackState *) arg;
	relkind = state->relkind;
	heap_lockmode = state->concurrent ?
	                ShareUpdateExclusiveLock : AccessExclusiveLock;

	Assert(relkind == RELKIND_INDEX);

	/*
	 * If we previously locked some other index's heap, and the name we're
	 * looking up no longer refers to that relation, release the now-useless
	 * lock.
	 */
	if (relOid != oldRelOid && OidIsValid(state->heapOid))
	{
		UnlockRelationOid(state->heapOid, heap_lockmode);
		state->heapOid = InvalidOid;
	}

	/* Didn't find a relation, so no need for locking or permission checks. */
	if (!OidIsValid(relOid))
		return;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relOid));
	if (!HeapTupleIsValid(tuple))
		return;					/* concurrently dropped, so nothing to do */
	Form_pg_class classform = (Form_pg_class) GETSTRUCT(tuple);

	/*
	 * PG 11 sends relkind as partitioned index for an index
	 * on partitioned table. It is handled the same
	 * as regular index as far as we are concerned here.
	 *
	 * See tablecmds.c:RangeVarCallbackForDropRelation()
	 */
	expected_relkind = classform->relkind;

	if (expected_relkind == RELKIND_PARTITIONED_INDEX)
		expected_relkind = RELKIND_INDEX;

	if (expected_relkind != relkind)
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
				errmsg("\"%s\" is not an index", rel->relname)));

	/* Allow DROP to either table owner or schema owner */
	if (!object_ownercheck(RelationRelationId, relOid, GetUserId()) &&
		!object_ownercheck(NamespaceRelationId, classform->relnamespace, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_INDEX, rel->relname);
	}

	if (!allowSystemTableMods && IsSystemClass(relOid, classform))
		ereport(ERROR,
		        (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				        errmsg("permission denied: \"%s\" is a system catalog",
				               rel->relname)));

	ReleaseSysCache(tuple);

	/*
	 * In DROP INDEX, attempt to acquire lock on the parent table before
	 * locking the index.  index_drop() will need this anyway, and since
	 * regular queries lock tables before their indexes, we risk deadlock if
	 * we do it the other way around.  No error if we don't find a pg_index
	 * entry, though --- the relation may have been dropped.
	 */
	if (relkind == RELKIND_INDEX && relOid != oldRelOid)
	{
		state->heapOid = IndexGetRelation(relOid, true);
		if (OidIsValid(state->heapOid))
			LockRelationOid(state->heapOid, heap_lockmode);
	}
	/* *INDENT-ON* */
}


/*
 * Check permissions on table before acquiring relation lock; also lock
 * the heap before the RangeVarGetRelidExtended takes the index lock, to avoid
 * deadlocks.
 *
 * This code is borrowed from RangeVarCallbackForReindexIndex() in
 * commands/indexcmds.c in Postgres source. We need this to ensure the right
 * order of locking while dealing with REINDEX statements.
 */
static void
RangeVarCallbackForReindexIndex(const RangeVar *relation, Oid relId, Oid oldRelId,
								void *arg)
{
	/* *INDENT-OFF* */
	char		relkind;
	struct ReindexIndexCallbackState *state = arg;
	LOCKMODE	table_lockmode;

	/*
	 * Lock level here should match table lock in reindex_index() for
	 * non-concurrent case and table locks used by index_concurrently_*() for
	 * concurrent case.
	 */
	table_lockmode = state->concurrent ? ShareUpdateExclusiveLock : ShareLock;

	/*
	 * If we previously locked some other index's heap, and the name we're
	 * looking up no longer refers to that relation, release the now-useless
	 * lock.
	 */
	if (relId != oldRelId && OidIsValid(oldRelId))
	{
		UnlockRelationOid(state->locked_table_oid, table_lockmode);
		state->locked_table_oid = InvalidOid;
	}

	/* If the relation does not exist, there's nothing more to do. */
	if (!OidIsValid(relId))
		return;

	/*
	 * If the relation does exist, check whether it's an index.  But note that
	 * the relation might have been dropped between the time we did the name
	 * lookup and now.  In that case, there's nothing to do.
	 */
	relkind = get_rel_relkind(relId);
	if (!relkind)
		return;
	if (relkind != RELKIND_INDEX && relkind != RELKIND_PARTITIONED_INDEX)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an index", relation->relname)));

	/* Check permissions */
	if (!object_ownercheck(RelationRelationId, relId, GetUserId()))
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_INDEX, relation->relname);

	/* Lock heap before index to avoid deadlock. */
	if (relId != oldRelId)
	{
		Oid			table_oid = IndexGetRelation(relId, true);

		/*
		 * If the OID isn't valid, it means the index was concurrently
		 * dropped, which is not a problem for us; just return normally.
		 */
		if (OidIsValid(table_oid))
		{
			LockRelationOid(table_oid, table_lockmode);
			state->locked_table_oid = table_oid;
		}
	}
	/* *INDENT-ON* */
}


/*
 * ErrorIfUnsupportedIndexStmt checks if the corresponding index statement is
 * supported for distributed tables and errors out if it is not.
 */
static void
ErrorIfUnsupportedIndexStmt(IndexStmt *createIndexStatement)
{
	if (createIndexStatement->tableSpace != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("specifying tablespaces with CREATE INDEX statements is "
							   "currently unsupported")));
	}

	if (createIndexStatement->unique)
	{
		RangeVar *relation = createIndexStatement->relation;
		bool missingOk = false;

		/* caller uses ShareLock for non-concurrent indexes, use the same lock here */
		LOCKMODE lockMode = ShareLock;
		Oid relationId = RangeVarGetRelid(relation, lockMode, missingOk);
		bool indexContainsPartitionColumn = false;

		/*
		 * Non-distributed tables do not have partition key, and unique constraints
		 * are allowed for them. Thus, we added a short-circuit for non-distributed tables.
		 */
		if (!HasDistributionKey(relationId))
		{
			return;
		}

		if (IsCitusTableType(relationId, APPEND_DISTRIBUTED))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("creating unique indexes on append-partitioned tables "
								   "is currently unsupported")));
		}

		if (AllowUnsafeConstraints)
		{
			/*
			 * The user explicitly wants to allow the constraint without
			 * distribution column.
			 */
			return;
		}

		Var *partitionKey = DistPartitionKeyOrError(relationId);
		List *indexParameterList = createIndexStatement->indexParams;
		IndexElem *indexElement = NULL;
		foreach_ptr(indexElement, indexParameterList)
		{
			const char *columnName = indexElement->name;

			/* column name is null for index expressions, skip it */
			if (columnName == NULL)
			{
				continue;
			}

			AttrNumber attributeNumber = get_attnum(relationId, columnName);
			if (attributeNumber == partitionKey->varattno)
			{
				indexContainsPartitionColumn = true;
			}
		}

		if (!indexContainsPartitionColumn)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("creating unique indexes on non-partition "
								   "columns is currently unsupported")));
		}
	}
}


/*
 * ErrorIfUnsupportedDropIndexStmt checks if the corresponding drop index statement is
 * supported for distributed tables and errors out if it is not.
 */
static void
ErrorIfUnsupportedDropIndexStmt(DropStmt *dropIndexStatement)
{
	Assert(dropIndexStatement->removeType == OBJECT_INDEX);

	if (list_length(dropIndexStatement->objects) > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot drop multiple distributed objects in a "
							   "single command"),
						errhint("Try dropping each object in a separate DROP "
								"command.")));
	}
}


/*
 * DropIndexTaskList builds a list of tasks to execute a DROP INDEX command
 * against a specified distributed table.
 */
static List *
DropIndexTaskList(Oid relationId, Oid indexId, DropStmt *dropStmt)
{
	List *taskList = NIL;
	List *shardIntervalList = LoadShardIntervalList(relationId);
	char *indexName = get_rel_name(indexId);
	Oid schemaId = get_rel_namespace(indexId);
	char *schemaName = get_namespace_name(schemaId);
	StringInfoData ddlString;
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	initStringInfo(&ddlString);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;
		char *shardIndexName = pstrdup(indexName);

		AppendShardIdToName(&shardIndexName, shardId);

		/* deparse shard-specific DROP INDEX command */
		appendStringInfo(&ddlString, "DROP INDEX %s %s %s %s",
						 (dropStmt->concurrent ? "CONCURRENTLY" : ""),
						 (dropStmt->missing_ok ? "IF EXISTS" : ""),
						 quote_qualified_identifier(schemaName, shardIndexName),
						 (dropStmt->behavior == DROP_RESTRICT ? "RESTRICT" : "CASCADE"));

		Task *task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, pstrdup(ddlString.data));
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependentTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);
		task->cannotBeExecutedInTransction = dropStmt->concurrent;

		taskList = lappend(taskList, task);

		resetStringInfo(&ddlString);
	}

	return taskList;
}


/*
 * MarkIndexValid marks an index as valid after a CONCURRENTLY command. We mark
 * indexes invalid in PostProcessIndexStmt and then commit, such that any failure
 * leaves behind an invalid index. We mark it as valid here when the command
 * completes.
 */
void
MarkIndexValid(IndexStmt *indexStmt)
{
	Assert(indexStmt->concurrent);
	Assert(IsCoordinator());

	/*
	 * We make sure schema name is not null in the PreprocessIndexStmt
	 */
	bool missingOk = false;
	Oid schemaId = get_namespace_oid(indexStmt->relation->schemaname, missingOk);

	Oid relationId PG_USED_FOR_ASSERTS_ONLY =
		get_relname_relid(indexStmt->relation->relname, schemaId);

	Assert(IsCitusTable(relationId));

	/* get the affected relation and index */
	Relation relation = table_openrv(indexStmt->relation, ShareUpdateExclusiveLock);
	Oid indexRelationId = get_relname_relid(indexStmt->idxname,
											schemaId);
	Relation indexRelation = index_open(indexRelationId, RowExclusiveLock);

	/* mark index as valid, in-place (cannot be rolled back) */
	index_set_state_flags(indexRelationId, INDEX_CREATE_SET_VALID);

	table_close(relation, NoLock);
	index_close(indexRelation, NoLock);
}
