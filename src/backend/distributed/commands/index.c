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

#if PG_VERSION_NUM >= 120000
#include "access/genam.h"
#endif
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/tablecmds.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_planner.h"
#include "distributed/listutils.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/resource_lock.h"
#include "distributed/version_compat.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/* Local functions forward declarations for helper functions */
static List * CreateIndexTaskList(Oid relationId, IndexStmt *indexStmt);
static List * CreateReindexTaskList(Oid relationId, ReindexStmt *reindexStmt);
static void RangeVarCallbackForDropIndex(const RangeVar *rel, Oid relOid, Oid oldRelOid,
										 void *arg);
static void RangeVarCallbackForReindexIndex(const RangeVar *rel, Oid relOid, Oid
											oldRelOid,
											void *arg);
static void ErrorIfUnsupportedIndexStmt(IndexStmt *createIndexStatement);
static void ErrorIfUnsupportedDropIndexStmt(DropStmt *dropIndexStatement);
static List * DropIndexTaskList(Oid relationId, Oid indexId, DropStmt *dropStmt);


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
#if PG_VERSION_NUM >= 120000
	bool concurrent;
#endif
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
 * the master node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PreprocessIndexStmt(Node *node, const char *createIndexCommand)
{
	IndexStmt *createIndexStatement = castNode(IndexStmt, node);
	List *ddlJobs = NIL;

	/*
	 * We first check whether a distributed relation is affected. For that, we need to
	 * open the relation. To prevent race conditions with later lookups, lock the table,
	 * and modify the rangevar to include the schema.
	 */
	if (createIndexStatement->relation != NULL)
	{
		LOCKMODE lockmode = ShareLock;
		MemoryContext relationContext = NULL;

		/*
		 * We don't support concurrently creating indexes for distributed
		 * tables, but till this point, we don't know if it is a regular or a
		 * distributed table.
		 */
		if (createIndexStatement->concurrent)
		{
			lockmode = ShareUpdateExclusiveLock;
		}

		/*
		 * XXX: Consider using RangeVarGetRelidExtended with a permission
		 * checking callback. Right now we'll acquire the lock before having
		 * checked permissions, and will only fail when executing the actual
		 * index statements.
		 */
		Relation relation = heap_openrv(createIndexStatement->relation, lockmode);
		Oid relationId = RelationGetRelid(relation);

		bool isCitusRelation = IsCitusTable(relationId);

		if (createIndexStatement->relation->schemaname == NULL)
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
			relationContext = GetMemoryChunkContext(createIndexStatement->relation);
			createIndexStatement->relation->schemaname = MemoryContextStrdup(
				relationContext, namespaceName);
		}

		heap_close(relation, NoLock);

		if (isCitusRelation)
		{
			char *indexName = createIndexStatement->idxname;
			char *namespaceName = createIndexStatement->relation->schemaname;

			ErrorIfUnsupportedIndexStmt(createIndexStatement);

			Oid namespaceId = get_namespace_oid(namespaceName, false);
			Oid indexRelationId = get_relname_relid(indexName, namespaceId);

			/* if index does not exist, send the command to workers */
			if (!OidIsValid(indexRelationId))
			{
				DDLJob *ddlJob = palloc0(sizeof(DDLJob));
				ddlJob->targetRelationId = relationId;
				ddlJob->concurrentIndexCmd = createIndexStatement->concurrent;
				ddlJob->commandString = createIndexCommand;
				ddlJob->taskList = CreateIndexTaskList(relationId, createIndexStatement);

				ddlJobs = list_make1(ddlJob);
			}
		}
	}

	return ddlJobs;
}


/*
 * PreprocessReindexStmt determines whether a given REINDEX statement involves
 * a distributed table. If so (and if the statement does not use unsupported
 * options), it modifies the input statement to ensure proper execution against
 * the master node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PreprocessReindexStmt(Node *node, const char *reindexCommand)
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
		Relation relation = NULL;
		Oid relationId = InvalidOid;
		bool isCitusRelation = false;
#if PG_VERSION_NUM >= 120000
		LOCKMODE lockmode = reindexStatement->concurrent ? ShareUpdateExclusiveLock :
							AccessExclusiveLock;
#else
		LOCKMODE lockmode = AccessExclusiveLock;
#endif
		MemoryContext relationContext = NULL;

		Assert(reindexStatement->kind == REINDEX_OBJECT_INDEX ||
			   reindexStatement->kind == REINDEX_OBJECT_TABLE);

		if (reindexStatement->kind == REINDEX_OBJECT_INDEX)
		{
			Oid indOid;
			struct ReindexIndexCallbackState state;
#if PG_VERSION_NUM >= 120000
			state.concurrent = reindexStatement->concurrent;
#endif
			state.locked_table_oid = InvalidOid;

			indOid = RangeVarGetRelidExtended(reindexStatement->relation,
											  lockmode, 0,
											  RangeVarCallbackForReindexIndex,
											  &state);
			relation = index_open(indOid, NoLock);
			relationId = IndexGetRelation(indOid, false);
		}
		else
		{
			RangeVarGetRelidExtended(reindexStatement->relation, lockmode, 0,
									 RangeVarCallbackOwnsTable, NULL);

			relation = heap_openrv(reindexStatement->relation, NoLock);
			relationId = RelationGetRelid(relation);
		}

		isCitusRelation = IsCitusTable(relationId);

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
			heap_close(relation, NoLock);
		}

		if (isCitusRelation)
		{
			DDLJob *ddlJob = palloc0(sizeof(DDLJob));
			ddlJob->targetRelationId = relationId;
#if PG_VERSION_NUM >= 120000
			ddlJob->concurrentIndexCmd = reindexStatement->concurrent;
#else
			ddlJob->concurrentIndexCmd = false;
#endif
			ddlJob->commandString = reindexCommand;
			ddlJob->taskList = CreateReindexTaskList(relationId, reindexStatement);

			ddlJobs = list_make1(ddlJob);
		}
	}

	return ddlJobs;
}


/*
 * PreprocessDropIndexStmt determines whether a given DROP INDEX statement involves
 * a distributed table. If so (and if the statement does not use unsupported
 * options), it modifies the input statement to ensure proper execution against
 * the master node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PreprocessDropIndexStmt(Node *node, const char *dropIndexCommand)
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

		ddlJob->targetRelationId = distributedRelationId;
		ddlJob->concurrentIndexCmd = dropIndexStatement->concurrent;
		ddlJob->commandString = dropIndexCommand;
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

	/* we are only processing CONCURRENT index statements */
	if (!indexStmt->concurrent)
	{
		return NIL;
	}

	/* this logic only applies to the coordinator */
	if (!IsCoordinator())
	{
		return NIL;
	}

	/* commit the current transaction and start anew */
	CommitTransactionCommand();
	StartTransactionCommand();

	/* get the affected relation and index */
	Relation relation = heap_openrv(indexStmt->relation, ShareUpdateExclusiveLock);
	Oid indexRelationId = get_relname_relid(indexStmt->idxname,
											RelationGetNamespace(relation));
	Relation indexRelation = index_open(indexRelationId, RowExclusiveLock);

	/* close relations but retain locks */
	heap_close(relation, NoLock);
	index_close(indexRelation, NoLock);

	/* mark index as invalid, in-place (cannot be rolled back) */
	index_set_state_flags(indexRelationId, INDEX_DROP_CLEAR_VALID);

	/* re-open a transaction command from here on out */
	CommitTransactionCommand();
	StartTransactionCommand();

	/* now, update index's validity in a way that can roll back */
	Relation pg_index = heap_open(IndexRelationId, RowExclusiveLock);

	HeapTuple indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(
												   indexRelationId));
	Assert(HeapTupleIsValid(indexTuple)); /* better be present, we have lock! */

	/* mark as valid, save, and update pg_index indexes */
	Form_pg_index indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
	indexForm->indisvalid = true;

	CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);

	/* clean up; index now marked valid, but ROLLBACK will mark invalid */
	heap_freetuple(indexTuple);
	heap_close(pg_index, RowExclusiveLock);

	return NIL;
}


/*
 * ErrorIfUnsupportedAlterIndexStmt checks if the corresponding alter index
 * statement is supported for distributed tables and errors out if it is not.
 * Currently, only the following commands are supported.
 *
 * ALTER INDEX SET ()
 * ALTER INDEX RESET ()
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
						 errdetail("Only RENAME TO, SET (), and RESET () "
								   "are supported.")));
				return; /* keep compiler happy */
			}
		}
	}
}


/*
 * CreateIndexTaskList builds a list of tasks to execute a CREATE INDEX command
 * against a specified distributed table.
 */
static List *
CreateIndexTaskList(Oid relationId, IndexStmt *indexStmt)
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
	if (!pg_class_ownercheck(relOid, GetUserId()) &&
	    !pg_namespace_ownercheck(classform->relnamespace, GetUserId()))
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
#if PG_VERSION_NUM >= 120000
	table_lockmode = state->concurrent ? ShareUpdateExclusiveLock : ShareLock;
#else
	table_lockmode = ShareLock;
#endif

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
	if (!pg_class_ownercheck(relId, GetUserId()))
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
	char *indexRelationName = createIndexStatement->idxname;
	if (indexRelationName == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("creating index without a name on a distributed table is "
							   "currently unsupported")));
	}

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
		char partitionMethod = PartitionMethod(relationId);
		bool indexContainsPartitionColumn = false;

		/*
		 * Reference tables do not have partition key, and unique constraints
		 * are allowed for them. Thus, we added a short-circuit for reference tables.
		 */
		if (partitionMethod == DISTRIBUTE_BY_NONE)
		{
			return;
		}

		if (partitionMethod == DISTRIBUTE_BY_APPEND)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("creating unique indexes on append-partitioned tables "
								   "is currently unsupported")));
		}

		Var *partitionKey = ForceDistPartitionKey(relationId);
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

		taskList = lappend(taskList, task);

		resetStringInfo(&ddlString);
	}

	return taskList;
}
