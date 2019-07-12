/*-------------------------------------------------------------------------
 *
 * index.c
 *    Commands for creating and altering indices on distributed tables.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if PG_VERSION_NUM > 12000
#include "access/genam.h"
#endif
#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/distributed_planner.h"
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
static void RangeVarCallbackForDropIndex(const RangeVar *rel, Oid relOid, Oid oldRelOid,
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
 * PlanIndexStmt determines whether a given CREATE INDEX statement involves
 * a distributed table. If so (and if the statement does not use unsupported
 * options), it modifies the input statement to ensure proper execution against
 * the master node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PlanIndexStmt(IndexStmt *createIndexStatement, const char *createIndexCommand)
{
	List *ddlJobs = NIL;

	/*
	 * We first check whether a distributed relation is affected. For that, we need to
	 * open the relation. To prevent race conditions with later lookups, lock the table,
	 * and modify the rangevar to include the schema.
	 */
	if (createIndexStatement->relation != NULL)
	{
		Relation relation = NULL;
		Oid relationId = InvalidOid;
		bool isDistributedRelation = false;
		char *namespaceName = NULL;
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
		relation = heap_openrv(createIndexStatement->relation, lockmode);
		relationId = RelationGetRelid(relation);

		isDistributedRelation = IsDistributedTable(relationId);

		/*
		 * Before we do any further processing, fix the schema name to make sure
		 * that a (distributed) table with the same name does not appear on the
		 * search path in front of the current schema. We do this even if the
		 * table is not distributed, since a distributed table may appear on the
		 * search path by the time postgres starts processing this statement.
		 */
		namespaceName = get_namespace_name(RelationGetNamespace(relation));

		/* ensure we copy string into proper context */
		relationContext = GetMemoryChunkContext(createIndexStatement->relation);
		namespaceName = MemoryContextStrdup(relationContext, namespaceName);
		createIndexStatement->relation->schemaname = namespaceName;

		heap_close(relation, NoLock);

		if (isDistributedRelation)
		{
			Oid namespaceId = InvalidOid;
			Oid indexRelationId = InvalidOid;
			char *indexName = createIndexStatement->idxname;

			ErrorIfUnsupportedIndexStmt(createIndexStatement);

			namespaceId = get_namespace_oid(namespaceName, false);
			indexRelationId = get_relname_relid(indexName, namespaceId);

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
 * PlanDropIndexStmt determines whether a given DROP INDEX statement involves
 * a distributed table. If so (and if the statement does not use unsupported
 * options), it modifies the input statement to ensure proper execution against
 * the master node table and creates a DDLJob to encapsulate information needed
 * during the worker node portion of DDL execution before returning that DDLJob
 * in a List. If no distributed table is involved, this function returns NIL.
 */
List *
PlanDropIndexStmt(DropStmt *dropIndexStatement, const char *dropIndexCommand)
{
	List *ddlJobs = NIL;
	ListCell *dropObjectCell = NULL;
	Oid distributedIndexId = InvalidOid;
	Oid distributedRelationId = InvalidOid;

	Assert(dropIndexStatement->removeType == OBJECT_INDEX);

	/* check if any of the indexes being dropped belong to a distributed table */
	foreach(dropObjectCell, dropIndexStatement->objects)
	{
		Oid indexId = InvalidOid;
		Oid relationId = InvalidOid;
		bool isDistributedRelation = false;
		struct DropRelationCallbackState state;
		uint32 rvrFlags = RVR_MISSING_OK;
		LOCKMODE lockmode = AccessExclusiveLock;

		List *objectNameList = (List *) lfirst(dropObjectCell);
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

		indexId = RangeVarGetRelidInternal(rangeVar, lockmode, rvrFlags,
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

		relationId = IndexGetRelation(indexId, false);
		isDistributedRelation = IsDistributedTable(relationId);
		if (isDistributedRelation)
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
 * PostProcessIndexStmt marks new indexes invalid if they were created using the
 * CONCURRENTLY flag. This (non-transactional) change provides the fallback
 * state if an error is raised, otherwise a sub-sequent change to valid will be
 * committed.
 */
void
PostProcessIndexStmt(IndexStmt *indexStmt)
{
	Relation relation = NULL;
	Oid indexRelationId = InvalidOid;
	Relation indexRelation = NULL;
	Relation pg_index = NULL;
	HeapTuple indexTuple = NULL;
	Form_pg_index indexForm = NULL;

	/* we are only processing CONCURRENT index statements */
	if (!indexStmt->concurrent)
	{
		return;
	}

	/* this logic only applies to the coordinator */
	if (!IsCoordinator())
	{
		return;
	}

	/* commit the current transaction and start anew */
	CommitTransactionCommand();
	StartTransactionCommand();

	/* get the affected relation and index */
	relation = heap_openrv(indexStmt->relation, ShareUpdateExclusiveLock);
	indexRelationId = get_relname_relid(indexStmt->idxname,
										RelationGetNamespace(relation));
	indexRelation = index_open(indexRelationId, RowExclusiveLock);

	/* close relations but retain locks */
	heap_close(relation, NoLock);
	index_close(indexRelation, NoLock);

	/* mark index as invalid, in-place (cannot be rolled back) */
	index_set_state_flags(indexRelationId, INDEX_DROP_CLEAR_VALID);

	/* re-open a transaction command from here on out */
	CommitTransactionCommand();
	StartTransactionCommand();

	/* now, update index's validity in a way that can roll back */
	pg_index = heap_open(IndexRelationId, RowExclusiveLock);

	indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexRelationId));
	Assert(HeapTupleIsValid(indexTuple)); /* better be present, we have lock! */

	/* mark as valid, save, and update pg_index indexes */
	indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
	indexForm->indisvalid = true;

	CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);

	/* clean up; index now marked valid, but ROLLBACK will mark invalid */
	heap_freetuple(indexTuple);
	heap_close(pg_index, RowExclusiveLock);
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
	List *commandList = alterTableStatement->cmds;
	ListCell *commandCell = NULL;

	/* error out if any of the subcommands are unsupported */
	foreach(commandCell, commandList)
	{
		AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);
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
	ListCell *shardIntervalCell = NULL;
	StringInfoData ddlString;
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	initStringInfo(&ddlString);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		Task *task = NULL;

		deparse_shard_index_statement(indexStmt, relationId, shardId, &ddlString);

		task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		task->queryString = pstrdup(ddlString.data);
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependedTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = FinalizedShardPlacementList(shardId);

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
	struct DropRelationCallbackState *state;
	char		relkind;
	char		expected_relkind;
	Form_pg_class classform;
	LOCKMODE	heap_lockmode;

	state = (struct DropRelationCallbackState *) arg;
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
	classform = (Form_pg_class) GETSTRUCT(tuple);

	/*
	 * PG 11 sends relkind as partitioned index for an index
	 * on partitioned table. It is handled the same
	 * as regular index as far as we are concerned here.
	 *
	 * See tablecmds.c:RangeVarCallbackForDropRelation()
	 */
	expected_relkind = classform->relkind;

#if PG_VERSION_NUM >= 110000
	if (expected_relkind == RELKIND_PARTITIONED_INDEX)
		expected_relkind = RELKIND_INDEX;
#endif

	if (expected_relkind != relkind)
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
				errmsg("\"%s\" is not an index", rel->relname)));

	/* Allow DROP to either table owner or schema owner */
	if (!pg_class_ownercheck(relOid, GetUserId()) &&
	    !pg_namespace_ownercheck(classform->relnamespace, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, ACLCHECK_OBJECT_INDEX, rel->relname);
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
		Var *partitionKey = DistPartitionKey(relationId);
		char partitionMethod = PartitionMethod(relationId);
		List *indexParameterList = NIL;
		ListCell *indexParameterCell = NULL;
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

		indexParameterList = createIndexStatement->indexParams;
		foreach(indexParameterCell, indexParameterList)
		{
			IndexElem *indexElement = (IndexElem *) lfirst(indexParameterCell);
			char *columnName = indexElement->name;
			AttrNumber attributeNumber = InvalidAttrNumber;

			/* column name is null for index expressions, skip it */
			if (columnName == NULL)
			{
				continue;
			}

			attributeNumber = get_attnum(relationId, columnName);
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
	ListCell *shardIntervalCell = NULL;
	char *indexName = get_rel_name(indexId);
	Oid schemaId = get_rel_namespace(indexId);
	char *schemaName = get_namespace_name(schemaId);
	StringInfoData ddlString;
	uint64 jobId = INVALID_JOB_ID;
	int taskId = 1;

	initStringInfo(&ddlString);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		char *shardIndexName = pstrdup(indexName);
		Task *task = NULL;

		AppendShardIdToName(&shardIndexName, shardId);

		/* deparse shard-specific DROP INDEX command */
		appendStringInfo(&ddlString, "DROP INDEX %s %s %s %s",
						 (dropStmt->concurrent ? "CONCURRENTLY" : ""),
						 (dropStmt->missing_ok ? "IF EXISTS" : ""),
						 quote_qualified_identifier(schemaName, shardIndexName),
						 (dropStmt->behavior == DROP_RESTRICT ? "RESTRICT" : "CASCADE"));

		task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		task->queryString = pstrdup(ddlString.data);
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->dependedTaskList = NULL;
		task->anchorShardId = shardId;
		task->taskPlacementList = FinalizedShardPlacementList(shardId);

		taskList = lappend(taskList, task);

		resetStringInfo(&ddlString);
	}

	return taskList;
}
