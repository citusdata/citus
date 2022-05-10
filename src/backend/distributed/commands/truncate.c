/*-------------------------------------------------------------------------
 *
 * truncate.c
 *    Commands for truncating distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "distributed/adaptive_executor.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/distributed_planner.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_join_order.h"
#include "distributed/reference_table_utils.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/worker_transaction.h"
#include "distributed/worker_shard_visibility.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"


/* Local functions forward declarations for unsupported command checks */
static void ErrorIfUnsupportedTruncateStmt(TruncateStmt *truncateStatement);
static void ExecuteTruncateStmtSequentialIfNecessary(TruncateStmt *command);
static void EnsurePartitionTableNotReplicatedForTruncate(TruncateStmt *truncateStatement);
static void LockTruncatedRelationMetadataInWorkers(TruncateStmt *truncateStatement);
static List * TruncateTaskList(Oid relationId);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_truncate_trigger);
PG_FUNCTION_INFO_V1(truncate_local_data_after_distributing_table);

void EnsureLocalTableCanBeTruncated(Oid relationId);


/*
 * citus_truncate_trigger is called as a trigger when a distributed
 * table is truncated.
 */
Datum
citus_truncate_trigger(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	TriggerData *triggerData = (TriggerData *) fcinfo->context;
	Relation truncatedRelation = triggerData->tg_relation;
	Oid relationId = RelationGetRelid(truncatedRelation);

	if (!EnableDDLPropagation)
	{
		PG_RETURN_DATUM(PointerGetDatum(NULL));
	}

	/* we might be truncating multiple relations */
	UseCoordinatedTransaction();

	if (IsCitusTableType(relationId, APPEND_DISTRIBUTED))
	{
		Oid schemaId = get_rel_namespace(relationId);
		char *schemaName = get_namespace_name(schemaId);
		char *relationName = get_rel_name(relationId);
		bool dropShardsMetadataOnly = false;

		DirectFunctionCall4(citus_drop_all_shards,
							ObjectIdGetDatum(relationId),
							CStringGetTextDatum(schemaName),
							CStringGetTextDatum(relationName),
							BoolGetDatum(dropShardsMetadataOnly));
	}
	else
	{
		List *taskList = TruncateTaskList(relationId);

		/*
		 * If it is a local placement of a distributed table or a reference table,
		 * then execute TRUNCATE command locally.
		 */
		bool localExecutionSupported = true;
		ExecuteUtilityTaskList(taskList, localExecutionSupported);
	}

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * TruncateTaskList returns a list of tasks to execute a TRUNCATE on a
 * distributed table. This is handled separately from other DDL commands
 * because we handle it via the TRUNCATE trigger, which is called whenever
 * a truncate cascades.
 */
static List *
TruncateTaskList(Oid relationId)
{
	/* resulting task list */
	List *taskList = NIL;

	/* enumerate the tasks when putting them to the taskList */
	int taskId = 1;

	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *relationName = get_rel_name(relationId);

	List *shardIntervalList = LoadShardIntervalList(relationId);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;
		char *shardRelationName = pstrdup(relationName);

		/* build shard relation name */
		AppendShardIdToName(&shardRelationName, shardId);

		char *quotedShardName = quote_qualified_identifier(schemaName, shardRelationName);

		StringInfo shardQueryString = makeStringInfo();
		appendStringInfo(shardQueryString, "TRUNCATE TABLE %s CASCADE", quotedShardName);

		Task *task = CitusMakeNode(Task);
		task->jobId = INVALID_JOB_ID;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, shardQueryString->data);
		task->dependentTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * truncate_local_data_after_distributing_table truncates the local records of a distributed table.
 *
 * The main advantage of this function is to truncate all local records after creating a
 * distributed table, and prevent constraints from failing due to outdated local records.
 */
Datum
truncate_local_data_after_distributing_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	Oid relationId = PG_GETARG_OID(0);

	EnsureLocalTableCanBeTruncated(relationId);

	TruncateStmt *truncateStmt = makeNode(TruncateStmt);

	char *relationName = generate_qualified_relation_name(relationId);
	List *names = stringToQualifiedNameList(relationName);
	truncateStmt->relations = list_make1(makeRangeVarFromNameList(names));
	truncateStmt->restart_seqs = false;
	truncateStmt->behavior = DROP_CASCADE;

	set_config_option("citus.enable_ddl_propagation", "false",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
	ExecuteTruncate(truncateStmt);
	set_config_option("citus.enable_ddl_propagation", "true",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);

	PG_RETURN_VOID();
}


/*
 * EnsureLocalTableCanBeTruncated performs the necessary checks to make sure it
 * is safe to truncate the local table of a distributed table
 */
void
EnsureLocalTableCanBeTruncated(Oid relationId)
{
	/* error out if the relation is not a distributed table */
	if (!IsCitusTable(relationId))
	{
		ereport(ERROR, (errmsg("supplied parameter is not a distributed relation"),
						errdetail("This UDF only truncates local records of distributed "
								  "tables.")));
	}

	List *referencingForeignConstaintsFromLocalTables =
		GetForeignKeysFromLocalTables(relationId);
	if (list_length(referencingForeignConstaintsFromLocalTables) > 0)
	{
		Oid foreignKeyId = linitial_oid(referencingForeignConstaintsFromLocalTables);
		Oid referencingRelation = GetReferencingTableId(foreignKeyId);
		char *referencedRelationName = get_rel_name(relationId);
		char *referencingRelationName = get_rel_name(referencingRelation);

		ereport(ERROR, (errmsg("cannot truncate a table referenced in a "
							   "foreign key constraint by a local table"),
						errdetail("Table \"%s\" references \"%s\"",
								  referencingRelationName,
								  referencedRelationName)));
	}
}


/*
 * PreprocessTruncateStatement handles few things that should be
 * done before standard process utility is called for truncate
 * command.
 */
void
PreprocessTruncateStatement(TruncateStmt *truncateStatement)
{
	ErrorIfUnsupportedTruncateStmt(truncateStatement);
	EnsurePartitionTableNotReplicatedForTruncate(truncateStatement);
	ExecuteTruncateStmtSequentialIfNecessary(truncateStatement);
	LockTruncatedRelationMetadataInWorkers(truncateStatement);
}


/*
 * ErrorIfUnsupportedTruncateStmt errors out if the command attempts to
 * truncate a distributed foreign table.
 */
static void
ErrorIfUnsupportedTruncateStmt(TruncateStmt *truncateStatement)
{
	List *relationList = truncateStatement->relations;
	RangeVar *rangeVar = NULL;
	foreach_ptr(rangeVar, relationList)
	{
		Oid relationId = RangeVarGetRelid(rangeVar, NoLock, false);

		ErrorIfIllegallyChangingKnownShard(relationId);

		/*
		 * We allow truncating foreign tables that are added to metadata
		 * only on the coordinator, as user mappings are not propagated.
		 */
		if (IsForeignTable(relationId) &&
			IsCitusTableType(relationId, CITUS_LOCAL_TABLE) &&
			!IsCoordinator())
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("truncating foreign tables that are added to metadata "
								   "can only be executed on the coordinator")));
		}
	}
}


/*
 * EnsurePartitionTableNotReplicatedForTruncate a simple wrapper around
 * EnsurePartitionTableNotReplicated for TRUNCATE command.
 */
static void
EnsurePartitionTableNotReplicatedForTruncate(TruncateStmt *truncateStatement)
{
	RangeVar *rangeVar = NULL;
	foreach_ptr(rangeVar, truncateStatement->relations)
	{
		Oid relationId = RangeVarGetRelid(rangeVar, NoLock, false);

		if (!IsCitusTable(relationId))
		{
			continue;
		}

		EnsurePartitionTableNotReplicated(relationId);
	}
}


/*
 * ExecuteTruncateStmtSequentialIfNecessary decides if the TRUNCATE stmt needs
 * to run sequential. If so, it calls SetLocalMultiShardModifyModeToSequential().
 *
 * If a reference table which has a foreign key from a distributed table is truncated
 * we need to execute the command sequentially to avoid self-deadlock.
 */
static void
ExecuteTruncateStmtSequentialIfNecessary(TruncateStmt *command)
{
	List *relationList = command->relations;
	bool failOK = false;

	RangeVar *rangeVar = NULL;
	foreach_ptr(rangeVar, relationList)
	{
		Oid relationId = RangeVarGetRelid(rangeVar, NoLock, failOK);

		if (IsCitusTableType(relationId, CITUS_TABLE_WITH_NO_DIST_KEY) &&
			TableReferenced(relationId))
		{
			char *relationName = get_rel_name(relationId);

			ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
							 errdetail(
								 "Table \"%s\" is modified, which might lead "
								 "to data inconsistencies or distributed deadlocks via "
								 "parallel accesses to hash distributed tables due to "
								 "foreign keys. Any parallel modification to "
								 "those hash distributed tables in the same "
								 "transaction can only be executed in sequential query "
								 "execution mode", relationName)));

			SetLocalMultiShardModifyModeToSequential();

			/* nothing to do more */
			return;
		}
	}
}


/*
 * LockTruncatedRelationMetadataInWorkers determines if distributed
 * lock is necessary for truncated relations, and acquire locks.
 *
 * LockTruncatedRelationMetadataInWorkers handles distributed locking
 * of truncated tables before standard utility takes over.
 *
 * Actual distributed truncation occurs inside truncate trigger.
 *
 * This is only for distributed serialization of truncate commands.
 * The function assumes that there is no foreign key relation between
 * non-distributed and distributed relations.
 */
static void
LockTruncatedRelationMetadataInWorkers(TruncateStmt *truncateStatement)
{
	/* nothing to do if there is no metadata at worker nodes */
	if (!ClusterHasKnownMetadataWorkers())
	{
		return;
	}

	List *distributedRelationList = NIL;
	List *referencingRelationIds = NIL;

	RangeVar *rangeVar = NULL;
	foreach_ptr(rangeVar, truncateStatement->relations)
	{
		Oid relationId = RangeVarGetRelid(rangeVar, NoLock, false);

		if (!IsCitusTable(relationId))
		{
			continue;
		}

		distributedRelationList = list_append_unique_oid(distributedRelationList,
														 relationId);

		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
		Assert(cacheEntry != NULL);

		List *referencingTableList = cacheEntry->referencingRelationsViaForeignKey;

		Oid referencingRelationId = InvalidOid;
		foreach_oid(referencingRelationId, referencingTableList)
		{
			referencingRelationIds = lappend_oid(referencingRelationIds,
												 referencingRelationId);
		}
	}

	distributedRelationList = list_concat_unique_oid(distributedRelationList,
													 referencingRelationIds);

	if (distributedRelationList != NIL)
	{
		AcquireDistributedLockOnRelations(distributedRelationList, AccessExclusiveLock);
	}
}
