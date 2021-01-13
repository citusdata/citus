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
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"


#define LOCK_RELATION_IF_EXISTS "SELECT lock_relation_if_exists(%s, '%s');"


/* Local functions forward declarations for unsupported command checks */
static void ErrorIfUnsupportedTruncateStmt(TruncateStmt *truncateStatement);
static void ExecuteTruncateStmtSequentialIfNecessary(TruncateStmt *command);
static void EnsurePartitionTableNotReplicatedForTruncate(TruncateStmt *truncateStatement);
static void LockTruncatedRelationMetadataInWorkers(TruncateStmt *truncateStatement);
static void AcquireDistributedLockOnRelations(List *relationIdList, LOCKMODE lockMode);
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

		DirectFunctionCall3(citus_drop_all_shards,
							ObjectIdGetDatum(relationId),
							CStringGetTextDatum(relationName),
							CStringGetTextDatum(schemaName));
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
	Oid relationId = PG_GETARG_OID(0);

	CheckCitusVersion(ERROR);
	EnsureCoordinator();
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

	/* make sure there are no foreign key references from a local table */
	SetForeignConstraintRelationshipGraphInvalid();
	List *referencingRelationList = ReferencingRelationIdList(relationId);

	Oid referencingRelation = InvalidOid;
	foreach_oid(referencingRelation, referencingRelationList)
	{
		/* we do not truncate a table if there is a local table referencing it */
		if (!IsCitusTable(referencingRelation))
		{
			char *referencedRelationName = get_rel_name(relationId);
			char *referencingRelationName = get_rel_name(referencingRelation);

			ereport(ERROR, (errmsg("cannot truncate a table referenced in a "
								   "foreign key constraint by a local table"),
							errdetail("Table \"%s\" references \"%s\"",
									  referencingRelationName,
									  referencedRelationName)));
		}
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
		char relationKind = get_rel_relkind(relationId);
		if (IsCitusTable(relationId) &&
			relationKind == RELKIND_FOREIGN_TABLE)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("truncating distributed foreign tables is "
								   "currently unsupported"),
							errhint("Use citus_drop_all_shards to remove "
									"foreign table's shards.")));
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
	List *distributedRelationList = NIL;

	/* nothing to do if there is no metadata at worker nodes */
	if (!ClusterHasKnownMetadataWorkers())
	{
		return;
	}

	RangeVar *rangeVar = NULL;
	foreach_ptr(rangeVar, truncateStatement->relations)
	{
		Oid relationId = RangeVarGetRelid(rangeVar, NoLock, false);
		Oid referencingRelationId = InvalidOid;

		if (!IsCitusTable(relationId))
		{
			continue;
		}

		if (list_member_oid(distributedRelationList, relationId))
		{
			continue;
		}

		distributedRelationList = lappend_oid(distributedRelationList, relationId);

		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
		Assert(cacheEntry != NULL);

		List *referencingTableList = cacheEntry->referencingRelationsViaForeignKey;
		foreach_oid(referencingRelationId, referencingTableList)
		{
			distributedRelationList = list_append_unique_oid(distributedRelationList,
															 referencingRelationId);
		}
	}

	if (distributedRelationList != NIL)
	{
		AcquireDistributedLockOnRelations(distributedRelationList, AccessExclusiveLock);
	}
}


/*
 * AcquireDistributedLockOnRelations acquire a distributed lock on worker nodes
 * for given list of relations ids. Relation id list and worker node list
 * sorted so that the lock is acquired in the same order regardless of which
 * node it was run on. Notice that no lock is acquired on coordinator node.
 *
 * Notice that the locking functions is sent to all workers regardless of if
 * it has metadata or not. This is because a worker node only knows itself
 * and previous workers that has metadata sync turned on. The node does not
 * know about other nodes that have metadata sync turned on afterwards.
 */
static void
AcquireDistributedLockOnRelations(List *relationIdList, LOCKMODE lockMode)
{
	Oid relationId = InvalidOid;
	List *workerNodeList = ActivePrimaryNodeList(NoLock);
	const char *lockModeText = LockModeToLockModeText(lockMode);

	/*
	 * We want to acquire locks in the same order accross the nodes.
	 * Although relation ids may change, their ordering will not.
	 */
	relationIdList = SortList(relationIdList, CompareOids);
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	UseCoordinatedTransaction();

	int32 localGroupId = GetLocalGroupId();

	foreach_oid(relationId, relationIdList)
	{
		/*
		 * We only acquire distributed lock on relation if
		 * the relation is sync'ed between mx nodes.
		 */
		if (ShouldSyncTableMetadata(relationId))
		{
			char *qualifiedRelationName = generate_qualified_relation_name(relationId);
			StringInfo lockRelationCommand = makeStringInfo();

			appendStringInfo(lockRelationCommand, LOCK_RELATION_IF_EXISTS,
							 quote_literal_cstr(qualifiedRelationName),
							 lockModeText);

			WorkerNode *workerNode = NULL;
			foreach_ptr(workerNode, workerNodeList)
			{
				const char *nodeName = workerNode->workerName;
				int nodePort = workerNode->workerPort;

				/* if local node is one of the targets, acquire the lock locally */
				if (workerNode->groupId == localGroupId)
				{
					LockRelationOid(relationId, lockMode);
					continue;
				}

				SendCommandToWorker(nodeName, nodePort, lockRelationCommand->data);
			}
		}
	}
}
