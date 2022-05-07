/*
 * multi_partitioning_utils.c
 *	  Utility functions for declarative partitioning
 *
 * Copyright (c) Citus Data, Inc.
 */
#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/partition.h"
#include "catalog/pg_class.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "commands/tablecmds.h"
#include "common/string.h"
#include "distributed/citus_nodes.h"
#include "distributed/adaptive_executor.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/shardinterval_utils.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "partitioning/partdesc.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

static char * PartitionBound(Oid partitionId);
static Relation try_relation_open_nolock(Oid relationId);
static List * CreateFixPartitionConstraintsTaskList(Oid relationId);
static List * WorkerFixPartitionConstraintCommandList(Oid relationId, uint64 shardId,
													  List *checkConstraintList);
static List * CreateFixPartitionShardIndexNamesTaskList(Oid parentRelationId,
														Oid partitionRelationId,
														Oid parentIndexOid);
static List * WorkerFixPartitionShardIndexNamesCommandList(uint64 parentShardId,
														   List *indexIdList,
														   Oid partitionRelationId);
static List * WorkerFixPartitionShardIndexNamesCommandListForParentShardIndex(
	char *qualifiedParentShardIndexName, Oid parentIndexId, Oid partitionRelationId);
static List * WorkerFixPartitionShardIndexNamesCommandListForPartitionIndex(Oid
																			partitionIndexId,
																			char *
																			qualifiedParentShardIndexName,
																			Oid
																			partitionId);
static List * CheckConstraintNameListForRelation(Oid relationId);
static bool RelationHasConstraint(Oid relationId, char *constraintName);
static char * RenameConstraintCommand(Oid relationId, char *constraintName,
									  char *newConstraintName);


PG_FUNCTION_INFO_V1(fix_pre_citus10_partitioned_table_constraint_names);
PG_FUNCTION_INFO_V1(worker_fix_pre_citus10_partitioned_table_constraint_names);
PG_FUNCTION_INFO_V1(fix_partition_shard_index_names);
PG_FUNCTION_INFO_V1(worker_fix_partition_shard_index_names);


/*
 * fix_pre_citus10_partitioned_table_constraint_names fixes the constraint names of
 * partitioned table shards on workers.
 *
 * Constraint names for partitioned table shards should have shardId suffixes if and only
 * if they are unique or foreign key constraints. We mistakenly appended shardIds to
 * constraint names on ALTER TABLE dist_part_table ADD CONSTRAINT .. queries prior to
 * Citus 10. fix_pre_citus10_partitioned_table_constraint_names determines if this is the
 * case, and renames constraints back to their original names on shards.
 */
Datum
fix_pre_citus10_partitioned_table_constraint_names(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	EnsureCoordinator();

	if (!PartitionedTable(relationId))
	{
		ereport(ERROR, (errmsg("could not fix partition constraints: "
							   "relation does not exist or is not partitioned")));
	}
	if (!IsCitusTable(relationId))
	{
		ereport(ERROR, (errmsg("fix_pre_citus10_partitioned_table_constraint_names can "
							   "only be called for distributed partitioned tables")));
	}

	List *taskList = CreateFixPartitionConstraintsTaskList(relationId);

	/* do not do anything if there are no constraints that should be fixed */
	if (taskList != NIL)
	{
		bool localExecutionSupported = true;
		ExecuteUtilityTaskList(taskList, localExecutionSupported);
	}

	PG_RETURN_VOID();
}


/*
 * worker_fix_pre_citus10_partitioned_table_constraint_names fixes the constraint names on a worker given a shell
 * table name and shard id.
 */
Datum
worker_fix_pre_citus10_partitioned_table_constraint_names(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	int64 shardId = PG_GETARG_INT32(1);
	text *constraintNameText = PG_GETARG_TEXT_P(2);

	if (!PartitionedTable(relationId))
	{
		ereport(ERROR, (errmsg("could not fix partition constraints: "
							   "relation does not exist or is not partitioned")));
	}

	char *constraintName = text_to_cstring(constraintNameText);
	char *shardIdAppendedConstraintName = pstrdup(constraintName);
	AppendShardIdToName(&shardIdAppendedConstraintName, shardId);

	/* if shardId was appended to the constraint name, rename back to original */
	if (RelationHasConstraint(relationId, shardIdAppendedConstraintName))
	{
		char *renameConstraintDDLCommand =
			RenameConstraintCommand(relationId, shardIdAppendedConstraintName,
									constraintName);
		ExecuteAndLogUtilityCommand(renameConstraintDDLCommand);
	}
	PG_RETURN_VOID();
}


/*
 * fix_partition_shard_index_names fixes the index names of shards of partitions of
 * partitioned tables on workers. If the input is a partition rather than a partitioned
 * table, we only fix the index names of shards of that particular partition.
 *
 * When running CREATE INDEX on parent_table, we didn't explicitly create the index on
 * each partition as well. Postgres created indexes for partitions in the coordinator,
 * and also in the workers. Actually, Postgres auto-generates index names when auto-creating
 * indexes on each partition shard of the parent shards. If index name is too long, it
 * truncates the name and adds _idx postfix to it. However, when truncating the name, the
 * shardId of the partition shard can be lost. This may result in the same index name used for
 * the partition shell table and one of the partition shards.
 * For more details, check issue #4962 https://github.com/citusdata/citus/issues/4962
 *
 * fix_partition_shard_index_names renames indexes of shards of partition tables to include
 * the shardId at the end of the name, regardless of whether index name was long or short
 * As a result there will be no index name ending in _idx, rather all will end in _{shardid}
 *
 * Algorithm is:
 * foreach parentShard in shardListOfParentTableId:
 *  foreach parentIndex on parent:
 *      generate qualifiedParentShardIndexName -> parentShardIndex
 *      foreach inheritedPartitionIndex on parentIndex:
 *          get table relation of inheritedPartitionIndex -> partitionId
 *          foreach partitionShard in shardListOfPartitionid:
 *              generate qualifiedPartitionShardName -> partitionShard
 *              generate newPartitionShardIndexName
 *              (the following happens in the worker node)
 *              foreach inheritedPartitionShardIndex on parentShardIndex:
 *                  if table relation of inheritedPartitionShardIndex is partitionShard:
 *                      if inheritedPartitionShardIndex does not have proper name:
 *                          Rename(inheritedPartitionShardIndex, newPartitionShardIndexName)
 *                      break
 */
Datum
fix_partition_shard_index_names(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);
	EnsureCoordinator();

	Oid relationId = PG_GETARG_OID(0);
	Oid parentIndexOid = InvalidOid; /* fix all the indexes */

	if (!IsCitusTable(relationId))
	{
		ereport(ERROR, (errmsg("fix_partition_shard_index_names can only be called "
							   "for Citus tables")));
	}

	EnsureTableOwner(relationId);

	FixPartitionShardIndexNames(relationId, parentIndexOid);

	PG_RETURN_VOID();
}


/*
 * worker_fix_partition_shard_index_names fixes the index name of the index on given
 * partition shard that has parent the given parent index.
 * The parent index should be the index of a shard of a distributed partitioned table.
 */
Datum
worker_fix_partition_shard_index_names(PG_FUNCTION_ARGS)
{
	Oid parentShardIndexId = PG_GETARG_OID(0);

	text *partitionShardName = PG_GETARG_TEXT_P(1);

	/* resolve partitionShardId from passed in schema and partition shard name */
	List *partitionShardNameList = textToQualifiedNameList(partitionShardName);
	RangeVar *partitionShard = makeRangeVarFromNameList(partitionShardNameList);

	/* lock the relation with the lock mode */
	bool missing_ok = true;
	Oid partitionShardId = RangeVarGetRelid(partitionShard, NoLock, missing_ok);

	if (!OidIsValid(partitionShardId))
	{
		PG_RETURN_VOID();
	}

	CheckCitusVersion(ERROR);
	EnsureTableOwner(partitionShardId);

	text *newPartitionShardIndexNameText = PG_GETARG_TEXT_P(2);
	char *newPartitionShardIndexName = text_to_cstring(
		newPartitionShardIndexNameText);

	if (!has_subclass(parentShardIndexId))
	{
		ereport(ERROR, (errmsg("could not fix child index names: "
							   "index is not partitioned")));
	}

	List *partitionShardIndexIds = find_inheritance_children(parentShardIndexId,
															 ShareRowExclusiveLock);
	Oid partitionShardIndexId = InvalidOid;
	foreach_oid(partitionShardIndexId, partitionShardIndexIds)
	{
		if (IndexGetRelation(partitionShardIndexId, false) == partitionShardId)
		{
			char *partitionShardIndexName = get_rel_name(partitionShardIndexId);
			if (ExtractShardIdFromTableName(partitionShardIndexName, missing_ok) ==
				INVALID_SHARD_ID)
			{
				/*
				 * ExtractShardIdFromTableName will return INVALID_SHARD_ID if
				 * partitionShardIndexName doesn't end in _shardid. In that case,
				 * we want to rename this partition shard index to newPartitionShardIndexName,
				 * which ends in _shardid, hence we maintain naming consistency:
				 * we can reach this partition shard index by conventional Citus naming
				 */
				RenameStmt *stmt = makeNode(RenameStmt);

				stmt->renameType = OBJECT_INDEX;
				stmt->missing_ok = false;
				char *idxNamespace = get_namespace_name(get_rel_namespace(
															partitionShardIndexId));
				stmt->relation = makeRangeVar(idxNamespace, partitionShardIndexName, -1);
				stmt->newname = newPartitionShardIndexName;

				RenameRelation(stmt);
			}
			break;
		}
	}

	PG_RETURN_VOID();
}


/*
 * FixPartitionShardIndexNames gets a relationId. The input relationId should be
 * either a parent or partition table. If it is a parent table, then all the
 * index names on all the partitions are fixed. If it is a partition, only the
 * specific partition is fixed.
 *
 * The second parentIndexOid parameter is optional. If provided a valid Oid, only
 * that specific index name is fixed.
 */
void
FixPartitionShardIndexNames(Oid relationId, Oid parentIndexOid)
{
	Relation relation = try_relation_open(relationId, AccessShareLock);

	if (relation == NULL)
	{
		ereport(NOTICE, (errmsg("relation with OID %u does not exist, skipping",
								relationId)));
		return;
	}

	/* at this point, we should only be dealing with Citus tables */
	Assert(IsCitusTable(relationId));

	Oid parentRelationId = InvalidOid;
	Oid partitionRelationId = InvalidOid;

	if (relation->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		parentRelationId = relationId;
	}
	else if (PartitionTable(relationId))
	{
		parentRelationId = PartitionParentOid(relationId);
		partitionRelationId = relationId;
	}
	else
	{
		relation_close(relation, NoLock);
		ereport(ERROR, (errmsg("Fixing shard index names is only applicable to "
							   "partitioned tables or partitions, "
							   "and \"%s\" is neither",
							   RelationGetRelationName(relation))));
	}

	List *taskList =
		CreateFixPartitionShardIndexNamesTaskList(parentRelationId,
												  partitionRelationId,
												  parentIndexOid);

	/* do not do anything if there are no index names to fix */
	if (taskList != NIL)
	{
		bool localExecutionSupported = true;
		ExecuteUtilityTaskList(taskList, localExecutionSupported);
	}

	relation_close(relation, NoLock);
}


/*
 * CreateFixPartitionConstraintsTaskList goes over all the partitions of a distributed
 * partitioned table, and creates the list of tasks to execute
 * worker_fix_pre_citus10_partitioned_table_constraint_names UDF on worker nodes.
 */
static List *
CreateFixPartitionConstraintsTaskList(Oid relationId)
{
	List *taskList = NIL;

	/* enumerate the tasks when putting them to the taskList */
	int taskId = 1;
	List *checkConstraintList = CheckConstraintNameListForRelation(relationId);

	/* early exit if the relation does not have any check constraints */
	if (checkConstraintList == NIL)
	{
		return NIL;
	}

	List *shardIntervalList = LoadShardIntervalList(relationId);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;

		List *queryStringList = WorkerFixPartitionConstraintCommandList(relationId,
																		shardId,
																		checkConstraintList);

		Task *task = CitusMakeNode(Task);
		task->jobId = INVALID_JOB_ID;
		task->taskId = taskId++;

		task->taskType = DDL_TASK;
		SetTaskQueryStringList(task, queryStringList);
		task->dependentTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * CheckConstraintNameListForRelation returns a list of names of CHECK constraints
 * for a relation.
 */
static List *
CheckConstraintNameListForRelation(Oid relationId)
{
	List *constraintNameList = NIL;

	int scanKeyCount = 2;
	ScanKeyData scanKey[2];

	Relation pgConstraint = table_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ, relationId);
	ScanKeyInit(&scanKey[1], Anum_pg_constraint_contype,
				BTEqualStrategyNumber, F_CHAREQ, CONSTRAINT_CHECK);

	bool useIndex = false;
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint, InvalidOid, useIndex,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(heapTuple);
		char *constraintName = NameStr(constraintForm->conname);
		constraintNameList = lappend(constraintNameList, pstrdup(constraintName));

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgConstraint, NoLock);

	return constraintNameList;
}


/*
 * WorkerFixPartitionConstraintCommandList creates a list of queries that will fix
 * all check constraint names of a shard.
 */
static List *
WorkerFixPartitionConstraintCommandList(Oid relationId, uint64 shardId,
										List *checkConstraintList)
{
	List *commandList = NIL;
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *relationName = get_rel_name(relationId);
	char *shardRelationName = pstrdup(relationName);

	/* build shard relation name */
	AppendShardIdToName(&shardRelationName, shardId);

	char *quotedShardName = quote_qualified_identifier(schemaName, shardRelationName);

	char *constraintName = NULL;
	foreach_ptr(constraintName, checkConstraintList)
	{
		StringInfo shardQueryString = makeStringInfo();
		appendStringInfo(shardQueryString,
						 "SELECT worker_fix_pre_citus10_partitioned_table_constraint_names(%s::regclass, "
						 UINT64_FORMAT ", %s::text)",
						 quote_literal_cstr(quotedShardName), shardId,
						 quote_literal_cstr(constraintName));
		commandList = lappend(commandList, shardQueryString->data);
	}

	return commandList;
}


/*
 * CreateFixPartitionShardIndexNamesTaskList goes over all the
 * indexes of a distributed partitioned table unless parentIndexOid
 * is valid. If it is valid, only the given index is processed.
 *
 * The function creates the list of tasks to execute
 * worker_fix_partition_shard_index_names() on worker nodes.
 *
 * When the partitionRelationId is a valid Oid, the function only operates on the
 * given partition. Otherwise, the function create tasks for all the partitions.
 *
 * So, for example, if a new partition is created, we only need to fix only for the
 * new partition, hence partitionRelationId should be a valid Oid. However, if a new
 * index/constraint is created on the parent, we should fix all the partitions, hence
 * partitionRelationId should be InvalidOid.
 *
 * As a reflection of the above, we always create parent_table_shard_count tasks.
 * When we need to fix all the partitions, each task with parent_indexes_count
 * times partition_count query strings. When we need to fix a single
 * partition each task will have parent_indexes_count query strings. When we need
 * to fix a single index, parent_indexes_count becomes 1.
 */
static List *
CreateFixPartitionShardIndexNamesTaskList(Oid parentRelationId, Oid partitionRelationId,
										  Oid parentIndexOid)
{
	List *partitionList = PartitionList(parentRelationId);
	if (partitionList == NIL)
	{
		/* early exit if the parent relation does not have any partitions */
		return NIL;
	}

	Relation parentRelation = RelationIdGetRelation(parentRelationId);
	List *parentIndexIdList = NIL;

	if (parentIndexOid != InvalidOid)
	{
		parentIndexIdList = list_make1_oid(parentIndexOid);
	}
	else
	{
		parentIndexIdList = RelationGetIndexList(parentRelation);
	}

	if (parentIndexIdList == NIL)
	{
		/* early exit if the parent relation does not have any indexes */
		RelationClose(parentRelation);
		return NIL;
	}

	/*
	 * Lock shard metadata, if a specific partition is provided, lock that. Otherwise,
	 * lock all partitions.
	 */
	if (OidIsValid(partitionRelationId))
	{
		/* if a partition was provided we only need to lock that partition's metadata */
		List *partitionShardIntervalList = LoadShardIntervalList(partitionRelationId);
		LockShardListMetadata(partitionShardIntervalList, ShareLock);
	}
	else
	{
		Oid partitionId = InvalidOid;
		foreach_oid(partitionId, partitionList)
		{
			List *partitionShardIntervalList = LoadShardIntervalList(partitionId);
			LockShardListMetadata(partitionShardIntervalList, ShareLock);
		}
	}

	List *parentShardIntervalList = LoadShardIntervalList(parentRelationId);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(parentShardIntervalList, ShareLock);

	int taskId = 1;
	List *taskList = NIL;

	ShardInterval *parentShardInterval = NULL;
	foreach_ptr(parentShardInterval, parentShardIntervalList)
	{
		uint64 parentShardId = parentShardInterval->shardId;

		List *queryStringList =
			WorkerFixPartitionShardIndexNamesCommandList(parentShardId,
														 parentIndexIdList,
														 partitionRelationId);

		if (queryStringList != NIL)
		{
			Task *task = CitusMakeNode(Task);
			task->jobId = INVALID_JOB_ID;
			task->taskId = taskId++;

			task->taskType = DDL_TASK;

			/*
			 * There could be O(#partitions * #indexes) queries in
			 * the queryStringList.
			 *
			 * In order to avoid round-trips per query in queryStringList,
			 * we join the string and send as a single command via the UDF.
			 * Otherwise, the executor sends each command with one
			 * round-trip.
			 */
			char *string = StringJoin(queryStringList, ';');
			StringInfo commandToRun = makeStringInfo();

			appendStringInfo(commandToRun,
							 "SELECT pg_catalog.citus_run_local_command($$%s$$)", string);
			SetTaskQueryString(task, commandToRun->data);

			task->dependentTaskList = NULL;
			task->replicationModel = REPLICATION_MODEL_INVALID;
			task->anchorShardId = parentShardId;
			task->taskPlacementList = ActiveShardPlacementList(parentShardId);

			taskList = lappend(taskList, task);
		}
	}

	RelationClose(parentRelation);

	return taskList;
}


/*
 * WorkerFixPartitionShardIndexNamesCommandList creates a list of queries that will fix
 * all child index names of parent indexes on given shard of parent partitioned table.
 */
static List *
WorkerFixPartitionShardIndexNamesCommandList(uint64 parentShardId,
											 List *parentIndexIdList,
											 Oid partitionRelationId)
{
	List *commandList = NIL;
	Oid parentIndexId = InvalidOid;
	foreach_oid(parentIndexId, parentIndexIdList)
	{
		if (!has_subclass(parentIndexId))
		{
			continue;
		}

		/*
		 * Get the qualified name of the corresponding index of given parent index
		 * in the parent shard with given parentShardId
		 */
		char *parentIndexName = get_rel_name(parentIndexId);
		char *parentShardIndexName = pstrdup(parentIndexName);
		AppendShardIdToName(&parentShardIndexName, parentShardId);
		Oid schemaId = get_rel_namespace(parentIndexId);
		char *schemaName = get_namespace_name(schemaId);
		char *qualifiedParentShardIndexName = quote_qualified_identifier(schemaName,
																		 parentShardIndexName);
		List *commands = WorkerFixPartitionShardIndexNamesCommandListForParentShardIndex(
			qualifiedParentShardIndexName, parentIndexId, partitionRelationId);
		commandList = list_concat(commandList, commands);
	}

	return commandList;
}


/*
 * WorkerFixPartitionShardIndexNamesCommandListForParentShardIndex creates a list
 * of queries that will fix the child index names of given index on shard
 * of parent partitioned table.
 *
 * In case a partition was provided as argument (partitionRelationId isn't InvalidOid)
 * the list of queries will include only the child indexes whose relation is the
 * given partition. Otherwise, all the partitions are included.
 */
static List *
WorkerFixPartitionShardIndexNamesCommandListForParentShardIndex(
	char *qualifiedParentShardIndexName, Oid parentIndexId, Oid partitionRelationId)
{
	List *commandList = NIL;

	/*
	 * Get the list of all partition indexes that are children of current
	 * index on parent
	 */
	List *partitionIndexIds =
		find_inheritance_children(parentIndexId, ShareRowExclusiveLock);

	bool addAllPartitions = (partitionRelationId == InvalidOid);
	Oid partitionIndexId = InvalidOid;
	foreach_oid(partitionIndexId, partitionIndexIds)
	{
		Oid partitionId = IndexGetRelation(partitionIndexId, false);
		if (addAllPartitions || partitionId == partitionRelationId)
		{
			List *commands =
				WorkerFixPartitionShardIndexNamesCommandListForPartitionIndex(
					partitionIndexId, qualifiedParentShardIndexName, partitionId);
			commandList = list_concat(commandList, commands);
		}
	}
	return commandList;
}


/*
 * WorkerFixPartitionShardIndexNamesCommandListForPartitionIndex creates a list of queries that will fix
 * all child index names of given index on shard of parent partitioned table, whose table relation is a shard
 * of the partition that is the table relation of given partitionIndexId, which is partitionId
 */
static List *
WorkerFixPartitionShardIndexNamesCommandListForPartitionIndex(Oid partitionIndexId,
															  char *
															  qualifiedParentShardIndexName,
															  Oid partitionId)
{
	List *commandList = NIL;

	/* get info for this partition relation of this index*/
	char *partitionIndexName = get_rel_name(partitionIndexId);
	char *partitionName = get_rel_name(partitionId);
	char *partitionSchemaName = get_namespace_name(get_rel_namespace(partitionId));
	List *partitionShardIntervalList = LoadShardIntervalList(partitionId);

	ShardInterval *partitionShardInterval = NULL;
	foreach_ptr(partitionShardInterval, partitionShardIntervalList)
	{
		/*
		 * Prepare commands for each shard of current partition
		 * to fix the index name that corresponds to the
		 * current parent index name
		 */
		uint64 partitionShardId = partitionShardInterval->shardId;

		/* get qualified partition shard name */
		char *partitionShardName = pstrdup(partitionName);
		AppendShardIdToName(&partitionShardName, partitionShardId);
		char *qualifiedPartitionShardName = quote_qualified_identifier(
			partitionSchemaName,
			partitionShardName);

		/* generate the new correct index name */
		char *newPartitionShardIndexName = pstrdup(partitionIndexName);
		AppendShardIdToName(&newPartitionShardIndexName, partitionShardId);

		/* create worker_fix_partition_shard_index_names command */
		StringInfo shardQueryString = makeStringInfo();
		appendStringInfo(shardQueryString,
						 "SELECT worker_fix_partition_shard_index_names(%s::regclass, %s, %s)",
						 quote_literal_cstr(qualifiedParentShardIndexName),
						 quote_literal_cstr(qualifiedPartitionShardName),
						 quote_literal_cstr(newPartitionShardIndexName));
		commandList = lappend(commandList, shardQueryString->data);
	}

	return commandList;
}


/*
 * RelationHasConstraint checks if a relation has a constraint with a given name.
 */
static bool
RelationHasConstraint(Oid relationId, char *constraintName)
{
	bool found = false;

	int scanKeyCount = 2;
	ScanKeyData scanKey[2];

	Relation pgConstraint = table_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_conrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relationId));
	ScanKeyInit(&scanKey[1], Anum_pg_constraint_conname,
				BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(constraintName));

	bool useIndex = false;
	SysScanDesc scanDescriptor = systable_beginscan(pgConstraint, InvalidOid, useIndex,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (HeapTupleIsValid(heapTuple))
	{
		found = true;
	}

	systable_endscan(scanDescriptor);
	table_close(pgConstraint, NoLock);

	return found;
}


/*
 * RenameConstraintCommand creates the query string that will rename a constraint
 */
static char *
RenameConstraintCommand(Oid relationId, char *constraintName, char *newConstraintName)
{
	char *qualifiedRelationName = generate_qualified_relation_name(relationId);
	const char *quotedConstraintName = quote_identifier(constraintName);
	const char *quotedNewConstraintName = quote_identifier(newConstraintName);

	StringInfo renameCommand = makeStringInfo();
	appendStringInfo(renameCommand, "ALTER TABLE %s RENAME CONSTRAINT %s TO %s",
					 qualifiedRelationName, quotedConstraintName,
					 quotedNewConstraintName);

	return renameCommand->data;
}


/*
 * Returns true if the given relation is a partitioned table.
 */
bool
PartitionedTable(Oid relationId)
{
	Relation rel = try_relation_open(relationId, AccessShareLock);

	/* don't error out for tables that are dropped */
	if (rel == NULL)
	{
		return false;
	}

	bool partitionedTable = false;

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		partitionedTable = true;
	}

	/* keep the lock */
	table_close(rel, NoLock);

	return partitionedTable;
}


/*
 * Returns true if the given relation is a partitioned table. The function
 * doesn't acquire any locks on the input relation, thus the caller is
 * reponsible for holding the appropriate locks.
 */
bool
PartitionedTableNoLock(Oid relationId)
{
	Relation rel = try_relation_open_nolock(relationId);
	bool partitionedTable = false;

	/* don't error out for tables that are dropped */
	if (rel == NULL)
	{
		return false;
	}

	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		partitionedTable = true;
	}

	/* keep the lock */
	table_close(rel, NoLock);

	return partitionedTable;
}


/*
 * Returns true if the given relation is a partition.
 */
bool
PartitionTable(Oid relationId)
{
	Relation rel = try_relation_open(relationId, AccessShareLock);

	/* don't error out for tables that are dropped */
	if (rel == NULL)
	{
		return false;
	}

	bool partitionTable = rel->rd_rel->relispartition;

	/* keep the lock */
	table_close(rel, NoLock);

	return partitionTable;
}


/*
 * Returns true if the given relation is a partition.  The function
 * doesn't acquire any locks on the input relation, thus the caller is
 * reponsible for holding the appropriate locks.
 */
bool
PartitionTableNoLock(Oid relationId)
{
	Relation rel = try_relation_open_nolock(relationId);

	/* don't error out for tables that are dropped */
	if (rel == NULL)
	{
		return false;
	}

	bool partitionTable = rel->rd_rel->relispartition;

	/* keep the lock */
	table_close(rel, NoLock);

	return partitionTable;
}


/*
 * try_relation_open_nolock opens a relation with given relationId without
 * acquiring locks. PostgreSQL's try_relation_open() asserts that caller
 * has already acquired a lock on the relation, which we don't always do.
 *
 * ATTENTION:
 *   1. Sync this with try_relation_open(). It hasn't changed for 10 to 12
 *      releases though.
 *   2. We should remove this after we fix the locking/distributed deadlock
 *      issues with MX Truncate. See https://github.com/citusdata/citus/pull/2894
 *      for more discussion.
 */
static Relation
try_relation_open_nolock(Oid relationId)
{
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId)))
	{
		return NULL;
	}

	Relation relation = RelationIdGetRelation(relationId);
	if (!RelationIsValid(relation))
	{
		return NULL;
	}

	pgstat_init_relation(relation);

	return relation;
}


/*
 * IsChildTable returns true if the table is inherited. Note that
 * partition tables inherites by default. However, this function
 * returns false if the given table is a partition.
 */
bool
IsChildTable(Oid relationId)
{
	ScanKeyData key[1];
	HeapTuple inheritsTuple = NULL;
	bool tableInherits = false;

	Relation pgInherits = table_open(InheritsRelationId, AccessShareLock);

	ScanKeyInit(&key[0], Anum_pg_inherits_inhrelid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	SysScanDesc scan = systable_beginscan(pgInherits, InvalidOid, false,
										  NULL, 1, key);

	while ((inheritsTuple = systable_getnext(scan)) != NULL)
	{
		Oid inheritedRelationId =
			((Form_pg_inherits) GETSTRUCT(inheritsTuple))->inhrelid;

		if (relationId == inheritedRelationId)
		{
			tableInherits = true;
			break;
		}
	}

	systable_endscan(scan);
	table_close(pgInherits, AccessShareLock);

	if (tableInherits && PartitionTable(relationId))
	{
		tableInherits = false;
	}

	return tableInherits;
}


/*
 * IsParentTable returns true if the table is inherited. Note that
 * partitioned tables inherited by default. However, this function
 * returns false if the given table is a partitioned table.
 */
bool
IsParentTable(Oid relationId)
{
	ScanKeyData key[1];
	bool tableInherited = false;

	Relation pgInherits = table_open(InheritsRelationId, AccessShareLock);

	ScanKeyInit(&key[0], Anum_pg_inherits_inhparent,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(relationId));

	SysScanDesc scan = systable_beginscan(pgInherits, InheritsParentIndexId, true,
										  NULL, 1, key);

	if (systable_getnext(scan) != NULL)
	{
		tableInherited = true;
	}
	systable_endscan(scan);
	table_close(pgInherits, AccessShareLock);

	if (tableInherited && PartitionedTable(relationId))
	{
		tableInherited = false;
	}

	return tableInherited;
}


/*
 * Wrapper around get_partition_parent
 *
 * Note: Because this function assumes that the relation whose OID is passed
 * as an argument will have precisely one parent, it should only be called
 * when it is known that the relation is a partition.
 */
Oid
PartitionParentOid(Oid partitionOid)
{
	Oid partitionParentOid = get_partition_parent_compat(partitionOid, false);

	return partitionParentOid;
}


/*
 * PartitionWithLongestNameRelationId is a utility function that returns the
 * oid of the partition table that has the longest name in terms of number of
 * characters.
 */
Oid
PartitionWithLongestNameRelationId(Oid parentRelationId)
{
	Oid longestNamePartitionId = InvalidOid;
	int longestNameLength = 0;
	List *partitionList = PartitionList(parentRelationId);

	Oid partitionRelationId = InvalidOid;
	foreach_oid(partitionRelationId, partitionList)
	{
		char *partitionName = get_rel_name(partitionRelationId);
		int partitionNameLength = strnlen(partitionName, NAMEDATALEN);
		if (partitionNameLength > longestNameLength)
		{
			longestNamePartitionId = partitionRelationId;
			longestNameLength = partitionNameLength;
		}
	}

	return longestNamePartitionId;
}


/*
 * Takes a parent relation and returns Oid list of its partitions. The
 * function errors out if the given relation is not a parent.
 */
List *
PartitionList(Oid parentRelationId)
{
	Relation rel = table_open(parentRelationId, AccessShareLock);
	List *partitionList = NIL;


	if (!PartitionedTable(parentRelationId))
	{
		char *relationName = get_rel_name(parentRelationId);

		ereport(ERROR, (errmsg("\"%s\" is not a parent table", relationName)));
	}
	PartitionDesc partDesc = RelationGetPartitionDesc_compat(rel, true);
	Assert(partDesc != NULL);

	int partitionCount = partDesc->nparts;
	for (int partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex)
	{
		partitionList =
			lappend_oid(partitionList, partDesc->oids[partitionIndex]);
	}

	/* keep the lock */
	table_close(rel, NoLock);

	return partitionList;
}


/*
 * GenerateDetachPartitionCommand gets a partition table and returns
 * "ALTER TABLE parent_table DETACH PARTITION partitionName" command.
 */
char *
GenerateDetachPartitionCommand(Oid partitionTableId)
{
	StringInfo detachPartitionCommand = makeStringInfo();

	if (!PartitionTable(partitionTableId))
	{
		char *relationName = get_rel_name(partitionTableId);

		ereport(ERROR, (errmsg("\"%s\" is not a partition", relationName)));
	}

	Oid parentId = get_partition_parent_compat(partitionTableId, false);
	char *tableQualifiedName = generate_qualified_relation_name(partitionTableId);
	char *parentTableQualifiedName = generate_qualified_relation_name(parentId);

	appendStringInfo(detachPartitionCommand,
					 "ALTER TABLE IF EXISTS %s DETACH PARTITION %s;",
					 parentTableQualifiedName, tableQualifiedName);

	return detachPartitionCommand->data;
}


/*
 * GenerateDetachPartitionCommandRelationIdList returns the necessary command list to
 * detach the given partitions from their parents.
 */
List *
GenerateDetachPartitionCommandRelationIdList(List *relationIds)
{
	List *detachPartitionCommands = NIL;
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIds)
	{
		Assert(PartitionTable(relationId));
		char *detachCommand = GenerateDetachPartitionCommand(relationId);
		detachPartitionCommands = lappend(detachPartitionCommands, detachCommand);
	}

	return detachPartitionCommands;
}


/*
 * GenereatePartitioningInformation returns the partitioning type and partition column
 * for the given parent table in the form of "PARTITION TYPE (partitioning column(s)/expression(s))".
 */
char *
GeneratePartitioningInformation(Oid parentTableId)
{
	char *partitionBoundCString = "";

	if (!PartitionedTable(parentTableId))
	{
		char *relationName = get_rel_name(parentTableId);

		ereport(ERROR, (errmsg("\"%s\" is not a parent table", relationName)));
	}

	Datum partitionBoundDatum = DirectFunctionCall1(pg_get_partkeydef,
													ObjectIdGetDatum(parentTableId));

	partitionBoundCString = TextDatumGetCString(partitionBoundDatum);

	return partitionBoundCString;
}


/*
 * GenerateAttachShardPartitionCommand generates command to attach a child table
 * table to its parent in a partitioning hierarchy.
 */
char *
GenerateAttachShardPartitionCommand(ShardInterval *shardInterval)
{
	Oid schemaId = get_rel_namespace(shardInterval->relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);

	char *command = GenerateAlterTableAttachPartitionCommand(shardInterval->relationId);
	char *escapedCommand = quote_literal_cstr(command);
	int shardIndex = ShardIndex(shardInterval);


	StringInfo attachPartitionCommand = makeStringInfo();

	Oid parentRelationId = PartitionParentOid(shardInterval->relationId);
	if (parentRelationId == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("cannot attach partition"),
						errdetail("Referenced relation cannot be found.")));
	}

	Oid parentSchemaId = get_rel_namespace(parentRelationId);
	char *parentSchemaName = get_namespace_name(parentSchemaId);
	char *escapedParentSchemaName = quote_literal_cstr(parentSchemaName);
	uint64 parentShardId = ColocatedShardIdInRelation(parentRelationId, shardIndex);

	appendStringInfo(attachPartitionCommand,
					 WORKER_APPLY_INTER_SHARD_DDL_COMMAND, parentShardId,
					 escapedParentSchemaName, shardInterval->shardId,
					 escapedSchemaName, escapedCommand);

	return attachPartitionCommand->data;
}


/*
 * GenerateAlterTableAttachPartitionCommand returns the necessary command to
 * attach the given partition to its parent.
 */
char *
GenerateAlterTableAttachPartitionCommand(Oid partitionTableId)
{
	StringInfo createPartitionCommand = makeStringInfo();


	if (!PartitionTable(partitionTableId))
	{
		char *relationName = get_rel_name(partitionTableId);

		ereport(ERROR, (errmsg("\"%s\" is not a partition", relationName)));
	}

	Oid parentId = get_partition_parent_compat(partitionTableId, false);
	char *tableQualifiedName = generate_qualified_relation_name(partitionTableId);
	char *parentTableQualifiedName = generate_qualified_relation_name(parentId);

	char *partitionBoundCString = PartitionBound(partitionTableId);

	appendStringInfo(createPartitionCommand, "ALTER TABLE %s ATTACH PARTITION %s %s;",
					 parentTableQualifiedName, tableQualifiedName,
					 partitionBoundCString);

	return createPartitionCommand->data;
}


/*
 * GenerateAttachPartitionCommandRelationIdList returns the necessary command list to
 * attach the given partitions to their parents.
 */
List *
GenerateAttachPartitionCommandRelationIdList(List *relationIds)
{
	List *attachPartitionCommands = NIL;
	Oid relationId = InvalidOid;
	foreach_oid(relationId, relationIds)
	{
		char *attachCommand = GenerateAlterTableAttachPartitionCommand(relationId);
		attachPartitionCommands = lappend(attachPartitionCommands, attachCommand);
	}

	return attachPartitionCommands;
}


/*
 * This function heaviliy inspired from RelationBuildPartitionDesc()
 * which is avaliable in src/backend/catalog/partition.c.
 *
 * The function simply reads the pg_class and gets the partition bound.
 * Later, converts it to text format and returns.
 */
static char *
PartitionBound(Oid partitionId)
{
	bool isnull = false;

	HeapTuple tuple = SearchSysCache1(RELOID, partitionId);
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "cache lookup failed for relation %u", partitionId);
	}

	/*
	 * It is possible that the pg_class tuple of a partition has not been
	 * updated yet to set its relpartbound field.  The only case where
	 * this happens is when we open the parent relation to check using its
	 * partition descriptor that a new partition's bound does not overlap
	 * some existing partition.
	 */
	if (!((Form_pg_class) GETSTRUCT(tuple))->relispartition)
	{
		ReleaseSysCache(tuple);
		return "";
	}

	Datum datum = SysCacheGetAttr(RELOID, tuple,
								  Anum_pg_class_relpartbound,
								  &isnull);
	Assert(!isnull);

	Datum partitionBoundDatum =
		DirectFunctionCall2(pg_get_expr, datum, ObjectIdGetDatum(partitionId));

	char *partitionBoundString = TextDatumGetCString(partitionBoundDatum);

	ReleaseSysCache(tuple);

	return partitionBoundString;
}
