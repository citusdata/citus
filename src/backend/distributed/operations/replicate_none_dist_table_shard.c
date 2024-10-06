/*-------------------------------------------------------------------------
 *
 * replicate_none_dist_table_shard.c
 *	  Routines to replicate shard of none-distributed table to
 *    a remote node.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "nodes/pg_list.h"

#include "distributed/adaptive_executor.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/replicate_none_dist_table_shard.h"
#include "distributed/shard_utils.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"


static void CreateForeignKeysFromReferenceTablesOnShards(Oid noneDistTableId);
static Oid ForeignConstraintGetReferencingTableId(const char *queryString);
static void EnsureNoneDistTableWithCoordinatorPlacement(Oid noneDistTableId);
static void SetLocalEnableManualChangesToShard(bool state);


/*
 * NoneDistTableReplicateCoordinatorPlacement replicates local (presumably
 * coordinator) shard placement of given none-distributed table to given
 * target nodes and inserts records for new placements into pg_dist_placement.
 */
void
NoneDistTableReplicateCoordinatorPlacement(Oid noneDistTableId,
										   List *targetNodeList)
{
	EnsureCoordinator();
	EnsureNoneDistTableWithCoordinatorPlacement(noneDistTableId);

	/*
	 * We don't expect callers try to replicate the shard to remote nodes
	 * if some of the remote nodes have a placement for the shard already.
	 */
	int64 shardId = GetFirstShardId(noneDistTableId);
	List *remoteShardPlacementList =
		FilterShardPlacementList(ActiveShardPlacementList(shardId),
								 IsRemoteShardPlacement);
	if (list_length(remoteShardPlacementList) > 0)
	{
		ereport(ERROR, (errmsg("table already has a remote shard placement")));
	}

	uint64 shardLength = ShardLength(shardId);

	/* insert new placements to pg_dist_placement */
	List *insertedPlacementList = NIL;
	WorkerNode *targetNode = NULL;
	foreach_declared_ptr(targetNode, targetNodeList)
	{
		ShardPlacement *shardPlacement =
			InsertShardPlacementRowGlobally(shardId, GetNextPlacementId(),
											shardLength, targetNode->groupId);

		/* and save the placement for shard creation on workers */
		insertedPlacementList = lappend(insertedPlacementList, shardPlacement);
	}

	/* create new placements */
	bool useExclusiveConnection = false;
	CreateShardsOnWorkers(noneDistTableId, insertedPlacementList,
						  useExclusiveConnection);

	/* fetch coordinator placement before deleting it */
	Oid localPlacementTableId = GetTableLocalShardOid(noneDistTableId, shardId);
	ShardPlacement *coordinatorPlacement =
		linitial(ActiveShardPlacementListOnGroup(shardId, COORDINATOR_GROUP_ID));

	/*
	 * CreateForeignKeysFromReferenceTablesOnShards and CopyFromLocalTableIntoDistTable
	 * need to ignore the local placement, hence we temporarily delete it before
	 * calling them.
	 */
	DeleteShardPlacementRowGlobally(coordinatorPlacement->placementId);

	/* and copy data from local placement to new placements */
	CopyFromLocalTableIntoDistTable(
		localPlacementTableId, noneDistTableId
		);

	/*
	 * CreateShardsOnWorkers only creates the foreign keys where given relation
	 * is the referencing one, so we need to create the foreign keys where given
	 * relation is the referenced one as well. We're only interested in the cases
	 * where the referencing relation is a reference table because the other
	 * possible table types --i.e., Citus local tables atm-- cannot have placements
	 * on remote nodes.
	 *
	 * Note that we need to create the foreign keys where given relation is the
	 * referenced one after copying the data so that constraint checks can pass.
	 */
	CreateForeignKeysFromReferenceTablesOnShards(noneDistTableId);

	/* using the same placement id, re-insert the deleted placement */
	InsertShardPlacementRowGlobally(shardId, coordinatorPlacement->placementId,
									shardLength, COORDINATOR_GROUP_ID);
}


/*
 * NoneDistTableDeleteCoordinatorPlacement deletes pg_dist_placement record for
 * local (presumably coordinator) shard placement of given none-distributed table.
 */
void
NoneDistTableDeleteCoordinatorPlacement(Oid noneDistTableId)
{
	EnsureCoordinator();
	EnsureNoneDistTableWithCoordinatorPlacement(noneDistTableId);

	int64 shardId = GetFirstShardId(noneDistTableId);

	/* we've already verified that table has a coordinator placement */
	ShardPlacement *coordinatorPlacement =
		linitial(ActiveShardPlacementListOnGroup(shardId, COORDINATOR_GROUP_ID));

	/* remove the old placement from metadata of local node, i.e., coordinator */
	DeleteShardPlacementRowGlobally(coordinatorPlacement->placementId);
}


/*
 * NoneDistTableDropCoordinatorPlacementTable drops local (presumably coordinator)
 * shard placement table of given none-distributed table.
 */
void
NoneDistTableDropCoordinatorPlacementTable(Oid noneDistTableId)
{
	EnsureCoordinator();

	if (HasDistributionKey(noneDistTableId))
	{
		ereport(ERROR, (errmsg("table is not a none-distributed table")));
	}

	/*
	 * We undistribute Citus local tables that are not chained with any reference
	 * tables via foreign keys at the end of the utility hook.
	 * Here we temporarily set the related GUC to off to disable the logic for
	 * internally executed DDL's that might invoke this mechanism unnecessarily.
	 *
	 * We also temporarily disable citus.enable_manual_changes_to_shards GUC to
	 * allow given command to modify shard. Note that we disable it only for
	 * local session because changes made to shards are allowed for Citus internal
	 * backends anyway.
	 */
	int saveNestLevel = NewGUCNestLevel();

	SetLocalEnableLocalReferenceForeignKeys(false);
	SetLocalEnableManualChangesToShard(true);

	StringInfo dropShardCommand = makeStringInfo();
	int64 shardId = GetFirstShardId(noneDistTableId);
	ShardInterval *shardInterval = LoadShardInterval(shardId);
	appendStringInfo(dropShardCommand, DROP_REGULAR_TABLE_COMMAND,
					 ConstructQualifiedShardName(shardInterval));

	Task *task = CitusMakeNode(Task);
	task->jobId = INVALID_JOB_ID;
	task->taskId = INVALID_TASK_ID;
	task->taskType = DDL_TASK;
	task->replicationModel = REPLICATION_MODEL_INVALID;
	SetTaskQueryString(task, dropShardCommand->data);

	ShardPlacement *targetPlacement = CitusMakeNode(ShardPlacement);
	SetPlacementNodeMetadata(targetPlacement, CoordinatorNodeIfAddedAsWorkerOrError());

	task->taskPlacementList = list_make1(targetPlacement);

	bool localExecutionSupported = true;
	ExecuteUtilityTaskList(list_make1(task), localExecutionSupported);

	AtEOXact_GUC(true, saveNestLevel);
}


/*
 * CreateForeignKeysFromReferenceTablesOnShards creates foreign keys on shards
 * where given none-distributed table is the referenced table and the referencing
 * one is a reference table.
 */
static void
CreateForeignKeysFromReferenceTablesOnShards(Oid noneDistTableId)
{
	EnsureCoordinator();

	if (HasDistributionKey(noneDistTableId))
	{
		ereport(ERROR, (errmsg("table is not a none-distributed table")));
	}

	List *ddlCommandList =
		GetForeignConstraintFromOtherReferenceTablesCommands(noneDistTableId);
	if (list_length(ddlCommandList) == 0)
	{
		return;
	}

	List *taskList = NIL;

	char *command = NULL;
	foreach_declared_ptr(command, ddlCommandList)
	{
		List *commandTaskList = InterShardDDLTaskList(
			ForeignConstraintGetReferencingTableId(command),
			noneDistTableId, command
			);
		taskList = list_concat(taskList, commandTaskList);
	}

	if (list_length(taskList) == 0)
	{
		return;
	}

	bool localExecutionSupported = true;
	ExecuteUtilityTaskList(taskList, localExecutionSupported);
}


/*
 * ForeignConstraintGetReferencedTableId parses given foreign constraint command and
 * extracts refenrencing table id from it.
 */
static Oid
ForeignConstraintGetReferencingTableId(const char *queryString)
{
	Node *queryNode = ParseTreeNode(queryString);
	if (!IsA(queryNode, AlterTableStmt))
	{
		ereport(ERROR, (errmsg("command is not an ALTER TABLE statement")));
	}

	AlterTableStmt *foreignConstraintStmt = (AlterTableStmt *) queryNode;
	if (list_length(foreignConstraintStmt->cmds) != 1)
	{
		ereport(ERROR, (errmsg("command does not contain a single command")));
	}

	AlterTableCmd *command = (AlterTableCmd *) linitial(foreignConstraintStmt->cmds);
	if (command->subtype == AT_AddConstraint)
	{
		Constraint *constraint = (Constraint *) command->def;
		if (constraint && constraint->contype == CONSTR_FOREIGN)
		{
			bool missingOk = false;
			return RangeVarGetRelid(foreignConstraintStmt->relation, NoLock,
									missingOk);
		}
	}

	ereport(ERROR, (errmsg("command does not contain a foreign constraint")));
}


/*
 * EnsureNoneDistTableWithCoordinatorPlacement throws an error if given
 * table is not a none-distributed that has a coordinator placement.
 */
static void
EnsureNoneDistTableWithCoordinatorPlacement(Oid noneDistTableId)
{
	if (HasDistributionKey(noneDistTableId))
	{
		ereport(ERROR, (errmsg("table is not a none-distributed table")));
	}

	int64 shardId = GetFirstShardId(noneDistTableId);
	if (!ActiveShardPlacementListOnGroup(shardId, COORDINATOR_GROUP_ID))
	{
		ereport(ERROR, (errmsg("table does not have a coordinator placement")));
	}
}


/*
 * SetLocalEnableManualChangesToShard locally enables
 * citus.enable_manual_changes_to_shards GUC.
 */
static void
SetLocalEnableManualChangesToShard(bool state)
{
	set_config_option("citus.enable_manual_changes_to_shards",
					  state ? "on" : "off",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
}
