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
#include "utils/builtins.h"

#include "distributed/adaptive_executor.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/listutils.h"
#include "distributed/replicate_none_dist_table_shard.h"
#include "distributed/shard_transfer.h"
#include "distributed/shard_utils.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"


static void ReplicateCoordinatorPlacementData(uint64 shardId, List *targetNodeList);
static void CreateForeignKeysFromReferenceTablesOnShards(Oid noneDistTableId);
static Oid ForeignConstraintGetReferencingTableId(const char *queryString);
static void EnsureNoneDistTableWithCoordinatorPlacement(Oid noneDistTableId);


/*
 * NoneDistTableReplicateCoordinatorPlacement replicates the coordinator
 * shard placement of given none-distributed table to given
 * target nodes and inserts records for new placements into pg_dist_placement.
 */
void
NoneDistTableReplicateCoordinatorPlacement(Oid noneDistTableId,
										   List *targetNodeList)
{
	EnsurePropagationToCoordinator();
	EnsureNoneDistTableWithCoordinatorPlacement(noneDistTableId);

	/*
	 * We don't expect callers try to replicate the shard to worker nodes
	 * if some of the worker nodes have a placement for the shard already.
	 */
	int64 shardId = GetFirstShardId(noneDistTableId);
	List *nonCoordShardPlacementList =
		FilterShardPlacementList(ActiveShardPlacementList(shardId),
								 IsNonCoordShardPlacement);
	if (list_length(nonCoordShardPlacementList) > 0)
	{
		ereport(ERROR, (errmsg("table already has a shard placement on a worker")));
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

	ShardPlacement *coordinatorPlacement =
		linitial(ActiveShardPlacementListOnGroup(shardId, COORDINATOR_GROUP_ID));

	/* copy data from coordinator placement to new placements */
	ReplicateCoordinatorPlacementData(shardId, targetNodeList);

	/*
	 * CreateForeignKeysFromReferenceTablesOnShards
	 * needs to ignore the local placement, hence we temporarily delete it.
	 */
	DeleteShardPlacementRowGlobally(coordinatorPlacement->placementId);

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
 * the coordinator shard placement of given none-distributed table.
 */
void
NoneDistTableDeleteCoordinatorPlacement(Oid noneDistTableId)
{
	EnsurePropagationToCoordinator();
	EnsureNoneDistTableWithCoordinatorPlacement(noneDistTableId);

	int64 shardId = GetFirstShardId(noneDistTableId);

	/* we've already verified that table has a coordinator placement */
	ShardPlacement *coordinatorPlacement =
		linitial(ActiveShardPlacementListOnGroup(shardId, COORDINATOR_GROUP_ID));

	/* remove the old placement from metadata */
	DeleteShardPlacementRowGlobally(coordinatorPlacement->placementId);
}


/*
 * NoneDistTableDropCoordinatorPlacementTable drops the coordinator
 * shard placement table of given none-distributed table.
 */
void
NoneDistTableDropCoordinatorPlacementTable(Oid noneDistTableId)
{
	EnsurePropagationToCoordinator();

	if (HasDistributionKey(noneDistTableId))
	{
		ereport(ERROR, (errmsg("table is not a none-distributed table")));
	}

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

	/*
	 * We undistribute Citus local tables that are not chained with any reference
	 * tables via foreign keys at the end of the utility hook.
	 * So we need to temporarily set the related GUC to off to disable the logic for
	 * internally executed DDL's that might invoke this mechanism unnecessarily.
	 *
	 * We also temporarily disable citus.enable_manual_changes_to_shards GUC to
	 * allow given command to modify shard.
	 */
	List *taskQueryStringList = list_make3(
		"SET LOCAL citus.enable_local_reference_table_foreign_keys TO OFF;",
		"SET LOCAL citus.enable_manual_changes_to_shards TO ON;",
		dropShardCommand->data
		);
	SetTaskQueryStringList(task, taskQueryStringList);

	ShardPlacement *targetPlacement = CitusMakeNode(ShardPlacement);
	SetPlacementNodeMetadata(targetPlacement, CoordinatorNodeIfAddedAsWorkerOrError());

	task->taskPlacementList = list_make1(targetPlacement);

	bool localExecutionSupported = true;
	ExecuteUtilityTaskList(list_make1(task), localExecutionSupported);
}


/*
 * ReplicateCoordinatorPlacementData copies data from the coordinator
 * shard placement to the target nodes.
 *
 * Assumes that the shard placements already exist on the coordinator and
 * target nodes.
 */
static void
ReplicateCoordinatorPlacementData(uint64 shardId, List *targetNodeList)
{
	List *taskList = NIL;

	uint64 jobId = INVALID_JOB_ID;
	uint32 taskId = 0;
	WorkerNode *targetNode = NULL;
	foreach_declared_ptr(targetNode, targetNodeList)
	{
		Task *task = CitusMakeNode(Task);
		task->jobId = jobId;
		task->taskId = taskId++;
		task->taskType = READ_TASK;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		char *shardCopyCommand = CreateShardCopyCommand(LoadShardInterval(shardId),
														targetNode);
		SetTaskQueryStringList(task, list_make1(shardCopyCommand));

		/* we already verified that coordinator is in the metadata */
		WorkerNode *coordinatorNode = CoordinatorNodeIfAddedAsWorkerOrError();

		/*
		 * Need execute the task at the source node as we'll copy the shard
		 * from there, i.e., the coordinator.
		 */
		ShardPlacement *taskPlacement = CitusMakeNode(ShardPlacement);
		SetPlacementNodeMetadata(taskPlacement, coordinatorNode);

		task->taskPlacementList = list_make1(taskPlacement);
		taskList = lappend(taskList, task);
	}

	ExecuteTaskList(ROW_MODIFY_READONLY, taskList);
}


/*
 * CreateForeignKeysFromReferenceTablesOnShards creates foreign keys on shards
 * where given none-distributed table is the referenced table and the referencing
 * one is a reference table.
 */
static void
CreateForeignKeysFromReferenceTablesOnShards(Oid noneDistTableId)
{
	EnsurePropagationToCoordinator();

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
