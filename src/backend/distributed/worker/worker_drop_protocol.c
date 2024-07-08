/*-------------------------------------------------------------------------
 *
 * worker_drop_protocol.c
 *
 * Routines for dropping distributed tables and their metadata on worker nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_foreign_server.h"
#include "foreign/foreign.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/distribution_column.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/worker_protocol.h"

PG_FUNCTION_INFO_V1(worker_drop_distributed_table);
PG_FUNCTION_INFO_V1(worker_drop_shell_table);
PG_FUNCTION_INFO_V1(worker_drop_sequence_dependency);

static void WorkerDropDistributedTable(Oid relationId);


/*
 * worker_drop_distributed_table drops the distributed table with the given oid,
 * then, removes the associated rows from pg_dist_partition, pg_dist_shard and
 * pg_dist_placement.
 *
 * Note that drop fails if any dependent objects are present for any of the
 * distributed tables. Also, shard placements of the distributed tables are
 * not dropped as in the case of "DROP TABLE distributed_table;" command.
 *
 * The function errors out if the input relation Oid is not a regular or foreign table.
 * The function is meant to be called only by the coordinator, therefore requires
 * superuser privileges.
 */
Datum
worker_drop_distributed_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *relationName = PG_GETARG_TEXT_P(0);
	Oid relationId = ResolveRelationId(relationName, true);

	if (!OidIsValid(relationId))
	{
		ereport(NOTICE, (errmsg("relation %s does not exist, skipping",
								text_to_cstring(relationName))));
		PG_RETURN_VOID();
	}

	EnsureTableOwner(relationId);

	if (PartitionedTable(relationId))
	{
		/*
		 * When "DROP SCHEMA .. CASCADE" happens, we rely on Postgres' drop trigger
		 * to send the individual DROP TABLE commands for tables.
		 *
		 * In case of partitioned tables, we have no control on the order of DROP
		 * commands that is sent to the extension. We can try to sort while processing
		 * on the coordinator, but we prefer to handle it in a more flexible manner.
		 *
		 * That's why, whenever we see a partitioned table, we drop all the corresponding
		 * partitions first. Otherwise, WorkerDropDistributedTable() would already drop
		 * the shell tables of the partitions (e.g., due to performDeletion(..CASCADE),
		 * and further WorkerDropDistributedTable() on the partitions would become no-op.
		 *
		 * If, say one partition has already been dropped earlier, that should also be fine
		 * because we read the existing partitions.
		 */
		List *partitionList = PartitionList(relationId);
		Oid partitionOid = InvalidOid;
		foreach_declared_oid(partitionOid, partitionList)
		{
			WorkerDropDistributedTable(partitionOid);
		}
	}

	WorkerDropDistributedTable(relationId);

	PG_RETURN_VOID();
}


/*
 * WorkerDropDistributedTable is a helper function for worker_drop_distributed_table, see
 * tha function for the details.
 */
static void
WorkerDropDistributedTable(Oid relationId)
{
	/* first check the relation type */
	Relation distributedRelation = relation_open(relationId, AccessShareLock);

	EnsureRelationKindSupported(relationId);

	/* close the relation since we do not need anymore */
	relation_close(distributedRelation, AccessShareLock);

	/* prepare distributedTableObject for dropping the table */
	ObjectAddress *distributedTableObject = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*distributedTableObject, RelationRelationId, relationId);

	/* Drop dependent sequences from pg_dist_object */
	List *ownedSequences = getOwnedSequences(relationId);

	Oid ownedSequenceOid = InvalidOid;
	foreach_declared_oid(ownedSequenceOid, ownedSequences)
	{
		ObjectAddress ownedSequenceAddress = { 0 };
		ObjectAddressSet(ownedSequenceAddress, RelationRelationId, ownedSequenceOid);
		UnmarkObjectDistributed(&ownedSequenceAddress);
	}

	UnmarkObjectDistributed(distributedTableObject);

	/*
	 * Remove metadata before object's itself to make functions no-op within
	 * drop event trigger for undistributed objects on worker nodes except
	 * removing pg_dist_object entries.
	 */
	List *shardList = LoadShardList(relationId);
	uint64 *shardIdPointer = NULL;
	foreach_declared_ptr(shardIdPointer, shardList)
	{
		uint64 shardId = *shardIdPointer;

		List *shardPlacementList = ShardPlacementList(shardId);
		ShardPlacement *placement = NULL;
		foreach_declared_ptr(placement, shardPlacementList)
		{
			/* delete the row from pg_dist_placement */
			DeleteShardPlacementRow(placement->placementId);
		}

		/* delete the row from pg_dist_shard */
		DeleteShardRow(shardId);
	}

	/* delete the row from pg_dist_partition */
	DeletePartitionRow(relationId);

	/*
	 * If the table is owned by an extension, we cannot drop it, nor should we
	 * until the user runs DROP EXTENSION. Therefore, we skip dropping the
	 * table.
	 */
	if (!IsAnyObjectAddressOwnedByExtension(list_make1(distributedTableObject), NULL))
	{
		StringInfo dropCommand = makeStringInfo();
		appendStringInfo(dropCommand, "DROP%sTABLE %s CASCADE",
						 IsForeignTable(relationId) ? " FOREIGN " : " ",
						 generate_qualified_relation_name(relationId));

		Node *dropCommandNode = ParseTreeNode(dropCommand->data);

		/*
		 * We use ProcessUtilityParseTree (instead of performDeletion) to make sure that
		 * we also drop objects that depend on the table and call the drop event trigger
		 * which removes them from pg_dist_object.
		 */
		ProcessUtilityParseTree(dropCommandNode, dropCommand->data,
								PROCESS_UTILITY_QUERY, NULL, None_Receiver, NULL);
	}
}


/*
 * worker_drop_shell_table drops the shell table of with the given distributed
 * table without deleting related entries on pg_dist_placement, pg_dist_shard
 * and pg_dist_placement. We've separated that logic since we handle object
 * dependencies and table metadata separately while activating nodes.
 */
Datum
worker_drop_shell_table(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	text *relationName = PG_GETARG_TEXT_P(0);
	Oid relationId = ResolveRelationId(relationName, true);

	if (!OidIsValid(relationId))
	{
		ereport(NOTICE, (errmsg("relation %s does not exist, skipping",
								text_to_cstring(relationName))));
		PG_RETURN_VOID();
	}

	EnsureTableOwner(relationId);

	if (GetLocalGroupId() == COORDINATOR_GROUP_ID)
	{
		ereport(ERROR, (errmsg("worker_drop_shell_table is only allowed to run"
							   " on worker nodes")));
	}

	/* first check the relation type */
	Relation distributedRelation = relation_open(relationId, AccessShareLock);
	EnsureRelationKindSupported(relationId);

	/* close the relation since we do not need anymore */
	relation_close(distributedRelation, AccessShareLock);

	/* prepare distributedTableObject for dropping the table */
	ObjectAddress *distributedTableObject = palloc0(sizeof(ObjectAddress));
	ObjectAddressSet(*distributedTableObject, RelationRelationId, relationId);
	if (IsAnyObjectAddressOwnedByExtension(list_make1(distributedTableObject), NULL))
	{
		PG_RETURN_VOID();
	}

	/* Drop dependent sequences from pg_dist_object */
	List *ownedSequences = getOwnedSequences(relationId);

	Oid ownedSequenceOid = InvalidOid;
	foreach_declared_oid(ownedSequenceOid, ownedSequences)
	{
		ObjectAddress ownedSequenceAddress = { 0 };
		ObjectAddressSet(ownedSequenceAddress, RelationRelationId, ownedSequenceOid);
		UnmarkObjectDistributed(&ownedSequenceAddress);
	}

	/*
	 * If the table is owned by an extension, we cannot drop it, nor should we
	 * until the user runs DROP EXTENSION. Therefore, we skip dropping the
	 * table and only delete the metadata.
	 *
	 * We drop the table with cascade since other tables may be referring to it.
	 */
	performDeletion(distributedTableObject, DROP_CASCADE,
					PERFORM_DELETION_INTERNAL);

	PG_RETURN_VOID();
}


/*
 * worker_drop_sequence_dependency is a UDF that removes the dependency
 * of all the sequences for the given table.
 *
 * The main purpose of this UDF is to prevent dropping the sequences while
 * re-creating the same table such as changing the shard count, converting
 * a citus local table to a distributed table or re-syncing the metadata.
 */
Datum
worker_drop_sequence_dependency(PG_FUNCTION_ARGS)
{
	text *relationName = PG_GETARG_TEXT_P(0);
	Oid relationId = ResolveRelationId(relationName, true);

	if (!OidIsValid(relationId))
	{
		ereport(NOTICE, (errmsg("relation %s does not exist, skipping",
								text_to_cstring(relationName))));
		PG_RETURN_VOID();
	}

	EnsureTableOwner(relationId);

	/* break the dependent sequences from the table */
	List *ownedSequences = getOwnedSequences(relationId);

	Oid ownedSequenceOid = InvalidOid;
	foreach_declared_oid(ownedSequenceOid, ownedSequences)
	{
		/* the caller doesn't want to drop the sequence, so break the dependency */
		deleteDependencyRecordsForSpecific(RelationRelationId, ownedSequenceOid,
										   DEPENDENCY_AUTO, RelationRelationId,
										   relationId);
	}

	if (list_length(ownedSequences) > 0)
	{
		/* if we delete at least one dependency, let next commands know */
		CommandCounterIncrement();
	}

	PG_RETURN_VOID();
}
