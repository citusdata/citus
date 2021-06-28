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
#include "catalog/pg_foreign_server.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/distribution_column.h"
#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata/distobject.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"


PG_FUNCTION_INFO_V1(worker_drop_distributed_table);


/*
 * worker_drop_distributed_table drops the distributed table with the given oid,
 * then, removes the associated rows from pg_dist_partition, pg_dist_shard and
 * pg_dist_placement. The function also drops the server for foreign tables.
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
	EnsureSuperUser();

	text *relationName = PG_GETARG_TEXT_P(0);
	Oid relationId = ResolveRelationId(relationName, true);

	ObjectAddress distributedTableObject = { InvalidOid, InvalidOid, 0 };
	char relationKind = '\0';

	if (!OidIsValid(relationId))
	{
		ereport(NOTICE, (errmsg("relation %s does not exist, skipping",
								text_to_cstring(relationName))));
		PG_RETURN_VOID();
	}

	List *shardList = LoadShardList(relationId);

	/* first check the relation type */
	Relation distributedRelation = relation_open(relationId, AccessShareLock);
	relationKind = distributedRelation->rd_rel->relkind;
	EnsureRelationKindSupported(relationId);

	/* close the relation since we do not need anymore */
	relation_close(distributedRelation, AccessShareLock);

	/* prepare distributedTableObject for dropping the table */
	distributedTableObject.classId = RelationRelationId;
	distributedTableObject.objectId = relationId;
	distributedTableObject.objectSubId = 0;

	/* drop the server for the foreign relations */
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		ObjectAddresses *objects = new_object_addresses();
		ObjectAddress foreignServerObject = { InvalidOid, InvalidOid, 0 };
		ForeignTable *foreignTable = GetForeignTable(relationId);
		Oid serverId = foreignTable->serverid;

		/* prepare foreignServerObject for dropping the server */
		foreignServerObject.classId = ForeignServerRelationId;
		foreignServerObject.objectId = serverId;
		foreignServerObject.objectSubId = 0;

		/* add the addresses that are going to be dropped */
		add_exact_object_address(&distributedTableObject, objects);
		add_exact_object_address(&foreignServerObject, objects);

		/* drop both the table and the server */
		performMultipleDeletions(objects, DROP_RESTRICT,
								 PERFORM_DELETION_INTERNAL);
	}
	else if (!IsObjectAddressOwnedByExtension(&distributedTableObject, NULL))
	{
		/*
		 * If the table is owned by an extension, we cannot drop it, nor should we
		 * until the user runs DROP EXTENSION. Therefore, we skip dropping the
		 * table and only delete the metadata.
		 *
		 * We drop the table with cascade since other tables may be referring to it.
		 */
		performDeletion(&distributedTableObject, DROP_CASCADE,
						PERFORM_DELETION_INTERNAL);
	}

	/* iterate over shardList to delete the corresponding rows */
	uint64 *shardIdPointer = NULL;
	foreach_ptr(shardIdPointer, shardList)
	{
		uint64 shardId = *shardIdPointer;

		List *shardPlacementList = ShardPlacementListIncludingOrphanedPlacements(shardId);
		ShardPlacement *placement = NULL;
		foreach_ptr(placement, shardPlacementList)
		{
			/* delete the row from pg_dist_placement */
			DeleteShardPlacementRow(placement->placementId);
		}

		/* delete the row from pg_dist_shard */
		DeleteShardRow(shardId);
	}

	/* delete the row from pg_dist_partition */
	DeletePartitionRow(relationId);

	PG_RETURN_VOID();
}
