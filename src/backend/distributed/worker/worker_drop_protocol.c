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
#include "distributed/citus_ruleutils.h"
#include "distributed/distribution_column.h"
#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/worker_protocol.h"
#include "foreign/foreign.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

PG_FUNCTION_INFO_V1(worker_drop_distributed_table);
PG_FUNCTION_INFO_V1(worker_drop_shell_table);
PG_FUNCTION_INFO_V1(worker_drop_sequence_dependency);


/*
 * worker_drop_distributed_table is a deprecated function that is no longer used.
 */
Datum
worker_drop_distributed_table(PG_FUNCTION_ARGS)
{
	ereport(INFO, (errmsg("this function is deprecated and no longer is used")));

	PG_RETURN_VOID();
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
	foreach_oid(ownedSequenceOid, ownedSequences)
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
	foreach_oid(ownedSequenceOid, ownedSequences)
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
