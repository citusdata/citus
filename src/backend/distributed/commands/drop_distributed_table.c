/*-------------------------------------------------------------------------
 *
 * drop_distributed_table.c
 *	  Routines related to dropping distributed relations from a trigger.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "distributed/colocation_utils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/tenant_schema_metadata.h"
#include "distributed/worker_transaction.h"


/* local function forward declarations */
static void MasterRemoveDistributedTableMetadataFromWorkers(Oid relationId,
															char *schemaName,
															char *tableName);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_drop_distributed_table_metadata);
PG_FUNCTION_INFO_V1(master_remove_partition_metadata);
PG_FUNCTION_INFO_V1(master_remove_distributed_table_metadata_from_workers);
PG_FUNCTION_INFO_V1(notify_constraint_dropped);


/*
 * master_drop_distributed_table_metadata UDF is a stub UDF to install Citus flawlessly.
 * Otherwise we need to delete them from our sql files, which is confusing and not a
 * common operation in the code-base.
 *
 * This function is basically replaced with
 * master_remove_distributed_table_metadata_from_workers() followed by
 * master_remove_partition_metadata().
 */
Datum
master_drop_distributed_table_metadata(PG_FUNCTION_ARGS)
{
	ereport(INFO, (errmsg("this function is deprecated and no longer is used")));

	PG_RETURN_VOID();
}


/*
 * master_remove_partition_metadata removes the entry of the specified distributed
 * table from pg_dist_partition.
 */
Datum
master_remove_partition_metadata(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	text *schemaNameText = PG_GETARG_TEXT_P(1);
	text *tableNameText = PG_GETARG_TEXT_P(2);

	char *schemaName = text_to_cstring(schemaNameText);
	char *tableName = text_to_cstring(tableNameText);

	uint32 colocationId = ColocationIdViaCatalog(relationId);

	/*
	 * The SQL_DROP trigger calls this function even for tables that are
	 * not distributed. In that case, silently ignore. This is not very
	 * user-friendly, but this function is really only meant to be called
	 * from the trigger.
	 */
	if (!IsCitusTableViaCatalog(relationId) || !EnableDDLPropagation)
	{
		PG_RETURN_VOID();
	}

	EnsureCoordinator();

	CheckTableSchemaNameForDrop(relationId, &schemaName, &tableName);

	DeletePartitionRow(relationId);

	/*
	 * We want to keep using the same colocation group for the tenant even if
	 * all the tables that belong to it are dropped and new tables are created
	 * for the tenant etc. For this reason, if a colocation group belongs to a
	 * tenant schema, we don't delete the colocation group even if there are no
	 * tables that belong to it.
	 *
	 * We do the same if system catalog cannot find the schema of the table
	 * because this means that the whole schema is dropped.
	 *
	 * In that case, we want to delete the colocation group regardless of
	 * whether the schema is a tenant schema or not. Even more, calling
	 * IsTenantSchema() with InvalidOid would cause an error, hence we check
	 * whether the schema is valid beforehand.
	 */
	bool missingOk = true;
	Oid schemaId = get_namespace_oid(schemaName, missingOk);
	if (!OidIsValid(schemaId) || !IsTenantSchema(schemaId))
	{
		DeleteColocationGroupIfNoTablesBelong(colocationId);
	}

	PG_RETURN_VOID();
}


/*
 * master_remove_distributed_table_metadata_from_workers removes the entry of the
 * specified distributed table from pg_dist_partition and drops the table from
 * the workers if needed.
 */
Datum
master_remove_distributed_table_metadata_from_workers(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	text *schemaNameText = PG_GETARG_TEXT_P(1);
	text *tableNameText = PG_GETARG_TEXT_P(2);

	char *schemaName = text_to_cstring(schemaNameText);
	char *tableName = text_to_cstring(tableNameText);

	CheckTableSchemaNameForDrop(relationId, &schemaName, &tableName);

	MasterRemoveDistributedTableMetadataFromWorkers(relationId, schemaName, tableName);

	PG_RETURN_VOID();
}


/*
 * MasterRemoveDistributedTableMetadataFromWorkers drops the table and removes
 * all the metadata belonging the distributed table in the worker nodes
 * with metadata. The function doesn't drop the tables that are
 * the shards on the workers.
 *
 * The function is a no-op for non-distributed tables and clusters that don't
 * have any workers with metadata. Also, the function errors out if called
 * from a worker node.
 *
 * This function assumed that it is called via a trigger. But we cannot do the
 * typical CALLED_AS_TRIGGER check because this is called via another trigger,
 * which CALLED_AS_TRIGGER does not cover.
 */
static void
MasterRemoveDistributedTableMetadataFromWorkers(Oid relationId, char *schemaName,
												char *tableName)
{
	/*
	 * The SQL_DROP trigger calls this function even for tables that are
	 * not distributed. In that case, silently ignore. This is not very
	 * user-friendly, but this function is really only meant to be called
	 * from the trigger.
	 */
	if (!IsCitusTableViaCatalog(relationId) || !EnableDDLPropagation)
	{
		return;
	}

	EnsureCoordinator();

	if (!ShouldSyncTableMetadataViaCatalog(relationId))
	{
		return;
	}

	if (PartitionTable(relationId))
	{
		/*
		 * MasterRemoveDistributedTableMetadataFromWorkers is only called from drop trigger.
		 * When parent is dropped in a drop trigger, we remove all the corresponding
		 * partitions via the parent, mostly for performance reasons.
		 */
		return;
	}

	/* drop the distributed table metadata on the workers */
	char *deleteDistributionCommand = DistributionDeleteCommand(schemaName, tableName);
	SendCommandToWorkersWithMetadata(deleteDistributionCommand);
}


/*
 * notify_constraint_dropped simply calls NotifyUtilityHookConstraintDropped
 * to set ConstraintDropped to true.
 * This udf is designed to be called from citus_drop_trigger to tell us we
 * dropped a table constraint.
 */
Datum
notify_constraint_dropped(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	/*
	 * We reset this only in utility hook, so we should not set this flag
	 * otherwise if we are not in utility hook.
	 * In some cases -where dropping foreign key not issued via utility
	 * hook-, we would not be able to undistribute such citus local tables
	 * but we are ok with that.
	 */
	if (UtilityHookLevel >= 1)
	{
		NotifyUtilityHookConstraintDropped();
	}

	PG_RETURN_VOID();
}
