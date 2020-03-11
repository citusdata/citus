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

#include "distributed/commands/utility_hook.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_sync.h"
#include "distributed/worker_transaction.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/* local function forward declarations */
static void MasterRemoveDistributedTableMetadataFromWorkers(Oid relationId,
															char *schemaName,
															char *tableName);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_drop_distributed_table_metadata);
PG_FUNCTION_INFO_V1(master_remove_partition_metadata);
PG_FUNCTION_INFO_V1(master_remove_distributed_table_metadata_from_workers);


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
	Oid relationId = PG_GETARG_OID(0);
	text *schemaNameText = PG_GETARG_TEXT_P(1);
	text *tableNameText = PG_GETARG_TEXT_P(2);

	char *schemaName = text_to_cstring(schemaNameText);
	char *tableName = text_to_cstring(tableNameText);

	CheckCitusVersion(ERROR);

	/*
	 * The SQL_DROP trigger calls this function even for tables that are
	 * not distributed. In that case, silently ignore. This is not very
	 * user-friendly, but this function is really only meant to be called
	 * from the trigger.
	 */
	if (!IsCitusTable(relationId) || !EnableDDLPropagation)
	{
		PG_RETURN_VOID();
	}

	EnsureCoordinator();

	CheckTableSchemaNameForDrop(relationId, &schemaName, &tableName);

	DeletePartitionRow(relationId);

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
	Oid relationId = PG_GETARG_OID(0);
	text *schemaNameText = PG_GETARG_TEXT_P(1);
	text *tableNameText = PG_GETARG_TEXT_P(2);

	char *schemaName = text_to_cstring(schemaNameText);
	char *tableName = text_to_cstring(tableNameText);

	CheckCitusVersion(ERROR);

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
	if (!IsCitusTable(relationId) || !EnableDDLPropagation)
	{
		return;
	}

	EnsureCoordinator();

	if (!ShouldSyncTableMetadata(relationId))
	{
		return;
	}

	/* drop the distributed table metadata on the workers */
	char *deleteDistributionCommand = DistributionDeleteCommand(schemaName, tableName);
	SendCommandToWorkersWithMetadata(deleteDistributionCommand);
}
