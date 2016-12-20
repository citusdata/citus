/*-------------------------------------------------------------------------
 *
 * drop_distributed_table.c
 *	  Routines related to dropping distributed relations from a trigger.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_sync.h"
#include "distributed/worker_transaction.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_drop_distributed_table_metadata);


/*
 * master_drop_distributed_table_metadata removes the entry of the specified distributed
 * table from pg_dist_partition and drops the table from the workers if needed.
 */
Datum
master_drop_distributed_table_metadata(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	text *schemaNameText = PG_GETARG_TEXT_P(1);
	text *tableNameText = PG_GETARG_TEXT_P(2);
	bool shouldSyncMetadata = false;

	char *schemaName = text_to_cstring(schemaNameText);
	char *tableName = text_to_cstring(tableNameText);

	CheckTableSchemaNameForDrop(relationId, &schemaName, &tableName);

	DeletePartitionRow(relationId);

	shouldSyncMetadata = ShouldSyncTableMetadata(relationId);
	if (shouldSyncMetadata)
	{
		char *deleteDistributionCommand = NULL;

		/* drop the distributed table metadata on the workers */
		deleteDistributionCommand = DistributionDeleteCommand(schemaName, tableName);
		SendCommandToWorkers(WORKERS_WITH_METADATA, deleteDistributionCommand);
	}

	PG_RETURN_VOID();
}
