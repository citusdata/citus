/*-------------------------------------------------------------------------
 *
 * worker_copy_table_to_node_udf.c
 *
 * This file implements the worker_copy_table_to_node UDF. This UDF can be
 * used to copy the data in a shard (or other table) from one worker node to
 * another.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_executor.h"
#include "distributed/priority.h"
#include "distributed/worker_shard_copy.h"

PG_FUNCTION_INFO_V1(worker_copy_table_to_node);

/*
 * worker_copy_table_to_node copies a shard from this worker to another worker
 *
 * SQL signature:
 *
 * worker_copy_table_to_node(
 *     source_table regclass,
 *     target_node_id integer,
 *     exclusive_connection boolean
 *  ) RETURNS VOID
 */
Datum
worker_copy_table_to_node(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	uint32_t targetNodeId = PG_GETARG_INT32(1);
	bool exclusiveConnection = PG_GETARG_BOOL(2);

	Oid schemaOid = get_rel_namespace(relationId);
	char *relationSchemaName = get_namespace_name(schemaOid);
	char *relationName = get_rel_name(relationId);
	char *relationQualifiedName = quote_qualified_identifier(
		relationSchemaName,
		relationName);

	EState *executor = CreateExecutorState();
	DestReceiver *destReceiver = CreateShardCopyDestReceiver(
		executor,
		list_make2(relationSchemaName, relationName),
		targetNodeId,
		exclusiveConnection);

	StringInfo selectShardQueryForCopy = makeStringInfo();

	/*
	 * Even though we do COPY(SELECT ...) all the columns, we can't just do SELECT * because we need to not COPY generated colums.
	 */
	const char *columnList = CopyableColumnNamesFromRelationName(relationSchemaName,
																 relationName);
	appendStringInfo(selectShardQueryForCopy,
					 "SELECT %s FROM %s;", columnList, relationQualifiedName);

	ParamListInfo params = NULL;
	ExecuteQueryStringIntoDestReceiver(selectShardQueryForCopy->data, params,
									   destReceiver);

	FreeExecutorState(executor);

	PG_RETURN_VOID();
}
