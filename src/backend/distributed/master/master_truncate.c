/*-------------------------------------------------------------------------
 *
 * master_truncate.c
 *
 * Routine for truncating local data after a table has been distributed.
 *
 * Copyright (c) 2014-2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stddef.h>

#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_join_order.h"
#include "distributed/pg_dist_partition.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_truncate_trigger);


/*
 * citus_truncate_trigger is called as a trigger when a distributed
 * table is truncated.
 */
Datum
citus_truncate_trigger(PG_FUNCTION_ARGS)
{
	TriggerData *triggerData = NULL;
	Relation truncatedRelation = NULL;
	Oid relationId = InvalidOid;
	char *relationName = NULL;
	Oid schemaId = InvalidOid;
	char *schemaName = NULL;
	char partitionMethod = 0;

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	triggerData = (TriggerData *) fcinfo->context;
	truncatedRelation = triggerData->tg_relation;
	relationId = RelationGetRelid(truncatedRelation);
	relationName = get_rel_name(relationId);
	schemaId = get_rel_namespace(relationId);
	schemaName = get_namespace_name(schemaId);
	partitionMethod = PartitionMethod(relationId);

	if (!EnableDDLPropagation)
	{
		PG_RETURN_DATUM(PointerGetDatum(NULL));
	}

	if (partitionMethod == DISTRIBUTE_BY_APPEND)
	{
		DirectFunctionCall3(master_drop_all_shards,
							ObjectIdGetDatum(relationId),
							CStringGetTextDatum(relationName),
							CStringGetTextDatum(schemaName));
	}
	else
	{
		StringInfo truncateStatement = makeStringInfo();
		char *qualifiedTableName = quote_qualified_identifier(schemaName, relationName);

		appendStringInfo(truncateStatement, "TRUNCATE TABLE %s CASCADE",
						 qualifiedTableName);

		DirectFunctionCall1(master_modify_multiple_shards,
							CStringGetTextDatum(truncateStatement->data));
	}

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}
