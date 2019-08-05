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
#include "distributed/multi_router_executor.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


static List * TruncateTaskList(Oid relationId);


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
	char partitionMethod = 0;

	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	triggerData = (TriggerData *) fcinfo->context;
	truncatedRelation = triggerData->tg_relation;
	relationId = RelationGetRelid(truncatedRelation);
	partitionMethod = PartitionMethod(relationId);

	if (!EnableDDLPropagation)
	{
		PG_RETURN_DATUM(PointerGetDatum(NULL));
	}

	if (partitionMethod == DISTRIBUTE_BY_APPEND)
	{
		Oid schemaId = get_rel_namespace(relationId);
		char *schemaName = get_namespace_name(schemaId);
		char *relationName = get_rel_name(relationId);

		DirectFunctionCall3(master_drop_all_shards,
							ObjectIdGetDatum(relationId),
							CStringGetTextDatum(relationName),
							CStringGetTextDatum(schemaName));
	}
	else
	{
		List *taskList = TruncateTaskList(relationId);

		ExecuteUtilityTaskListWithoutResults(taskList, MaxAdaptiveExecutorPoolSize);
	}

	PG_RETURN_DATUM(PointerGetDatum(NULL));
}


/*
 * TruncateTaskList returns a list of tasks to execute a TRUNCATE on a
 * distributed table. This is handled separately from other DDL commands
 * because we handle it via the TRUNCATE trigger, which is called whenever
 * a truncate cascades.
 */
static List *
TruncateTaskList(Oid relationId)
{
	List *shardIntervalList = LoadShardIntervalList(relationId);
	ListCell *shardIntervalCell = NULL;
	List *taskList = NIL;
	int taskId = 1;

	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *relationName = get_rel_name(relationId);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		StringInfo shardQueryString = makeStringInfo();
		Task *task = NULL;
		char *shardName = pstrdup(relationName);

		AppendShardIdToName(&shardName, shardId);

		appendStringInfo(shardQueryString, "TRUNCATE TABLE %s CASCADE",
						 quote_qualified_identifier(schemaName, shardName));

		task = CitusMakeNode(Task);
		task->jobId = INVALID_JOB_ID;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		task->queryString = shardQueryString->data;
		task->dependedTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = shardId;
		task->taskPlacementList = FinalizedShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}
