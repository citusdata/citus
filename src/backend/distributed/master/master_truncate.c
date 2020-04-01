/*-------------------------------------------------------------------------
 *
 * master_truncate.c
 *
 * Routine for truncating local data after a table has been distributed.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include <stddef.h>

#include "commands/tablecmds.h"
#include "commands/trigger.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/adaptive_executor.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparse_shard_query.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_executor.h"
#include "distributed/multi_join_order.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/resource_lock.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/rel.h"

static List * TruncateTaskList(Oid relationId);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_truncate_trigger);
PG_FUNCTION_INFO_V1(truncate_local_data_after_distributing_table);

void EnsureLocalTableCanBeTruncated(Oid relationId);


/*
 * citus_truncate_trigger is called as a trigger when a distributed
 * table is truncated.
 */
Datum
citus_truncate_trigger(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR, (errcode(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
						errmsg("must be called as trigger")));
	}

	TriggerData *triggerData = (TriggerData *) fcinfo->context;
	Relation truncatedRelation = triggerData->tg_relation;
	Oid relationId = RelationGetRelid(truncatedRelation);
	char partitionMethod = PartitionMethod(relationId);

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

		/*
		 * If it is a local placement of a distributed table or a reference table,
		 * then execute TRUNCATE command locally.
		 */
		bool localExecutionSupported = true;
		ExecuteUtilityTaskList(taskList, localExecutionSupported);
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
	/* resulting task list */
	List *taskList = NIL;

	/* enumerate the tasks when putting them to the taskList */
	int taskId = 1;

	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *relationName = get_rel_name(relationId);

	List *shardIntervalList = LoadShardIntervalList(relationId);

	/* lock metadata before getting placement lists */
	LockShardListMetadata(shardIntervalList, ShareLock);

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;
		char *shardRelationName = pstrdup(relationName);

		/* build shard relation name */
		AppendShardIdToName(&shardRelationName, shardId);

		char *quotedShardName = quote_qualified_identifier(schemaName, shardRelationName);

		StringInfo shardQueryString = makeStringInfo();
		appendStringInfo(shardQueryString, "TRUNCATE TABLE %s CASCADE", quotedShardName);

		Task *task = CitusMakeNode(Task);
		task->jobId = INVALID_JOB_ID;
		task->taskId = taskId++;
		task->taskType = DDL_TASK;
		SetTaskQueryString(task, shardQueryString->data);
		task->dependentTaskList = NULL;
		task->replicationModel = REPLICATION_MODEL_INVALID;
		task->anchorShardId = shardId;
		task->taskPlacementList = ActiveShardPlacementList(shardId);

		taskList = lappend(taskList, task);
	}

	return taskList;
}


/*
 * truncate_local_data_after_distributing_table truncates the local records of a distributed table.
 *
 * The main advantage of this function is to truncate all local records after creating a
 * distributed table, and prevent constraints from failing due to outdated local records.
 */
Datum
truncate_local_data_after_distributing_table(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);

	CheckCitusVersion(ERROR);
	EnsureCoordinator();
	EnsureLocalTableCanBeTruncated(relationId);

	TruncateStmt *truncateStmt = makeNode(TruncateStmt);

	char *relationName = generate_qualified_relation_name(relationId);
	List *names = stringToQualifiedNameList(relationName);
	truncateStmt->relations = list_make1(makeRangeVarFromNameList(names));
	truncateStmt->restart_seqs = false;
	truncateStmt->behavior = DROP_CASCADE;

	set_config_option("citus.enable_ddl_propagation", "false",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);
	ExecuteTruncate(truncateStmt);
	set_config_option("citus.enable_ddl_propagation", "true",
					  (superuser() ? PGC_SUSET : PGC_USERSET), PGC_S_SESSION,
					  GUC_ACTION_LOCAL, true, 0, false);

	PG_RETURN_VOID();
}


/*
 * EnsureLocalTableCanBeTruncated performs the necessary checks to make sure it
 * is safe to truncate the local table of a distributed table
 */
void
EnsureLocalTableCanBeTruncated(Oid relationId)
{
	/* error out if the relation is not a distributed table */
	if (!IsCitusTable(relationId))
	{
		ereport(ERROR, (errmsg("supplied parameter is not a distributed relation"),
						errdetail("This UDF only truncates local records of distributed "
								  "tables.")));
	}

	/* make sure there are no foreign key references from a local table */
	SetForeignConstraintRelationshipGraphInvalid();
	List *referencingRelationList = ReferencingRelationIdList(relationId);

	Oid referencingRelation = InvalidOid;
	foreach_oid(referencingRelation, referencingRelationList)
	{
		/* we do not truncate a table if there is a local table referencing it */
		if (!IsCitusTable(referencingRelation))
		{
			char *referencedRelationName = get_rel_name(relationId);
			char *referencingRelationName = get_rel_name(referencingRelation);

			ereport(ERROR, (errmsg("cannot truncate a table referenced in a "
								   "foreign key constraint by a local table"),
							errdetail("Table \"%s\" references \"%s\"",
									  referencingRelationName,
									  referencedRelationName)));
		}
	}
}
