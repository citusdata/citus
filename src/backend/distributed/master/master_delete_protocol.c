/*-------------------------------------------------------------------------
 *
 * master_delete_protocol.c
 *
 * Routine for deleting shards in the distributed cluster. This function takes
 * in a delete command and deletes a shard if and only if all rows in the shard
 * satisfy the conditions in the delete command.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"

#include <stddef.h>

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/connection_management.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/placement_connection.h"
#include "distributed/relay_utility.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_transaction.h"
#include "lib/stringinfo.h"
#if PG_VERSION_NUM >= 120000
#include "nodes/nodeFuncs.h"
#endif
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "optimizer/clauses.h"
#if PG_VERSION_NUM >= 120000
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#else
#include "nodes/relation.h"
#include "optimizer/predtest.h"
#endif
#include "optimizer/restrictinfo.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/varlena.h"


/* Local functions forward declarations */
static void CheckTableCount(Query *deleteQuery);
static void CheckDeleteCriteria(Node *deleteCriteria);
static void CheckPartitionColumn(Oid relationId, Node *whereClause);
static List * ShardsMatchingDeleteCriteria(Oid relationId, List *shardList,
										   Node *deleteCriteria);
static int DropShards(Oid relationId, char *schemaName, char *relationName,
					  List *deletableShardIntervalList);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(master_apply_delete_command);
PG_FUNCTION_INFO_V1(master_drop_all_shards);
PG_FUNCTION_INFO_V1(master_drop_sequences);


/*
 * master_apply_delete_command takes in a delete command, finds shards that
 * match the criteria defined in the delete command, drops the found shards from
 * the worker nodes, and updates the corresponding metadata on the master node.
 * This function drops a shard if and only if all rows in the shard satisfy
 * the conditions in the delete command. Note that this function only accepts
 * conditions on the partition key and if no condition is provided then all
 * shards are deleted.
 *
 * We mark shard placements that we couldn't drop as to be deleted later. If a
 * shard satisfies the given conditions, we delete it from shard metadata table
 * even though related shard placements are not deleted.
 */
Datum
master_apply_delete_command(PG_FUNCTION_ARGS)
{
	text *queryText = PG_GETARG_TEXT_P(0);
	char *queryString = text_to_cstring(queryText);
	char *relationName = NULL;
	char *schemaName = NULL;
	Oid relationId = InvalidOid;
	List *shardIntervalList = NIL;
	List *deletableShardIntervalList = NIL;
	List *queryTreeList = NIL;
	Query *deleteQuery = NULL;
	Node *whereClause = NULL;
	Node *deleteCriteria = NULL;
	Node *queryTreeNode = NULL;
	DeleteStmt *deleteStatement = NULL;
	int droppedShardCount = 0;
	LOCKMODE lockMode = 0;
	char partitionMethod = 0;
	bool failOK = false;
	RawStmt *rawStmt = (RawStmt *) ParseTreeRawStmt(queryString);
	queryTreeNode = rawStmt->stmt;

	EnsureCoordinator();
	CheckCitusVersion(ERROR);

	if (!IsA(queryTreeNode, DeleteStmt))
	{
		ereport(ERROR, (errmsg("query \"%s\" is not a delete statement",
							   ApplyLogRedaction(queryString))));
	}

	deleteStatement = (DeleteStmt *) queryTreeNode;

	schemaName = deleteStatement->relation->schemaname;
	relationName = deleteStatement->relation->relname;

	/*
	 * We take an exclusive lock while dropping shards to prevent concurrent
	 * writes. We don't want to block SELECTs, which means queries might fail
	 * if they access a shard that has just been dropped.
	 */
	lockMode = ExclusiveLock;

	relationId = RangeVarGetRelid(deleteStatement->relation, lockMode, failOK);

	/* schema-prefix if it is not specified already */
	if (schemaName == NULL)
	{
		Oid schemaId = get_rel_namespace(relationId);
		schemaName = get_namespace_name(schemaId);
	}

	CheckDistributedTable(relationId);
	EnsureTablePermissions(relationId, ACL_DELETE);

	queryTreeList = pg_analyze_and_rewrite(rawStmt, queryString, NULL, 0, NULL);
	deleteQuery = (Query *) linitial(queryTreeList);
	CheckTableCount(deleteQuery);

	/* get where clause and flatten it */
	whereClause = (Node *) deleteQuery->jointree->quals;
	deleteCriteria = eval_const_expressions(NULL, whereClause);

	partitionMethod = PartitionMethod(relationId);
	if (partitionMethod == DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot delete from hash distributed table with this "
							   "command"),
						errdetail("Delete statements on hash-partitioned tables "
								  "are not supported with master_apply_delete_command."),
						errhint("Use the DELETE command instead.")));
	}
	else if (partitionMethod == DISTRIBUTE_BY_NONE)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot delete from distributed table"),
						errdetail("Delete statements on reference tables "
								  "are not supported.")));
	}


	CheckDeleteCriteria(deleteCriteria);
	CheckPartitionColumn(relationId, deleteCriteria);

	shardIntervalList = LoadShardIntervalList(relationId);

	/* drop all shards if where clause is not present */
	if (deleteCriteria == NULL)
	{
		deletableShardIntervalList = shardIntervalList;
		ereport(DEBUG2, (errmsg("dropping all shards for \"%s\"", relationName)));
	}
	else
	{
		deletableShardIntervalList = ShardsMatchingDeleteCriteria(relationId,
																  shardIntervalList,
																  deleteCriteria);
	}

	droppedShardCount = DropShards(relationId, schemaName, relationName,
								   deletableShardIntervalList);

	PG_RETURN_INT32(droppedShardCount);
}


/*
 * master_drop_all_shards attempts to drop all shards for a given relation.
 * Unlike master_apply_delete_command, this function can be called even
 * if the table has already been dropped.
 */
Datum
master_drop_all_shards(PG_FUNCTION_ARGS)
{
	Oid relationId = PG_GETARG_OID(0);
	text *schemaNameText = PG_GETARG_TEXT_P(1);
	text *relationNameText = PG_GETARG_TEXT_P(2);

	List *shardIntervalList = NIL;
	int droppedShardCount = 0;

	char *schemaName = text_to_cstring(schemaNameText);
	char *relationName = text_to_cstring(relationNameText);

	CheckCitusVersion(ERROR);

	/*
	 * The SQL_DROP trigger calls this function even for tables that are
	 * not distributed. In that case, silently ignore and return -1.
	 */
	if (!IsDistributedTable(relationId) || !EnableDDLPropagation)
	{
		PG_RETURN_INT32(-1);
	}

	EnsureCoordinator();
	CheckTableSchemaNameForDrop(relationId, &schemaName, &relationName);

	/*
	 * master_drop_all_shards is typically called from the DROP TABLE trigger,
	 * but could be called by a user directly. Make sure we have an
	 * AccessExlusiveLock to prevent any other commands from running on this table
	 * concurrently.
	 */
	LockRelationOid(relationId, AccessExclusiveLock);

	shardIntervalList = LoadShardIntervalList(relationId);
	droppedShardCount = DropShards(relationId, schemaName, relationName,
								   shardIntervalList);

	PG_RETURN_INT32(droppedShardCount);
}


/*
 * master_drop_sequences attempts to drop a list of sequences on worker nodes.
 * The "IF EXISTS" clause is used to permit dropping sequences even if they may not
 * exist. If the commands fail on the workers, the operation is rolled back.
 * If ddl propagation (citus.enable_ddl_propagation) is set to off, then the function
 * returns without doing anything.
 */
Datum
master_drop_sequences(PG_FUNCTION_ARGS)
{
	ArrayType *sequenceNamesArray = PG_GETARG_ARRAYTYPE_P(0);
	ArrayIterator sequenceIterator = NULL;
	Datum sequenceNameDatum = 0;
	bool isNull = false;
	StringInfo dropSeqCommand = makeStringInfo();

	CheckCitusVersion(ERROR);

	/*
	 * Do nothing if DDL propagation is switched off or we're not on
	 * the coordinator. Here we prefer to not error out on the workers
	 * because this function is called on every dropped sequence and
	 * we don't want to mess up the sequences that are not associated
	 * with distributed tables.
	 */
	if (!EnableDDLPropagation || !IsCoordinator())
	{
		PG_RETURN_VOID();
	}

	/* iterate over sequence names to build single command to DROP them all */
	sequenceIterator = array_create_iterator(sequenceNamesArray, 0, NULL);
	while (array_iterate(sequenceIterator, &sequenceNameDatum, &isNull))
	{
		text *sequenceNameText = NULL;
		Oid sequenceOid = InvalidOid;

		if (isNull)
		{
			ereport(ERROR, (errmsg("unexpected NULL sequence name"),
							errcode(ERRCODE_INVALID_PARAMETER_VALUE)));
		}

		sequenceNameText = DatumGetTextP(sequenceNameDatum);
		sequenceOid = ResolveRelationId(sequenceNameText, true);
		if (OidIsValid(sequenceOid))
		{
			/*
			 * This case (e.g., OID is valid) could only happen when a user manually calls
			 * the UDF. So, ensure that the user has right to drop the sequence.
			 *
			 * In case the UDF is called via the DROP trigger, the OID wouldn't be valid since
			 * the trigger is called after DROP happens.
			 */
			EnsureSequenceOwner(sequenceOid);
		}

		/* append command portion if we haven't added any sequence names yet */
		if (dropSeqCommand->len == 0)
		{
			appendStringInfoString(dropSeqCommand, "DROP SEQUENCE IF EXISTS");
		}
		else
		{
			/* otherwise, add a comma to separate subsequent sequence names */
			appendStringInfoChar(dropSeqCommand, ',');
		}

		appendStringInfo(dropSeqCommand, " %s", TextDatumGetCString(sequenceNameText));
	}

	if (dropSeqCommand->len != 0)
	{
		appendStringInfoString(dropSeqCommand, " CASCADE");

		SendCommandToWorkers(WORKERS_WITH_METADATA, DISABLE_DDL_PROPAGATION);
		SendCommandToWorkers(WORKERS_WITH_METADATA, dropSeqCommand->data);
	}

	PG_RETURN_VOID();
}


/*
 * CheckTableSchemaNameForDrop errors out if the current user does not
 * have permission to undistribute the given relation, taking into
 * account that it may be called from the drop trigger. If the table exists,
 * the function rewrites the given table and schema name.
 */
void
CheckTableSchemaNameForDrop(Oid relationId, char **schemaName, char **tableName)
{
	char *tempTableName = get_rel_name(relationId);

	if (tempTableName != NULL)
	{
		/* ensure proper values are used if the table exists */
		Oid schemaId = get_rel_namespace(relationId);
		(*schemaName) = get_namespace_name(schemaId);
		(*tableName) = tempTableName;

		EnsureTableOwner(relationId);
	}
}


/*
 * DropShards drops all given shards in a relation. The id, name and schema
 * for the relation are explicitly provided, since this function may be
 * called when the table is already dropped.
 *
 * We mark shard placements that we couldn't drop as to be deleted later, but
 * we do delete the shard metadadata.
 */
static int
DropShards(Oid relationId, char *schemaName, char *relationName,
		   List *deletableShardIntervalList)
{
	ListCell *shardIntervalCell = NULL;
	int droppedShardCount = 0;

	BeginOrContinueCoordinatedTransaction();

	/* At this point we intentionally decided to not use 2PC for reference tables */
	if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
	{
		CoordinatedTransactionUse2PC();
	}

	foreach(shardIntervalCell, deletableShardIntervalList)
	{
		List *shardPlacementList = NIL;
		ListCell *shardPlacementCell = NULL;
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		char *quotedShardName = NULL;
		char *shardRelationName = pstrdup(relationName);

		Assert(shardInterval->relationId == relationId);

		/* Build shard relation name. */
		AppendShardIdToName(&shardRelationName, shardId);
		quotedShardName = quote_qualified_identifier(schemaName, shardRelationName);

		shardPlacementList = ShardPlacementList(shardId);
		foreach(shardPlacementCell, shardPlacementList)
		{
			ShardPlacement *shardPlacement =
				(ShardPlacement *) lfirst(shardPlacementCell);
			char *workerName = shardPlacement->nodeName;
			uint32 workerPort = shardPlacement->nodePort;
			StringInfo workerDropQuery = makeStringInfo();
			MultiConnection *connection = NULL;
			uint32 connectionFlags = FOR_DDL;

			char storageType = shardInterval->storageType;
			if (storageType == SHARD_STORAGE_TABLE)
			{
				appendStringInfo(workerDropQuery, DROP_REGULAR_TABLE_COMMAND,
								 quotedShardName);
			}
			else if (storageType == SHARD_STORAGE_COLUMNAR ||
					 storageType == SHARD_STORAGE_FOREIGN)
			{
				appendStringInfo(workerDropQuery, DROP_FOREIGN_TABLE_COMMAND,
								 quotedShardName);
			}

			/*
			 * The active DROP SCHEMA/DATABASE ... CASCADE will drop the shard, if we
			 * try to drop it over another connection, we will get into a distributed
			 * deadlock.
			 */
			if (shardPlacement->groupId == COORDINATOR_GROUP_ID &&
				IsCoordinator() &&
				DropSchemaOrDBInProgress())
			{
				DeleteShardPlacementRow(shardPlacement->placementId);
				continue;
			}

			connection = GetPlacementConnection(connectionFlags, shardPlacement,
												NULL);

			RemoteTransactionBeginIfNecessary(connection);

			if (PQstatus(connection->pgConn) != CONNECTION_OK)
			{
				uint64 placementId = shardPlacement->placementId;

				ereport(WARNING, (errmsg("could not connect to shard \"%s\" on node "
										 "\"%s:%u\"", shardRelationName, workerName,
										 workerPort),
								  errdetail("Marking this shard placement for "
											"deletion")));

				UpdateShardPlacementState(placementId, FILE_TO_DELETE);

				continue;
			}

			MarkRemoteTransactionCritical(connection);

			ExecuteCriticalRemoteCommand(connection, workerDropQuery->data);

			DeleteShardPlacementRow(shardPlacement->placementId);
		}

		DeleteShardRow(shardId);
	}

	droppedShardCount = list_length(deletableShardIntervalList);

	return droppedShardCount;
}


/* Checks that delete is only on one table. */
static void
CheckTableCount(Query *deleteQuery)
{
	int rangeTableCount = list_length(deleteQuery->rtable);
	if (rangeTableCount > 1)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot delete from distributed table"),
						errdetail("Delete on multiple tables is not supported")));
	}
}


/* Checks that delete criteria only consists of simple operator expressions. */
static void
CheckDeleteCriteria(Node *deleteCriteria)
{
	bool simpleOpExpression = true;

	if (deleteCriteria == NULL)
	{
		return;
	}

	if (is_opclause(deleteCriteria))
	{
		simpleOpExpression = SimpleOpExpression((Expr *) deleteCriteria);
	}
	else if (IsA(deleteCriteria, BoolExpr))
	{
		ListCell *opExpressionCell = NULL;
		BoolExpr *deleteCriteriaExpression = (BoolExpr *) deleteCriteria;
		List *opExpressionList = deleteCriteriaExpression->args;

		foreach(opExpressionCell, opExpressionList)
		{
			Expr *opExpression = (Expr *) lfirst(opExpressionCell);
			if (!SimpleOpExpression(opExpression))
			{
				simpleOpExpression = false;
				break;
			}
		}
	}
	else
	{
		simpleOpExpression = false;
	}

	if (!simpleOpExpression)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot delete from distributed table"),
						errdetail("Delete query has a complex operator expression")));
	}
}


/*
 * CheckPartitionColumn checks that the given where clause is based only on the
 * partition key of the given relation id.
 */
static void
CheckPartitionColumn(Oid relationId, Node *whereClause)
{
	Var *partitionColumn = DistPartitionKey(relationId);
	ListCell *columnCell = NULL;

	List *columnList = pull_var_clause_default(whereClause);
	foreach(columnCell, columnList)
	{
		Var *var = (Var *) lfirst(columnCell);
		if (var->varattno != partitionColumn->varattno)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot delete from distributed table"),
							errdetail("Where clause includes a column other than "
									  "partition column")));
		}
	}
}


/*
 * ShardsMatchingDeleteCriteria selects shards to be deleted from the shard
 * interval list based on the delete criteria, and returns selected shards in
 * another list. We add a shard to the list if and only if all rows in the shard
 * satisfy the delete criteria. Note that this function does not expect
 * deleteCriteria to be NULL.
 */
static List *
ShardsMatchingDeleteCriteria(Oid relationId, List *shardIntervalList,
							 Node *deleteCriteria)
{
	List *dropShardIntervalList = NIL;
	List *deleteCriteriaList = NIL;
	ListCell *shardIntervalCell = NULL;

	/* build the base expression for constraint */
	Index rangeTableIndex = 1;
	Var *partitionColumn = PartitionColumn(relationId, rangeTableIndex);
	Node *baseConstraint = BuildBaseConstraint(partitionColumn);

	Assert(deleteCriteria != NULL);
	deleteCriteriaList = list_make1(deleteCriteria);

	/* walk over shard list and check if shards can be dropped */
	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		if (shardInterval->minValueExists && shardInterval->maxValueExists)
		{
			List *restrictInfoList = NIL;
			bool dropShard = false;
			BoolExpr *andExpr = NULL;
			Expr *lessThanExpr = NULL;
			Expr *greaterThanExpr = NULL;
			RestrictInfo *lessThanRestrictInfo = NULL;
			RestrictInfo *greaterThanRestrictInfo = NULL;

			/* set the min/max values in the base constraint */
			UpdateConstraint(baseConstraint, shardInterval);

			andExpr = (BoolExpr *) baseConstraint;
			lessThanExpr = (Expr *) linitial(andExpr->args);
			greaterThanExpr = (Expr *) lsecond(andExpr->args);

			lessThanRestrictInfo = make_simple_restrictinfo(lessThanExpr);
			greaterThanRestrictInfo = make_simple_restrictinfo(greaterThanExpr);

			restrictInfoList = lappend(restrictInfoList, lessThanRestrictInfo);
			restrictInfoList = lappend(restrictInfoList, greaterThanRestrictInfo);

			dropShard = predicate_implied_by(deleteCriteriaList, restrictInfoList, false);
			if (dropShard)
			{
				dropShardIntervalList = lappend(dropShardIntervalList, shardInterval);
				ereport(DEBUG2, (errmsg("delete criteria includes shardId "
										UINT64_FORMAT, shardInterval->shardId)));
			}
		}
	}

	return dropShardIntervalList;
}
