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
#include "miscadmin.h"
#include "port.h"

#include <stddef.h>

#include "access/xact.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "distributed/connection_management.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_server_executor.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/remote_commands.h"
#include "distributed/relay_utility.h"
#include "distributed/worker_protocol.h"
#include "lib/stringinfo.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "optimizer/clauses.h"
#include "optimizer/predtest.h"
#include "optimizer/restrictinfo.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"


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
	LOCKTAG lockTag;
	bool sessionLock = false;
	bool dontWait = false;
	char partitionMethod = 0;
	bool failOK = false;
	bool isTopLevel = true;

	PreventTransactionChain(isTopLevel, "master_apply_delete_command");

	queryTreeNode = ParseTreeNode(queryString);
	if (!IsA(queryTreeNode, DeleteStmt))
	{
		ereport(ERROR, (errmsg("query \"%s\" is not a delete statement",
							   queryString)));
	}

	deleteStatement = (DeleteStmt *) queryTreeNode;

	schemaName = deleteStatement->relation->schemaname;
	relationName = deleteStatement->relation->relname;
	relationId = RangeVarGetRelid(deleteStatement->relation, NoLock, failOK);

	/* schema-prefix if it is not specified already */
	if (schemaName == NULL)
	{
		Oid schemaId = get_rel_namespace(relationId);
		schemaName = get_namespace_name(schemaId);
	}

	CheckDistributedTable(relationId);
	EnsureTablePermissions(relationId, ACL_DELETE);

	queryTreeList = pg_analyze_and_rewrite(queryTreeNode, queryString, NULL, 0);
	deleteQuery = (Query *) linitial(queryTreeList);
	CheckTableCount(deleteQuery);

	/* get where clause and flatten it */
	whereClause = (Node *) deleteQuery->jointree->quals;
	deleteCriteria = eval_const_expressions(NULL, whereClause);

	partitionMethod = PartitionMethod(relationId);
	if ((partitionMethod == DISTRIBUTE_BY_HASH) && (deleteCriteria != NULL))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot delete from distributed table"),
						errdetail("Delete statements on hash-partitioned tables "
								  "with where clause is not supported")));
	}

	CheckDeleteCriteria(deleteCriteria);
	CheckPartitionColumn(relationId, deleteCriteria);

	/* acquire lock */
	SET_LOCKTAG_ADVISORY(lockTag, MyDatabaseId, relationId, 0, 0);
	LockAcquire(&lockTag, ExclusiveLock, sessionLock, dontWait);

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

	char *schemaName = NULL;
	char *relationName = NULL;
	List *shardIntervalList = NIL;
	int droppedShardCount = 0;

	BeginOrContinueCoordinatedTransaction();

	relationName = get_rel_name(relationId);

	if (relationName != NULL)
	{
		/* ensure proper values are used if the table exists */
		Oid schemaId = get_rel_namespace(relationId);
		schemaName = get_namespace_name(schemaId);

		/*
		 * Only allow the owner to drop all shards, this is more akin to DDL
		 * than DELETE.
		 */
		EnsureTableOwner(relationId);
	}
	else
	{
		/* table has been dropped, rely on user-supplied values */
		schemaName = text_to_cstring(schemaNameText);
		relationName = text_to_cstring(relationNameText);

		/*
		 * Verify that this only is run as superuser - that's how it's used in
		 * our drop event trigger, and we can't verify permissions for an
		 * already dropped relation.
		 */
		if (!superuser())
		{
			ereport(ERROR, (errmsg("cannot drop all shards of a dropped table as "
								   "non-superuser")));
		}
	}

	shardIntervalList = LoadShardIntervalList(relationId);
	droppedShardCount = DropShards(relationId, schemaName, relationName,
								   shardIntervalList);

	PG_RETURN_INT32(droppedShardCount);
}


/*
 * master_drop_sequences attempts to drop a list of sequences on a specified
 * node. The "IF EXISTS" clause is used to permit dropping sequences even if
 * they may not exist. Returns true on success, false on failure.
 */
Datum
master_drop_sequences(PG_FUNCTION_ARGS)
{
	ArrayType *sequenceNamesArray = PG_GETARG_ARRAYTYPE_P(0);
	text *nodeText = PG_GETARG_TEXT_P(1);
	int64 nodePort = PG_GETARG_INT64(2);
	bool dropSuccessful = false;
	char *nodeName = TextDatumGetCString(nodeText);

	ArrayIterator sequenceIterator = NULL;
	Datum sequenceText = 0;
	bool isNull = false;
	MultiConnection *connection = NULL;
	StringInfo dropSeqCommand = makeStringInfo();

	BeginOrContinueCoordinatedTransaction();

	/* iterate over sequence names to build single command to DROP them all */
	sequenceIterator = array_create_iterator(sequenceNamesArray, 0, NULL);
	while (array_iterate(sequenceIterator, &sequenceText, &isNull))
	{
		if (isNull)
		{
			ereport(ERROR, (errmsg("unexpected NULL sequence name"),
							errcode(ERRCODE_INVALID_PARAMETER_VALUE)));
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

		appendStringInfo(dropSeqCommand, " %s", TextDatumGetCString(sequenceText));
	}

	connection = GetNodeConnection(NEW_CONNECTION | CACHED_CONNECTION,
								   nodeName, nodePort);
	dropSuccessful = ExecuteCheckStatement(connection, dropSeqCommand->data);
	if (!dropSuccessful)
	{
		ereport(WARNING, (errmsg("could not delete sequences from node \"%s:" INT64_FORMAT
								 "\"", nodeName, nodePort)));
	}

	PG_RETURN_BOOL(dropSuccessful);
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
	List *commandList = NIL;
	ListCell *commandCell = NULL;

	BeginOrContinueCoordinatedTransaction();

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
			ShardPlacement *placement = (ShardPlacement *) lfirst(shardPlacementCell);
			BatchCommand *command = (BatchCommand *) palloc0(sizeof(BatchCommand));
			StringInfo workerDropQuery = makeStringInfo();
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

			command->placement = placement;
			command->connectionFlags = NEW_CONNECTION | CACHED_CONNECTION | FOR_DDL;
			command->commandString = workerDropQuery->data;
			command->userData = shardRelationName; /* for failure reporting */

			commandList = lappend(commandList, command);
		}

		DeleteShardRow(shardId);
	}

	ExecuteBatchCommands(commandList);

	foreach(commandCell, commandList)
	{
		BatchCommand *command = (BatchCommand *) lfirst(commandCell);
		ShardPlacement *placement = command->placement;
		uint64 shardId = placement->shardId;
		uint64 placementId = placement->placementId;
		char *workerName = placement->nodeName;
		uint32 workerPort = placement->nodePort;
		uint64 oldShardLength = placement->shardLength;
		const char *shardName = command->userData;

		/* mark shard placements that we couldn't drop as to be deleted */
		if (command->failed)
		{
			DeleteShardPlacementRow(shardId, workerName, workerPort);
			InsertShardPlacementRow(shardId, placementId, FILE_TO_DELETE, oldShardLength,
									workerName, workerPort);

			ereport(WARNING, (errmsg("could not delete shard \"%s\" on node \"%s:%u\"",
									 shardName, workerName, workerPort),
							  errdetail("Marking this shard placement for deletion")));
		}
		else
		{
			DeleteShardPlacementRow(shardId, workerName, workerPort);
		}
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
	Var *partitionColumn = PartitionKey(relationId);
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

			dropShard = predicate_implied_by(deleteCriteriaList, restrictInfoList);
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
