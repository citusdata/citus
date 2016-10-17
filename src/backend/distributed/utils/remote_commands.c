/*-------------------------------------------------------------------------
 *
 * remote_commands.c
 *   Helpers to make it easier to execute command on remote nodes.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq-fe.h"

#include "distributed/connection_management.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "utils/builtins.h"
#include "utils/int8.h"
#include "utils/lsyscache.h"


/* GUC, determining whether statements sent to remote nodes are logged */
bool LogRemoteCommands = false;


static BatchCommand ** BatchCommandListToArray(List *batchCommandList);
static int CompareBatchCommands(const void *leftElement, const void *rightElement);
static void HandlePlacementFailures(List *goodPlacements, List *failedPlacements);


/* simple helpers */

/*
 * IsResponseOK checks whether the result is a successful one.
 */
bool
IsResponseOK(PGresult *result)
{
	ExecStatusType resultStatus = PQresultStatus(result);

	if (resultStatus == PGRES_SINGLE_TUPLE || resultStatus == PGRES_TUPLES_OK ||
		resultStatus == PGRES_COMMAND_OK)
	{
		return true;
	}

	return false;
}


/*
 * Clear connection from current activity.
 *
 * FIXME: This probably should use PQcancel() if results would require network
 * IO.
 */
void
ForgetResults(MultiConnection *connection)
{
	while (true)
	{
		PGresult *result = NULL;
		result = PQgetResult(connection->conn);
		if (result == NULL)
		{
			break;
		}
		if (PQresultStatus(result) == PGRES_COPY_IN)
		{
			PQputCopyEnd(connection->conn, NULL);

			/* FIXME: mark connection as failed? */
		}
		PQclear(result);
	}
}


/*
 * SqlStateMatchesCategory returns true if the given sql state (which may be
 * NULL if unknown) is in the given error category. Note that we use
 * ERRCODE_TO_CATEGORY macro to determine error category of the sql state and
 * expect the caller to use the same macro for the error category.
 */
bool
SqlStateMatchesCategory(char *sqlStateString, int category)
{
	bool sqlStateMatchesCategory = false;
	int sqlState = 0;
	int sqlStateCategory = 0;

	if (sqlStateString == NULL)
	{
		return false;
	}

	sqlState = MAKE_SQLSTATE(sqlStateString[0], sqlStateString[1], sqlStateString[2],
							 sqlStateString[3], sqlStateString[4]);

	sqlStateCategory = ERRCODE_TO_CATEGORY(sqlState);
	if (sqlStateCategory == category)
	{
		sqlStateMatchesCategory = true;
	}

	return sqlStateMatchesCategory;
}


/* report errors & warnings */

/*
 * Report libpq failure that's not associated with a result.
 */
void
ReportConnectionError(MultiConnection *connection, int elevel)
{
	char *nodeName = connection->hostname;
	int nodePort = connection->port;

	ereport(elevel, (errmsg("connection error: %s:%d", nodeName, nodePort),
					 errdetail("%s", PQerrorMessage(connection->conn))));
}


/*
 * Report libpq failure associated with a result.
 */
void
ReportResultError(MultiConnection *connection, PGresult *result, int elevel)
{
	char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	char *messagePrimary = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
	char *messageDetail = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);
	char *messageHint = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT);
	char *messageContext = PQresultErrorField(result, PG_DIAG_CONTEXT);

	char *nodeName = connection->hostname;
	int nodePort = connection->port;
	int sqlState = ERRCODE_INTERNAL_ERROR;

	if (sqlStateString != NULL)
	{
		sqlState = MAKE_SQLSTATE(sqlStateString[0], sqlStateString[1], sqlStateString[2],
								 sqlStateString[3], sqlStateString[4]);
	}

	/*
	 * If the PGresult did not contain a message, the connection may provide a
	 * suitable top level one. At worst, this is an empty string.
	 */
	if (messagePrimary == NULL)
	{
		char *lastNewlineIndex = NULL;

		messagePrimary = PQerrorMessage(connection->conn);
		lastNewlineIndex = strrchr(messagePrimary, '\n');

		/* trim trailing newline, if any */
		if (lastNewlineIndex != NULL)
		{
			*lastNewlineIndex = '\0';
		}
	}

	ereport(elevel, (errcode(sqlState), errmsg("%s", messagePrimary),
					 messageDetail ? errdetail("%s", messageDetail) : 0,
					 messageHint ? errhint("%s", messageHint) : 0,
					 messageContext ? errcontext("%s", messageContext) : 0,
					 errcontext("while executing command on %s:%d",
								nodeName, nodePort)));
}


/*
 * Log commands send to remote nodes if citus.log_remote_commands wants us to
 * do so.
 */
void
LogRemoteCommand(MultiConnection *connection, const char *command)
{
	if (!LogRemoteCommands)
	{
		return;
	}

	ereport(LOG, (errmsg("issuing %s", command),
				  errdetail("on server %s:%d", connection->hostname, connection->port)));
}


/* wrappers around libpq functions, with command logging support */

/*
 * Tiny PQsendQuery wrapper that logs remote commands, and accepts a
 * MultiConnection instead of a plain PGconn.
 */
int
SendRemoteCommand(MultiConnection *connection, const char *command)
{
	LogRemoteCommand(connection, command);
	return PQsendQuery(connection->conn, command);
}


/*
 * Execute a statement over the connection. Basically equivalent to PQexec(),
 * except for logging and error handling integration.
 *
 * NULL is returned upon errors, the query's results otherwise.
 */
PGresult *
ExecuteStatement(MultiConnection *connection, const char *statement)
{
	return ExecuteStatementParams(connection, statement, 0, NULL, NULL);
}


/*
 * Execute a statement over the connection. Basically equivalent to
 * PQexecParams(), except for logging and error handling integration.
 *
 * NULL is returned upon errors, the query's results otherwise.
 */
PGresult *
ExecuteStatementParams(MultiConnection *connection, const char *statement,
					   int paramCount, const Oid *paramTypes,
					   const char *const *paramValues)
{
	PGresult *result = NULL;

	AdjustRemoteTransactionState(connection);

	if (connection->remoteTransaction.transactionFailed)
	{
		return NULL;
	}

	LogRemoteCommand(connection, statement);
	if (!PQsendQueryParams(connection->conn, statement, paramCount, paramTypes,
						   paramValues, NULL, NULL, 0))
	{
		ReportConnectionError(connection, WARNING);
		MarkRemoteTransactionFailed(connection, true);
		return NULL;
	}

	result = PQgetResult(connection->conn);

	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, WARNING);
		MarkRemoteTransactionFailed(connection, true);

		PQclear(result);
		result = PQgetResult(connection->conn);
		Assert(result == NULL);

		return NULL;
	}

	return result;
}


/*
 * Execute a statement over the connection. Basically equivalent to PQexec(),
 * except for logging and error handling integration.
 *
 * Returns true if the command succeeded, false otherwise.
 */
bool
ExecuteCheckStatement(MultiConnection *connection, const char *statement)
{
	return ExecuteCheckStatementParams(connection, statement, 0, NULL, NULL);
}


/*
 * Execute a statement over the connection. Basically equivalent to
 * PQexecParams(), except for logging and error handling integration.
 *
 * Returns true if the command succeeded, false otherwise.
 */
bool
ExecuteCheckStatementParams(MultiConnection *connection, const char *statement,
							int paramCount, const Oid *paramTypes,
							const char *const *paramValues)
{
	bool resultOk = false;
	PGresult *result = ExecuteStatementParams(connection, statement, paramCount,
											  paramTypes, paramValues);

	resultOk = result != NULL;
	PQclear(result);

	result = PQgetResult(connection->conn);
	Assert(result == NULL);

	return resultOk;
}


/* -------------------------------------------------------------------------
 * Higher level command execution functions
 * -------------------------------------------------------------------------
 */

/*
 * Execute placement associated commands in parallel.
 *
 * TODO: Use less than one one connection per placement.
 */
void
ExecuteBatchCommands(List *batchCommandList)
{
	List *connectionList = NIL;
	int64 ncommands = list_length(batchCommandList);
	BatchCommand **batchCommands = NULL;
	int i = 0;

	/* convert into usefully sorted array */
	batchCommands = BatchCommandListToArray(batchCommandList);

	/*
	 * Initiate connection establishment if necessary. All connections might
	 * be already existing and, possibly, fully established.
	 */
	for (i = 0; i < ncommands; i++)
	{
		BatchCommand *command = batchCommands[i];
		ShardPlacement *placement = command->placement;
		MultiConnection *connection = NULL;

		/* asynchronously open connection to remote node */
		connection =
			StartPlacementConnection(command->connectionFlags,
									 placement);

		/* couldn't work with that */
		Assert(PQtransactionStatus(connection->conn) != PQTRANS_ACTIVE);

		/* every command should get its own connection for now */
		ClaimConnectionExclusively(connection);

		command->connection = connection;
		connectionList = lappend(connectionList, connection);
	}

	/* wait for connection establishment */
	for (i = 0; i < ncommands; i++)
	{
		BatchCommand *command = batchCommands[i];

		/*
		 * It'd better to wait for all connections at once. Especially when
		 * SSL (or complex authentication protocols), it's quite beneficial to
		 * do connection establishment fully in parallel using nonblocking
		 * IO. This way we'll currently do the initial connect() in parallel,
		 * but afterwards block in SSL connection establishment, which often
		 * takes the bulk of the time.
		 */
		FinishConnectionEstablishment(command->connection);
	}

	/* BEGIN transaction if necessary */
	AdjustRemoteTransactionStates(connectionList);

	/* Finally send commands to all connections in parallel */
	for (i = 0; i < ncommands; i++)
	{
		BatchCommand *command = batchCommands[i];
		MultiConnection *connection = command->connection;

		if (connection->remoteTransaction.transactionFailed)
		{
			continue;
		}

		if (!SendRemoteCommand(connection, command->commandString))
		{
			ReportConnectionError(connection, WARNING);
			MarkRemoteTransactionFailed(connection, true);
		}
	}

	/*
	 * Wait for command results to come in.
	 *
	 * TODO: We should really wait asynchronously, using nonblocking IO, on
	 * all these connections. As long as they all only tranfer miniscule
	 * amounts of data, it doesn't matter much, but as soon that that's not
	 * the case...
	 */
	for (i = 0; i < ncommands; i++)
	{
		BatchCommand *command = batchCommands[i];
		MultiConnection *connection = command->connection;
		PGresult *result = NULL;

		result = PQgetResult(connection->conn);

		if (!IsResponseOK(result))
		{
			connection->remoteTransaction.transactionFailed = true;
			command->failed = true;

			ReportResultError(connection, result, WARNING);
			MarkRemoteTransactionFailed(connection, true);
		}
		else
		{
			char *affectedTuples = PQcmdTuples(result);
			if (strlen(affectedTuples) > 0)
			{
				scanint8(affectedTuples, false, &command->tuples);
			}

			command->failed = false;
		}

		/* XXX: allow for result processing? */
		PQclear(result);

		/* clear NULL result(s) */
		ForgetResults(connection);

		/* allow connection to be used again */
		UnclaimConnection(connection);
	}
}


/*
 * Deparse and execute query on all finalized placements for the shards in
 * shardIntervalList.
 *
 * Failed placements are marked as invalid, unless all placements for a shard
 * fail.
 *
 * Returns the number of modified tuples.
 */
int64
ExecuteQueryOnPlacements(Query *query, List *shardIntervalList, Oid relationId)
{
	List *commandList = NIL;
	ListCell *intervalCell = NULL;
	ListCell *commandCell = NULL;
	int64 ntuples = 0;
	int64 lastSuccessfulShardId = INVALID_SHARD_ID;

	foreach(intervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(intervalCell);
		List *shardPlacementList = FinalizedShardPlacementList(shardInterval->shardId);
		ListCell *placementCell = NULL;
		StringInfoData shardQueryString;

		initStringInfo(&shardQueryString);

		deparse_shard_query(query, relationId, shardInterval->shardId, &shardQueryString);

		foreach(placementCell, shardPlacementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
			BatchCommand *command = (BatchCommand *) palloc0(sizeof(BatchCommand));

			command->placement = placement;
			command->connectionFlags = NEW_CONNECTION | CACHED_CONNECTION | FOR_DML;
			command->commandString = shardQueryString.data;

			commandList = lappend(commandList, command);
		}
	}

	ExecuteBatchCommands(commandList);
	InvalidateFailedPlacements(commandList);

	foreach(commandCell, commandList)
	{
		BatchCommand *command = (BatchCommand *) lfirst(commandCell);
		ShardPlacement *placement = command->placement;

		if (!command->failed)
		{
			if (lastSuccessfulShardId != placement->shardId)
			{
				ntuples += command->tuples;
			}
			lastSuccessfulShardId = placement->shardId;
		}
	}

	return ntuples;
}


/*
 * Execute DDL on all finalized placements. All errors abort the command,
 * i.e. shards are not marked as invalid (to avoid schema divergence).
 */
void
ExecuteDDLOnRelationPlacements(Oid relationId, const char *command)
{
	/* FIXME: for correct locking we need to acquire metadata locks before */
	List *shardIntervalList = LoadShardIntervalList(relationId);
	Oid schemaId = get_rel_namespace(relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *escapedSchemaName = quote_literal_cstr(schemaName);
	char *escapedCommandString = quote_literal_cstr(command);
	List *commandList = NIL;
	StringInfo applyCommand = makeStringInfo();
	ListCell *intervalCell = NULL;

	BeginOrContinueCoordinatedTransaction();

	LockShards(shardIntervalList, ShareLock);

	foreach(intervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(intervalCell);
		List *placementList = FinalizedShardPlacementList(shardInterval->shardId);
		uint64 shardId = shardInterval->shardId;
		ListCell *placementCell = NULL;

		/* build the shard ddl command -- perhaps add parametrized variant instead? */
		appendStringInfo(applyCommand, WORKER_APPLY_SHARD_DDL_COMMAND, shardId,
						 escapedSchemaName, escapedCommandString);

		foreach(placementCell, placementList)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
			BatchCommand *command = (BatchCommand *) palloc0(sizeof(BatchCommand));

			command->placement = placement;
			command->connectionFlags = NEW_CONNECTION | CACHED_CONNECTION |
									   FOR_DDL | CRITICAL_CONNECTION;
			command->commandString = pstrdup(applyCommand->data);

			commandList = lappend(commandList, command);
		}

		resetStringInfo(applyCommand);
	}

	ExecuteBatchCommands(commandList);
}


/*
 * Mark placements that failed in ExecuteBatchCommands as invalid, unless all
 * placements in a shard failed.
 */
void
InvalidateFailedPlacements(List *batchCommandList)
{
	BatchCommand **batchCommands = NULL;
	int i = 0;
	int64 lastShardId = INVALID_SHARD_ID;
	List *failedPlacements = NIL;
	List *goodPlacements = NIL;
	int64 ncommands = list_length(batchCommandList);

	/* convert into usefully sorted array */
	batchCommands = BatchCommandListToArray(batchCommandList);

	for (i = 0; i < ncommands; i++)
	{
		BatchCommand *command = batchCommands[i];
		ShardPlacement *placement = command->placement;

		/*
		 * If we're looking at the next shard, check whether some or all of
		 * the placements failed, and need to be marked as invalid.
		 */
		if (lastShardId != INVALID_SHARD_ID && lastShardId != placement->shardId)
		{
			HandlePlacementFailures(goodPlacements, failedPlacements);
			failedPlacements = NIL;
			goodPlacements = NIL;
		}

		if (command->failed)
		{
			failedPlacements = lappend(failedPlacements, placement);
		}
		else
		{
			goodPlacements = lappend(goodPlacements, placement);
		}
	}

	HandlePlacementFailures(goodPlacements, failedPlacements);
}


/*
 * Convert list of BatchCommands to a sorted array of BatchCommand*s.
 */
static BatchCommand **
BatchCommandListToArray(List *batchCommandList)
{
	int64 ncommands = list_length(batchCommandList);
	ListCell *commandCell = NULL;
	BatchCommand **commands = NULL;
	int off = 0;

	commands = (BatchCommand **) palloc(sizeof(BatchCommand *) * ncommands);

	foreach(commandCell, batchCommandList)
	{
		commands[off++] = (BatchCommand *) lfirst(commandCell);
	}

	qsort(commands, ncommands, sizeof(BatchCommand *),
		  CompareBatchCommands);

	return commands;
}


/*
 * Sorting helper for BatchCommand's. Sorts in a way that guarantees that all
 * placements for a shard are consecutive.
 */
static int
CompareBatchCommands(const void *leftElement, const void *rightElement)
{
	const BatchCommand *leftCommand = *((const BatchCommand **) leftElement);
	const BatchCommand *rightCommand = *((const BatchCommand **) rightElement);
	const ShardPlacement *leftPlacement = leftCommand->placement;
	const ShardPlacement *rightPlacement = rightCommand->placement;
	int compare = 0;

	if (leftPlacement->shardId < rightPlacement->shardId)
	{
		return -1;
	}

	if (leftPlacement->shardId > rightPlacement->shardId)
	{
		return 1;
	}

	compare = strcmp(leftPlacement->nodeName, rightPlacement->nodeName);
	if (compare != 0)
	{
		return compare;
	}
	if (leftPlacement->nodePort < rightPlacement->nodePort)
	{
		return -1;
	}
	if (leftPlacement->nodePort > rightPlacement->nodePort)
	{
		return 1;
	}

	if (leftPlacement->placementId < rightPlacement->placementId)
	{
		return -1;
	}

	if (leftPlacement->placementId > rightPlacement->placementId)
	{
		return 1;
	}

	/* other elements irrelevant for our purpose */

	return 0;
}


/*
 * Helper for InvalidateFailedPlacements.
 */
static void
HandlePlacementFailures(List *goodPlacements, List *failedPlacements)
{
	if (list_length(failedPlacements) > 0 &&
		list_length(goodPlacements) == 0)
	{
		elog(ERROR, "all placements failed");
	}
	else if (list_length(failedPlacements) > 0)
	{
		ListCell *placementCell = NULL;

		elog(LOG, "some placements failed, marking as invalid");

		foreach(placementCell, failedPlacements)
		{
			ShardPlacement *placement = (ShardPlacement *) lfirst(placementCell);
			UpdateShardPlacementState(placement->placementId, FILE_INACTIVE);
		}
	}
}
