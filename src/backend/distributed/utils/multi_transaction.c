/*-------------------------------------------------------------------------
 *
 * multi_transaction.c
 *     This file contains functions for managing 1PC or 2PC transactions
 *     across many shard placements.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "distributed/connection_cache.h"
#include "distributed/multi_transaction.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


#define INITIAL_CONNECTION_CACHE_SIZE 1001


/* Local functions forward declarations */
static uint32 DistributedTransactionId = 0;


/* Local functions forward declarations */
static StringInfo BuildTransactionName(int connectionId);


/* the commit protocol to use for COPY commands */
int MultiShardCommitProtocol = COMMIT_PROTOCOL_1PC;


/*
 * InitializeDistributedTransaction prepares the distributed transaction ID
 * used in transaction names.
 */
void
InitializeDistributedTransaction(void)
{
	DistributedTransactionId++;
}


/*
 * PrepareRemoteTransactions prepares all transactions on connections in
 * connectionList for commit if the 2PC commit protocol is enabled.
 * On failure, it reports an error and stops.
 */
void
PrepareRemoteTransactions(List *connectionList)
{
	ListCell *connectionCell = NULL;

	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;
		int64 connectionId = transactionConnection->connectionId;

		PGresult *result = NULL;
		StringInfo command = makeStringInfo();
		StringInfo transactionName = BuildTransactionName(connectionId);

		appendStringInfo(command, "PREPARE TRANSACTION '%s'", transactionName->data);

		result = PQexec(connection, command->data);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			/* a failure to prepare is an implicit rollback */
			transactionConnection->transactionState = TRANSACTION_STATE_CLOSED;

			WarnRemoteError(connection, result);
			PQclear(result);

			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
							errmsg("failed to prepare transaction")));
		}

		PQclear(result);

		transactionConnection->transactionState = TRANSACTION_STATE_PREPARED;
	}
}


/*
 * AbortRemoteTransactions aborts all transactions on connections in connectionList.
 * On failure, it reports a warning and continues to abort all of them.
 */
void
AbortRemoteTransactions(List *connectionList)
{
	ListCell *connectionCell = NULL;

	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;
		int64 connectionId = transactionConnection->connectionId;
		PGresult *result = NULL;

		if (transactionConnection->transactionState == TRANSACTION_STATE_PREPARED)
		{
			StringInfo command = makeStringInfo();
			StringInfo transactionName = BuildTransactionName(connectionId);

			appendStringInfo(command, "ROLLBACK PREPARED '%s'", transactionName->data);

			result = PQexec(connection, command->data);
			if (PQresultStatus(result) != PGRES_COMMAND_OK)
			{
				char *nodeName = ConnectionGetOptionValue(connection, "host");
				char *nodePort = ConnectionGetOptionValue(connection, "port");

				/* log a warning so the user may abort the transaction later */
				ereport(WARNING, (errmsg("failed to roll back prepared transaction '%s'",
										 transactionName->data),
								  errhint("Run \"%s\" on %s:%s",
										  command->data, nodeName, nodePort)));
			}

			PQclear(result);
		}
		else if (transactionConnection->transactionState == TRANSACTION_STATE_OPEN)
		{
			/* try to roll back cleanly, if it fails then we won't commit anyway */
			result = PQexec(connection, "ROLLBACK");
			PQclear(result);
		}

		transactionConnection->transactionState = TRANSACTION_STATE_CLOSED;
	}
}


/*
 * CommitRemoteTransactions commits all transactions on connections in connectionList.
 * If stopOnFailure is true, then CommitRemoteTransactions reports an error on
 * failure, otherwise it reports a warning.
 * Note that if the caller of this function wants the transactions to roll back
 * on a failing commit, stopOnFailure should be used as true. On the other hand,
 * if the caller does not want the transactions to roll back on a failing commit,
 * stopOnFailure should be used as false.
 */
void
CommitRemoteTransactions(List *connectionList, bool stopOnFailure)
{
	ListCell *connectionCell = NULL;

	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;
		int64 connectionId = transactionConnection->connectionId;
		PGresult *result = NULL;

		if (transactionConnection->transactionState == TRANSACTION_STATE_PREPARED)
		{
			StringInfo command = makeStringInfo();
			StringInfo transactionName = BuildTransactionName(connectionId);

			/* we shouldn't be committing if any transactions are not prepared */
			Assert(transactionConnection->transactionState == TRANSACTION_STATE_PREPARED);

			appendStringInfo(command, "COMMIT PREPARED '%s'", transactionName->data);

			result = PQexec(connection, command->data);
			if (PQresultStatus(result) != PGRES_COMMAND_OK)
			{
				char *nodeName = ConnectionGetOptionValue(connection, "host");
				char *nodePort = ConnectionGetOptionValue(connection, "port");

				/*
				 * If stopOnFailure is false, log a warning so the user may
				 * commit the transaction later.
				 */
				if (stopOnFailure)
				{
					ereport(ERROR, (errmsg("failed to commit prepared transaction '%s'",
										   transactionName->data),
									errhint("Run \"%s\" on %s:%s",
											command->data, nodeName, nodePort)));
				}
				else
				{
					ereport(WARNING, (errmsg("failed to commit prepared transaction '%s'",
											 transactionName->data),
									  errhint("Run \"%s\" on %s:%s",
											  command->data, nodeName, nodePort)));
				}
			}
		}
		else
		{
			/* we shouldn't be committing if any transactions are not open */
			Assert(transactionConnection->transactionState == TRANSACTION_STATE_OPEN);

			/*
			 * Try to commit, if it fails and stopOnFailure is false then
			 * the user might lose data.
			 */
			result = PQexec(connection, "COMMIT");
			if (PQresultStatus(result) != PGRES_COMMAND_OK)
			{
				char *nodeName = ConnectionGetOptionValue(connection, "host");
				char *nodePort = ConnectionGetOptionValue(connection, "port");

				if (stopOnFailure)
				{
					ereport(ERROR, (errmsg("failed to commit transaction on %s:%s",
										   nodeName, nodePort)));
				}
				else
				{
					ereport(WARNING, (errmsg("failed to commit transaction on %s:%s",
											 nodeName, nodePort)));
				}
			}
		}

		PQclear(result);

		transactionConnection->transactionState = TRANSACTION_STATE_CLOSED;
	}
}


/*
 * BuildTransactionName constructs a transaction name that ensures there are no
 * collisions with concurrent transactions by the same master node, subsequent
 * transactions by the same backend, or transactions on a different shard.
 *
 * Collisions may occur over time if transactions fail to commit or abort and
 * are left to linger. This would cause a PREPARE failure for the second
 * transaction, which causes it to be rolled back. In general, the user
 * should ensure that prepared transactions do not linger.
 */
static StringInfo
BuildTransactionName(int connectionId)
{
	StringInfo commandString = makeStringInfo();

	appendStringInfo(commandString, "citus_%d_%u_%d", MyProcPid,
					 DistributedTransactionId, connectionId);

	return commandString;
}


/*
 * CloseConnections closes all connections in connectionList.
 */
void
CloseConnections(List *connectionList)
{
	ListCell *connectionCell = NULL;

	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;

		PQfinish(connection);
	}
}


/*
 * CreateShardConnectionHash constructs a hash table used for shardId->Connection
 * mapping.
 */
HTAB *
CreateShardConnectionHash(void)
{
	HTAB *shardConnectionsHash = NULL;
	int hashFlags = 0;
	HASHCTL info;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(int64);
	info.entrysize = sizeof(ShardConnections);
	info.hash = tag_hash;

	hashFlags = HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT;
	shardConnectionsHash = hash_create("Shard Connections Hash",
									   INITIAL_CONNECTION_CACHE_SIZE, &info,
									   hashFlags);

	return shardConnectionsHash;
}


/*
 * GetShardConnections finds existing connections for a shard in the hash.
 * If not found, then a ShardConnections structure with empty connectionList
 * is returned.
 */
ShardConnections *
GetShardConnections(HTAB *shardConnectionHash, int64 shardId,
					bool *shardConnectionsFound)
{
	ShardConnections *shardConnections = NULL;

	shardConnections = (ShardConnections *) hash_search(shardConnectionHash,
														&shardId,
														HASH_ENTER,
														shardConnectionsFound);
	if (!*shardConnectionsFound)
	{
		shardConnections->shardId = shardId;
		shardConnections->connectionList = NIL;
	}

	return shardConnections;
}


/*
 * ConnectionList flattens the connection hash to a list of placement connections.
 */
List *
ConnectionList(HTAB *connectionHash)
{
	List *connectionList = NIL;
	HASH_SEQ_STATUS status;
	ShardConnections *shardConnections = NULL;

	hash_seq_init(&status, connectionHash);

	shardConnections = (ShardConnections *) hash_seq_search(&status);
	while (shardConnections != NULL)
	{
		List *shardConnectionsList = list_copy(shardConnections->connectionList);
		connectionList = list_concat(connectionList, shardConnectionsList);

		shardConnections = (ShardConnections *) hash_seq_search(&status);
	}

	return connectionList;
}
