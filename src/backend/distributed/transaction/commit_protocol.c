/*-------------------------------------------------------------------------
 *
 * commit_protocol.c
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

#include "distributed/commit_protocol.h"
#include "distributed/connection_cache.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/multi_shard_transaction.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"


/* Local functions forward declarations */
static uint32 DistributedTransactionId = 0;


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

		ereport(DEBUG2, (errmsg("sent PREPARE TRANSACTION over connection %ld",
								connectionId)));

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

			ereport(DEBUG2, (errmsg("sent ROLLBACK over connection %ld", connectionId)));

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

			ereport(DEBUG2, (errmsg("sent COMMIT PREPARED over connection %ld",
									connectionId)));
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

			ereport(DEBUG2, (errmsg("sent COMMIT over connection %ld", connectionId)));
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
StringInfo
BuildTransactionName(int connectionId)
{
	StringInfo commandString = makeStringInfo();

	appendStringInfo(commandString, "citus_%d_%u_%d", MyProcPid,
					 DistributedTransactionId, connectionId);

	return commandString;
}
