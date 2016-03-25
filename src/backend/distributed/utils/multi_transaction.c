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


/* Local functions forward declarations */
static StringInfo BuildTransactionName(int connectionId);


/*
 * PrepareRemoteTransactions prepares all transactions on connections in
 * connectionList for commit if the 2PC transaction manager is enabled.
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

			ReportRemoteError(connection, result);
			PQclear(result);

			ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
							errmsg("Failed to prepare transaction")));
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
		char *nodeName = ConnectionGetOptionValue(connection, "host");
		char *nodePort = ConnectionGetOptionValue(connection, "port");
		PGresult *result = NULL;

		if (transactionConnection->transactionState == TRANSACTION_STATE_PREPARED)
		{
			StringInfo command = makeStringInfo();
			StringInfo transactionName = BuildTransactionName(connectionId);

			appendStringInfo(command, "ROLLBACK PREPARED '%s'", transactionName->data);

			result = PQexec(connection, command->data);
			if (PQresultStatus(result) != PGRES_COMMAND_OK)
			{
				/* log a warning so the user may abort the transaction later */
				ereport(WARNING, (errmsg("Failed to roll back prepared transaction '%s'",
										 transactionName->data),
								  errhint("Run ROLLBACK TRANSACTION '%s' on %s:%s",
										  transactionName->data, nodeName, nodePort)));
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
 * On failure, it reports a warning and continues committing all of them.
 */
void
CommitRemoteTransactions(List *connectionList)
{
	ListCell *connectionCell = NULL;

	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);
		PGconn *connection = transactionConnection->connection;
		int64 connectionId = transactionConnection->connectionId;
		char *nodeName = ConnectionGetOptionValue(connection, "host");
		char *nodePort = ConnectionGetOptionValue(connection, "port");
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
				/* log a warning so the user may commit the transaction later */
				ereport(WARNING, (errmsg("Failed to commit prepared transaction '%s'",
										 transactionName->data),
								  errhint("Run COMMIT TRANSACTION '%s' on %s:%s",
										  transactionName->data, nodeName, nodePort)));
			}
		}
		else
		{
			/* we shouldn't be committing if any transactions are not open */
			Assert(transactionConnection->transactionState == TRANSACTION_STATE_OPEN);

			/* try to commit, if it fails then the user might lose data */
			result = PQexec(connection, "COMMIT");
			if (PQresultStatus(result) != PGRES_COMMAND_OK)
			{
				ereport(WARNING, (errmsg("Failed to commit transaction on %s:%s",
										 nodeName, nodePort)));
			}
		}

		PQclear(result);

		transactionConnection->transactionState = TRANSACTION_STATE_CLOSED;
	}
}


/*
 * BuildTransactionName constructs a unique transaction name from an ID.
 */
static StringInfo
BuildTransactionName(int connectionId)
{
	StringInfo commandString = makeStringInfo();

	appendStringInfo(commandString, "citus_%d_%u_%d", MyProcPid,
					 GetCurrentTransactionId(), connectionId);

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
