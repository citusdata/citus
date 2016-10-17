/*-------------------------------------------------------------------------
 *
 * remote_transaction.c
 *   Management of transaction spanning more than one node.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq-fe.h"

#include "miscadmin.h"

#include "access/xact.h"
#include "distributed/connection_management.h"
#include "distributed/transaction_management.h"
#include "distributed/remote_commands.h"
#include "distributed/remote_transaction.h"
#include "utils/hsearch.h"


static void Assign2PCIdentifier(MultiConnection *connection);
static void WarnAboutLeakedPreparedTransaction(MultiConnection *connection, bool commit);


/*
 * Begin, if necessary according to this session's coordinated transaction
 * state, and the connection's state, an explicit transaction on all the
 * connections.  This is done in parallel, to lessen latency penalties.
 */
void
AdjustRemoteTransactionStates(List *connectionList)
{
	ListCell *connectionCell = NULL;

	if (!InCoordinatedTransaction())
	{
		return;
	}

	/* issue BEGIN to all connections needing it */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		RemoteTransaction *transaction = &connection->remoteTransaction;

		Assert(PQtransactionStatus(connection->conn) != PQTRANS_ACTIVE);

		if (transaction->transactionFailed ||
			transaction->transactionState != REMOTE_TRANS_INVALID)
		{
			continue;
		}

		if (PQtransactionStatus(connection->conn) != PQTRANS_INTRANS)
		{
			/*
			 * Check whether we're right now allowed to start new client
			 * transaction.  FIXME: This likely can be removed soon.
			 */
			if (XactModificationLevel > XACT_MODIFICATION_NONE)
			{
				ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
								errmsg("cannot open new connections after the first "
									   "modification command within a transaction")));
			}

			/*
			 * Explicitly specify READ COMMITTED, the default on the remote
			 * side might have been changed, and that would cause problematic
			 * behaviour.
			 */
			if (!SendRemoteCommand(connection, "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;"))
			{
				ReportConnectionError(connection, WARNING);
				MarkRemoteTransactionFailed(connection, true);
			}
			else
			{
				transaction->transactionState = REMOTE_TRANS_STARTING;
			}
		}
	}


	/* get result of all the BEGINs */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		RemoteTransaction *transaction = &connection->remoteTransaction;
		PGresult *result = NULL;

		if (transaction->transactionFailed)
		{
			continue;
		}

		if (!(transaction->transactionState == REMOTE_TRANS_STARTING))
		{
			continue;
		}

		result = PQgetResult(connection->conn);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, WARNING);
			MarkRemoteTransactionFailed(connection, true);
		}
		else
		{
			transaction->transactionState = REMOTE_TRANS_STARTED;
		}

		PQclear(result);

		ForgetResults(connection);

		Assert(PQtransactionStatus(connection->conn) == PQTRANS_INTRANS);
	}
}


/*
 * Begin, if necessary according to this session's coordinated transaction
 * state, and the connection's state, an explicit transaction on the
 * connections.
 */
void
AdjustRemoteTransactionState(MultiConnection *connection)
{
	/* just delegate */
	if (InCoordinatedTransaction())
	{
		List *connectionList = list_make1(connection);

		AdjustRemoteTransactionStates(connectionList);
		list_free(connectionList);
	}
}


/*
 * Record a connection as being failed. That'll, if a coordinated transaction
 * is in progress, mean coordinated transactions will take appropriate action
 * to handle with the failure.
 *
 * If the connection is marked as critical, and allowErrorPromotion is true,
 * this routine will ERROR out. The allowErrorPromotion case is primarily
 * required for the transaction management code itself. Usually it is helpful
 * to fail as soon as possible.
 */
void
MarkRemoteTransactionFailed(MultiConnection *connection, bool allowErrorPromotion)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;

	transaction->transactionFailed = true;

	/*
	 * If the connection is marked as critical, fail the entire coordinated
	 * transaction. If allowed.
	 */
	if (transaction->criticalTransaction && allowErrorPromotion)
	{
		ereport(ERROR, (errmsg("failure on connection marked as essential: %s:%d",
							   connection->hostname, connection->port)));
	}
}


/*
 * Perform distributed transactions handling at commit time. This will be
 * called at XACT_EVENT_PRE_COMMIT if 1PC commits are used - so shards can
 * still be invalidated - and at XACT_EVENT_COMMIT if 2PC is being used.
 *
 * Note that this routine has to issue rollbacks for failed transactions. In
 * that case affected placements will be marked as invalid (via
 * CheckForFailedPlacements()).
 */
void
CoordinatedRemoteTransactionsCommit(void)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;
	ListCell *connectionCell;

	/*
	 * Issue appropriate transaction commands to remote nodes. If everything
	 * went well that's going to be COMMIT or COMMIT PREPARED, if individual
	 * connections had errors, some or all of them might require a ROLLBACK.
	 */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
			RemoteTransaction *transaction = &connection->remoteTransaction;

			/* nothing to do if no transaction active */
			if (transaction->transactionState == REMOTE_TRANS_INVALID)
			{
				continue;
			}

			if (transaction->transactionFailed)
			{
				/*
				 * Try sending an ROLLBACK; Depending on the state that won't
				 * succeed, but let's try.  Have to clear previous results
				 * first.
				 */
				ForgetResults(connection); /* try to clear pending stuff */
				if (!SendRemoteCommand(connection, "ROLLBACK;"))
				{
					/* no point in reporting a likely redundant message */
					MarkRemoteTransactionFailed(connection, false);
				}
				else
				{
					transaction->transactionState = REMOTE_TRANS_1PC_ABORTING;
				}
			}
			else if (transaction->transactionState == REMOTE_TRANS_PREPARED)
			{
				StringInfoData command;

				initStringInfo(&command);
				appendStringInfo(&command, "COMMIT PREPARED '%s';",
								 transaction->preparedName);

				transaction->transactionState = REMOTE_TRANS_2PC_COMMITTING;

				if (!SendRemoteCommand(connection, command.data))
				{
					ReportConnectionError(connection, WARNING);
					MarkRemoteTransactionFailed(connection, false);

					WarnAboutLeakedPreparedTransaction(connection, true);
				}
			}
			else
			{
				transaction->transactionState = REMOTE_TRANS_1PC_COMMITTING;

				if (!SendRemoteCommand(connection, "COMMIT;"))
				{
					/* for a moment there I thought we were in trouble */
					ReportConnectionError(connection, WARNING);
					MarkRemoteTransactionFailed(connection, false);
				}
			}
		}
	}

	/* Wait for result */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
			RemoteTransaction *transaction = &connection->remoteTransaction;
			PGresult *result = NULL;

			if (transaction->transactionState != REMOTE_TRANS_1PC_COMMITTING &&
				transaction->transactionState != REMOTE_TRANS_2PC_COMMITTING &&
				transaction->transactionState != REMOTE_TRANS_1PC_ABORTING &&
				transaction->transactionState != REMOTE_TRANS_2PC_ABORTING)
			{
				continue;
			}

			result = PQgetResult(connection->conn);

			if (!IsResponseOK(result))
			{
				ReportResultError(connection, result, WARNING);
				MarkRemoteTransactionFailed(connection, false);

				if (transaction->transactionState == REMOTE_TRANS_2PC_COMMITTING)
				{
					WarnAboutLeakedPreparedTransaction(connection, true);
				}
			}
			else if (transaction->transactionState == REMOTE_TRANS_1PC_ABORTING ||
					 transaction->transactionState == REMOTE_TRANS_2PC_ABORTING)
			{
				transaction->transactionState = REMOTE_TRANS_ABORTED;
			}
			else
			{
				transaction->transactionState = REMOTE_TRANS_COMMITTED;
			}

			PQclear(result);

			ForgetResults(connection);
		}
	}
}


/*
 * Perform distributed transactions handling at abort time. This issues
 * ROLLBACKS and ROLLBACK PREPARED depending on whether the remote transaction
 * has been prepared or not.
 */
void
CoordinatedRemoteTransactionsAbort(void)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;
	ListCell *connectionCell;

	/* issue ROLLBACK; to all relevant remote nodes */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
			RemoteTransaction *transaction = &connection->remoteTransaction;

			if (transaction->transactionState == REMOTE_TRANS_INVALID)
			{
				continue;
			}

			if (transaction->transactionState == REMOTE_TRANS_PREPARED)
			{
				StringInfoData command;

				initStringInfo(&command);
				appendStringInfo(&command, "ROLLBACK PREPARED '%s';",
								 transaction->preparedName);

				if (!SendRemoteCommand(connection, command.data))
				{
					ReportConnectionError(connection, WARNING);
					MarkRemoteTransactionFailed(connection, false);

					WarnAboutLeakedPreparedTransaction(connection, false);
				}
				else
				{
					transaction->transactionState = REMOTE_TRANS_2PC_ABORTING;
				}
			}
			else
			{
				/*
				 * Try sending an ROLLBACK; Depending on the state
				 * that won't have success, but let's try.  Have
				 * to clear previous results first.
				 */
				ForgetResults(connection);
				if (!SendRemoteCommand(connection, "ROLLBACK;"))
				{
					/* no point in reporting a likely redundant message */
					MarkRemoteTransactionFailed(connection, false);
				}
				else
				{
					transaction->transactionState = REMOTE_TRANS_1PC_ABORTING;
				}
			}
		}
	}

	/* Wait for result */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
			RemoteTransaction *transaction = &connection->remoteTransaction;
			PGresult *result = NULL;

			if (transaction->transactionState != REMOTE_TRANS_1PC_ABORTING &&
				transaction->transactionState != REMOTE_TRANS_2PC_ABORTING)
			{
				continue;
			}

			result = PQgetResult(connection->conn);

			if (!IsResponseOK(result))
			{
				ReportResultError(connection, result, WARNING);
				MarkRemoteTransactionFailed(connection, false);

				if (transaction->transactionState == REMOTE_TRANS_1PC_ABORTING)
				{
					ereport(WARNING,
							(errmsg("failed to abort 2PC transaction \"%s\" on %s:%d",
									transaction->preparedName, connection->hostname,
									connection->port)));
				}
				else
				{
					WarnAboutLeakedPreparedTransaction(connection, false);
				}
			}

			PQclear(result);

			result = PQgetResult(connection->conn);
			Assert(!result);

			transaction->transactionState = REMOTE_TRANS_ABORTED;
		}
	}
}


/*
 * Perform 2PC prepare on all non-failed transactions participating in the
 * coordinated transaction.
 */
void
CoordinatedRemoteTransactionsPrepare(void)
{
	HASH_SEQ_STATUS status;
	ConnectionHashEntry *entry;
	ListCell *connectionCell;

	/* issue PREPARE TRANSACTION; to all relevant remote nodes */

	/* TODO: skip connections that haven't done any DML/DDL */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
			RemoteTransaction *transaction = &connection->remoteTransaction;

			if (transaction->transactionState == REMOTE_TRANS_INVALID)
			{
				continue;
			}

			if (!(transaction->transactionFailed))
			{
				StringInfoData command;

				initStringInfo(&command);

				Assign2PCIdentifier(connection);

				appendStringInfo(&command, "PREPARE TRANSACTION '%s'",
								 transaction->preparedName);

				if (!SendRemoteCommand(connection, command.data))
				{
					ReportConnectionError(connection, WARNING);
					MarkRemoteTransactionFailed(connection, false);
				}
				else
				{
					transaction->transactionState = REMOTE_TRANS_PREPARING;
				}
			}
		}
	}

	/* Wait for result */
	hash_seq_init(&status, ConnectionHash);
	while ((entry = (ConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		foreach(connectionCell, entry->connections)
		{
			MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
			RemoteTransaction *transaction = &connection->remoteTransaction;
			PGresult *result = NULL;

			if (transaction->transactionState != REMOTE_TRANS_PREPARING)
			{
				continue;
			}

			result = PQgetResult(connection->conn);

			if (!IsResponseOK(result))
			{
				ReportResultError(connection, result, WARNING);
				MarkRemoteTransactionFailed(connection, false);
			}
			else
			{
				transaction->transactionState = REMOTE_TRANS_PREPARED;
			}

			result = PQgetResult(connection->conn);
			Assert(!result);
		}
	}

	CurrentCoordinatedTransactionState = COORD_TRANS_PREPARED;
}


/*
 * Compute the 2PC transaction name to use. Every 2PC transaction should get a
 * new name, i.e. this function will need to be called again.
 *
 * NB: we rely on the fact that we don't need to do full escaping on the names
 * generated here.
 */
static void
Assign2PCIdentifier(MultiConnection *connection)
{
	static uint64 sequence = 0;
	snprintf(connection->remoteTransaction.preparedName, NAMEDATALEN,
			 "citus_%d_"UINT64_FORMAT,
			 MyProcPid, sequence++);
}


static void
WarnAboutLeakedPreparedTransaction(MultiConnection *connection, bool commit)
{
	StringInfoData command;
	RemoteTransaction *transaction = &connection->remoteTransaction;

	initStringInfo(&command);

	if (commit)
		appendStringInfo(&command, "COMMIT PREPARED '%s';",
						 transaction->preparedName);
	else
		appendStringInfo(&command, "ROLLBACK PREPARED '%s';",
						 transaction->preparedName);

	/* log a warning so the user may abort the transaction later */
	ereport(WARNING, (errmsg("failed to roll back prepared transaction '%s'",
							 transaction->preparedName),
					  errhint("Run \"%s\" on %s:%u",
							  command.data, connection->hostname, connection->port)));

}
