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
#include "distributed/backend_data.h"
#include "distributed/connection_management.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/remote_transaction.h"
#include "distributed/transaction_identifier.h"
#include "distributed/transaction_management.h"
#include "distributed/transaction_recovery.h"
#include "distributed/worker_manager.h"
#include "utils/hsearch.h"


#define PREPARED_TRANSACTION_NAME_FORMAT "citus_%u_%u_"UINT64_FORMAT "_%u"


static void StartRemoteTransactionSavepointBegin(MultiConnection *connection,
												 SubTransactionId subId);
static void FinishRemoteTransactionSavepointBegin(MultiConnection *connection,
												  SubTransactionId subId);
static void StartRemoteTransactionSavepointRelease(MultiConnection *connection,
												   SubTransactionId subId);
static void FinishRemoteTransactionSavepointRelease(MultiConnection *connection,
													SubTransactionId subId);
static void StartRemoteTransactionSavepointRollback(MultiConnection *connection,
													SubTransactionId subId);
static void FinishRemoteTransactionSavepointRollback(MultiConnection *connection,
													 SubTransactionId subId);

static void CheckTransactionHealth(void);
static void Assign2PCIdentifier(MultiConnection *connection);
static void WarnAboutLeakedPreparedTransaction(MultiConnection *connection, bool commit);


/*
 * StartRemoteTransactionBeging initiates beginning the remote transaction in
 * a non-blocking manner. The function sends "BEGIN" followed by
 * assign_distributed_transaction_id() to assign the distributed transaction
 * id on the remote node.
 */
void
StartRemoteTransactionBegin(struct MultiConnection *connection)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;
	StringInfo beginAndSetDistributedTransactionId = makeStringInfo();
	DistributedTransactionId *distributedTransactionId = NULL;
	ListCell *subIdCell = NULL;
	List *activeSubXacts = NIL;

	Assert(transaction->transactionState == REMOTE_TRANS_INVALID);

	/* remember transaction as being in-progress */
	dlist_push_tail(&InProgressTransactions, &connection->transactionNode);

	transaction->transactionState = REMOTE_TRANS_STARTING;

	/*
	 * Explicitly specify READ COMMITTED, the default on the remote
	 * side might have been changed, and that would cause problematic
	 * behaviour.
	 */
	appendStringInfoString(beginAndSetDistributedTransactionId,
						   "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;");

	/*
	 * Append BEGIN and assign_distributed_transaction_id() statements into a single command
	 * and send both in one step. The reason is purely performance, we don't want
	 * seperate roundtrips for these two statements.
	 */
	distributedTransactionId = GetCurrentDistributedTransactionId();
	appendStringInfo(beginAndSetDistributedTransactionId,
					 "SELECT assign_distributed_transaction_id(%d, " UINT64_FORMAT
					 ", '%s');",
					 distributedTransactionId->initiatorNodeIdentifier,
					 distributedTransactionId->transactionNumber,
					 timestamptz_to_str(distributedTransactionId->timestamp));

	/* append in-progress savepoints for this transaction */
	activeSubXacts = ActiveSubXacts();
	transaction->lastSuccessfulSubXact = TopSubTransactionId;
	transaction->lastQueuedSubXact = TopSubTransactionId;
	foreach(subIdCell, activeSubXacts)
	{
		SubTransactionId subId = lfirst_int(subIdCell);
		appendStringInfo(beginAndSetDistributedTransactionId,
						 "SAVEPOINT savepoint_%u;", subId);
		transaction->lastQueuedSubXact = subId;
	}

	if (!SendRemoteCommand(connection, beginAndSetDistributedTransactionId->data))
	{
		ReportConnectionError(connection, WARNING);
		MarkRemoteTransactionFailed(connection, true);
	}
}


/*
 * FinishRemoteTransactionBegin finishes the work StartRemoteTransactionBegin
 * initiated. It blocks if necessary (i.e. if PQisBusy() would return true).
 */
void
FinishRemoteTransactionBegin(struct MultiConnection *connection)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;
	bool clearSuccessful = true;
	bool raiseErrors = true;

	Assert(transaction->transactionState == REMOTE_TRANS_STARTING);

	clearSuccessful = ClearResults(connection, raiseErrors);
	if (clearSuccessful)
	{
		transaction->transactionState = REMOTE_TRANS_STARTED;
		transaction->lastSuccessfulSubXact = transaction->lastQueuedSubXact;
	}

	if (!transaction->transactionFailed)
	{
		Assert(PQtransactionStatus(connection->pgConn) == PQTRANS_INTRANS);
	}
}


/*
 * RemoteTransactionBegin begins a remote transaction in a blocking manner.
 */
void
RemoteTransactionBegin(struct MultiConnection *connection)
{
	StartRemoteTransactionBegin(connection);
	FinishRemoteTransactionBegin(connection);
}


/*
 * RemoteTransactionListBegin sends BEGIN over all connections in the
 * given connection list and waits for all of them to finish.
 */
void
RemoteTransactionListBegin(List *connectionList)
{
	ListCell *connectionCell = NULL;

	/* send BEGIN to all nodes */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		StartRemoteTransactionBegin(connection);
	}

	/* wait for BEGIN to finish on all nodes */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		FinishRemoteTransactionBegin(connection);
	}
}


/*
 * StartRemoteTransactionCommit initiates transaction commit in a non-blocking
 * manner.  If the transaction is in a failed state, it'll instead get rolled
 * back.
 */
void
StartRemoteTransactionCommit(MultiConnection *connection)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;
	const bool dontRaiseError = false;
	const bool isCommit = true;

	/* can only commit if transaction is in progress */
	Assert(transaction->transactionState != REMOTE_TRANS_INVALID);

	/* can't commit if we already started to commit or abort */
	Assert(transaction->transactionState < REMOTE_TRANS_1PC_ABORTING);

	if (transaction->transactionFailed)
	{
		/* abort the transaction if it failed */
		transaction->transactionState = REMOTE_TRANS_1PC_ABORTING;

		/*
		 * Try sending an ROLLBACK; Depending on the state that won't
		 * succeed, but let's try.  Have to clear previous results
		 * first.
		 */
		ForgetResults(connection); /* try to clear pending stuff */
		if (!SendRemoteCommand(connection, "ROLLBACK"))
		{
			/* no point in reporting a likely redundant message */
		}
	}
	else if (transaction->transactionState == REMOTE_TRANS_PREPARED)
	{
		/* commit the prepared transaction */
		StringInfoData command;

		initStringInfo(&command);
		appendStringInfo(&command, "COMMIT PREPARED '%s'",
						 transaction->preparedName);

		transaction->transactionState = REMOTE_TRANS_2PC_COMMITTING;

		if (!SendRemoteCommand(connection, command.data))
		{
			ReportConnectionError(connection, WARNING);
			MarkRemoteTransactionFailed(connection, dontRaiseError);

			WarnAboutLeakedPreparedTransaction(connection, isCommit);
		}
	}
	else
	{
		/* initiate remote transaction commit */
		transaction->transactionState = REMOTE_TRANS_1PC_COMMITTING;

		if (!SendRemoteCommand(connection, "COMMIT"))
		{
			/*
			 * For a moment there I thought we were in trouble.
			 *
			 * Failing in this state means that we don't know whether the the
			 * commit has succeeded.
			 */
			ReportConnectionError(connection, WARNING);
			MarkRemoteTransactionFailed(connection, dontRaiseError);
		}
	}
}


/*
 * FinishRemoteTransactionCommit finishes the work
 * StartRemoteTransactionCommit initiated. It blocks if necessary (i.e. if
 * PQisBusy() would return true).
 */
void
FinishRemoteTransactionCommit(MultiConnection *connection)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;
	PGresult *result = NULL;
	const bool dontRaiseErrors = false;
	const bool isCommit = true;

	Assert(transaction->transactionState == REMOTE_TRANS_1PC_ABORTING ||
		   transaction->transactionState == REMOTE_TRANS_1PC_COMMITTING ||
		   transaction->transactionState == REMOTE_TRANS_2PC_COMMITTING);

	result = GetRemoteCommandResult(connection, dontRaiseErrors);

	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, WARNING);
		MarkRemoteTransactionFailed(connection, dontRaiseErrors);

		/*
		 * Failing in this state means that we will often not know whether
		 * the the commit has succeeded (particularly in case of network
		 * troubles).
		 *
		 * XXX: It might be worthwhile to discern cases where we got a
		 * proper error back from postgres (i.e. COMMIT was received but
		 * produced an error) from cases where the connection failed
		 * before getting a reply.
		 */

		if (transaction->transactionState == REMOTE_TRANS_1PC_COMMITTING)
		{
			if (transaction->transactionCritical)
			{
				ereport(WARNING, (errmsg("failed to commit critical transaction "
										 "on %s:%d, metadata is likely out of sync",
										 connection->hostname, connection->port)));
			}
			else
			{
				ereport(WARNING, (errmsg("failed to commit transaction on %s:%d",
										 connection->hostname, connection->port)));
			}
		}
		else if (transaction->transactionState == REMOTE_TRANS_2PC_COMMITTING)
		{
			ereport(WARNING, (errmsg("failed to commit transaction on %s:%d",
									 connection->hostname, connection->port)));
			WarnAboutLeakedPreparedTransaction(connection, isCommit);
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


/*
 * RemoteTransactionCommit commits (or aborts, if the transaction failed) a
 * remote transaction in a blocking manner.
 */
void
RemoteTransactionCommit(MultiConnection *connection)
{
	StartRemoteTransactionCommit(connection);
	FinishRemoteTransactionCommit(connection);
}


/*
 * StartRemoteTransactionAbort initiates abortin the transaction in a
 * non-blocking manner.
 */
void
StartRemoteTransactionAbort(MultiConnection *connection)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;
	const bool dontRaiseErrors = false;
	const bool isNotCommit = false;

	Assert(transaction->transactionState != REMOTE_TRANS_INVALID);

	/*
	 * Clear previous results, so we have a better chance to send ROLLBACK
	 * [PREPARED]. If we've previously sent a PREPARE TRANSACTION, we always
	 * want to wait for that result, as that shouldn't take long and will
	 * reserve resources.  But if there's another query running, we don't want
	 * to wait, because a longrunning statement may be running, force it to be
	 * killed in that case.
	 */
	if (transaction->transactionState == REMOTE_TRANS_PREPARING ||
		transaction->transactionState == REMOTE_TRANS_PREPARED)
	{
		StringInfoData command;

		/* await PREPARE TRANSACTION results, closing the connection would leave it dangling */
		ForgetResults(connection);

		initStringInfo(&command);
		appendStringInfo(&command, "ROLLBACK PREPARED '%s'",
						 transaction->preparedName);

		if (!SendRemoteCommand(connection, command.data))
		{
			ReportConnectionError(connection, WARNING);
			MarkRemoteTransactionFailed(connection, dontRaiseErrors);

			WarnAboutLeakedPreparedTransaction(connection, isNotCommit);
		}
		else
		{
			transaction->transactionState = REMOTE_TRANS_2PC_ABORTING;
		}
	}
	else
	{
		if (!NonblockingForgetResults(connection))
		{
			ShutdownConnection(connection);

			/* FinishRemoteTransactionAbort will emit warning */
			return;
		}

		if (!SendRemoteCommand(connection, "ROLLBACK"))
		{
			/* no point in reporting a likely redundant message */
			MarkRemoteTransactionFailed(connection, dontRaiseErrors);
		}
		else
		{
			transaction->transactionState = REMOTE_TRANS_1PC_ABORTING;
		}
	}
}


/*
 * FinishRemoteTransactionAbort finishes the work StartRemoteTransactionAbort
 * initiated. It blocks if necessary (i.e. if PQisBusy() would return true).
 */
void
FinishRemoteTransactionAbort(MultiConnection *connection)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;
	PGresult *result = NULL;
	const bool dontRaiseErrors = false;
	const bool isNotCommit = false;

	result = GetRemoteCommandResult(connection, dontRaiseErrors);

	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, WARNING);
		MarkRemoteTransactionFailed(connection, dontRaiseErrors);

		if (transaction->transactionState == REMOTE_TRANS_2PC_ABORTING)
		{
			WarnAboutLeakedPreparedTransaction(connection, isNotCommit);
		}
		else
		{
			ereport(WARNING,
					(errmsg("failed to abort 1PC transaction \"%s\" on %s:%d",
							transaction->preparedName, connection->hostname,
							connection->port)));
		}
	}

	PQclear(result);

	result = GetRemoteCommandResult(connection, dontRaiseErrors);
	Assert(!result);

	transaction->transactionState = REMOTE_TRANS_ABORTED;
}


/*
 * RemoteTransactionAbort aborts a remote transaction in a blocking manner.
 */
void
RemoteTransactionAbort(MultiConnection *connection)
{
	StartRemoteTransactionAbort(connection);
	FinishRemoteTransactionAbort(connection);
}


/*
 * StartRemoteTransactionPrepare initiates preparing the transaction in a
 * non-blocking manner.
 */
void
StartRemoteTransactionPrepare(struct MultiConnection *connection)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;
	StringInfoData command;
	const bool raiseErrors = true;
	WorkerNode *workerNode = NULL;

	/* can't prepare a nonexistant transaction */
	Assert(transaction->transactionState != REMOTE_TRANS_INVALID);

	/* can't prepare in a failed transaction */
	Assert(!transaction->transactionFailed);

	/* can't prepare if already started to prepare/abort/commit */
	Assert(transaction->transactionState < REMOTE_TRANS_PREPARING);

	Assign2PCIdentifier(connection);

	/* log transactions to workers in pg_dist_transaction */
	workerNode = FindWorkerNode(connection->hostname, connection->port);
	if (workerNode != NULL)
	{
		LogTransactionRecord(workerNode->groupId, transaction->preparedName);
	}

	initStringInfo(&command);
	appendStringInfo(&command, "PREPARE TRANSACTION '%s'",
					 transaction->preparedName);

	if (!SendRemoteCommand(connection, command.data))
	{
		ReportConnectionError(connection, WARNING);
		MarkRemoteTransactionFailed(connection, raiseErrors);
	}
	else
	{
		transaction->transactionState = REMOTE_TRANS_PREPARING;
	}
}


/*
 * FinishRemoteTransactionPrepare finishes the work
 * StartRemoteTransactionPrepare initiated. It blocks if necessary (i.e. if
 * PQisBusy() would return true).
 */
void
FinishRemoteTransactionPrepare(struct MultiConnection *connection)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;
	PGresult *result = NULL;
	const bool raiseErrors = true;

	Assert(transaction->transactionState == REMOTE_TRANS_PREPARING);

	result = GetRemoteCommandResult(connection, raiseErrors);

	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, WARNING);
		transaction->transactionState = REMOTE_TRANS_ABORTED;
		MarkRemoteTransactionFailed(connection, raiseErrors);
	}
	else
	{
		transaction->transactionState = REMOTE_TRANS_PREPARED;
	}

	result = GetRemoteCommandResult(connection, raiseErrors);
	Assert(!result);
}


/*
 * RemoteTransactionPrepare prepares a remote transaction in a blocking
 * manner.
 */
void
RemoteTransactionPrepare(struct MultiConnection *connection)
{
	StartRemoteTransactionPrepare(connection);
	FinishRemoteTransactionPrepare(connection);
}


/*
 * RemoteTransactionBeginIfNecessary is a convenience wrapper around
 * RemoteTransactionsBeginIfNecessary(), for a single connection.
 */
void
RemoteTransactionBeginIfNecessary(MultiConnection *connection)
{
	/* just delegate */
	if (InCoordinatedTransaction())
	{
		List *connectionList = list_make1(connection);

		RemoteTransactionsBeginIfNecessary(connectionList);
		list_free(connectionList);
	}
}


/*
 * RemoteTransactionsBeginIfNecessary begins, if necessary according to this
 * session's coordinated transaction state, and the remote transaction's
 * state, an explicit transaction on all the connections.  This is done in
 * parallel, to lessen latency penalties.
 */
void
RemoteTransactionsBeginIfNecessary(List *connectionList)
{
	ListCell *connectionCell = NULL;
	bool raiseInterrupts = true;

	/*
	 * Don't do anything if not in a coordinated transaction. That allows the
	 * same code to work both in situations that uses transactions, and when
	 * not.
	 */
	if (!InCoordinatedTransaction())
	{
		return;
	}

	/* issue BEGIN to all connections needing it */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		RemoteTransaction *transaction = &connection->remoteTransaction;

		/* can't send BEGIN if a command already is in progress */
		Assert(PQtransactionStatus(connection->pgConn) != PQTRANS_ACTIVE);

		/*
		 * If a transaction already is in progress (including having failed),
		 * don't start it again.  Thats quite normal if a piece of code allows
		 * cached connections.
		 */
		if (transaction->transactionState != REMOTE_TRANS_INVALID)
		{
			continue;
		}

		StartRemoteTransactionBegin(connection);
	}

	raiseInterrupts = true;
	WaitForAllConnections(connectionList, raiseInterrupts);

	/* get result of all the BEGINs */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		RemoteTransaction *transaction = &connection->remoteTransaction;

		/*
		 * Only handle BEGIN results on connections that are in process of
		 * starting a transaction, and haven't already failed (e.g. by not
		 * being able to send BEGIN due to a network failure).
		 */
		if (transaction->transactionFailed ||
			transaction->transactionState != REMOTE_TRANS_STARTING)
		{
			continue;
		}

		FinishRemoteTransactionBegin(connection);
	}
}


/*
 * MarkRemoteTransactionFailed records a transaction as having failed.
 *
 * If the connection is marked as critical, and allowErrorPromotion is true,
 * this routine will ERROR out. The allowErrorPromotion case is primarily
 * required for the transaction management code itself. Usually it is helpful
 * to fail as soon as possible.  If !allowErrorPromotion transaction commit
 * will instead issue an error before committing on any node.
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
	if (transaction->transactionCritical && allowErrorPromotion)
	{
		ereport(ERROR, (errmsg("failure on connection marked as essential: %s:%d",
							   connection->hostname, connection->port)));
	}
}


/*
 * MarkRemoteTransactionCritical signals that failures on this remote
 * transaction should fail the entire coordinated transaction.
 */
void
MarkRemoteTransactionCritical(struct MultiConnection *connection)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;

	transaction->transactionCritical = true;
}


/*
 * CloseRemoteTransaction handles closing a connection that, potentially, is
 * part of a coordinated transaction.  This should only ever be called from
 * connection_management.c, while closing a connection during a transaction.
 */
void
CloseRemoteTransaction(struct MultiConnection *connection)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;

	/* unlink from list of open transactions, if necessary */
	if (transaction->transactionState != REMOTE_TRANS_INVALID)
	{
		/* XXX: Should we error out for a critical transaction? */

		dlist_delete(&connection->transactionNode);
	}
}


/*
 * ResetRemoteTransaction resets the state of the transaction after the end of
 * the main transaction, if the connection is being reused.
 */
void
ResetRemoteTransaction(struct MultiConnection *connection)
{
	RemoteTransaction *transaction = &connection->remoteTransaction;

	/* just reset the entire state, relying on 0 being invalid/false */
	memset(transaction, 0, sizeof(*transaction));
}


/*
 * CoordinatedRemoteTransactionsPrepare PREPAREs a 2PC transaction on all
 * non-failed transactions participating in the coordinated transaction.
 */
void
CoordinatedRemoteTransactionsPrepare(void)
{
	dlist_iter iter;
	bool raiseInterrupts = false;
	List *connectionList = NIL;

	/* issue PREPARE TRANSACTION; to all relevant remote nodes */

	/* asynchronously send PREPARE */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;

		Assert(transaction->transactionState != REMOTE_TRANS_INVALID);

		/* can't PREPARE a transaction that failed */
		if (transaction->transactionFailed)
		{
			continue;
		}

		StartRemoteTransactionPrepare(connection);
		connectionList = lappend(connectionList, connection);
	}

	raiseInterrupts = true;
	WaitForAllConnections(connectionList, raiseInterrupts);

	/* Wait for result */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;

		if (transaction->transactionState != REMOTE_TRANS_PREPARING)
		{
			continue;
		}

		FinishRemoteTransactionPrepare(connection);
	}

	CurrentCoordinatedTransactionState = COORD_TRANS_PREPARED;
}


/*
 * CoordinatedRemoteTransactionsCommit performs distributed transactions
 * handling at commit time. This will be called at XACT_EVENT_PRE_COMMIT if
 * 1PC commits are used - so shards can still be invalidated - and at
 * XACT_EVENT_COMMIT if 2PC is being used.
 *
 * Note that this routine has to issue rollbacks for failed transactions.
 */
void
CoordinatedRemoteTransactionsCommit(void)
{
	dlist_iter iter;
	List *connectionList = NIL;
	bool raiseInterrupts = false;

	/*
	 * Before starting to commit on any of the nodes - after which we can't
	 * completely roll-back anymore - check that things are in a good state.
	 */
	CheckTransactionHealth();

	/*
	 * Issue appropriate transaction commands to remote nodes. If everything
	 * went well that's going to be COMMIT or COMMIT PREPARED, if individual
	 * connections had errors, some or all of them might require a ROLLBACK.
	 *
	 * First send the command asynchronously over all connections.
	 */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;

		if (transaction->transactionState == REMOTE_TRANS_INVALID ||
			transaction->transactionState == REMOTE_TRANS_1PC_COMMITTING ||
			transaction->transactionState == REMOTE_TRANS_2PC_COMMITTING ||
			transaction->transactionState == REMOTE_TRANS_COMMITTED ||
			transaction->transactionState == REMOTE_TRANS_ABORTED)
		{
			continue;
		}

		StartRemoteTransactionCommit(connection);
		connectionList = lappend(connectionList, connection);
	}

	raiseInterrupts = false;
	WaitForAllConnections(connectionList, raiseInterrupts);

	/* wait for the replies to the commands to come in */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;

		/* nothing to do if not committing / aborting */
		if (transaction->transactionState != REMOTE_TRANS_1PC_COMMITTING &&
			transaction->transactionState != REMOTE_TRANS_2PC_COMMITTING &&
			transaction->transactionState != REMOTE_TRANS_1PC_ABORTING &&
			transaction->transactionState != REMOTE_TRANS_2PC_ABORTING)
		{
			continue;
		}

		FinishRemoteTransactionCommit(connection);
	}
}


/*
 * CoordinatedRemoteTransactionsAbort performs distributed transactions
 * handling at abort time.
 *
 * This issues ROLLBACKS and ROLLBACK PREPARED depending on whether the remote
 * transaction has been prepared or not.
 */
void
CoordinatedRemoteTransactionsAbort(void)
{
	dlist_iter iter;
	List *connectionList = NIL;
	bool raiseInterrupts = false;

	/* asynchronously send ROLLBACK [PREPARED] */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;

		if (transaction->transactionState == REMOTE_TRANS_INVALID ||
			transaction->transactionState == REMOTE_TRANS_1PC_ABORTING ||
			transaction->transactionState == REMOTE_TRANS_2PC_ABORTING ||
			transaction->transactionState == REMOTE_TRANS_ABORTED)
		{
			continue;
		}

		StartRemoteTransactionAbort(connection);
		connectionList = lappend(connectionList, connection);
	}

	raiseInterrupts = false;
	WaitForAllConnections(connectionList, raiseInterrupts);

	/* and wait for the results */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;

		if (transaction->transactionState != REMOTE_TRANS_1PC_ABORTING &&
			transaction->transactionState != REMOTE_TRANS_2PC_ABORTING)
		{
			continue;
		}

		FinishRemoteTransactionAbort(connection);
	}
}


/*
 * CoordinatedRemoteTransactionsSavepointBegin sends the SAVEPOINT command for
 * the given sub-transaction id to all connections participating in the current
 * transaction.
 */
void
CoordinatedRemoteTransactionsSavepointBegin(SubTransactionId subId)
{
	dlist_iter iter;
	const bool raiseInterrupts = true;
	List *connectionList = NIL;

	/* asynchronously send SAVEPOINT */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;
		if (transaction->transactionFailed)
		{
			continue;
		}

		StartRemoteTransactionSavepointBegin(connection, subId);
		connectionList = lappend(connectionList, connection);
	}

	WaitForAllConnections(connectionList, raiseInterrupts);

	/* and wait for the results */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;
		if (transaction->transactionFailed)
		{
			continue;
		}

		FinishRemoteTransactionSavepointBegin(connection, subId);

		if (!transaction->transactionFailed)
		{
			transaction->lastSuccessfulSubXact = subId;
		}
	}
}


/*
 * CoordinatedRemoteTransactionsSavepointRelease sends the RELEASE SAVEPOINT
 * command for the given sub-transaction id to all connections participating in
 * the current transaction.
 */
void
CoordinatedRemoteTransactionsSavepointRelease(SubTransactionId subId)
{
	dlist_iter iter;
	const bool raiseInterrupts = true;
	List *connectionList = NIL;

	/* asynchronously send RELEASE SAVEPOINT */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;
		if (transaction->transactionFailed)
		{
			continue;
		}

		StartRemoteTransactionSavepointRelease(connection, subId);
		connectionList = lappend(connectionList, connection);
	}

	WaitForAllConnections(connectionList, raiseInterrupts);

	/* and wait for the results */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;
		if (transaction->transactionFailed)
		{
			continue;
		}

		FinishRemoteTransactionSavepointRelease(connection, subId);
	}
}


/*
 * CoordinatedRemoteTransactionsSavepointRollback sends the ROLLBACK TO SAVEPOINT
 * command for the given sub-transaction id to all connections participating in
 * the current transaction.
 */
void
CoordinatedRemoteTransactionsSavepointRollback(SubTransactionId subId)
{
	dlist_iter iter;
	const bool dontRaiseInterrupts = false;
	List *connectionList = NIL;

	/* asynchronously send ROLLBACK TO SAVEPOINT */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;
		if (transaction->transactionFailed)
		{
			if (transaction->lastSuccessfulSubXact <= subId)
			{
				transaction->transactionRecovering = true;

				/*
				 * Clear the results of the failed query so we can send the ROLLBACK
				 * TO SAVEPOINT command for a savepoint that can recover the transaction
				 * from failure.
				 */
				ForgetResults(connection);
			}
			else
			{
				continue;
			}
		}
		StartRemoteTransactionSavepointRollback(connection, subId);
		connectionList = lappend(connectionList, connection);
	}

	WaitForAllConnections(connectionList, dontRaiseInterrupts);

	/* and wait for the results */
	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;
		if (transaction->transactionFailed && !transaction->transactionRecovering)
		{
			continue;
		}

		FinishRemoteTransactionSavepointRollback(connection, subId);
	}
}


/*
 * StartRemoteTransactionSavepointBegin initiates SAVEPOINT command for the given
 * subtransaction id in a non-blocking manner.
 */
static void
StartRemoteTransactionSavepointBegin(MultiConnection *connection, SubTransactionId subId)
{
	const bool raiseErrors = true;
	StringInfo savepointCommand = makeStringInfo();
	appendStringInfo(savepointCommand, "SAVEPOINT savepoint_%u", subId);

	if (!SendRemoteCommand(connection, savepointCommand->data))
	{
		ReportConnectionError(connection, WARNING);
		MarkRemoteTransactionFailed(connection, raiseErrors);
	}
}


/*
 * FinishRemoteTransactionSavepointBegin finishes the work
 * StartRemoteTransactionSavepointBegin initiated. It blocks if necessary (i.e.
 * if PQisBusy() would return true).
 */
static void
FinishRemoteTransactionSavepointBegin(MultiConnection *connection, SubTransactionId subId)
{
	const bool raiseErrors = true;
	PGresult *result = GetRemoteCommandResult(connection, raiseErrors);
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, WARNING);
		MarkRemoteTransactionFailed(connection, raiseErrors);
	}

	PQclear(result);
	ForgetResults(connection);
}


/*
 * StartRemoteTransactionSavepointRelease initiates RELEASE SAVEPOINT command for
 * the given subtransaction id in a non-blocking manner.
 */
static void
StartRemoteTransactionSavepointRelease(MultiConnection *connection,
									   SubTransactionId subId)
{
	const bool raiseErrors = true;
	StringInfo savepointCommand = makeStringInfo();
	appendStringInfo(savepointCommand, "RELEASE SAVEPOINT savepoint_%u", subId);

	if (!SendRemoteCommand(connection, savepointCommand->data))
	{
		ReportConnectionError(connection, WARNING);
		MarkRemoteTransactionFailed(connection, raiseErrors);
	}
}


/*
 * FinishRemoteTransactionSavepointRelease finishes the work
 * StartRemoteTransactionSavepointRelease initiated. It blocks if necessary (i.e.
 * if PQisBusy() would return true).
 */
static void
FinishRemoteTransactionSavepointRelease(MultiConnection *connection,
										SubTransactionId subId)
{
	const bool raiseErrors = true;
	PGresult *result = GetRemoteCommandResult(connection, raiseErrors);
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, WARNING);
		MarkRemoteTransactionFailed(connection, raiseErrors);
	}

	PQclear(result);
	ForgetResults(connection);
}


/*
 * StartRemoteTransactionSavepointRollback initiates ROLLBACK TO SAVEPOINT command
 * for the given subtransaction id in a non-blocking manner.
 */
static void
StartRemoteTransactionSavepointRollback(MultiConnection *connection,
										SubTransactionId subId)
{
	const bool dontRaiseErrors = false;
	StringInfo savepointCommand = makeStringInfo();
	appendStringInfo(savepointCommand, "ROLLBACK TO SAVEPOINT savepoint_%u", subId);

	if (!SendRemoteCommand(connection, savepointCommand->data))
	{
		ReportConnectionError(connection, WARNING);
		MarkRemoteTransactionFailed(connection, dontRaiseErrors);
	}
}


/*
 * FinishRemoteTransactionSavepointRollback finishes the work
 * StartRemoteTransactionSavepointRollback initiated. It blocks if necessary (i.e.
 * if PQisBusy() would return true). It also recovers the transaction from failure
 * if transaction is recovering and the rollback command succeeds.
 */
static void
FinishRemoteTransactionSavepointRollback(MultiConnection *connection, SubTransactionId
										 subId)
{
	const bool dontRaiseErrors = false;
	RemoteTransaction *transaction = &connection->remoteTransaction;

	PGresult *result = GetRemoteCommandResult(connection, dontRaiseErrors);
	if (!IsResponseOK(result))
	{
		ReportResultError(connection, result, WARNING);
		MarkRemoteTransactionFailed(connection, dontRaiseErrors);
	}

	/* ROLLBACK TO SAVEPOINT succeeded, check if it recovers the transaction */
	else if (transaction->transactionRecovering)
	{
		transaction->transactionFailed = false;
		transaction->transactionRecovering = false;
	}

	PQclear(result);
	ForgetResults(connection);
}


/*
 * CheckTransactionHealth checks if any of the participating transactions in a
 * coordinated transaction failed, and what consequence that should have.
 * This needs to be called before the coordinated transaction commits (but
 * after they've been PREPAREd if 2PC is in use).
 */
static void
CheckTransactionHealth(void)
{
	dlist_iter iter;

	dlist_foreach(iter, &InProgressTransactions)
	{
		MultiConnection *connection = dlist_container(MultiConnection, transactionNode,
													  iter.cur);
		RemoteTransaction *transaction = &connection->remoteTransaction;
		PGTransactionStatusType status = PQtransactionStatus(connection->pgConn);

		/* if the connection is in a bad state, so is the transaction's state */
		if (status == PQTRANS_INERROR || status == PQTRANS_UNKNOWN)
		{
			transaction->transactionFailed = true;
		}

		/*
		 * If a critical connection is marked as failed (and no error has been
		 * raised yet) do so now.
		 */
		if (transaction->transactionFailed && transaction->transactionCritical)
		{
			ereport(ERROR, (errmsg("failure on connection marked as essential: %s:%d",
								   connection->hostname, connection->port)));
		}
	}
}


/*
 * Assign2PCIdentifier computes the 2PC transaction name to use for a
 * transaction. Every prepared transaction should get a new name, i.e. this
 * function will need to be called again.
 *
 * The format of the name is:
 *
 * citus_<source group>_<pid>_<distributed transaction number>_<connection number>
 *
 * (at most 5+1+10+1+10+20+1+10 = 58 characters, while limit is 64)
 *
 * The source group is used to distinguish 2PCs started by different
 * coordinators. A coordinator will only attempt to recover its own 2PCs.
 *
 * The pid is used to distinguish different processes on the coordinator, mainly
 * to provide some entropy across restarts.
 *
 * The distributed transaction number is used to distinguish different
 * transactions originating from the same node (since restart).
 *
 * The connection number is used to distinguish connections made to a node
 * within the same transaction.
 *
 * NB: we rely on the fact that we don't need to do full escaping on the names
 * generated here.
 */
static void
Assign2PCIdentifier(MultiConnection *connection)
{
	/* local sequence number used to distinguish different connections */
	static uint32 connectionNumber = 0;

	/* transaction identifier that is unique across processes */
	uint64 transactionNumber = CurrentDistributedTransactionNumber();

	/* print all numbers as unsigned to guarantee no minus symbols appear in the name */
	snprintf(connection->remoteTransaction.preparedName, NAMEDATALEN,
			 PREPARED_TRANSACTION_NAME_FORMAT, GetLocalGroupId(), MyProcPid,
			 transactionNumber, connectionNumber++);
}


/*
 * ParsePreparedTransactionName parses a prepared transaction name to extract
 * the initiator group ID, initiator process ID, distributed transaction number,
 * and the connection number. If the transaction name does not match the expected
 * format ParsePreparedTransactionName returns false, and true otherwise.
 */
bool
ParsePreparedTransactionName(char *preparedTransactionName, int *groupId, int *procId,
							 uint64 *transactionNumber, uint32 *connectionNumber)
{
	const int expectedFieldCount = 4;
	int parsedFieldCount = 0;
	bool nameValid = false;

	parsedFieldCount = sscanf(preparedTransactionName, PREPARED_TRANSACTION_NAME_FORMAT,
							  groupId, procId, transactionNumber, connectionNumber);
	if (parsedFieldCount == expectedFieldCount)
	{
		nameValid = true;
	}

	return nameValid;
}


/*
 * WarnAboutLeakedPreparedTransaction issues a WARNING explaining that a
 * prepared transaction could not be committed or rolled back, and explains
 * how to perform cleanup.
 */
static void
WarnAboutLeakedPreparedTransaction(MultiConnection *connection, bool commit)
{
	StringInfoData command;
	RemoteTransaction *transaction = &connection->remoteTransaction;

	initStringInfo(&command);

	if (commit)
	{
		appendStringInfo(&command, "COMMIT PREPARED '%s'",
						 transaction->preparedName);
	}
	else
	{
		appendStringInfo(&command, "ROLLBACK PREPARED '%s'",
						 transaction->preparedName);
	}

	/* log a warning so the user may abort the transaction later */
	ereport(WARNING, (errmsg("failed to roll back prepared transaction '%s'",
							 transaction->preparedName),
					  errhint("Run \"%s\" on %s:%u",
							  command.data, connection->hostname, connection->port)));
}
