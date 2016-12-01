/*-------------------------------------------------------------------------
 *
 * worker_transaction.c
 *
 * Routines for performing transactions across all workers.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "distributed/commit_protocol.h"
#include "distributed/connection_cache.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/resource_lock.h"
#include "distributed/pg_dist_node.h"
#include "distributed/pg_dist_transaction.h"
#include "distributed/transaction_recovery.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "utils/memutils.h"


/* Local functions forward declarations */
static void EnableXactCallback(void);
static void CompleteWorkerTransactions(XactEvent event, void *arg);
static List * OpenWorkerTransactions(void);
static TransactionConnection * GetWorkerTransaction(char *nodeName, int32 nodePort);
static List * GetTargetWorkerTransactions(TargetWorkerSet targetWorkerSet);
static bool IsResponseOK(ExecStatusType resultStatus);


/* Global worker connection list */
static List *workerConnectionList = NIL;
static bool isXactCallbackRegistered = false;


/*
 * GetWorkerTransactions opens connections to all workers and starts
 * a transaction block that is committed or aborted when the local
 * transaction commits or aborts. Multiple invocations of
 * GetWorkerTransactions within the same transaction will return
 * the same list of connections.
 */
List *
GetWorkerTransactions(void)
{
	if (workerConnectionList == NIL)
	{
		InitializeDistributedTransaction();
		EnableXactCallback();

		workerConnectionList = OpenWorkerTransactions();
	}

	return workerConnectionList;
}


/*
 * SendCommandToWorker sends a command to a particular worker as part of the
 * 2PC.
 */
void
SendCommandToWorker(char *nodeName, int32 nodePort, char *command)
{
	TransactionConnection *transactionConnection = NULL;
	PGresult *queryResult = NULL;
	ExecStatusType resultStatus = PGRES_EMPTY_QUERY;

	transactionConnection = GetWorkerTransaction(nodeName, nodePort);
	if (transactionConnection == NULL)
	{
		ereport(ERROR, (errmsg("worker %s:%d is not part of current transaction",
							   nodeName, nodePort)));
	}

	queryResult = PQexec(transactionConnection->connection, command);
	resultStatus = PQresultStatus(queryResult);
	if (resultStatus != PGRES_COMMAND_OK && resultStatus != PGRES_TUPLES_OK)
	{
		ReraiseRemoteError(transactionConnection->connection, queryResult);
	}

	PQclear(queryResult);
}


/*
 * SendCommandToWorkers sends a command to all workers in
 * parallel. Commands are committed on the workers when the local
 * transaction commits. The connection are made as the extension
 * owner to ensure write access to the Citus metadata tables.
 */
void
SendCommandToWorkers(TargetWorkerSet targetWorkerSet, char *command)
{
	SendCommandToWorkersParams(targetWorkerSet, command, 0, NULL, NULL);
}


/*
 * SendCommandToWorkersParams sends a command to all workers in parallel.
 * Commands are committed on the workers when the local transaction commits. The
 * connection are made as the extension owner to ensure write access to the Citus
 * metadata tables. Parameters can be specified as for PQexecParams, except that
 * paramLengths, paramFormats and resultFormat are hard-coded to NULL, NULL and 0
 * respectively.
 */
void
SendCommandToWorkersParams(TargetWorkerSet targetWorkerSet, char *command,
						   int parameterCount, const Oid *parameterTypes,
						   const char *const *parameterValues)
{
	ListCell *connectionCell = NULL;
	List *targetConnectionList = GetTargetWorkerTransactions(targetWorkerSet);

	foreach(connectionCell, targetConnectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);

		PGconn *connection = transactionConnection->connection;

		int querySent = PQsendQueryParams(connection, command, parameterCount,
										  parameterTypes, parameterValues, NULL, NULL, 0);
		if (querySent == 0)
		{
			ReraiseRemoteError(connection, NULL);
		}
	}

	foreach(connectionCell, targetConnectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);

		PGconn *connection = transactionConnection->connection;
		PGresult *result = PQgetResult(connection);
		ExecStatusType resultStatus = PQresultStatus(result);

		if (!IsResponseOK(resultStatus))
		{
			ReraiseRemoteError(connection, result);
		}

		PQclear(result);

		/* clear NULL result */
		PQgetResult(connection);
	}
}


/*
 * SendCommandListToWorkerInSingleTransaction opens connection to the node with the given
 * nodeName and nodePort. Then, the connection starts a transaction on the remote
 * node and executes the commands in the transaction. The function raises error if
 * any of the queries fails.
 */
void
SendCommandListToWorkerInSingleTransaction(char *nodeName, int32 nodePort, char *nodeUser,
										   List *commandList)
{
	PGconn *workerConnection = NULL;
	PGresult *queryResult = NULL;
	ListCell *commandCell = NULL;

	if (XactModificationLevel > XACT_MODIFICATION_NONE)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("cannot open new connections after the first modification "
							   "command within a transaction")));
	}

	workerConnection = ConnectToNode(nodeName, nodePort, nodeUser);
	if (workerConnection == NULL)
	{
		ereport(ERROR, (errmsg("could not open connection to %s:%d as %s",
							   nodeName, nodePort, nodeUser)));
	}

	PG_TRY();
	{
		/* start the transaction on the worker node */
		queryResult = PQexec(workerConnection, "BEGIN");
		if (PQresultStatus(queryResult) != PGRES_COMMAND_OK)
		{
			ReraiseRemoteError(workerConnection, queryResult);
		}

		PQclear(queryResult);

		/* iterate over the commands and execute them in the same connection */
		foreach(commandCell, commandList)
		{
			char *commandString = lfirst(commandCell);
			ExecStatusType resultStatus = PGRES_EMPTY_QUERY;

			CHECK_FOR_INTERRUPTS();

			queryResult = PQexec(workerConnection, commandString);
			resultStatus = PQresultStatus(queryResult);
			if (!(resultStatus == PGRES_SINGLE_TUPLE || resultStatus == PGRES_TUPLES_OK ||
				  resultStatus == PGRES_COMMAND_OK))
			{
				ReraiseRemoteError(workerConnection, queryResult);
			}

			PQclear(queryResult);
		}

		/* commit the transaction on the worker node */
		queryResult = PQexec(workerConnection, "COMMIT");
		if (PQresultStatus(queryResult) != PGRES_COMMAND_OK)
		{
			ReraiseRemoteError(workerConnection, queryResult);
		}

		PQclear(queryResult);

		/* clear NULL result */
		PQgetResult(workerConnection);

		/* we no longer need this connection */
		CloseConnectionByPGconn(workerConnection);
	}
	PG_CATCH();
	{
		/* close the connection */
		CloseConnectionByPGconn(workerConnection);

		PG_RE_THROW();
	}
	PG_END_TRY();
}


/*
 * IsWorkerTransactionActive returns true if there exists any on going
 * worker transactions.
 */
bool
IsWorkerTransactionActive(void)
{
	bool isWorkerTransactionActive = false;

	if (workerConnectionList != NIL)
	{
		isWorkerTransactionActive = true;
	}

	return isWorkerTransactionActive;
}


/*
 * RemoveWorkerTransaction removes the transaction connection to the specified node from
 * the transaction connection list.
 */
void
RemoveWorkerTransaction(char *nodeName, int32 nodePort)
{
	TransactionConnection *transactionConnection =
		GetWorkerTransaction(nodeName, nodePort);

	/* transactionConnection = NULL if the worker transactions have not opened before */
	if (transactionConnection != NULL)
	{
		PGconn *connection = transactionConnection->connection;

		/* closing the connection will rollback all uncommited transactions */
		CloseConnectionByPGconn(connection);

		workerConnectionList = list_delete(workerConnectionList, transactionConnection);
	}
}


/*
 * EnableXactCallback registers the CompleteWorkerTransactions function as the callback
 * of the worker transactions.
 */
static void
EnableXactCallback(void)
{
	if (!isXactCallbackRegistered)
	{
		RegisterXactCallback(CompleteWorkerTransactions, NULL);
		isXactCallbackRegistered = true;
	}
}


/*
 * CompleteWorkerTransaction commits or aborts pending worker transactions
 * when the local transaction commits or aborts.
 */
static void
CompleteWorkerTransactions(XactEvent event, void *arg)
{
	if (workerConnectionList == NIL)
	{
		/* nothing to do */
		return;
	}
	else if (event == XACT_EVENT_PRE_COMMIT)
	{
		if (MultiShardCommitProtocol == COMMIT_PROTOCOL_2PC)
		{
			/*
			 * Any failure here will cause local changes to be rolled back,
			 * and may leave a prepared transaction on the remote node.
			 */

			PrepareRemoteTransactions(workerConnectionList);

			/*
			 * We are now ready to commit the local transaction, followed
			 * by the remote transaction. As a final step, write commit
			 * records to a table. If there is a last-minute crash
			 * on the local machine, then the absence of these records
			 * will indicate that the remote transactions should be rolled
			 * back. Otherwise, the presence of these records indicates
			 * that the remote transactions should be committed.
			 */

			LogPreparedTransactions(workerConnectionList);
		}

		return;
	}
	else if (event == XACT_EVENT_COMMIT)
	{
		/*
		 * A failure here may cause some prepared transactions to be
		 * left pending. However, the local change have already been
		 * committed and a commit record exists to indicate that the
		 * remote transaction should be committed as well.
		 */

		CommitRemoteTransactions(workerConnectionList, false);

		/*
		 * At this point, it is safe to remove the transaction records
		 * for all commits that have succeeded. However, we are no
		 * longer in a transaction and therefore cannot make changes
		 * to the metadata.
		 */
	}
	else if (event == XACT_EVENT_ABORT)
	{
		/*
		 * A failure here may cause some prepared transactions to be
		 * left pending. The local changes have already been rolled
		 * back and the absence of a commit record indicates that
		 * the remote transaction should be rolled back as well.
		 */

		AbortRemoteTransactions(workerConnectionList);
	}
	else if (event == XACT_EVENT_PREPARE || event == XACT_EVENT_PRE_PREPARE)
	{
		/*
		 * If we allow a prepare we might not get to the commit handler
		 * in this session. We could resolve that if we intercept
		 * COMMIT/ABORT PREPARED commands. For now, we just error out.
		 */
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot prepare a transaction that modified "
							   "distributed tables")));
	}
	else
	{
		return;
	}

	CloseConnections(workerConnectionList);

	/*
	 * Memory allocated in workerConnectionList will be reclaimed when
	 * TopTransactionContext is released.
	 */

	workerConnectionList = NIL;
}


/*
 * OpenWorkerTransactions opens connections to all primary workers and sends
 * BEGIN commands. The returned TransactionConnection's are allocated in the
 * top transaction context, such that they can still be used in the commit
 * handler. The connections are made as the extension owner, such that they
 * have write access to the Citus metadata tables.
 */
static List *
OpenWorkerTransactions(void)
{
	ListCell *workerNodeCell = NULL;
	List *connectionList = NIL;
	MemoryContext oldContext = NULL;
	List *workerList = NIL;

	/*
	 * A new node addition might be in progress which will invalidate the
	 * worker list. The following statement blocks until the node addition and
	 * metadata syncing finishes after which we reload the worker list.
	 * It also ensures that no new node addition and metadata synchronization
	 * will run until this transaction finishes.
	 */
	LockMetadataSnapshot(AccessShareLock);

	workerList = WorkerNodeList();

	oldContext = MemoryContextSwitchTo(TopTransactionContext);

	foreach(workerNodeCell, workerList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeUser = CitusExtensionOwnerName();
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		PGconn *connection = NULL;

		TransactionConnection *transactionConnection = NULL;
		PGresult *result = NULL;

		connection = ConnectToNode(nodeName, nodePort, nodeUser);
		if (connection == NULL)
		{
			ereport(ERROR, (errmsg("could not open connection to %s:%d as %s",
								   nodeName, nodePort, nodeUser)));
		}

		result = PQexec(connection, "BEGIN");
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			ReraiseRemoteError(connection, result);
		}

		PQclear(result);

		transactionConnection = palloc0(sizeof(TransactionConnection));

		transactionConnection->groupId = workerNode->groupId;
		transactionConnection->connectionId = 0;
		transactionConnection->transactionState = TRANSACTION_STATE_OPEN;
		transactionConnection->connection = connection;
		transactionConnection->nodeName = pstrdup(nodeName);
		transactionConnection->nodePort = nodePort;

		connectionList = lappend(connectionList, transactionConnection);
	}

	MemoryContextSwitchTo(oldContext);

	return connectionList;
}


/*
 * GetNodeTransactionConnection finds the opened connection for the specified
 * node. Note that it opens transaction connections to all workers, by
 * calling GetWorkerTransactions therefore, it is suggested to use this
 * function in operations that sends commands to all workers inside a
 * distributed transaction.
 *
 * GetNodeTransactionConnection returns NULL, if the node with the specified
 * nodeName and nodePort is not found. Note that this worker may open
 * connections to all workers if there were not open already.
 */
static TransactionConnection *
GetWorkerTransaction(char *nodeName, int32 nodePort)
{
	List *connectionList = NIL;
	ListCell *connectionCell = NULL;
	TransactionConnection *workerTransaction = NULL;

	connectionList = GetWorkerTransactions();

	foreach(connectionCell, connectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);

		if (strcmp(transactionConnection->nodeName, nodeName) == 0 &&
			transactionConnection->nodePort == nodePort)
		{
			workerTransaction = transactionConnection;
			break;
		}
	}

	return workerTransaction;
}


/*
 * GetTargetWorkerTransactions returns a subset of all worker transactions
 * matching the given target worker set.
 */
static List *
GetTargetWorkerTransactions(TargetWorkerSet targetWorkerSet)
{
	List *allWorkerConnectionsList = GetWorkerTransactions();
	List *targetConnectionList = NIL;
	ListCell *connectionCell = NULL;

	if (targetWorkerSet == WORKERS_WITH_METADATA)
	{
		foreach(connectionCell, allWorkerConnectionsList)
		{
			TransactionConnection *transactionConnection =
				(TransactionConnection *) lfirst(connectionCell);
			char *nodeName = pstrdup(transactionConnection->nodeName);
			int nodePort = transactionConnection->nodePort;
			WorkerNode *workerNode = FindWorkerNode(nodeName, nodePort);

			if (workerNode->hasMetadata)
			{
				targetConnectionList = lappend(targetConnectionList,
											   transactionConnection);
			}
		}
	}
	else
	{
		targetConnectionList = allWorkerConnectionsList;
	}

	return targetConnectionList;
}


/*
 * IsResponseOK checks the resultStatus and returns true if the status is OK.
 */
static bool
IsResponseOK(ExecStatusType resultStatus)
{
	if (resultStatus == PGRES_SINGLE_TUPLE || resultStatus == PGRES_TUPLES_OK ||
		resultStatus == PGRES_COMMAND_OK)
	{
		return true;
	}

	return false;
}
