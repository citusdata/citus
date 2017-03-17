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
#include "libpq-fe.h"

#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "distributed/connection_management.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/resource_lock.h"
#include "distributed/remote_commands.h"
#include "distributed/pg_dist_node.h"
#include "distributed/pg_dist_transaction.h"
#include "distributed/transaction_recovery.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "utils/memutils.h"


/*
 * SendCommandToWorker sends a command to a particular worker as part of the
 * 2PC.
 */
void
SendCommandToWorker(char *nodeName, int32 nodePort, char *command)
{
	MultiConnection *transactionConnection = NULL;
	char *nodeUser = CitusExtensionOwnerName();
	int connectionFlags = 0;

	BeginOrContinueCoordinatedTransaction();
	CoordinatedTransactionUse2PC();

	transactionConnection = GetNodeUserDatabaseConnection(connectionFlags, nodeName,
														  nodePort, nodeUser, NULL);

	MarkRemoteTransactionCritical(transactionConnection);
	RemoteTransactionBeginIfNecessary(transactionConnection);
	ExecuteCriticalRemoteCommand(transactionConnection, command);
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
 * SendBareCommandListToWorkers sends a list of commands to a set of target
 * workers in serial. Commands are committed immediately: new connections are
 * always used and no transaction block is used (hence "bare"). The connections
 * are made as the extension owner to ensure write access to the Citus metadata
 * tables. Primarly useful for INDEX commands using CONCURRENTLY.
 */
void
SendBareCommandListToWorkers(TargetWorkerSet targetWorkerSet, List *commandList)
{
	List *workerNodeList = ActiveWorkerNodeList();
	ListCell *workerNodeCell = NULL;
	char *nodeUser = CitusExtensionOwnerName();
	ListCell *commandCell = NULL;

	/* run commands serially */
	foreach(workerNodeCell, workerNodeList)
	{
		MultiConnection *workerConnection = NULL;
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		int connectionFlags = FORCE_NEW_CONNECTION;

		if (targetWorkerSet == WORKERS_WITH_METADATA && !workerNode->hasMetadata)
		{
			continue;
		}

		workerConnection = GetNodeUserDatabaseConnection(connectionFlags, nodeName,
														 nodePort, nodeUser, NULL);

		/* iterate over the commands and execute them in the same connection */
		foreach(commandCell, commandList)
		{
			char *commandString = lfirst(commandCell);

			ExecuteCriticalRemoteCommand(workerConnection, commandString);
		}

		CloseConnection(workerConnection);
	}
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
	List *connectionList = NIL;
	ListCell *connectionCell = NULL;
	List *workerNodeList = ActiveWorkerNodeList();
	ListCell *workerNodeCell = NULL;
	char *nodeUser = CitusExtensionOwnerName();

	BeginOrContinueCoordinatedTransaction();
	CoordinatedTransactionUse2PC();

	/* open connections in parallel */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		MultiConnection *connection = NULL;
		int connectionFlags = 0;

		if (targetWorkerSet == WORKERS_WITH_METADATA && !workerNode->hasMetadata)
		{
			continue;
		}

		connection = StartNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
													 nodeUser, NULL);

		MarkRemoteTransactionCritical(connection);

		connectionList = lappend(connectionList, connection);
	}

	/* finish opening connections */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		FinishConnectionEstablishment(connection);
	}

	RemoteTransactionsBeginIfNecessary(connectionList);

	/* send commands in parallel */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		int querySent = SendRemoteCommandParams(connection, command, parameterCount,
												parameterTypes, parameterValues);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	/* get results */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);

		PGresult *result = GetRemoteCommandResult(connection, true);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, ERROR);
		}

		PQclear(result);

		ForgetResults(connection);
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
	MultiConnection *workerConnection = NULL;
	ListCell *commandCell = NULL;
	int connectionFlags = FORCE_NEW_CONNECTION;

	if (XactModificationLevel > XACT_MODIFICATION_NONE)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("cannot open new connections after the first modification "
							   "command within a transaction")));
	}

	workerConnection = GetNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
													 nodeUser, NULL);

	MarkRemoteTransactionCritical(workerConnection);
	RemoteTransactionBegin(workerConnection);

	/* iterate over the commands and execute them in the same connection */
	foreach(commandCell, commandList)
	{
		char *commandString = lfirst(commandCell);

		ExecuteCriticalRemoteCommand(workerConnection, commandString);
	}

	RemoteTransactionCommit(workerConnection);
	CloseConnection(workerConnection);
}
