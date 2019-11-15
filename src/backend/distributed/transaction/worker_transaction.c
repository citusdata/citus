/*-------------------------------------------------------------------------
 *
 * worker_transaction.c
 *
 * Routines for performing transactions across all workers.
 *
 * Copyright (c) Citus Data, Inc.
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
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
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
SendCommandToWorker(char *nodeName, int32 nodePort, const char *command)
{
	const char *nodeUser = CitusExtensionOwnerName();
	SendCommandToWorkerAsUser(nodeName, nodePort, nodeUser, command);
}


/*
 * SendCommandToWorkersAsUser sends a command to targetWorkerSet as a particular user
 * as part of the 2PC.
 */
void
SendCommandToWorkersAsUser(TargetWorkerSet targetWorkerSet, const char *nodeUser,
						   const char *command)
{
	List *workerNodeList = TargetWorkerSetNodeList(targetWorkerSet, ShareLock);
	ListCell *workerNodeCell = NULL;

	/* run commands serially */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;

		SendCommandToWorkerAsUser(nodeName, nodePort, nodeUser, command);
	}
}


/*
 * SendCommandToWorkerAsUser sends a command to a particular worker as a particular user
 * as part of the 2PC.
 */
void
SendCommandToWorkerAsUser(char *nodeName, int32 nodePort, const char *nodeUser,
						  const char *command)
{
	MultiConnection *transactionConnection = NULL;
	uint connectionFlags = 0;

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
SendCommandToWorkers(TargetWorkerSet targetWorkerSet, const char *command)
{
	SendCommandToWorkersParams(targetWorkerSet, command, CitusExtensionOwnerName(),
							   0, NULL, NULL);
}


/*
 * TargetWorkerSetNodeList returns a list of WorkerNode's that satisfies the
 * TargetWorkerSet.
 */
List *
TargetWorkerSetNodeList(TargetWorkerSet targetWorkerSet, LOCKMODE lockMode)
{
	List *workerNodeList = ActivePrimaryWorkerNodeList(lockMode);
	ListCell *workerNodeCell = NULL;
	List *result = NIL;

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

		if (targetWorkerSet == WORKERS_WITH_METADATA &&
			(!workerNode->hasMetadata || !workerNode->metadataSynced))
		{
			continue;
		}
		if (targetWorkerSet == OTHER_WORKERS &&
			workerNode->groupId == GetLocalGroupId())
		{
			continue;
		}

		result = lappend(result, workerNode);
	}

	return result;
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
	List *workerNodeList = TargetWorkerSetNodeList(targetWorkerSet, ShareLock);
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
 * SendBareOptionalCommandListToWorkersAsUser sends a list of commands to a set of target
 * workers in serial. Commands are committed immediately: new connections are
 * always used and no transaction block is used (hence "bare").
 */
int
SendBareOptionalCommandListToWorkersAsUser(TargetWorkerSet targetWorkerSet,
										   List *commandList, const char *user)
{
	List *workerNodeList = TargetWorkerSetNodeList(targetWorkerSet, ShareLock);
	ListCell *workerNodeCell = NULL;
	ListCell *commandCell = NULL;
	int maxError = RESPONSE_OKAY;

	/* run commands serially */
	foreach(workerNodeCell, workerNodeList)
	{
		MultiConnection *workerConnection = NULL;
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		int connectionFlags = FORCE_NEW_CONNECTION;

		workerConnection = GetNodeUserDatabaseConnection(connectionFlags, nodeName,
														 nodePort, user, NULL);

		/* iterate over the commands and execute them in the same connection */
		foreach(commandCell, commandList)
		{
			char *commandString = lfirst(commandCell);
			int result = ExecuteOptionalRemoteCommand(workerConnection, commandString,
													  NULL);
			if (result != RESPONSE_OKAY)
			{
				maxError = Max(maxError, result);
				break;
			}
		}

		CloseConnection(workerConnection);
	}

	return maxError;
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
SendCommandToWorkersParams(TargetWorkerSet targetWorkerSet, const char *command,
						   const char *user, int parameterCount,
						   const Oid *parameterTypes, const char *const *parameterValues)
{
	List *connectionList = NIL;
	ListCell *connectionCell = NULL;
	List *workerNodeList = TargetWorkerSetNodeList(targetWorkerSet, ShareLock);
	ListCell *workerNodeCell = NULL;

	BeginOrContinueCoordinatedTransaction();
	CoordinatedTransactionUse2PC();

	/* open connections in parallel */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		MultiConnection *connection = NULL;
		int32 connectionFlags = 0;

		connection = StartNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
													 user, NULL);

		MarkRemoteTransactionCritical(connection);

		connectionList = lappend(connectionList, connection);
	}

	/* finish opening connections */
	FinishConnectionListEstablishment(connectionList);

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
 * EnsureNoModificationsHaveBeenDone reports an error if we have performed any
 * modification in the current transaction to prevent opening a connection is such cases.
 */
void
EnsureNoModificationsHaveBeenDone()
{
	if (XactModificationLevel > XACT_MODIFICATION_NONE)
	{
		ereport(ERROR, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						errmsg("cannot open new connections after the first modification "
							   "command within a transaction")));
	}
}


/*
 * SendCommandListToWorkerInSingleTransaction opens connection to the node with the given
 * nodeName and nodePort. Then, the connection starts a transaction on the remote
 * node and executes the commands in the transaction. The function raises error if
 * any of the queries fails.
 */
void
SendCommandListToWorkerInSingleTransaction(const char *nodeName, int32 nodePort,
										   const char *nodeUser, List *commandList)
{
	MultiConnection *workerConnection = NULL;
	ListCell *commandCell = NULL;
	int connectionFlags = FORCE_NEW_CONNECTION;

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
