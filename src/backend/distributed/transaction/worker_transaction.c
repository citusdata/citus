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
#include "distributed/metadata_sync.h"
#include "distributed/remote_commands.h"
#include "distributed/pg_dist_node.h"
#include "distributed/pg_dist_transaction.h"
#include "distributed/transaction_recovery.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "utils/memutils.h"


static void SendCommandToMetadataWorkersParams(const char *command,
											   const char *user, int parameterCount,
											   const Oid *parameterTypes,
											   const char *const *parameterValues);
static void SendCommandToWorkersParamsInternal(TargetWorkerSet targetWorkerSet,
											   const char *command, const char *user,
											   int parameterCount,
											   const Oid *parameterTypes,
											   const char *const *parameterValues);
static void ErrorIfAnyMetadataNodeOutOfSync(List *metadataNodeList);
static List * OpenConnectionsToWorkersInParallel(TargetWorkerSet targetWorkerSet,
												 const char *user);
static void GetConnectionsResults(List *connectionList, bool failOnError);
static void SendCommandToWorkersOutsideTransaction(TargetWorkerSet targetWorkerSet,
												   const char *command, const char *user,
												   bool
												   failOnError);

/*
 * SendCommandToWorker sends a command to a particular worker as part of the
 * 2PC.
 */
void
SendCommandToWorker(const char *nodeName, int32 nodePort, const char *command)
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

	/* run commands serially */
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;

		SendCommandToWorkerAsUser(nodeName, nodePort, nodeUser, command);
	}
}


/*
 * SendCommandToWorkerAsUser sends a command to a particular worker as a particular user
 * as part of the 2PC.
 */
void
SendCommandToWorkerAsUser(const char *nodeName, int32 nodePort, const char *nodeUser,
						  const char *command)
{
	uint32 connectionFlags = 0;

	UseCoordinatedTransaction();
	CoordinatedTransactionUse2PC();

	MultiConnection *transactionConnection = GetNodeUserDatabaseConnection(
		connectionFlags, nodeName,
		nodePort,
		nodeUser, NULL);

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
SendCommandToWorkersWithMetadata(const char *command)
{
	SendCommandToMetadataWorkersParams(command, CitusExtensionOwnerName(),
									   0, NULL, NULL);
}


/*
 * TargetWorkerSetNodeList returns a list of WorkerNode's that satisfies the
 * TargetWorkerSet.
 */
List *
TargetWorkerSetNodeList(TargetWorkerSet targetWorkerSet, LOCKMODE lockMode)
{
	List *workerNodeList = NIL;
	if (targetWorkerSet == ALL_SHARD_NODES)
	{
		workerNodeList = ActivePrimaryNodeList(lockMode);
	}
	else
	{
		workerNodeList = ActivePrimaryNonCoordinatorNodeList(lockMode);
	}
	List *result = NIL;

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		if (targetWorkerSet == NON_COORDINATOR_METADATA_NODES && !workerNode->hasMetadata)
		{
			continue;
		}

		result = lappend(result, workerNode);
	}

	return result;
}


/*
 * SendBareCommandListToMetadataWorkers sends a list of commands to metadata
 * workers in serial. Commands are committed immediately: new connections are
 * always used and no transaction block is used (hence "bare"). The connections
 * are made as the extension owner to ensure write access to the Citus metadata
 * tables. Primarly useful for INDEX commands using CONCURRENTLY.
 */
void
SendBareCommandListToMetadataWorkers(List *commandList)
{
	TargetWorkerSet targetWorkerSet = NON_COORDINATOR_METADATA_NODES;
	List *workerNodeList = TargetWorkerSetNodeList(targetWorkerSet, ShareLock);
	char *nodeUser = CitusExtensionOwnerName();

	ErrorIfAnyMetadataNodeOutOfSync(workerNodeList);

	/* run commands serially */
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		int connectionFlags = FORCE_NEW_CONNECTION;

		MultiConnection *workerConnection = GetNodeUserDatabaseConnection(connectionFlags,
																		  nodeName,
																		  nodePort,
																		  nodeUser, NULL);

		/* iterate over the commands and execute them in the same connection */
		const char *commandString = NULL;
		foreach_ptr(commandString, commandList)
		{
			ExecuteCriticalRemoteCommand(workerConnection, commandString);
		}

		CloseConnection(workerConnection);
	}
}


/*
 * SendBareOptionalCommandListToAllWorkersAsUser sends a list of commands
 * to all workers in serial. Commands are committed immediately: new
 * connections are always used and no transaction block is used (hence "bare").
 */
int
SendBareOptionalCommandListToAllWorkersAsUser(List *commandList, const char *user)
{
	TargetWorkerSet targetWorkerSet = NON_COORDINATOR_NODES;
	List *workerNodeList = TargetWorkerSetNodeList(targetWorkerSet, ShareLock);
	int maxError = RESPONSE_OKAY;

	/* run commands serially */
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		int connectionFlags = FORCE_NEW_CONNECTION;

		MultiConnection *workerConnection = GetNodeUserDatabaseConnection(connectionFlags,
																		  nodeName,
																		  nodePort, user,
																		  NULL);

		/* iterate over the commands and execute them in the same connection */
		const char *commandString = NULL;
		foreach_ptr(commandString, commandList)
		{
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
 * SendCommandToMetadataWorkersParams is a wrapper around
 * SendCommandToWorkersParamsInternal() enforcing some extra checks.
 */
static void
SendCommandToMetadataWorkersParams(const char *command,
								   const char *user, int parameterCount,
								   const Oid *parameterTypes,
								   const char *const *parameterValues)
{
	List *workerNodeList = TargetWorkerSetNodeList(NON_COORDINATOR_METADATA_NODES,
												   ShareLock);

	ErrorIfAnyMetadataNodeOutOfSync(workerNodeList);

	SendCommandToWorkersParamsInternal(NON_COORDINATOR_METADATA_NODES, command, user,
									   parameterCount, parameterTypes,
									   parameterValues);
}


/*
 * SendCommandToWorkersOptionalInParallel sends the given command to workers in parallel.
 * It does error if there is a problem while sending the query, but it doesn't error
 * if there is a problem while executing the query.
 */
void
SendCommandToWorkersOptionalInParallel(TargetWorkerSet targetWorkerSet, const
									   char *command,
									   const char *user)
{
	bool failOnError = false;
	SendCommandToWorkersOutsideTransaction(targetWorkerSet, command, user,
										   failOnError);
}


/*
 * SendCommandToWorkersInParallel sends the given command to workers in parallel.
 * It does error if there is a problem while sending the query, it errors if there
 * was any problem when sending/receiving.
 */
void
SendCommandToWorkersInParallel(TargetWorkerSet targetWorkerSet, const
							   char *command,
							   const char *user)
{
	bool failOnError = true;
	SendCommandToWorkersOutsideTransaction(targetWorkerSet, command, user,
										   failOnError);
}


/*
 * SendCommandToWorkersOutsideTransaction sends the given command to workers in parallel.
 */
static void
SendCommandToWorkersOutsideTransaction(TargetWorkerSet targetWorkerSet, const
									   char *command, const char *user, bool
									   failOnError)
{
	List *connectionList = OpenConnectionsToWorkersInParallel(targetWorkerSet, user);

	/* finish opening connections */
	FinishConnectionListEstablishment(connectionList);

	/* send commands in parallel */
	MultiConnection *connection = NULL;
	foreach_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommand(connection, command);
		if (failOnError && querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	GetConnectionsResults(connectionList, failOnError);
}


/*
 * OpenConnectionsToWorkersInParallel opens connections to the given target worker set in parallel,
 * as the given user.
 */
static List *
OpenConnectionsToWorkersInParallel(TargetWorkerSet targetWorkerSet, const char *user)
{
	List *connectionList = NIL;

	List *workerNodeList = TargetWorkerSetNodeList(targetWorkerSet, ShareLock);

	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		int32 connectionFlags = OUTSIDE_TRANSACTION;

		MultiConnection *connection = StartNodeUserDatabaseConnection(connectionFlags,
																	  nodeName, nodePort,
																	  user, NULL);

		/*
		 * connection can only be NULL for optional connections, which we don't
		 * support in this codepath.
		 */
		Assert((connectionFlags & OPTIONAL_CONNECTION) == 0);
		Assert(connection != NULL);
		connectionList = lappend(connectionList, connection);
	}
	return connectionList;
}


/*
 * GetConnectionsResults gets remote command results
 * for the given connections. It raises any error if failOnError is true.
 */
static void
GetConnectionsResults(List *connectionList, bool failOnError)
{
	MultiConnection *connection = NULL;
	foreach_ptr(connection, connectionList)
	{
		bool raiseInterrupt = false;
		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupt);

		bool isResponseOK = result != NULL && IsResponseOK(result);
		if (failOnError && !isResponseOK)
		{
			ReportResultError(connection, result, ERROR);
		}

		PQclear(result);

		if (isResponseOK)
		{
			ForgetResults(connection);
		}
	}
}


/*
 * SendCommandToWorkersParamsInternal sends a command to all workers in parallel.
 * Commands are committed on the workers when the local transaction commits. The
 * connection are made as the extension owner to ensure write access to the Citus
 * metadata tables. Parameters can be specified as for PQexecParams, except that
 * paramLengths, paramFormats and resultFormat are hard-coded to NULL, NULL and 0
 * respectively.
 */
static void
SendCommandToWorkersParamsInternal(TargetWorkerSet targetWorkerSet, const char *command,
								   const char *user, int parameterCount,
								   const Oid *parameterTypes,
								   const char *const *parameterValues)
{
	List *connectionList = NIL;
	List *workerNodeList = TargetWorkerSetNodeList(targetWorkerSet, ShareLock);

	UseCoordinatedTransaction();
	CoordinatedTransactionUse2PC();

	/* open connections in parallel */
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		int32 connectionFlags = 0;

		MultiConnection *connection = StartNodeUserDatabaseConnection(connectionFlags,
																	  nodeName, nodePort,
																	  user, NULL);

		/*
		 * connection can only be NULL for optional connections, which we don't
		 * support in this codepath.
		 */
		Assert((connectionFlags & OPTIONAL_CONNECTION) == 0);
		Assert(connection != NULL);
		MarkRemoteTransactionCritical(connection);

		connectionList = lappend(connectionList, connection);
	}

	/* finish opening connections */
	FinishConnectionListEstablishment(connectionList);

	RemoteTransactionsBeginIfNecessary(connectionList);

	/* send commands in parallel */
	MultiConnection *connection = NULL;
	foreach_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommandParams(connection, command, parameterCount,
												parameterTypes, parameterValues, false);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	/* get results */
	foreach_ptr(connection, connectionList)
	{
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
	int connectionFlags = FORCE_NEW_CONNECTION;

	MultiConnection *workerConnection = GetNodeUserDatabaseConnection(connectionFlags,
																	  nodeName, nodePort,
																	  nodeUser, NULL);

	MarkRemoteTransactionCritical(workerConnection);
	RemoteTransactionBegin(workerConnection);

	/* iterate over the commands and execute them in the same connection */
	const char *commandString = NULL;
	foreach_ptr(commandString, commandList)
	{
		ExecuteCriticalRemoteCommand(workerConnection, commandString);
	}

	RemoteTransactionCommit(workerConnection);
	CloseConnection(workerConnection);
}


/*
 * ErrorIfAnyMetadataNodeOutOfSync raises an error if any of the given
 * metadata nodes are out of sync. It is safer to avoid metadata changing
 * commands (e.g. DDL or node addition) until all metadata nodes have
 * been synced.
 *
 * An example of we could get in a bad situation without doing so is:
 *  1. Create a reference table
 *  2. After the node becomes out of sync, add a new active node
 *  3. Insert into the reference table from the out of sync node
 *
 * Since the out-of-sync might not know about the new node, it won't propagate
 * the changes to the new node and replicas will be in an inconsistent state.
 */
static void
ErrorIfAnyMetadataNodeOutOfSync(List *metadataNodeList)
{
	WorkerNode *metadataNode = NULL;
	foreach_ptr(metadataNode, metadataNodeList)
	{
		Assert(metadataNode->hasMetadata);

		if (!metadataNode->metadataSynced)
		{
			const char *workerName = metadataNode->workerName;
			int workerPort = metadataNode->workerPort;
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("%s:%d is a metadata node, but is out of sync",
								   workerName, workerPort),
							errhint("If the node is up, wait until metadata"
									" gets synced to it and try again.")));
		}
	}
}
