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

#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "libpq-fe.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "distributed/connection_management.h"
#include "distributed/jsonbutils.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/pg_dist_node.h"
#include "distributed/pg_dist_transaction.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_recovery.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"

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


/*
 * SendCommandToWorker sends a command to a particular worker as part of the
 * 2PC.
 */
void
SendCommandToWorker(const char *nodeName, int32 nodePort, const char *command)
{
	const char *nodeUser = CurrentUserName();
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
	List *workerNodeList = TargetWorkerSetNodeList(targetWorkerSet, RowShareLock);

	/* run commands serially */
	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodeList)
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
	Use2PCForCoordinatedTransaction();

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
 * transaction commits.
 */
void
SendCommandToWorkersWithMetadata(const char *command)
{
	SendCommandToMetadataWorkersParams(command, CurrentUserName(),
									   0, NULL, NULL);
}


/*
 * SendCommandToWorkersWithMetadataViaSuperUser sends a command to all workers in
 * parallel by opening a super user connection. Commands are committed on the workers
 * when the local transaction commits. The connection are made as the extension
 * owner to ensure write access to the Citus metadata tables.
 *
 * Since we prevent to open superuser connections for metadata tables, it is
 * discouraged to use it. Consider using it only for propagating pg_dist_object
 * tuples for dependent objects.
 */
void
SendCommandToWorkersWithMetadataViaSuperUser(const char *command)
{
	SendCommandToMetadataWorkersParams(command, CitusExtensionOwnerName(),
									   0, NULL, NULL);
}


/*
 * SendCommandListToWorkersWithMetadata sends all commands to all metadata workers
 * with the current user. See `SendCommandToWorkersWithMetadata`for details.
 */
void
SendCommandListToWorkersWithMetadata(List *commands)
{
	char *command = NULL;
	foreach_declared_ptr(command, commands)
	{
		SendCommandToWorkersWithMetadata(command);
	}
}


/*
 * TargetWorkerSetNodeList returns a list of WorkerNode's that satisfies the
 * TargetWorkerSet.
 */
List *
TargetWorkerSetNodeList(TargetWorkerSet targetWorkerSet, LOCKMODE lockMode)
{
	List *workerNodeList = NIL;
	if (targetWorkerSet == ALL_SHARD_NODES || targetWorkerSet == METADATA_NODES)
	{
		workerNodeList = ActivePrimaryNodeList(lockMode);
	}
	else
	{
		workerNodeList = ActivePrimaryNonCoordinatorNodeList(lockMode);
	}
	List *result = NIL;

	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodeList)
	{
		if ((targetWorkerSet == NON_COORDINATOR_METADATA_NODES || targetWorkerSet ==
			 METADATA_NODES) &&
			!workerNode->hasMetadata)
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
	List *workerNodeList = TargetWorkerSetNodeList(targetWorkerSet, RowShareLock);
	char *nodeUser = CurrentUserName();

	ErrorIfAnyMetadataNodeOutOfSync(workerNodeList);

	/* run commands serially */
	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodeList)
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
		foreach_declared_ptr(commandString, commandList)
		{
			ExecuteCriticalRemoteCommand(workerConnection, commandString);
		}

		CloseConnection(workerConnection);
	}
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
												   RowShareLock);

	ErrorIfAnyMetadataNodeOutOfSync(workerNodeList);

	SendCommandToWorkersParamsInternal(NON_COORDINATOR_METADATA_NODES, command, user,
									   parameterCount, parameterTypes,
									   parameterValues);
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
	List *workerNodeList = TargetWorkerSetNodeList(targetWorkerSet, RowShareLock);

	UseCoordinatedTransaction();
	Use2PCForCoordinatedTransaction();

	/* open connections in parallel */
	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		int32 connectionFlags = REQUIRE_METADATA_CONNECTION;

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
	foreach_declared_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommandParams(connection, command, parameterCount,
												parameterTypes, parameterValues, false);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	/* get results */
	foreach_declared_ptr(connection, connectionList)
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
 * SendCommandListToWorkerOutsideTransaction forces to open a new connection
 * to the node with the given nodeName and nodePort. Then, the connection starts
 * a transaction on the remote node and executes the commands in the transaction.
 * The function raises error if any of the queries fails.
 */
void
SendCommandListToWorkerOutsideTransaction(const char *nodeName, int32 nodePort,
										  const char *nodeUser, List *commandList)
{
	int connectionFlags = FORCE_NEW_CONNECTION;

	MultiConnection *workerConnection = GetNodeUserDatabaseConnection(connectionFlags,
																	  nodeName, nodePort,
																	  nodeUser, NULL);

	SendCommandListToWorkerOutsideTransactionWithConnection(workerConnection,
															commandList);
	CloseConnection(workerConnection);
}


/*
 * SendCommandListToWorkerOutsideTransactionWithConnection sends the command list
 * over the specified connection. This opens a new transaction on the
 * connection, thus it's important that no transaction is currently open.
 * This function is mainly useful to avoid opening an closing
 * connections excessively by allowing reusing a single connection to send
 * multiple separately committing transactions. The function raises an error if
 * any of the queries fail.
 */
void
SendCommandListToWorkerOutsideTransactionWithConnection(MultiConnection *workerConnection,
														List *commandList)
{
	MarkRemoteTransactionCritical(workerConnection);
	RemoteTransactionBegin(workerConnection);

	/* iterate over the commands and execute them in the same connection */
	const char *commandString = NULL;
	foreach_declared_ptr(commandString, commandList)
	{
		ExecuteCriticalRemoteCommand(workerConnection, commandString);
	}

	RemoteTransactionCommit(workerConnection);
	ResetRemoteTransaction(workerConnection);
}


/*
 * SendCommandListToWorkerListWithBareConnections sends the command list
 * over the specified bare connections. This function is mainly useful to
 * avoid opening an closing connections excessively by allowing reusing
 * connections to send multiple separate bare commands. The function
 * raises an error if any of the queries fail.
 */
void
SendCommandListToWorkerListWithBareConnections(List *workerConnectionList,
											   List *commandList)
{
	Assert(!InCoordinatedTransaction());
	Assert(!GetCoordinatedTransactionShouldUse2PC());

	if (list_length(commandList) == 0 || list_length(workerConnectionList) == 0)
	{
		/* nothing to do */
		return;
	}

	/*
	 * In order to avoid round-trips per query in queryStringList,
	 * we join the string and send as a single command. Also,
	 * if there is only a single command, avoid additional call to
	 * StringJoin given that some strings can be quite large.
	 */
	char *stringToSend = (list_length(commandList) == 1) ?
						 linitial(commandList) : StringJoin(commandList, ';');

	/* send commands in parallel */
	MultiConnection *connection = NULL;
	foreach_declared_ptr(connection, workerConnectionList)
	{
		int querySent = SendRemoteCommand(connection, stringToSend);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	bool failOnError = true;
	foreach_declared_ptr(connection, workerConnectionList)
	{
		ClearResults(connection, failOnError);
	}
}


/*
 * SendCommandListToWorkerInCoordinatedTransaction opens connection to the node
 * with the given nodeName and nodePort. The commands are sent as part of the
 * coordinated transaction. Any failures aborts the coordinated transaction.
 */
void
SendMetadataCommandListToWorkerListInCoordinatedTransaction(List *workerNodeList,
															const char *nodeUser,
															List *commandList)
{
	if (list_length(commandList) == 0 || list_length(workerNodeList) == 0)
	{
		/* nothing to do */
		return;
	}

	ErrorIfAnyMetadataNodeOutOfSync(workerNodeList);

	UseCoordinatedTransaction();

	List *connectionList = NIL;

	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		int connectionFlags = REQUIRE_METADATA_CONNECTION;

		MultiConnection *connection =
			StartNodeConnection(connectionFlags, nodeName, nodePort);

		MarkRemoteTransactionCritical(connection);

		/*
		 * connection can only be NULL for optional connections, which we don't
		 * support in this codepath.
		 */
		Assert((connectionFlags & OPTIONAL_CONNECTION) == 0);
		Assert(connection != NULL);
		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);

	/* must open transaction blocks to use intermediate results */
	RemoteTransactionsBeginIfNecessary(connectionList);

	/*
	 * In order to avoid round-trips per query in queryStringList,
	 * we join the string and send as a single command. Also,
	 * if there is only a single command, avoid additional call to
	 * StringJoin given that some strings can be quite large.
	 */
	char *stringToSend = (list_length(commandList) == 1) ?
						 linitial(commandList) : StringJoin(commandList, ';');

	/* send commands in parallel */
	bool failOnError = true;
	MultiConnection *connection = NULL;
	foreach_declared_ptr(connection, connectionList)
	{
		int querySent = SendRemoteCommand(connection, stringToSend);
		if (querySent == 0)
		{
			ReportConnectionError(connection, ERROR);
		}
	}

	foreach_declared_ptr(connection, connectionList)
	{
		ClearResults(connection, failOnError);
	}
}


/*
 * SendOptionalCommandListToWorkerOutsideTransactionWithConnection sends the
 * given command list over a specified connection in a single transaction that
 * is outside of the coordinated tranaction.
 *
 * If any of the commands fail, it rollbacks the transaction, and otherwise commits.
 * A successful commit is indicated by returning true, and a failed commit by returning
 * false.
 */
bool
SendOptionalCommandListToWorkerOutsideTransactionWithConnection(
	MultiConnection *workerConnection, List *commandList)
{
	if (PQstatus(workerConnection->pgConn) != CONNECTION_OK)
	{
		return false;
	}
	RemoteTransactionBegin(workerConnection);

	/* iterate over the commands and execute them in the same connection */
	bool failed = false;
	const char *commandString = NULL;
	foreach_declared_ptr(commandString, commandList)
	{
		if (ExecuteOptionalRemoteCommand(workerConnection, commandString, NULL) != 0)
		{
			failed = true;
			break;
		}
	}

	if (failed)
	{
		RemoteTransactionAbort(workerConnection);
	}
	else
	{
		RemoteTransactionCommit(workerConnection);
	}

	ResetRemoteTransaction(workerConnection);

	return !failed;
}


/*
 * SendOptionalCommandListToWorkerOutsideTransaction sends the given command
 * list to the given worker in a single transaction that is outside of the
 * coordinated tranaction. If any of the commands fail, it rollbacks the
 * transaction, and otherwise commits.
 */
bool
SendOptionalCommandListToWorkerOutsideTransaction(const char *nodeName, int32 nodePort,
												  const char *nodeUser, List *commandList)
{
	int connectionFlags = FORCE_NEW_CONNECTION;

	MultiConnection *workerConnection = GetNodeUserDatabaseConnection(connectionFlags,
																	  nodeName, nodePort,
																	  nodeUser, NULL);
	bool failed = SendOptionalCommandListToWorkerOutsideTransactionWithConnection(
		workerConnection,
		commandList);
	CloseConnection(workerConnection);

	return !failed;
}


/*
 * SendOptionalMetadataCommandListToWorkerInCoordinatedTransaction sends the given
 * command list to the given worker as part of the coordinated transaction.
 * If any of the commands fail, the function returns false.
 */
bool
SendOptionalMetadataCommandListToWorkerInCoordinatedTransaction(const char *nodeName,
																int32 nodePort,
																const char *nodeUser,
																List *commandList)
{
	int connectionFlags = REQUIRE_METADATA_CONNECTION;
	bool failed = false;

	UseCoordinatedTransaction();

	MultiConnection *workerConnection =
		GetNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
									  nodeUser, NULL);
	if (PQstatus(workerConnection->pgConn) != CONNECTION_OK)
	{
		return false;
	}

	RemoteTransactionsBeginIfNecessary(list_make1(workerConnection));

	/* iterate over the commands and execute them in the same connection */
	const char *commandString = NULL;
	foreach_declared_ptr(commandString, commandList)
	{
		if (ExecuteOptionalRemoteCommand(workerConnection, commandString, NULL) !=
			RESPONSE_OKAY)
		{
			failed = true;

			bool raiseErrors = false;
			MarkRemoteTransactionFailed(workerConnection, raiseErrors);
			break;
		}
	}

	return !failed;
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
	foreach_declared_ptr(metadataNode, metadataNodeList)
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


/*
 * IsWorkerTheCurrentNode checks if the given worker refers to the
 * the current node by comparing the server id of the worker and of the
 * current nodefrom pg_dist_node_metadata
 */
bool
IsWorkerTheCurrentNode(WorkerNode *workerNode)
{
	int connectionFlags = REQUIRE_METADATA_CONNECTION;

	MultiConnection *workerConnection =
		GetNodeUserDatabaseConnection(connectionFlags,
									  workerNode->workerName,
									  workerNode->workerPort,
									  CurrentUserName(),
									  NULL);

	const char *command =
		"SELECT metadata ->> 'server_id' AS server_id FROM pg_dist_node_metadata";

	int resultCode = SendRemoteCommand(workerConnection, command);

	if (resultCode == 0)
	{
		CloseConnection(workerConnection);
		return false;
	}

	PGresult *result = GetRemoteCommandResult(workerConnection, true);

	if (result == NULL)
	{
		return false;
	}

	List *commandResult = ReadFirstColumnAsText(result);

	PQclear(result);
	ForgetResults(workerConnection);

	if ((list_length(commandResult) != 1))
	{
		return false;
	}

	StringInfo resultInfo = (StringInfo) linitial(commandResult);
	char *workerServerId = resultInfo->data;

	Datum metadata = DistNodeMetadata();
	text *currentServerIdTextP = ExtractFieldTextP(metadata, "server_id");

	if (currentServerIdTextP == NULL)
	{
		return false;
	}

	char *currentServerId = text_to_cstring(currentServerIdTextP);

	return strcmp(workerServerId, currentServerId) == 0;
}
