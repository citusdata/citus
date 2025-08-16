#include "postgres.h"

#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "utils/fmgrprotos.h"
#include "utils/pg_lsn.h"

#include "distributed/argutils.h"
#include "distributed/clonenode_utils.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_rebalancer.h"

/*
 * GetReplicationLag calculates the replication lag between the primary and replica nodes.
 * It returns the lag in bytes.
 */
int64
GetReplicationLag(WorkerNode *primaryWorkerNode, WorkerNode *replicaWorkerNode)
{
	/* Input validation */
	if (primaryWorkerNode == NULL || replicaWorkerNode == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("primary or replica worker node is NULL")));
	}

#if PG_VERSION_NUM >= 100000
	const char *primary_lsn_query = "SELECT pg_current_wal_lsn()";
	const char *replica_lsn_query = "SELECT pg_last_wal_replay_lsn()";
#else
	const char *primary_lsn_query = "SELECT pg_current_xlog_location()";
	const char *replica_lsn_query = "SELECT pg_last_xlog_replay_location()";
#endif

	int connectionFlag = 0;
	MultiConnection *primaryConnection = GetNodeConnection(connectionFlag,
														   primaryWorkerNode->workerName,
														   primaryWorkerNode->workerPort);
	if (PQstatus(primaryConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg(
							"cannot connect to primary node %s:%d to fetch replication status",
							primaryWorkerNode->workerName, primaryWorkerNode->
							workerPort)));
	}
	MultiConnection *replicaConnection = GetNodeConnection(connectionFlag,
														   replicaWorkerNode->workerName,
														   replicaWorkerNode->workerPort);

	if (PQstatus(replicaConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg(
							"cannot connect to clone node %s:%d to fetch replication status",
							replicaWorkerNode->workerName, replicaWorkerNode->
							workerPort)));
	}

	int primaryResultCode = SendRemoteCommand(primaryConnection, primary_lsn_query);
	if (primaryResultCode == 0)
	{
		ReportConnectionError(primaryConnection, ERROR);
	}

	PGresult *primaryResult = GetRemoteCommandResult(primaryConnection, true);
	if (!IsResponseOK(primaryResult))
	{
		ReportResultError(primaryConnection, primaryResult, ERROR);
	}

	int replicaResultCode = SendRemoteCommand(replicaConnection, replica_lsn_query);
	if (replicaResultCode == 0)
	{
		ReportConnectionError(replicaConnection, ERROR);
	}
	PGresult *replicaResult = GetRemoteCommandResult(replicaConnection, true);
	if (!IsResponseOK(replicaResult))
	{
		ReportResultError(replicaConnection, replicaResult, ERROR);
	}


	List *primaryLsnList = ReadFirstColumnAsText(primaryResult);
	if (list_length(primaryLsnList) != 1)
	{
		PQclear(primaryResult);
		ClearResults(primaryConnection, true);
		CloseConnection(primaryConnection);
		PQclear(replicaResult);
		ClearResults(replicaConnection, true);
		CloseConnection(replicaConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("cannot parse primary LSN result from %s:%d",
							   primaryWorkerNode->workerName,
							   primaryWorkerNode->workerPort),
						errdetail("Expected exactly one row with LSN value")));
	}
	StringInfo primaryLsnQueryResInfo = (StringInfo) linitial(primaryLsnList);
	char *primary_lsn_str = primaryLsnQueryResInfo->data;

	List *replicaLsnList = ReadFirstColumnAsText(replicaResult);
	if (list_length(replicaLsnList) != 1)
	{
		PQclear(primaryResult);
		ClearResults(primaryConnection, true);
		CloseConnection(primaryConnection);
		PQclear(replicaResult);
		ClearResults(replicaConnection, true);
		CloseConnection(replicaConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("cannot parse clone LSN result from %s:%d",
							   replicaWorkerNode->workerName,
							   replicaWorkerNode->workerPort),
						errdetail("Expected exactly one row with LSN value")));
	}
	StringInfo replicaLsnQueryResInfo = (StringInfo) linitial(replicaLsnList);
	char *replica_lsn_str = replicaLsnQueryResInfo->data;

	int64 primary_lsn = DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(
															primary_lsn_str)));
	int64 replica_lsn = DatumGetLSN(DirectFunctionCall1(pg_lsn_in, CStringGetDatum(
															replica_lsn_str)));

	int64 lag_bytes = primary_lsn - replica_lsn;

	PQclear(primaryResult);
	ForgetResults(primaryConnection);
	CloseConnection(primaryConnection);

	PQclear(replicaResult);
	ForgetResults(replicaConnection);
	CloseConnection(replicaConnection);

	ereport(DEBUG1, (errmsg(
						 "successfully measured replication lag: primary LSN %s, clone LSN %s",
						 primary_lsn_str, replica_lsn_str)));
	ereport(NOTICE, (errmsg("replication lag between %s:%d and %s:%d is %ld bytes",
							primaryWorkerNode->workerName, primaryWorkerNode->workerPort,
							replicaWorkerNode->workerName, replicaWorkerNode->workerPort,
							lag_bytes)));
	return lag_bytes;
}


/*
 * EnsureReplicaIsNotSynchronous verifies that the clone is not configured as a synchronous replica
 * and that it is actually replicating from the specified primary node.
 * If the clone is synchronous or not connected to the primary, it throws an ERROR.
 */
void
EnsureReplicaIsNotSynchronous(WorkerNode *primaryWorkerNode,
							  WorkerNode *replicaWorkerNode,
							  char *operation)
{
	EnsureValidCloneMode(primaryWorkerNode, replicaWorkerNode->workerName,
						 replicaWorkerNode->workerPort, operation);
}


void
EnsureValidCloneMode(WorkerNode *primaryWorkerNode,
					 char *cloneHostname, int clonePort, char *operation)
{
	Assert(operation != NULL);

	if (primaryWorkerNode == NULL || cloneHostname == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("primary or clone worker node is NULL")));
	}

	ereport(NOTICE, (errmsg(
						 "checking replication relationship between primary %s:%d and clone %s:%d",
						 primaryWorkerNode->workerName, primaryWorkerNode->workerPort,
						 cloneHostname, clonePort)));

	/* Connect to primary node to check replication status */
	int connectionFlag = 0;
	MultiConnection *primaryConnection = GetNodeConnection(connectionFlag,
														   primaryWorkerNode->workerName,
														   primaryWorkerNode->workerPort);
	if (PQstatus(primaryConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg(
							"cannot connect to primary node %s:%d to check replication status",
							primaryWorkerNode->workerName, primaryWorkerNode->
							workerPort),
						errdetail(
							"This is required to verify clone relationship and sync state")));
	}

	/* Build query to check if clone is connected and get its sync state */
	StringInfo replicationCheckQuery = makeStringInfo();

	/* First, try to resolve the hostname to IP address for more robust matching */
	char *resolvedIP = NULL;
	struct addrinfo hints, *result, *rp;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;     /* Allow IPv4 or IPv6 */
	hints.ai_socktype = SOCK_STREAM; /* TCP socket */
	hints.ai_flags = AI_PASSIVE;     /* For wildcard IP address */

	int getaddrinfo_result = getaddrinfo(cloneHostname, NULL, &hints, &result);
	if (getaddrinfo_result == 0)
	{
		/* Get the first resolved IP address */
		for (rp = result; rp != NULL; rp = rp->ai_next)
		{
			if (rp->ai_family == AF_INET)
			{
				/* IPv4 */
				struct sockaddr_in *addr_in = (struct sockaddr_in *) rp->ai_addr;
				resolvedIP = palloc(INET_ADDRSTRLEN);
				inet_ntop(AF_INET, &(addr_in->sin_addr), resolvedIP, INET_ADDRSTRLEN);
				break;
			}
			else if (rp->ai_family == AF_INET6)
			{
				/* IPv6 */
				struct sockaddr_in6 *addr_in6 = (struct sockaddr_in6 *) rp->ai_addr;
				resolvedIP = palloc(INET6_ADDRSTRLEN);
				inet_ntop(AF_INET6, &(addr_in6->sin6_addr), resolvedIP, INET6_ADDRSTRLEN);
				break;
			}
		}
		freeaddrinfo(result);
	}

	ereport(NOTICE, (errmsg("checking replication for node %s (resolved IP: %s)",
							cloneHostname,
							resolvedIP ? resolvedIP : "unresolved")));

	/* Build query to check if clone is connected and get its sync state */

	/* We check multiple fields to handle different scenarios:
	 * 1. application_name - if it's set to the node name
	 * 2. client_hostname - if it's the hostname
	 * 3. client_addr - if it's the IP address (most reliable)
	 */
	if (resolvedIP != NULL)
	{
		appendStringInfo(replicationCheckQuery,
						 "SELECT sync_state, state FROM pg_stat_replication WHERE "
						 "application_name = '%s' OR "
						 "client_hostname = '%s' OR "
						 "client_addr = '%s'",
						 cloneHostname,
						 cloneHostname,
						 resolvedIP);
		pfree(resolvedIP);
	}
	else
	{
		/* Fallback to hostname-only check if IP resolution fails */
		appendStringInfo(replicationCheckQuery,
						 "SELECT sync_state, state FROM pg_stat_replication WHERE "
						 "application_name = '%s' OR "
						 "client_hostname = '%s'",
						 cloneHostname,
						 cloneHostname);
	}

	int replicationCheckResultCode = SendRemoteCommand(primaryConnection,
													   replicationCheckQuery->data);
	if (replicationCheckResultCode == 0)
	{
		pfree(replicationCheckQuery->data);
		pfree(replicationCheckQuery);
		CloseConnection(primaryConnection);
		ReportConnectionError(primaryConnection, ERROR);
	}

	PGresult *replicationCheckResult = GetRemoteCommandResult(primaryConnection, true);
	if (!IsResponseOK(replicationCheckResult))
	{
		pfree(replicationCheckQuery->data);
		pfree(replicationCheckQuery);
		CloseConnection(primaryConnection);
		ReportResultError(primaryConnection, replicationCheckResult, ERROR);
	}

	List *replicationStateList = ReadFirstColumnAsText(replicationCheckResult);

	/* Check if clone is connected to this primary */
	if (list_length(replicationStateList) == 0)
	{
		pfree(replicationCheckQuery->data);
		pfree(replicationCheckQuery);
		PQclear(replicationCheckResult);
		ClearResults(primaryConnection, true);
		CloseConnection(primaryConnection);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("clone %s:%d is not connected to primary %s:%d",
							   cloneHostname, clonePort,
							   primaryWorkerNode->workerName, primaryWorkerNode->
							   workerPort),
						errdetail(
							"The clone must be actively replicating from the specified primary node. "
							"Check that the clone is running and properly configured for replication.")));
	}

	/* Check if clone is synchronous */
	if (list_length(replicationStateList) > 0)
	{
		StringInfo syncStateInfo = (StringInfo) linitial(replicationStateList);
		if (syncStateInfo && syncStateInfo->data &&
			(strcmp(syncStateInfo->data, "sync") == 0 || strcmp(syncStateInfo->data,
																"quorum") == 0))
		{
			pfree(replicationCheckQuery->data);
			pfree(replicationCheckQuery);
			PQclear(replicationCheckResult);
			ClearResults(primaryConnection, true);
			CloseConnection(primaryConnection);

			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg(
								"cannot %s clone %s:%d as it is configured as a synchronous replica",
								operation, cloneHostname, clonePort),
							errdetail(
								"Promoting a synchronous clone can cause data consistency issues. "
								"Please configure it as an asynchronous replica first.")))
			;
		}
	}

	/* Cleanup resources */
	pfree(replicationCheckQuery->data);
	pfree(replicationCheckQuery);
	PQclear(replicationCheckResult);
	ClearResults(primaryConnection, true);
	CloseConnection(primaryConnection);

	ereport(NOTICE, (errmsg(
						 "clone %s:%d is properly connected to primary %s:%d and is not synchronous",
						 cloneHostname, clonePort,
						 primaryWorkerNode->workerName, primaryWorkerNode->workerPort))
			);
}


/*
 * EnsureValidStreamingReplica checks if the given replica is a valid streaming
 * replica. It connects to the replica and checks if it is in recovery mode.
 * If it is not, it errors out.
 * It also checks the system identifier of the replica and the primary
 * to ensure they match.
 */
void
EnsureValidStreamingReplica(WorkerNode *primaryWorkerNode, char *replicaHostname, int
							replicaPort)
{
	int connectionFlag = FORCE_NEW_CONNECTION;
	MultiConnection *replicaConnection = GetNodeConnection(connectionFlag, replicaHostname
														   ,
														   replicaPort);

	const char *replica_recovery_query = "SELECT pg_is_in_recovery()";

	int resultCode = SendRemoteCommand(replicaConnection, replica_recovery_query);

	if (resultCode == 0)
	{
		CloseConnection(replicaConnection);
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg(
							"could not connect to %s:%d to execute pg_is_in_recovery()",
							replicaHostname, replicaPort)));
	}

	PGresult *result = GetRemoteCommandResult(replicaConnection, true);

	if (result == NULL)
	{
		CloseConnection(replicaConnection);

		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("node %s:%d does not support pg_is_in_recovery()",
							   replicaHostname, replicaPort)));
	}

	List *sizeList = ReadFirstColumnAsText(result);
	if (list_length(sizeList) != 1)
	{
		PQclear(result);
		ClearResults(replicaConnection, true);
		CloseConnection(replicaConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("cannot parse pg_is_in_recovery() result from %s:%d",
							   replicaHostname,
							   replicaPort)));
	}

	StringInfo isInRecoveryQueryResInfo = (StringInfo) linitial(sizeList);
	char *isInRecoveryQueryResStr = isInRecoveryQueryResInfo->data;

	if (strcmp(isInRecoveryQueryResStr, "t") != 0 && strcmp(isInRecoveryQueryResStr,
															"true") != 0)
	{
		PQclear(result);
		ClearResults(replicaConnection, true);
		CloseConnection(replicaConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("%s:%d is not a streaming replica",
							   replicaHostname,
							   replicaPort)));
	}

	PQclear(result);
	ForgetResults(replicaConnection);

	/* Step2: Get the system identifier from replica */
	const char *sysidQuery =
		"SELECT system_identifier FROM pg_control_system()";

	resultCode = SendRemoteCommand(replicaConnection, sysidQuery);

	if (resultCode == 0)
	{
		CloseConnection(replicaConnection);
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("could not connect to %s:%d to get system identifier",
							   replicaHostname, replicaPort)));
	}

	result = GetRemoteCommandResult(replicaConnection, true);
	if (!IsResponseOK(result))
	{
		ReportResultError(replicaConnection, result, ERROR);
	}

	List *sysidList = ReadFirstColumnAsText(result);
	if (list_length(sysidList) != 1)
	{
		PQclear(result);
		ClearResults(replicaConnection, true);
		CloseConnection(replicaConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("cannot parse get system identifier result from %s:%d",
							   replicaHostname,
							   replicaPort)));
	}

	StringInfo sysidQueryResInfo = (StringInfo) linitial(sysidList);
	char *sysidQueryResStr = sysidQueryResInfo->data;

	ereport(DEBUG2, (errmsg("system identifier of %s:%d is %s",
							replicaHostname, replicaPort, sysidQueryResStr)));

	/* We do not need the connection anymore */
	PQclear(result);
	ForgetResults(replicaConnection);
	CloseConnection(replicaConnection);

	/* Step3: Get system identifier from primary */
	int primaryConnectionFlag = 0;
	MultiConnection *primaryConnection = GetNodeConnection(primaryConnectionFlag,
														   primaryWorkerNode->workerName,
														   primaryWorkerNode->workerPort);

	if (PQstatus(primaryConnection->pgConn) != CONNECTION_OK)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg(
							"cannot connect to primary node %s:%d to fetch replication status",
							primaryWorkerNode->workerName, primaryWorkerNode->
							workerPort)));
	}

	int primaryResultCode = SendRemoteCommand(primaryConnection, sysidQuery);
	if (primaryResultCode == 0)
	{
		ReportConnectionError(primaryConnection, ERROR);
	}

	PGresult *primaryResult = GetRemoteCommandResult(primaryConnection, true);
	if (primaryResult == NULL)
	{
		CloseConnection(primaryConnection);
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("node %s:%d does not support pg_control_system queries",
							   primaryWorkerNode->workerName, primaryWorkerNode->
							   workerPort)));
	}
	List *primarySizeList = ReadFirstColumnAsText(primaryResult);
	if (list_length(primarySizeList) != 1)
	{
		PQclear(primaryResult);
		ClearResults(primaryConnection, true);
		CloseConnection(primaryConnection);

		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("cannot parse get system identifier result from %s:%d",
							   primaryWorkerNode->workerName,
							   primaryWorkerNode->workerPort)));
	}
	StringInfo primarySysidQueryResInfo = (StringInfo) linitial(primarySizeList);
	char *primarySysidQueryResStr = primarySysidQueryResInfo->data;

	ereport(DEBUG2, (errmsg("system identifier of %s:%d is %s",
							primaryWorkerNode->workerName, primaryWorkerNode->workerPort,
							primarySysidQueryResStr)));

	/* verify both identifiers */
	if (strcmp(sysidQueryResStr, primarySysidQueryResStr) != 0)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg(
							"system identifiers do not match: %s (replica) vs %s (primary)",
							sysidQueryResStr, primarySysidQueryResStr)));
	}
	PQclear(primaryResult);
	ClearResults(primaryConnection, true);
	CloseConnection(primaryConnection);
}
