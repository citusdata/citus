#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "postgres.h"

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

	ereport(DEBUG2, (errmsg(
						 "successfully measured replication lag: primary LSN %s, clone LSN %s",
						 primary_lsn_str, replica_lsn_str)));
	ereport(DEBUG1, (errmsg("replication lag between %s:%d and %s:%d is %ld bytes",
							primaryWorkerNode->workerName, primaryWorkerNode->workerPort,
							replicaWorkerNode->workerName, replicaWorkerNode->workerPort,
							lag_bytes)));
	return lag_bytes;
}


/*
 * EnsureValidCloneMode verifies that a clone node has a valid replication
 * relationship with the specified primary node.
 *
 * This function performs several critical checks:
 * 1. Validates that the clone is actually connected to and replicating from
 *    the specified primary node
 * 2. Ensures the clone is not configured as a synchronous replica, which
 *    would block 2PC commits on the primary when the clone gets promoted
 * 3. Verifies the replication connection is active and healthy
 *
 * The function connects to the primary node and queries pg_stat_replication
 * to find the clone's replication slot. It resolves hostnames to IP addresses
 * for robust matching since PostgreSQL may report different address formats.
 *
 * Parameters:
 *   primaryWorkerNode - The primary node that should be sending replication data
 *   cloneHostname - Hostname/IP of the clone node to verify
 *   clonePort - Port of the clone node to verify
 *   operation - Description of the operation being performed (for error messages)
 *
 * Throws ERROR if:
 *   - Primary or clone parameters are invalid
 *   - Cannot connect to the primary node
 *   - Clone is not found in the primary's replication slots
 *   - Clone is configured as a synchronous replica
 *   - Replication connection is not active
 */
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
		ReportConnectionError(primaryConnection, ERROR);
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

	ereport(NOTICE, (errmsg("checking replication status of clone node %s:%d",
							cloneHostname,
							clonePort)));

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

	ereport(DEBUG2, (errmsg("sending replication status check query: %s to primary %s:%d",
							replicationCheckQuery->data,
							primaryWorkerNode->workerName,
							primaryWorkerNode->workerPort)));

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
		ReportResultError(primaryConnection, replicationCheckResult, ERROR);
	}

	List *replicationStateList = ReadFirstColumnAsText(replicationCheckResult);

	/* Check if clone is connected to this primary */
	if (list_length(replicationStateList) == 0)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("clone %s:%d is not connected to primary %s:%d",
							   cloneHostname, clonePort,
							   primaryWorkerNode->workerName, primaryWorkerNode->
							   workerPort),
						errdetail(
							"The clone must be actively replicating from the specified primary node"),
						errhint(
							"Verify the clone is running and properly configured for replication")));
	}

	/* Check if clone is synchronous */
	if (list_length(replicationStateList) > 0)
	{
		StringInfo syncStateInfo = (StringInfo) linitial(replicationStateList);
		if (syncStateInfo && syncStateInfo->data &&
			(strcmp(syncStateInfo->data, "sync") == 0 || strcmp(syncStateInfo->data,
																"quorum") == 0))
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg(
								"cannot %s clone %s:%d as it is configured as a synchronous replica",
								operation, cloneHostname, clonePort),
							errdetail(
								"Promoting a synchronous clone can cause data consistency issues"),
							errhint(
								"Configure clone as an asynchronous replica")));
		}
	}

	/* Cleanup resources */
	bool raiseErrors = false;
	PQclear(replicationCheckResult);
	ClearResults(primaryConnection, raiseErrors);
	pfree(replicationCheckQuery->data);
	pfree(replicationCheckQuery);
	CloseConnection(primaryConnection);

	ereport(NOTICE, (errmsg(
						 "clone %s:%d is properly connected to primary %s:%d and is not synchronous",
						 cloneHostname, clonePort,
						 primaryWorkerNode->workerName, primaryWorkerNode->workerPort))
			);
}


/*
 * EnsureValidStreamingReplica verifies that a node is a valid streaming replica
 * of the specified primary node.
 *
 * This function performs comprehensive validation to ensure the replica is:
 * 1. Currently in recovery mode (acting as a replica, not a primary)
 * 2. Has the same system identifier as the primary (ensuring they're part of
 *    the same PostgreSQL cluster/timeline)
 *
 * The function connects to both the replica and primary nodes to perform these
 * checks. This validation is critical before performing operations like promotion
 * or failover to ensure data consistency and prevent split-brain scenarios.
 *
 * Parameters:
 *   primaryWorkerNode - The primary node that should be the source of replication
 *   replicaHostname - Hostname/IP of the replica node to validate
 *   replicaPort - Port of the replica node to validate
 *
 * Throws ERROR if:
 *   - Cannot connect to the replica or primary node
 *   - Replica is not in recovery mode (indicating it's not acting as a replica)
 *   - System identifiers don't match between primary and replica
 *   - Any database queries fail during validation
 *
 */
void
EnsureValidStreamingReplica(WorkerNode *primaryWorkerNode, char *replicaHostname, int
							replicaPort)
{
	int connectionFlag = FORCE_NEW_CONNECTION;
	MultiConnection *replicaConnection = GetNodeConnection(connectionFlag, replicaHostname
														   ,
														   replicaPort);

	if (PQstatus(replicaConnection->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(replicaConnection, ERROR);
	}

	const char *replica_recovery_query = "SELECT pg_is_in_recovery()";

	int resultCode = SendRemoteCommand(replicaConnection, replica_recovery_query);

	if (resultCode == 0)
	{
		ereport(DEBUG2, (errmsg(
							 "cannot connect to %s:%d to check if it is in recovery mode",
							 replicaHostname, replicaPort)));
		ReportConnectionError(replicaConnection, ERROR);
	}

	bool raiseInterrupts = true;
	PGresult *result = GetRemoteCommandResult(replicaConnection, raiseInterrupts);

	if (!IsResponseOK(result))
	{
		ereport(DEBUG2, (errmsg("failed to execute pg_is_in_recovery")));
		ReportResultError(replicaConnection, result, ERROR);
	}

	List *sizeList = ReadFirstColumnAsText(result);
	if (list_length(sizeList) != 1)
	{
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
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("node %s:%d is not in recovery mode",
							   replicaHostname, replicaPort)));
	}

	PQclear(result);
	ForgetResults(replicaConnection);

	/* Step2: Get the system identifier from replica */
	const char *sysidQuery =
		"SELECT system_identifier FROM pg_control_system()";

	resultCode = SendRemoteCommand(replicaConnection, sysidQuery);

	if (resultCode == 0)
	{
		ereport(DEBUG2, (errmsg("cannot connect to %s:%d to get system identifier",
								replicaHostname, replicaPort)));
		ReportConnectionError(replicaConnection, ERROR);
	}

	result = GetRemoteCommandResult(replicaConnection, raiseInterrupts);
	if (!IsResponseOK(result))
	{
		ereport(DEBUG2, (errmsg("failed to execute get system identifier")));
		ReportResultError(replicaConnection, result, ERROR);
	}

	List *sysidList = ReadFirstColumnAsText(result);
	if (list_length(sysidList) != 1)
	{
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
	ereport(DEBUG2, (errmsg("getting system identifier from primary %s:%d",
							primaryWorkerNode->workerName,
							primaryWorkerNode->workerPort)));

	int primaryConnectionFlag = 0;
	MultiConnection *primaryConnection = GetNodeConnection(primaryConnectionFlag,
														   primaryWorkerNode->workerName,
														   primaryWorkerNode->workerPort);

	if (PQstatus(primaryConnection->pgConn) != CONNECTION_OK)
	{
		ReportConnectionError(primaryConnection, ERROR);
	}

	int primaryResultCode = SendRemoteCommand(primaryConnection, sysidQuery);
	if (primaryResultCode == 0)
	{
		ReportConnectionError(primaryConnection, ERROR);
	}

	PGresult *primaryResult = GetRemoteCommandResult(primaryConnection, raiseInterrupts);
	if (!IsResponseOK(primaryResult))
	{
		ereport(DEBUG2, (errmsg("failed to execute get system identifier")));
		ReportResultError(primaryConnection, primaryResult, ERROR);
	}
	List *primarySizeList = ReadFirstColumnAsText(primaryResult);
	if (list_length(primarySizeList) != 1)
	{
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
							"system identifiers do not match: %s (clone) vs %s (primary)",
							sysidQueryResStr, primarySysidQueryResStr)));
	}
	PQclear(primaryResult);
	ClearResults(primaryConnection, true);
	CloseConnection(primaryConnection);
}
