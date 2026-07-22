/*-------------------------------------------------------------------------
 *
 * cluster_version.c
 *
 * UDFs to reason about the Citus version running across the whole cluster.
 *
 * The minimum cluster version is computed on demand by asking every active
 * primary node for its own loaded Citus version (citus_version_num()) and
 * taking the smallest value. The local node is answered from the compile-time
 * constant without opening a connection to itself.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/builtins.h"

#include "citus_version.h"

#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/worker_manager.h"

#define CLUSTER_VERSION_QUERY "SELECT citus_version_num()"

PG_FUNCTION_INFO_V1(citus_minimum_cluster_version);

static int32 MinimumClusterCitusVersion(void);
static int32 RemoteNodeCitusVersion(WorkerNode *workerNode);
static text * CitusVersionNumToText(int32 versionNum);


/*
 * citus_minimum_cluster_version returns the oldest (smallest) Citus version
 * that is running on any active primary node in the cluster, formatted as a
 * human-readable "major.minor.patch" string (e.g. "15.0.0"). The comparison is
 * performed on the integer encoding used by citus_version_num(); only the final
 * result is decoded to text for presentation.
 */
Datum
citus_minimum_cluster_version(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	int32 minimumVersion = MinimumClusterCitusVersion();

	PG_RETURN_TEXT_P(CitusVersionNumToText(minimumVersion));
}


/*
 * CitusVersionNumToText decodes a citus_version_num() style integer
 * (major * 10000 + minor * 100 + patch) back into a "major.minor.patch" string,
 * matching how Citus versions are written (e.g. 120105 -> "12.1.5").
 */
static text *
CitusVersionNumToText(int32 versionNum)
{
	int32 major = versionNum / 10000;
	int32 minor = (versionNum / 100) % 100;
	int32 patch = versionNum % 100;

	return cstring_to_text(psprintf("%d.%d.%d", major, minor, patch));
}


/*
 * MinimumClusterCitusVersion walks over all active primary nodes and returns
 * the smallest citus_version_num() among them. The local node is answered from
 * the CITUS_VERSION_NUM constant, every other node is queried over a
 * connection. If a remote node cannot be reached we error out, because a
 * minimum that silently ignores unreachable nodes could be used to make an
 * unsafe decision.
 */
static int32
MinimumClusterCitusVersion(void)
{
	/* the local node always contributes its own loaded version */
	int32 minimumVersion = CITUS_VERSION_NUM;

	int32 localGroupId = GetLocalGroupId();
	List *nodeList = ActivePrimaryNodeList(NoLock);

	WorkerNode *workerNode = NULL;
	foreach_declared_ptr(workerNode, nodeList)
	{
		int32 nodeVersion = 0;

		if (workerNode->groupId == localGroupId)
		{
			nodeVersion = CITUS_VERSION_NUM;
		}
		else
		{
			nodeVersion = RemoteNodeCitusVersion(workerNode);
		}

		if (nodeVersion < minimumVersion)
		{
			minimumVersion = nodeVersion;
		}
	}

	return minimumVersion;
}


/*
 * RemoteNodeCitusVersion opens a connection to the given node and returns the
 * result of citus_version_num() executed on it.
 */
static int32
RemoteNodeCitusVersion(WorkerNode *workerNode)
{
	int connectionFlags = 0;
	MultiConnection *connection = GetNodeConnection(connectionFlags,
												   workerNode->workerName,
												   workerNode->workerPort);

	PGresult *result = NULL;
	int executionResult = ExecuteOptionalRemoteCommand(connection,
													   CLUSTER_VERSION_QUERY, &result);

	if (executionResult != RESPONSE_OKAY || result == NULL || PQntuples(result) != 1)
	{
		PQclear(result);
		ForgetResults(connection);

		ereport(ERROR, (errmsg("could not get Citus version from node \"%s:%d\"",
							   workerNode->workerName, workerNode->workerPort),
						errhint("Ensure the node is reachable and running Citus.")));
	}

	int32 nodeVersion = (int32) ParseIntField(result, 0, 0);

	PQclear(result);
	ForgetResults(connection);

	return nodeVersion;
}
