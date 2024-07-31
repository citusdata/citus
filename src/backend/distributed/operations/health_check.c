/*-------------------------------------------------------------------------
 *
 * health_check.c
 *
 * UDFs to run health check operations by coordinating simple queries to test connectivity
 * between connection pairs in the cluster.
 *
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/builtins.h"

#include "distributed/argutils.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/tuplestore.h"
#include "distributed/worker_manager.h"

/* simple query to run on workers to check connectivity */
#define CONNECTIVITY_CHECK_QUERY "SELECT 1"
#define CONNECTIVITY_CHECK_COLUMNS 5

PG_FUNCTION_INFO_V1(citus_check_connection_to_node);
PG_FUNCTION_INFO_V1(citus_check_cluster_node_health);

static bool CheckConnectionToNode(char *nodeName, uint32 nodePort);

static void StoreAllConnectivityChecks(Tuplestorestate *tupleStore,
									   TupleDesc tupleDescriptor);
static char * GetConnectivityCheckCommand(const char *nodeName, const uint32 nodePort);


/*
 * citus_check_connection_to_node sends a simple query from a worker node to another
 * node, and returns success status.
 */
Datum
citus_check_connection_to_node(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	char *nodeName = PG_GETARG_TEXT_TO_CSTRING(0);
	uint32 nodePort = PG_GETARG_UINT32(1);

	bool success = CheckConnectionToNode(nodeName, nodePort);
	PG_RETURN_BOOL(success);
}


/*
 * CheckConnectionToNode sends a simple query to a node and returns success status
 */
static bool
CheckConnectionToNode(char *nodeName, uint32 nodePort)
{
	int connectionFlags = 0;
	MultiConnection *connection = GetNodeConnection(connectionFlags, nodeName, nodePort);
	int responseStatus = ExecuteOptionalRemoteCommand(connection,
													  CONNECTIVITY_CHECK_QUERY, NULL);

	return responseStatus == RESPONSE_OKAY;
}


/*
 * citus_check_cluster_node_health UDF performs connectivity checks from all the nodes to
 * all the nodes, and report success status
 */
Datum
citus_check_cluster_node_health(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	StoreAllConnectivityChecks(tupleStore, tupleDescriptor);

	PG_RETURN_VOID();
}


/*
 * StoreAllConnectivityChecks performs connectivity checks from all the nodes to all the
 * nodes, and report success status.
 *
 * Algorithm is:
 * for sourceNode in activeReadableNodeList:
 *   c = connectToNode(sourceNode)
 *   for targetNode in activeReadableNodeList:
 *     result = c.execute("SELECT citus_check_connection_to_node(targetNode.name, targetNode.port")
 *     emit sourceNode.name, sourceNode.port, targetNode.name, targetNode.port, result
 *
 * -- result -> true  -> connection attempt from source to target succeeded
 * -- result -> false -> connection attempt from source to target failed
 * -- result -> NULL  -> connection attempt from the current node to source node failed
 */
static void
StoreAllConnectivityChecks(Tuplestorestate *tupleStore, TupleDesc tupleDescriptor)
{
	Datum values[CONNECTIVITY_CHECK_COLUMNS];
	bool isNulls[CONNECTIVITY_CHECK_COLUMNS];

	/*
	 * Get all the readable node list so that we will check connectivity to followers in
	 * the cluster as well.
	 */
	List *workerNodeList = ActiveReadableNodeList();

	/* we want to check for connectivity in a deterministic order */
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	/*
	 * We iterate over the workerNodeList twice, for source and target worker nodes. This
	 * operation is safe for foreach_declared_ptr macro, as long as we use different variables for
	 * each iteration.
	 */
	WorkerNode *sourceWorkerNode = NULL;
	foreach_declared_ptr(sourceWorkerNode, workerNodeList)
	{
		const char *sourceNodeName = sourceWorkerNode->workerName;
		const int sourceNodePort = sourceWorkerNode->workerPort;
		int32 connectionFlags = 0;

		/* open a connection to the source node using the synchronous api */
		MultiConnection *connectionToSourceNode =
			GetNodeConnection(connectionFlags, sourceNodeName, sourceNodePort);

		/* the second iteration over workerNodeList for the target worker nodes. */
		WorkerNode *targetWorkerNode = NULL;
		foreach_declared_ptr(targetWorkerNode, workerNodeList)
		{
			const char *targetNodeName = targetWorkerNode->workerName;
			const int targetNodePort = targetWorkerNode->workerPort;

			char *connectivityCheckCommandToTargetNode =
				GetConnectivityCheckCommand(targetNodeName, targetNodePort);

			PGresult *result = NULL;
			int executionResult =
				ExecuteOptionalRemoteCommand(connectionToSourceNode,
											 connectivityCheckCommandToTargetNode,
											 &result);

			/* get ready for the next tuple */
			memset(values, 0, sizeof(values));
			memset(isNulls, false, sizeof(isNulls));

			values[0] = PointerGetDatum(cstring_to_text(sourceNodeName));
			values[1] = Int32GetDatum(sourceNodePort);
			values[2] = PointerGetDatum(cstring_to_text(targetNodeName));
			values[3] = Int32GetDatum(targetNodePort);

			/*
			 * If we could not send the query or the result was not ok, set success field
			 * to NULL. This may indicate connection errors to a worker node, however that
			 * node can potentially connect to other nodes.
			 *
			 * Therefore, we mark the success as NULL to indicate that the connectivity
			 * status is unknown.
			 */
			if (executionResult != RESPONSE_OKAY)
			{
				isNulls[4] = true;
			}
			else
			{
				int rowIndex = 0;
				int columnIndex = 0;
				values[4] = BoolGetDatum(ParseBoolField(result, rowIndex, columnIndex));
			}

			tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);

			PQclear(result);
			ForgetResults(connectionToSourceNode);
		}
	}
}


/*
 * GetConnectivityCheckCommand returns the command to check connections to a node
 */
static char *
GetConnectivityCheckCommand(const char *nodeName, const uint32 nodePort)
{
	StringInfo connectivityCheckCommand = makeStringInfo();
	appendStringInfo(connectivityCheckCommand,
					 "SELECT citus_check_connection_to_node('%s', %d)",
					 nodeName, nodePort);

	return connectivityCheckCommand->data;
}
