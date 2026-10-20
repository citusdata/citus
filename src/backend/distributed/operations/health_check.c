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
 * connections = connectConcurrently(activeReadableNodeList)
 * for targetNode in activeReadableNodeList:
 *   for c in connections:
 *     c.send("SELECT citus_check_connection_to_node(targetNode.name, targetNode.port")
 *   wait for all connections
 *   collect the result of each source node for targetNode
 * for sourceNode in activeReadableNodeList:
 *   for targetNode in activeReadableNodeList:
 *     emit sourceNode.name, sourceNode.port, targetNode.name, targetNode.port, result
 *
 * Each source node still checks the target nodes one at a time and in the sorted
 * order, but all source nodes check the same target node concurrently. Hence, at most
 * one probe is in flight per source node, and a target node may receive one probe
 * connection from each of the source nodes at the same time.
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

	int nodeCount = list_length(workerNodeList);

	/* open the connections to all source nodes concurrently */
	MultiConnection **sourceConnections = palloc0(nodeCount * sizeof(MultiConnection *));
	List *sourceConnectionList = NIL;
	int sourceIndex = 0;

	WorkerNode *sourceWorkerNode = NULL;
	foreach_ptr(sourceWorkerNode, workerNodeList)
	{
		int32 connectionFlags = 0;
		MultiConnection *connection =
			StartNodeConnection(connectionFlags, sourceWorkerNode->workerName,
								sourceWorkerNode->workerPort);

		sourceConnections[sourceIndex++] = connection;
		sourceConnectionList = lappend(sourceConnectionList, connection);
	}

	FinishConnectionListEstablishment(sourceConnectionList);

	/* results are indexed by [sourceIndex * nodeCount + targetIndex] */
	bool *checkSucceeded = palloc0(nodeCount * nodeCount * sizeof(bool));
	bool *checkResultIsNull = palloc0(nodeCount * nodeCount * sizeof(bool));
	bool *commandSent = palloc0(nodeCount * sizeof(bool));

	int targetIndex = 0;
	WorkerNode *targetWorkerNode = NULL;
	foreach_ptr(targetWorkerNode, workerNodeList)
	{
		char *connectivityCheckCommandToTargetNode =
			GetConnectivityCheckCommand(targetWorkerNode->workerName,
										targetWorkerNode->workerPort);
		List *pendingConnectionList = NIL;

		for (sourceIndex = 0; sourceIndex < nodeCount; sourceIndex++)
		{
			MultiConnection *connection = sourceConnections[sourceIndex];
			int resultIndex = sourceIndex * nodeCount + targetIndex;

			commandSent[sourceIndex] =
				SendRemoteCommand(connection, connectivityCheckCommandToTargetNode) != 0;

			/*
			 * If we could not send the query, set success field to NULL. This may
			 * indicate connection errors to a worker node, however that node can
			 * potentially connect to other nodes.
			 *
			 * Therefore, we mark the success as NULL to indicate that the
			 * connectivity status is unknown.
			 */
			if (!commandSent[sourceIndex])
			{
				ReportConnectionError(connection, WARNING);
				checkResultIsNull[resultIndex] = true;
				continue;
			}

			pendingConnectionList = lappend(pendingConnectionList, connection);
		}

		bool raiseInterrupts = true;
		WaitForAllConnections(pendingConnectionList, raiseInterrupts);

		for (sourceIndex = 0; sourceIndex < nodeCount; sourceIndex++)
		{
			if (!commandSent[sourceIndex])
			{
				continue;
			}

			MultiConnection *connection = sourceConnections[sourceIndex];
			int resultIndex = sourceIndex * nodeCount + targetIndex;

			PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);

			/* similarly, the connectivity status is unknown if the result is not ok */
			if (!IsResponseOK(result))
			{
				ReportResultError(connection, result, WARNING);
				checkResultIsNull[resultIndex] = true;
			}
			else
			{
				int rowIndex = 0;
				int columnIndex = 0;
				checkSucceeded[resultIndex] =
					ParseBoolField(result, rowIndex, columnIndex);
			}

			PQclear(result);
			ForgetResults(connection);
		}

		list_free(pendingConnectionList);
		pfree(connectivityCheckCommandToTargetNode);
		targetIndex++;
	}

	/*
	 * We iterate over the workerNodeList twice, for source and target worker nodes. This
	 * operation is safe for foreach_ptr macro, as long as we use different
	 * variables for each iteration.
	 */
	sourceIndex = 0;
	foreach_ptr(sourceWorkerNode, workerNodeList)
	{
		targetIndex = 0;
		foreach_ptr(targetWorkerNode, workerNodeList)
		{
			int resultIndex = sourceIndex * nodeCount + targetIndex;

			/* get ready for the next tuple */
			memset(values, 0, sizeof(values));
			memset(isNulls, false, sizeof(isNulls));

			values[0] = PointerGetDatum(cstring_to_text(sourceWorkerNode->workerName));
			values[1] = Int32GetDatum(sourceWorkerNode->workerPort);
			values[2] = PointerGetDatum(cstring_to_text(targetWorkerNode->workerName));
			values[3] = Int32GetDatum(targetWorkerNode->workerPort);

			if (checkResultIsNull[resultIndex])
			{
				isNulls[4] = true;
			}
			else
			{
				values[4] = BoolGetDatum(checkSucceeded[resultIndex]);
			}

			tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
			targetIndex++;
		}

		sourceIndex++;
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
