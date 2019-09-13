/*-------------------------------------------------------------------------
 *
 * citus_dist_stat_activity.c
 *
 *	This file contains functions for monitoring the distributed transactions
 *	accross the cluster.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "postmaster/postmaster.h"

#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "distributed/backend_data.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/remote_commands.h"
#include "distributed/transaction_identifier.h"
#include "distributed/tuplestore.h"
#include "executor/spi.h"
#include "nodes/execnodes.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "storage/s_lock.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/inet.h"
#include "utils/timestamp.h"


/*
 *  citus_dist_stat_activity() and citus_worker_stat_activity() is similar to
 *  pg_stat_activity. Those functions basically return join of
 *  pg_stat_activity and get_all_active_transactions() on each node
 *  in the cluster. The only difference is that citus_dist_stat_activity()
 *  gets transactions where worker_query = false and citus_worker_stat_activity()
 *  gets transactions where worker_query = true.
 *
 *  In other words, citus_dist_stat_activity returns only the queries that are the
 *  distributed queries. citus_worker_stat_activity returns only the queries that
 *  are worker queries (e.g., queries on the shards) initiated by those distributed
 *  queries. To understand this better, let us give an example. If a users starts
 *  a query like "UPDATE table SET value = 1", this query would show up on
 *  citus_dist_stat_activity. The same query would generate #shard worker queries,
 *  all of which would show up on citus_worker_stat_activity.
 *
 *  An important note on this views is that they only show the activity
 *  that are inside distributed transactions. Distributed transactions
 *  cover the following:
 *     - All multi-shard modifications (DDLs, COPY, UPDATE, DELETE, INSERT .. SELECT)
 *     - All multi-shard queries with CTEs (modifying CTEs, read-only CTEs)
 *     - All recursively planned subqueries
 *     - All queries within transaction blocks (BEGIN; query; COMMMIT;)
 *
 *  In other words, the following types of queries won't be observed in these
 *  views:
 *      - Router queries that are not inside transaction blocks
 *      - Real-time queries that are not inside transaction blocks
 *      - Task-tracker queries
 *
 *
 *  The following information for all the distributed transactions:
 *	query_host_name					text
 *	query_host_port					int
 *	database_id						oid
 *	databaese_name					name
 *	process_id						integer
 *  initiator_node_host				text
 *  initiator_node_port				int
 *	distributed_transaction_number	bigint
 *	distributed_transaction_stamp	timestamp with time zone
 *	usesysid						oid
 *	usename							name
 *	application_name                text
 *	client_addr                     inet
 *	client_hostname                 text
 *	client_port                     integer
 *	backend_start                   timestamp with time zone
 *	xact_start                      timestamp with time zone
 *	query_start                     timestamp with time zone
 *	state_change                    timestamp with time zone
 *	wait_event_type                 text
 *	wait_event                      text
 *	state                           text
 *	backend_xid                     xid
 *	backend_xmin                    xid
 *	query                           text
 *	backend_type                    text
 */

/*
 * We get CITUS_DIST_STAT_ACTIVITY_QUERY_COLS from workers and manually add
 * CITUS_DIST_STAT_ADDITIONAL_COLS for hostname and hostport. Also, instead of
 * showing the initiator_node_id we expand it to initiator_node_host and
 * initiator_node_port.
 */
#define CITUS_DIST_STAT_ACTIVITY_QUERY_COLS 23
#define CITUS_DIST_STAT_ADDITIONAL_COLS 3
#define CITUS_DIST_STAT_ACTIVITY_COLS \
	CITUS_DIST_STAT_ACTIVITY_QUERY_COLS + CITUS_DIST_STAT_ADDITIONAL_COLS


#define coordinator_host_name "coordinator_host"

/*
 * We get the query_host_name and query_host_port while opening the connection to
 * the node. We also replace initiator_node_identifier with initiator_node_host
 * and initiator_node_port. Thus, they are not in the query below.
 */

#define CITUS_DIST_STAT_ACTIVITY_QUERY \
	"\
SELECT \
	dist_txs.initiator_node_identifier, \
	dist_txs.transaction_number, \
	dist_txs.transaction_stamp, \
	pg_stat_activity.datid, \
	pg_stat_activity.datname, \
	pg_stat_activity.pid, \
	pg_stat_activity.usesysid, \
	pg_stat_activity.usename, \
	pg_stat_activity.application_name, \
	pg_stat_activity.client_addr, \
	pg_stat_activity.client_hostname, \
	pg_stat_activity.client_port, \
	pg_stat_activity.backend_start, \
	pg_stat_activity.xact_start, \
	pg_stat_activity.query_start, \
	pg_stat_activity.state_change, \
	pg_stat_activity.wait_event_type, \
	pg_stat_activity.wait_event, \
	pg_stat_activity.state, \
	pg_stat_activity.backend_xid, \
	pg_stat_activity.backend_xmin, \
	pg_stat_activity.query, \
	pg_stat_activity.backend_type \
FROM \
	pg_stat_activity \
	INNER JOIN \
	get_all_active_transactions() AS dist_txs(database_id, process_id, initiator_node_identifier, worker_query, transaction_number, transaction_stamp) \
	ON pg_stat_activity.pid = dist_txs.process_id \
WHERE \
	dist_txs.worker_query = false;"

#define CITUS_WORKER_STAT_ACTIVITY_QUERY \
	"\
SELECT \
	dist_txs.initiator_node_identifier, \
	dist_txs.transaction_number, \
	dist_txs.transaction_stamp, \
	pg_stat_activity.datid, \
	pg_stat_activity.datname, \
	pg_stat_activity.pid, \
	pg_stat_activity.usesysid, \
	pg_stat_activity.usename, \
	pg_stat_activity.application_name, \
	pg_stat_activity.client_addr, \
	pg_stat_activity.client_hostname, \
	pg_stat_activity.client_port, \
	pg_stat_activity.backend_start, \
	pg_stat_activity.xact_start, \
	pg_stat_activity.query_start, \
	pg_stat_activity.state_change, \
	pg_stat_activity.wait_event_type, \
	pg_stat_activity.wait_event, \
	pg_stat_activity.state, \
	pg_stat_activity.backend_xid, \
	pg_stat_activity.backend_xmin, \
	pg_stat_activity.query, \
	pg_stat_activity.backend_type \
FROM \
	pg_stat_activity \
	LEFT JOIN \
	get_all_active_transactions() AS dist_txs(database_id, process_id, initiator_node_identifier, worker_query, transaction_number, transaction_stamp) \
	ON pg_stat_activity.pid = dist_txs.process_id \
WHERE \
	pg_stat_activity.application_name = 'citus' \
	AND \
	pg_stat_activity.query NOT ILIKE '%stat_activity%';"

typedef struct CitusDistStat
{
	text *query_host_name;
	int query_host_port;

	text *master_query_host_name;
	int master_query_host_port;
	uint64 distributed_transaction_number;
	TimestampTz distributed_transaction_stamp;

	/* fields from pg_stat_statement */
	Oid database_id;
	Name databaese_name;
	int process_id;
	Oid usesysid;
	Name usename;
	text *application_name;
	inet *client_addr;
	text *client_hostname;
	int client_port;
	TimestampTz backend_start;
	TimestampTz xact_start;
	TimestampTz query_start;
	TimestampTz state_change;
	text *wait_event_type;
	text *wait_event;
	text *state;
	TransactionId backend_xid;
	TransactionId backend_xmin;
	text *query;
	text *backend_type;
} CitusDistStat;


/* local forward declarations */
static List * CitusStatActivity(const char *statQuery);
static void ReturnCitusDistStats(List *citusStatsList, FunctionCallInfo fcinfo);
static CitusDistStat * ParseCitusDistStat(PGresult *result, int64 rowIndex);

/* utility functions to parse the fields from PGResult */
static text * ParseTextField(PGresult *result, int rowIndex, int colIndex);
static Name ParseNameField(PGresult *result, int rowIndex, int colIndex);
static inet * ParseInetField(PGresult *result, int rowIndex, int colIndex);
static TransactionId ParseXIDField(PGresult *result, int rowIndex, int colIndex);

/* utility functions to fetch the fields from heapTuple */
static List * GetLocalNodeCitusDistStat(const char *statQuery);
static List * LocalNodeCitusDistStat(const char *statQuery, const char *hostname, int
									 port);
static CitusDistStat * HeapTupleToCitusDistStat(HeapTuple result, TupleDesc
												rowDescriptor);
static int64 ParseIntFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc, int colIndex);
static text * ParseTextFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc, int
										  colIndex);
static Name ParseNameFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc, int colIndex);
static inet * ParseInetFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc, int
										  colIndex);
static TimestampTz ParseTimestampTzFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc,
													  int colIndex);
static TransactionId ParseXIDFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc, int
												colIndex);


PG_FUNCTION_INFO_V1(citus_dist_stat_activity);
PG_FUNCTION_INFO_V1(citus_worker_stat_activity);


/*
 * citus_dist_stat_activity connects to all nodes in the cluster and returns
 * pg_stat_activity like result set but only consisting of queries that are
 * on the distributed tables and inside distributed transactions.
 */
Datum
citus_dist_stat_activity(PG_FUNCTION_ARGS)
{
	List *citusDistStatStatements = NIL;

	CheckCitusVersion(ERROR);

	citusDistStatStatements = CitusStatActivity(CITUS_DIST_STAT_ACTIVITY_QUERY);

	ReturnCitusDistStats(citusDistStatStatements, fcinfo);

	PG_RETURN_VOID();
}


/*
 * citus_worker_stat_activity connects to all nodes in the cluster and returns
 * pg_stat_activity like result set but only consisting of queries that are
 * on the shards of distributed tables and inside distributed transactions.
 */
Datum
citus_worker_stat_activity(PG_FUNCTION_ARGS)
{
	List *citusWorkerStatStatements = NIL;

	CheckCitusVersion(ERROR);

	citusWorkerStatStatements = CitusStatActivity(CITUS_WORKER_STAT_ACTIVITY_QUERY);

	ReturnCitusDistStats(citusWorkerStatStatements, fcinfo);

	PG_RETURN_VOID();
}


/*
 * CitusStatActivity gets the stats query, connects to each node in the
 * cluster, executes the query and parses the results. The function returns
 * list of CitusDistStat struct for further processing.
 *
 * The function connects to each active primary node in the pg_dist_node. Plus,
 * if the query is being executed on the coordinator, the function connects to
 * localhost as well. The implication of this is that whenever the query is executed
 * from a MX worker node, it wouldn't be able to get information from the queries
 * executed on the coordinator given that there is not metadata information about that.
 */
static List *
CitusStatActivity(const char *statQuery)
{
	List *citusStatsList = NIL;

	List *workerNodeList = ActivePrimaryNodeList(NoLock);
	ListCell *workerNodeCell = NULL;
	char *nodeUser = NULL;
	List *connectionList = NIL;
	ListCell *connectionCell = NULL;

	/*
	 * For the local node, we can avoid opening connections. This might be
	 * important when we're on the coordinator since it requires configuring
	 * the authentication for self-connection via any user who calls the citus
	 * stat activity functions.
	 */
	citusStatsList = GetLocalNodeCitusDistStat(statQuery);

	/*
	 * We prefer to connect with the current user to the remote nodes. This will
	 * ensure that we have the same privilage restrictions that pg_stat_activity
	 * enforces.
	 */
	nodeUser = CurrentUserName();

	/* open connections in parallel */
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		MultiConnection *connection = NULL;
		int connectionFlags = 0;

		if (workerNode->groupId == GetLocalGroupId())
		{
			/* we already get these stats via GetLocalNodeCitusDistStat() */
			continue;
		}

		connection = StartNodeUserDatabaseConnection(connectionFlags, nodeName, nodePort,
													 nodeUser, NULL);

		connectionList = lappend(connectionList, connection);
	}

	FinishConnectionListEstablishment(connectionList);

	/* send commands in parallel */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		int querySent = false;

		querySent = SendRemoteCommand(connection, statQuery);
		if (querySent == 0)
		{
			ReportConnectionError(connection, WARNING);
		}
	}

	/* receive query results */
	foreach(connectionCell, connectionList)
	{
		MultiConnection *connection = (MultiConnection *) lfirst(connectionCell);
		PGresult *result = NULL;
		bool raiseInterrupts = true;
		int64 rowIndex = 0;
		int64 rowCount = 0;
		int64 colCount = 0;

		result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, WARNING);
			continue;
		}

		rowCount = PQntuples(result);
		colCount = PQnfields(result);

		if (colCount != CITUS_DIST_STAT_ACTIVITY_QUERY_COLS)
		{
			/*
			 * We don't expect to hit this error, but keep it here in case there
			 * is a version mistmatch.
			 */
			ereport(WARNING, (errmsg("unexpected number of columns from "
									 "citus stat query")));
			continue;
		}

		for (rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			CitusDistStat *citusDistStat = ParseCitusDistStat(result, rowIndex);

			/*
			 * Add the query_host_name and query_host_port which denote where
			 * the query is being running.
			 */
			citusDistStat->query_host_name = cstring_to_text(connection->hostname);
			citusDistStat->query_host_port = connection->port;

			citusStatsList = lappend(citusStatsList, citusDistStat);
		}

		PQclear(result);
		ForgetResults(connection);
	}

	return citusStatsList;
}


/*
 * GetLocalNodeCitusDistStat simple executes the given query with SPI to get
 * the result of the given stat query on the local node.
 */
static List *
GetLocalNodeCitusDistStat(const char *statQuery)
{
	List *citusStatsList = NIL;

	List *workerNodeList = NIL;
	ListCell *workerNodeCell = NULL;
	int localGroupId = -1;

	if (IsCoordinator())
	{
		/*
		 * Coordinator's nodename and nodeport doesn't show-up in the metadata,
		 * so mark it manually as executing from the coordinator.
		 */
		citusStatsList = LocalNodeCitusDistStat(statQuery, coordinator_host_name,
												PostPortNumber);

		return citusStatsList;
	}

	localGroupId = GetLocalGroupId();

	/* get the current worker's node stats */
	workerNodeList = ActivePrimaryNodeList(NoLock);
	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

		if (workerNode->groupId == localGroupId)
		{
			char *nodeName = workerNode->workerName;
			int nodePort = workerNode->workerPort;

			citusStatsList = LocalNodeCitusDistStat(statQuery, nodeName, nodePort);

			break;
		}
	}

	return citusStatsList;
}


/*
 * ParseCitusDistStat is a helper function which basically gets a PGresult
 * and parses the results for rowIndex. Finally, returns CitusDistStat for
 * further processing of the data retrieved.
 *
 * HeapTupleToCitusDistStat() and ParseCitusDistStat() are doing the same thing on
 * different input data structures. Thus, any change to here should be reflected in
 * the other function as well.
 */
static CitusDistStat *
ParseCitusDistStat(PGresult *result, int64 rowIndex)
{
	CitusDistStat *citusDistStat = (CitusDistStat *) palloc0(sizeof(CitusDistStat));
	int initiator_node_identifier = 0;
	WorkerNode *initiatorWorkerNode = NULL;

	/*
	 * Replace initiator_node_identifier with initiator_node_hostname
	 * and initiator_node_port given that those are a lot more useful.
	 *
	 * The rules are following:
	 *    - If initiator_node_identifier belongs to a worker, simply get it
	 *      from the metadata
	 *   - If the initiator_node_identifier belongs to the coordinator and
	 *     we're executing the function on the coordinator, get the localhost
	 *     and port
	 *   - If the initiator_node_identifier belongs to the coordinator and
	 *     we're executing the function on a worker node, manually mark it
	 *     as "coordinator_host" given that we cannot know the host and port
	 *   - If the initiator_node_identifier doesn't equal to zero, we know that
	 *     it is a worker query initiated outside of a distributed
	 *     transaction. However, we cannot know which node has initiated
	 *     the worker query.
	 */
	initiator_node_identifier =
		PQgetisnull(result, rowIndex, 0) ? -1 : ParseIntField(result, rowIndex, 0);
	if (initiator_node_identifier > 0)
	{
		bool nodeExists = false;

		initiatorWorkerNode = PrimaryNodeForGroup(initiator_node_identifier, &nodeExists);

		/* a query should run on an existing node */
		Assert(nodeExists);
		citusDistStat->master_query_host_name =
			cstring_to_text(initiatorWorkerNode->workerName);
		citusDistStat->master_query_host_port = initiatorWorkerNode->workerPort;
	}
	else if (initiator_node_identifier == 0 && IsCoordinator())
	{
		citusDistStat->master_query_host_name = cstring_to_text(coordinator_host_name);
		citusDistStat->master_query_host_port = PostPortNumber;
	}
	else if (initiator_node_identifier == 0)
	{
		citusDistStat->master_query_host_name = cstring_to_text(coordinator_host_name);
		citusDistStat->master_query_host_port = 0;
	}
	else
	{
		citusDistStat->master_query_host_name = NULL;
		citusDistStat->master_query_host_port = 0;
	}

	citusDistStat->distributed_transaction_number = ParseIntField(result, rowIndex, 1);
	citusDistStat->distributed_transaction_stamp =
		ParseTimestampTzField(result, rowIndex, 2);

	/* fields from pg_stat_statement */
	citusDistStat->database_id = ParseIntField(result, rowIndex, 3);
	citusDistStat->databaese_name = ParseNameField(result, rowIndex, 4);
	citusDistStat->process_id = ParseIntField(result, rowIndex, 5);
	citusDistStat->usesysid = ParseIntField(result, rowIndex, 6);
	citusDistStat->usename = ParseNameField(result, rowIndex, 7);
	citusDistStat->application_name = ParseTextField(result, rowIndex, 8);
	citusDistStat->client_addr = ParseInetField(result, rowIndex, 9);
	citusDistStat->client_hostname = ParseTextField(result, rowIndex, 10);
	citusDistStat->client_port = ParseIntField(result, rowIndex, 11);
	citusDistStat->backend_start = ParseTimestampTzField(result, rowIndex, 12);
	citusDistStat->xact_start = ParseTimestampTzField(result, rowIndex, 13);
	citusDistStat->query_start = ParseTimestampTzField(result, rowIndex, 14);
	citusDistStat->state_change = ParseTimestampTzField(result, rowIndex, 15);
	citusDistStat->wait_event_type = ParseTextField(result, rowIndex, 16);
	citusDistStat->wait_event = ParseTextField(result, rowIndex, 17);
	citusDistStat->state = ParseTextField(result, rowIndex, 18);
	citusDistStat->backend_xid = ParseXIDField(result, rowIndex, 19);
	citusDistStat->backend_xmin = ParseXIDField(result, rowIndex, 20);
	citusDistStat->query = ParseTextField(result, rowIndex, 21);
	citusDistStat->backend_type = ParseTextField(result, rowIndex, 22);

	return citusDistStat;
}


/*
 * LocalNodeCitusDistStat simply executes the given query via SPI and parses
 * the results back in a list for further processing.
 *
 * hostname and port is provided for filling the fields on the return list, obviously
 * not for executing the SPI.
 */
static List *
LocalNodeCitusDistStat(const char *statQuery, const char *hostname, int port)
{
	List *localNodeCitusDistStatList = NIL;
	int spiConnectionResult = 0;
	int spiQueryResult = 0;
	bool readOnly = true;
	uint32 rowIndex = 0;

	MemoryContext upperContext = CurrentMemoryContext, oldContext = NULL;

	spiConnectionResult = SPI_connect();
	if (spiConnectionResult != SPI_OK_CONNECT)
	{
		ereport(WARNING, (errmsg("could not connect to SPI manager to get "
								 "the local stat activity")));

		SPI_finish();

		return NIL;
	}

	spiQueryResult = SPI_execute(statQuery, readOnly, 0);
	if (spiQueryResult != SPI_OK_SELECT)
	{
		ereport(WARNING, (errmsg("execution was not successful while trying to get "
								 "the local stat activity")));

		SPI_finish();

		return NIL;
	}

	/*
	 * SPI_connect switches to its own memory context, which is destroyed by
	 * the call to SPI_finish. SPI_palloc is provided to allocate memory in
	 * the previous ("upper") context, but that is inadequate when we need to
	 * call other functions that themselves use the normal palloc (such as
	 * lappend). So we switch to the upper context ourselves as needed.
	 */
	oldContext = MemoryContextSwitchTo(upperContext);

	for (rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		HeapTuple row = NULL;
		TupleDesc rowDescriptor = SPI_tuptable->tupdesc;
		CitusDistStat *citusDistStat = NULL;

		/* we use pointers from the tuple, so copy it before processing */
		row = SPI_copytuple(SPI_tuptable->vals[rowIndex]);
		citusDistStat = HeapTupleToCitusDistStat(row, rowDescriptor);

		/*
		 * Add the query_host_name and query_host_port which denote where
		 * the query is being running.
		 */
		citusDistStat->query_host_name = cstring_to_text(hostname);
		citusDistStat->query_host_port = port;

		localNodeCitusDistStatList = lappend(localNodeCitusDistStatList, citusDistStat);
	}

	MemoryContextSwitchTo(oldContext);

	SPI_finish();

	return localNodeCitusDistStatList;
}


/*
 * HeapTupleToCitusDistStat is a helper function which basically gets a heapTuple
 * and fetches the results for the given tuple. Finally, returns CitusDistStat for
 * further processing of the data retrieved.
 *
 * HeapTupleToCitusDistStat() and ParseCitusDistStat() are doing the same thing on
 * different input data structures. Thus, any change to here should be reflected in
 * the other function as well.
 */
static CitusDistStat *
HeapTupleToCitusDistStat(HeapTuple result, TupleDesc rowDescriptor)
{
	CitusDistStat *citusDistStat = (CitusDistStat *) palloc0(sizeof(CitusDistStat));
	int initiator_node_identifier = 0;
	WorkerNode *initiatorWorkerNode = NULL;

	/*
	 * Replace initiator_node_identifier with initiator_node_hostname
	 * and initiator_node_port given that those are a lot more useful.
	 *
	 * The rules are following:
	 *    - If initiator_node_identifier belongs to a worker, simply get it
	 *      from the metadata
	 *   - If the initiator_node_identifier belongs to the coordinator and
	 *     we're executing the function on the coordinator, get the localhost
	 *     and port
	 *   - If the initiator_node_identifier belongs to the coordinator and
	 *     we're executing the function on a worker node, manually mark it
	 *     as "coordinator_host" given that we cannot know the host and port
	 *   - If the initiator_node_identifier doesn't equal to zero, we know that
	 *     it is a worker query initiated outside of a distributed
	 *     transaction. However, we cannot know which node has initiated
	 *     the worker query.
	 */
	initiator_node_identifier = ParseIntFieldFromHeapTuple(result, rowDescriptor, 1);
	if (initiator_node_identifier > 0)
	{
		bool nodeExists = false;

		initiatorWorkerNode = PrimaryNodeForGroup(initiator_node_identifier, &nodeExists);

		/* a query should run on an existing node */
		Assert(nodeExists);
		citusDistStat->master_query_host_name =
			cstring_to_text(initiatorWorkerNode->workerName);
		citusDistStat->master_query_host_port = initiatorWorkerNode->workerPort;
	}
	else if (initiator_node_identifier == 0 && IsCoordinator())
	{
		citusDistStat->master_query_host_name = cstring_to_text(coordinator_host_name);
		citusDistStat->master_query_host_port = PostPortNumber;
	}
	else if (initiator_node_identifier == 0)
	{
		citusDistStat->master_query_host_name = cstring_to_text(coordinator_host_name);
		citusDistStat->master_query_host_port = 0;
	}
	else
	{
		citusDistStat->master_query_host_name = NULL;
		citusDistStat->master_query_host_port = 0;
	}

	citusDistStat->distributed_transaction_number =
		ParseIntFieldFromHeapTuple(result, rowDescriptor, 2);
	citusDistStat->distributed_transaction_stamp =
		ParseTimestampTzFieldFromHeapTuple(result, rowDescriptor, 3);

	/* fields from pg_stat_statement */
	citusDistStat->database_id = ParseIntFieldFromHeapTuple(result, rowDescriptor, 4);
	citusDistStat->databaese_name = ParseNameFieldFromHeapTuple(result, rowDescriptor, 5);
	citusDistStat->process_id = ParseIntFieldFromHeapTuple(result, rowDescriptor, 6);
	citusDistStat->usesysid = ParseIntFieldFromHeapTuple(result, rowDescriptor, 7);
	citusDistStat->usename = ParseNameFieldFromHeapTuple(result, rowDescriptor, 8);
	citusDistStat->application_name =
		ParseTextFieldFromHeapTuple(result, rowDescriptor, 9);
	citusDistStat->client_addr = ParseInetFieldFromHeapTuple(result, rowDescriptor, 10);
	citusDistStat->client_hostname =
		ParseTextFieldFromHeapTuple(result, rowDescriptor, 11);
	citusDistStat->client_port = ParseIntFieldFromHeapTuple(result, rowDescriptor, 12);
	citusDistStat->backend_start =
		ParseTimestampTzFieldFromHeapTuple(result, rowDescriptor, 13);
	citusDistStat->xact_start =
		ParseTimestampTzFieldFromHeapTuple(result, rowDescriptor, 14);
	citusDistStat->query_start =
		ParseTimestampTzFieldFromHeapTuple(result, rowDescriptor, 15);
	citusDistStat->state_change =
		ParseTimestampTzFieldFromHeapTuple(result, rowDescriptor, 16);
	citusDistStat->wait_event_type =
		ParseTextFieldFromHeapTuple(result, rowDescriptor, 17);
	citusDistStat->wait_event = ParseTextFieldFromHeapTuple(result, rowDescriptor, 18);
	citusDistStat->state = ParseTextFieldFromHeapTuple(result, rowDescriptor, 19);
	citusDistStat->backend_xid = ParseXIDFieldFromHeapTuple(result, rowDescriptor, 20);
	citusDistStat->backend_xmin = ParseXIDFieldFromHeapTuple(result, rowDescriptor, 21);
	citusDistStat->query = ParseTextFieldFromHeapTuple(result, rowDescriptor, 22);
	citusDistStat->backend_type = ParseTextFieldFromHeapTuple(result, rowDescriptor, 23);

	return citusDistStat;
}


/*
 * ParseIntFieldFromHeapTuple fetches an int64 from a heapTuple or returns 0 if the
 * result is NULL.
 */
static int64
ParseIntFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc, int colIndex)
{
	Datum resultDatum;
	bool isNull = false;

	resultDatum = SPI_getbinval(tuple, tupdesc, colIndex, &isNull);
	if (isNull)
	{
		return 0;
	}

	return DatumGetInt64(resultDatum);
}


/*
 * ParseTextFieldFromHeapTuple parses a text from a heapTuple or returns
 * NULL if the result is NULL.
 */
static text *
ParseTextFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc, int colIndex)
{
	Datum resultDatum;
	bool isNull = false;

	resultDatum = SPI_getbinval(tuple, tupdesc, colIndex, &isNull);
	if (isNull)
	{
		return NULL;
	}

	return (text *) DatumGetPointer(resultDatum);
}


/*
 * ParseNameFieldFromHeapTuple fetches a name from a heapTuple result or returns NULL if the
 * result is NULL.
 */
static Name
ParseNameFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc, int colIndex)
{
	Datum resultDatum;
	bool isNull = false;

	resultDatum = SPI_getbinval(tuple, tupdesc, colIndex, &isNull);
	if (isNull)
	{
		return NULL;
	}

	return (Name) DatumGetPointer(resultDatum);
}


/*
 * ParseInetFieldFromHeapTuple fetcges an inet from a heapTuple or returns NULL if the
 * result is NULL.
 */
static inet *
ParseInetFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc, int colIndex)
{
	Datum resultDatum;
	bool isNull = false;

	resultDatum = SPI_getbinval(tuple, tupdesc, colIndex, &isNull);
	if (isNull)
	{
		return NULL;
	}

	return DatumGetInetP(resultDatum);
}


/*
 * ParseTimestampTzFieldFromHeapTuple parses a timestamptz from a heapTuple or returns
 * DT_NOBEGIN if the result is NULL.
 */
static TimestampTz
ParseTimestampTzFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc, int colIndex)
{
	Datum resultDatum;
	bool isNull = false;

	resultDatum = SPI_getbinval(tuple, tupdesc, colIndex, &isNull);
	if (isNull)
	{
		return DT_NOBEGIN;
	}

	return DatumGetTimestampTz(resultDatum);
}


/*
 * ParseXIDFieldFromHeapTuple parses a XID from a heapTuple or returns
 * PG_UINT32_MAX if the result is NULL.
 */
static TransactionId
ParseXIDFieldFromHeapTuple(HeapTuple tuple, TupleDesc tupdesc, int colIndex)
{
	Datum resultDatum;
	bool isNull = false;

	resultDatum = SPI_getbinval(tuple, tupdesc, colIndex, &isNull);
	if (isNull)
	{
		/*
		 * We'd show NULL if user hits the max transaction id, but that should be
		 * one of the minor problems they'd probably hit.
		 */
		return PG_UINT32_MAX;
	}

	return DatumGetTransactionId(resultDatum);
}


/*
 * ParseTextField parses a text from a remote result or returns NULL if the
 * result is NULL.
 */
static text *
ParseTextField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;
	Datum resultStringDatum = 0;
	Datum textDatum = 0;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return NULL;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);
	resultStringDatum = CStringGetDatum(resultString);
	textDatum = DirectFunctionCall1(textin, resultStringDatum);

	return (text *) DatumGetPointer(textDatum);
}


/*
 * ParseNameField parses a name from a remote result or returns NULL if the
 * result is NULL.
 */
static Name
ParseNameField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;
	Datum resultStringDatum = 0;
	Datum nameDatum = 0;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return (Name) nameDatum;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);
	resultStringDatum = CStringGetDatum(resultString);
	nameDatum = DirectFunctionCall1(namein, resultStringDatum);

	return (Name) DatumGetPointer(nameDatum);
}


/*
 * ParseInetField parses an inet from a remote result or returns NULL if the
 * result is NULL.
 */
static inet *
ParseInetField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;
	Datum resultStringDatum = 0;
	Datum inetDatum = 0;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return NULL;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);
	resultStringDatum = CStringGetDatum(resultString);
	inetDatum = DirectFunctionCall1(inet_in, resultStringDatum);

	return DatumGetInetP(inetDatum);
}


/*
 * ParseXIDField parses an XID from a remote result or returns 0 if the
 * result is NULL.
 */
static TransactionId
ParseXIDField(PGresult *result, int rowIndex, int colIndex)
{
	char *resultString = NULL;
	Datum resultStringDatum = 0;
	Datum XIDDatum = 0;

	if (PQgetisnull(result, rowIndex, colIndex))
	{
		/*
		 * We'd show NULL if user hits the max transaction id, but that should be
		 * one of the minor problems they'd probably hit.
		 */
		return PG_UINT32_MAX;
	}

	resultString = PQgetvalue(result, rowIndex, colIndex);
	resultStringDatum = CStringGetDatum(resultString);
	XIDDatum = DirectFunctionCall1(xidin, resultStringDatum);

	return DatumGetTransactionId(XIDDatum);
}


/*
 * ReturnCitusDistStats returns the stats for a set returning function.
 */
static void
ReturnCitusDistStats(List *citusStatsList, FunctionCallInfo fcinfo)
{
	ListCell *citusStatsCell = NULL;

	TupleDesc tupleDesc;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDesc);

	foreach(citusStatsCell, citusStatsList)
	{
		CitusDistStat *citusDistStat = (CitusDistStat *) lfirst(citusStatsCell);

		Datum values[CITUS_DIST_STAT_ACTIVITY_COLS];
		bool nulls[CITUS_DIST_STAT_ACTIVITY_COLS];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		if (citusDistStat->query_host_name != NULL)
		{
			values[0] = PointerGetDatum(citusDistStat->query_host_name);
		}
		else
		{
			nulls[0] = true;
		}

		values[1] = Int32GetDatum(citusDistStat->query_host_port);

		if (citusDistStat->master_query_host_name != NULL)
		{
			values[2] = PointerGetDatum(citusDistStat->master_query_host_name);
		}
		else
		{
			nulls[2] = true;
		}

		values[3] = Int32GetDatum(citusDistStat->master_query_host_port);
		values[4] = UInt64GetDatum(citusDistStat->distributed_transaction_number);

		if (citusDistStat->distributed_transaction_stamp != DT_NOBEGIN)
		{
			values[5] = TimestampTzGetDatum(citusDistStat->distributed_transaction_stamp);
		}
		else
		{
			nulls[5] = true;
		}

		values[6] = ObjectIdGetDatum(citusDistStat->database_id);

		if (citusDistStat->databaese_name != NULL)
		{
			values[7] = CStringGetDatum(NameStr(*citusDistStat->databaese_name));
		}
		else
		{
			nulls[7] = true;
		}

		values[8] = Int32GetDatum(citusDistStat->process_id);
		values[9] = ObjectIdGetDatum(citusDistStat->usesysid);

		if (citusDistStat->usename != NULL)
		{
			values[10] = CStringGetDatum(NameStr(*citusDistStat->usename));
		}
		else
		{
			nulls[10] = true;
		}

		if (citusDistStat->application_name != NULL)
		{
			values[11] = PointerGetDatum(citusDistStat->application_name);
		}
		else
		{
			nulls[11] = true;
		}

		if (citusDistStat->client_addr != NULL)
		{
			values[12] = InetPGetDatum(citusDistStat->client_addr);
		}
		else
		{
			nulls[12] = true;
		}

		if (citusDistStat->client_hostname != NULL)
		{
			values[13] = PointerGetDatum(citusDistStat->client_hostname);
		}
		else
		{
			nulls[13] = true;
		}

		values[14] = Int32GetDatum(citusDistStat->client_port);

		if (citusDistStat->backend_start != DT_NOBEGIN)
		{
			values[15] = TimestampTzGetDatum(citusDistStat->backend_start);
		}
		else
		{
			nulls[15] = true;
		}

		if (citusDistStat->xact_start != DT_NOBEGIN)
		{
			values[16] = TimestampTzGetDatum(citusDistStat->xact_start);
		}
		else
		{
			nulls[16] = true;
		}

		if (citusDistStat->query_start != DT_NOBEGIN)
		{
			values[17] = TimestampTzGetDatum(citusDistStat->query_start);
		}
		else
		{
			nulls[17] = true;
		}

		if (citusDistStat->state_change != DT_NOBEGIN)
		{
			values[18] = TimestampTzGetDatum(citusDistStat->state_change);
		}
		else
		{
			nulls[18] = true;
		}

		if (citusDistStat->wait_event_type != NULL)
		{
			values[19] = PointerGetDatum(citusDistStat->wait_event_type);
		}
		else
		{
			nulls[19] = true;
		}

		if (citusDistStat->wait_event != NULL)
		{
			values[20] = PointerGetDatum(citusDistStat->wait_event);
		}
		else
		{
			nulls[20] = true;
		}

		if (citusDistStat->state != NULL)
		{
			values[21] = PointerGetDatum(citusDistStat->state);
		}
		else
		{
			nulls[21] = true;
		}

		if (citusDistStat->backend_xid != PG_UINT32_MAX)
		{
			values[22] = TransactionIdGetDatum(citusDistStat->backend_xid);
		}
		else
		{
			nulls[22] = true;
		}

		if (citusDistStat->backend_xmin != PG_UINT32_MAX)
		{
			values[23] = TransactionIdGetDatum(citusDistStat->backend_xmin);
		}
		else
		{
			nulls[23] = true;
		}

		if (citusDistStat->query != NULL)
		{
			values[24] = PointerGetDatum(citusDistStat->query);
		}
		else
		{
			nulls[24] = true;
		}

		if (citusDistStat->backend_type != NULL)
		{
			values[25] = PointerGetDatum(citusDistStat->backend_type);
		}
		else
		{
			nulls[25] = true;
		}

		tuplestore_putvalues(tupleStore, tupleDesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupleStore);
}
