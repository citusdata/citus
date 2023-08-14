/*-------------------------------------------------------------------------
 *
 * metadata_utility.c
 *    Routines for reading and modifying master node's metadata.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include <sys/statvfs.h>

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "distributed/pg_version_constants.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_namespace.h"
#if PG_VERSION_NUM >= PG_VERSION_16
#include "catalog/pg_proc_d.h"
#endif
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "commands/sequence.h"
#include "distributed/background_jobs.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/citus_nodes.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/metadata_utility.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/pg_dist_background_job.h"
#include "distributed/pg_dist_background_task.h"
#include "distributed/pg_dist_backrgound_task_depend.h"
#include "distributed/pg_dist_colocation.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/pg_dist_placement.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relay_utility.h"
#include "distributed/resource_lock.h"
#include "distributed/remote_commands.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/tuplestore.h"
#include "distributed/utils/array_type.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "nodes/makefuncs.h"
#include "parser/scansup.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#define DISK_SPACE_FIELDS 2

/* Local functions forward declarations */
static uint64 * AllocateUint64(uint64 value);
static void RecordDistributedRelationDependencies(Oid distributedRelationId);
static GroupShardPlacement * TupleToGroupShardPlacement(TupleDesc tupleDesc,
														HeapTuple heapTuple);
static bool DistributedTableSize(Oid relationId, SizeQueryType sizeQueryType,
								 bool failOnError, uint64 *tableSize);
static bool DistributedTableSizeOnWorker(WorkerNode *workerNode, Oid relationId,
										 SizeQueryType sizeQueryType, bool failOnError,
										 uint64 *tableSize);
static List * ShardIntervalsOnWorkerGroup(WorkerNode *workerNode, Oid relationId);
static char * GenerateShardIdNameValuesForShardList(List *shardIntervalList,
													bool firstValue);
static char * GenerateSizeQueryForRelationNameList(List *quotedShardNames,
												   char *sizeFunction);
static char * GetWorkerPartitionedSizeUDFNameBySizeQueryType(SizeQueryType sizeQueryType);
static char * GetSizeQueryBySizeQueryType(SizeQueryType sizeQueryType);
static char * GenerateAllShardStatisticsQueryForNode(WorkerNode *workerNode,
													 List *citusTableIds);
static List * GenerateShardStatisticsQueryList(List *workerNodeList, List *citusTableIds);
static void ErrorIfNotSuitableToGetSize(Oid relationId);
static List * OpenConnectionToNodes(List *workerNodeList);
static void ReceiveShardIdAndSizeResults(List *connectionList,
										 Tuplestorestate *tupleStore,
										 TupleDesc tupleDescriptor);
static void AppendShardIdNameValues(StringInfo selectQuery, ShardInterval *shardInterval);

static HeapTuple CreateDiskSpaceTuple(TupleDesc tupleDesc, uint64 availableBytes,
									  uint64 totalBytes);
static bool GetLocalDiskSpaceStats(uint64 *availableBytes, uint64 *totalBytes);
static BackgroundTask * DeformBackgroundTaskHeapTuple(TupleDesc tupleDescriptor,
													  HeapTuple taskTuple);

static bool SetFieldValue(int attno, Datum values[], bool isnull[], bool replace[],
						  Datum newValue);
static bool SetFieldText(int attno, Datum values[], bool isnull[], bool replace[],
						 const char *newValue);
static bool SetFieldNull(int attno, Datum values[], bool isnull[], bool replace[]);

#define InitFieldValue(attno, values, isnull, initValue) \
	(void) SetFieldValue((attno), (values), (isnull), NULL, (initValue))
#define InitFieldText(attno, values, isnull, initValue) \
	(void) SetFieldText((attno), (values), (isnull), NULL, (initValue))
#define InitFieldNull(attno, values, isnull) \
	(void) SetFieldNull((attno), (values), (isnull), NULL)

/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(citus_local_disk_space_stats);
PG_FUNCTION_INFO_V1(citus_table_size);
PG_FUNCTION_INFO_V1(citus_total_relation_size);
PG_FUNCTION_INFO_V1(citus_relation_size);
PG_FUNCTION_INFO_V1(citus_shard_sizes);


/*
 * CreateDiskSpaceTuple creates a tuple that is used as the return value
 * for citus_local_disk_space_stats.
 */
static HeapTuple
CreateDiskSpaceTuple(TupleDesc tupleDescriptor, uint64 availableBytes, uint64 totalBytes)
{
	Datum values[DISK_SPACE_FIELDS];
	bool isNulls[DISK_SPACE_FIELDS];

	/* form heap tuple for remote disk space statistics */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[0] = UInt64GetDatum(availableBytes);
	values[1] = UInt64GetDatum(totalBytes);

	HeapTuple diskSpaceTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	return diskSpaceTuple;
}


/*
 * citus_local_disk_space_stats returns total disk space and available disk
 * space for the disk that contains PGDATA.
 */
Datum
citus_local_disk_space_stats(PG_FUNCTION_ARGS)
{
	uint64 availableBytes = 0;
	uint64 totalBytes = 0;

	if (!GetLocalDiskSpaceStats(&availableBytes, &totalBytes))
	{
		ereport(WARNING, (errmsg("could not get disk space")));
	}

	TupleDesc tupleDescriptor = NULL;

	TypeFuncClass resultTypeClass = get_call_result_type(fcinfo, NULL,
														 &tupleDescriptor);
	if (resultTypeClass != TYPEFUNC_COMPOSITE)
	{
		ereport(ERROR, (errmsg("return type must be a row type")));
	}

	HeapTuple diskSpaceTuple = CreateDiskSpaceTuple(tupleDescriptor, availableBytes,
													totalBytes);

	PG_RETURN_DATUM(HeapTupleGetDatum(diskSpaceTuple));
}


/*
 * GetLocalDiskSpaceStats returns total and available disk space for the disk containing
 * PGDATA (not considering tablespaces, quota).
 */
static bool
GetLocalDiskSpaceStats(uint64 *availableBytes, uint64 *totalBytes)
{
	struct statvfs buffer;
	if (statvfs(DataDir, &buffer) != 0)
	{
		return false;
	}

	/*
	 * f_bfree: number of free blocks
	 * f_frsize: fragment size, same as f_bsize usually
	 * f_blocks: Size of fs in f_frsize units
	 */
	*availableBytes = buffer.f_bfree * buffer.f_frsize;
	*totalBytes = buffer.f_blocks * buffer.f_frsize;

	return true;
}


/*
 * GetNodeDiskSpaceStatsForConnection fetches the disk space statistics for the node
 * that is on the given connection, or returns false if unsuccessful.
 */
bool
GetNodeDiskSpaceStatsForConnection(MultiConnection *connection, uint64 *availableBytes,
								   uint64 *totalBytes)
{
	PGresult *result = NULL;

	char *sizeQuery = "SELECT available_disk_size, total_disk_size "
					  "FROM pg_catalog.citus_local_disk_space_stats()";


	int queryResult = ExecuteOptionalRemoteCommand(connection, sizeQuery, &result);
	if (queryResult != RESPONSE_OKAY || !IsResponseOK(result) || PQntuples(result) != 1)
	{
		ereport(WARNING, (errcode(ERRCODE_CONNECTION_FAILURE),
						  errmsg("cannot get the disk space statistics for node %s:%d",
								 connection->hostname, connection->port)));

		PQclear(result);
		ForgetResults(connection);

		return false;
	}

	char *availableBytesString = PQgetvalue(result, 0, 0);
	char *totalBytesString = PQgetvalue(result, 0, 1);

	*availableBytes = SafeStringToUint64(availableBytesString);
	*totalBytes = SafeStringToUint64(totalBytesString);

	PQclear(result);
	ForgetResults(connection);

	return true;
}


/*
 * citus_shard_sizes returns all shard ids and their sizes.
 */
Datum
citus_shard_sizes(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	List *allCitusTableIds = AllCitusTableIds();

	/* we don't need a distributed transaction here */
	bool useDistributedTransaction = false;

	List *connectionList =
		SendShardStatisticsQueriesInParallel(allCitusTableIds, useDistributedTransaction);

	TupleDesc tupleDescriptor = NULL;
	Tuplestorestate *tupleStore = SetupTuplestore(fcinfo, &tupleDescriptor);

	ReceiveShardIdAndSizeResults(connectionList, tupleStore, tupleDescriptor);

	PG_RETURN_VOID();
}


/*
 * citus_total_relation_size accepts a table name and returns a distributed table
 * and its indexes' total relation size.
 */
Datum
citus_total_relation_size(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	bool failOnError = PG_GETARG_BOOL(1);

	SizeQueryType sizeQueryType = TOTAL_RELATION_SIZE;
	uint64 tableSize = 0;

	if (!DistributedTableSize(relationId, sizeQueryType, failOnError, &tableSize))
	{
		Assert(!failOnError);
		PG_RETURN_NULL();
	}

	PG_RETURN_INT64(tableSize);
}


/*
 * citus_table_size accepts a table name and returns a distributed table's total
 * relation size.
 */
Datum
citus_table_size(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	bool failOnError = true;
	SizeQueryType sizeQueryType = TABLE_SIZE;
	uint64 tableSize = 0;

	if (!DistributedTableSize(relationId, sizeQueryType, failOnError, &tableSize))
	{
		Assert(!failOnError);
		PG_RETURN_NULL();
	}

	PG_RETURN_INT64(tableSize);
}


/*
 * citus_relation_size accept a table name and returns a relation's 'main'
 * fork's size.
 */
Datum
citus_relation_size(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	bool failOnError = true;
	SizeQueryType sizeQueryType = RELATION_SIZE;
	uint64 relationSize = 0;

	if (!DistributedTableSize(relationId, sizeQueryType, failOnError, &relationSize))
	{
		Assert(!failOnError);
		PG_RETURN_NULL();
	}

	PG_RETURN_INT64(relationSize);
}


/*
 * SendShardStatisticsQueriesInParallel generates query lists for obtaining shard
 * statistics and then sends the commands in parallel by opening connections
 * to available nodes. It returns the connection list.
 */
List *
SendShardStatisticsQueriesInParallel(List *citusTableIds, bool useDistributedTransaction)
{
	List *workerNodeList = ActivePrimaryNodeList(NoLock);

	List *shardSizesQueryList = GenerateShardStatisticsQueryList(workerNodeList,
																 citusTableIds);

	List *connectionList = OpenConnectionToNodes(workerNodeList);
	FinishConnectionListEstablishment(connectionList);

	if (useDistributedTransaction)
	{
		/*
		 * For now, in the case we want to include shard min and max values, we also
		 * want to update the entries in pg_dist_placement and pg_dist_shard with the
		 * latest statistics. In order to detect distributed deadlocks, we assign a
		 * distributed transaction ID to the current transaction
		 */
		UseCoordinatedTransaction();
	}

	/* send commands in parallel */
	for (int i = 0; i < list_length(connectionList); i++)
	{
		MultiConnection *connection = (MultiConnection *) list_nth(connectionList, i);
		char *shardSizesQuery = (char *) list_nth(shardSizesQueryList, i);

		if (useDistributedTransaction)
		{
			/* run the size query in a distributed transaction */
			RemoteTransactionBeginIfNecessary(connection);
		}

		int querySent = SendRemoteCommand(connection, shardSizesQuery);

		if (querySent == 0)
		{
			ReportConnectionError(connection, WARNING);
		}
	}
	return connectionList;
}


/*
 * OpenConnectionToNodes opens a single connection per node
 * for the given workerNodeList.
 */
static List *
OpenConnectionToNodes(List *workerNodeList)
{
	List *connectionList = NIL;
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		const char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		int connectionFlags = 0;

		MultiConnection *connection = StartNodeConnection(connectionFlags, nodeName,
														  nodePort);

		connectionList = lappend(connectionList, connection);
	}
	return connectionList;
}


/*
 * GenerateShardStatisticsQueryList generates a query per node that will return:
 * shard_id, shard_name, shard_size for all shard placements on the node
 */
static List *
GenerateShardStatisticsQueryList(List *workerNodeList, List *citusTableIds)
{
	List *shardStatisticsQueryList = NIL;
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		char *shardStatisticsQuery =
			GenerateAllShardStatisticsQueryForNode(workerNode, citusTableIds);

		shardStatisticsQueryList = lappend(shardStatisticsQueryList,
										   shardStatisticsQuery);
	}
	return shardStatisticsQueryList;
}


/*
 * ReceiveShardIdAndSizeResults receives shard id and size results from the given
 * connection list.
 */
static void
ReceiveShardIdAndSizeResults(List *connectionList, Tuplestorestate *tupleStore,
							 TupleDesc tupleDescriptor)
{
	MultiConnection *connection = NULL;
	foreach_ptr(connection, connectionList)
	{
		bool raiseInterrupts = true;
		Datum values[SHARD_SIZES_COLUMN_COUNT];
		bool isNulls[SHARD_SIZES_COLUMN_COUNT];

		if (PQstatus(connection->pgConn) != CONNECTION_OK)
		{
			continue;
		}

		PGresult *result = GetRemoteCommandResult(connection, raiseInterrupts);
		if (!IsResponseOK(result))
		{
			ReportResultError(connection, result, WARNING);
			continue;
		}

		int64 rowCount = PQntuples(result);
		int64 colCount = PQnfields(result);

		/* Although it is not expected */
		if (colCount != SHARD_SIZES_COLUMN_COUNT)
		{
			ereport(WARNING, (errmsg("unexpected number of columns from "
									 "citus_shard_sizes")));
			continue;
		}

		for (int64 rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			memset(values, 0, sizeof(values));
			memset(isNulls, false, sizeof(isNulls));

			/* format is [0] shard id, [1] size */
			values[0] = ParseIntField(result, rowIndex, 0);
			values[1] = ParseIntField(result, rowIndex, 1);

			tuplestore_putvalues(tupleStore, tupleDescriptor, values, isNulls);
		}

		PQclear(result);
		ForgetResults(connection);
	}
}


/*
 * DistributedTableSize is helper function for each kind of citus size functions.
 * It first checks whether the table is distributed and size query can be run on
 * it. Connection to each node has to be established to get the size of the table.
 */
static bool
DistributedTableSize(Oid relationId, SizeQueryType sizeQueryType, bool failOnError,
					 uint64 *tableSize)
{
	int logLevel = WARNING;

	if (failOnError)
	{
		logLevel = ERROR;
	}

	uint64 sumOfSizes = 0;

	if (XactModificationLevel == XACT_MODIFICATION_DATA)
	{
		ereport(logLevel, (errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
						   errmsg("citus size functions cannot be called in transaction "
								  "blocks which contain multi-shard data "
								  "modifications")));

		return false;
	}

	Relation relation = try_relation_open(relationId, AccessShareLock);

	if (relation == NULL)
	{
		ereport(logLevel,
				(errmsg("could not compute table size: relation does not exist")));

		return false;
	}

	ErrorIfNotSuitableToGetSize(relationId);

	table_close(relation, AccessShareLock);

	List *workerNodeList = ActiveReadableNodeList();
	WorkerNode *workerNode = NULL;
	foreach_ptr(workerNode, workerNodeList)
	{
		uint64 relationSizeOnNode = 0;

		bool gotSize = DistributedTableSizeOnWorker(workerNode, relationId, sizeQueryType,
													failOnError, &relationSizeOnNode);
		if (!gotSize)
		{
			return false;
		}

		sumOfSizes += relationSizeOnNode;
	}

	*tableSize = sumOfSizes;

	return true;
}


/*
 * DistributedTableSizeOnWorker gets the workerNode and relationId to calculate
 * size of that relation on the given workerNode by summing up the size of each
 * shard placement.
 */
static bool
DistributedTableSizeOnWorker(WorkerNode *workerNode, Oid relationId,
							 SizeQueryType sizeQueryType,
							 bool failOnError, uint64 *tableSize)
{
	int logLevel = WARNING;

	if (failOnError)
	{
		logLevel = ERROR;
	}

	char *workerNodeName = workerNode->workerName;
	uint32 workerNodePort = workerNode->workerPort;
	uint32 connectionFlag = 0;
	PGresult *result = NULL;

	List *shardIntervalsOnNode = ShardIntervalsOnWorkerGroup(workerNode, relationId);

	/*
	 * We pass false here, because if we optimize this, we would include child tables.
	 * But citus size functions shouldn't include them, like PG.
	 */
	bool optimizePartitionCalculations = false;
	StringInfo tableSizeQuery = GenerateSizeQueryOnMultiplePlacements(
		shardIntervalsOnNode,
		sizeQueryType,
		optimizePartitionCalculations);

	MultiConnection *connection = GetNodeConnection(connectionFlag, workerNodeName,
													workerNodePort);
	int queryResult = ExecuteOptionalRemoteCommand(connection, tableSizeQuery->data,
												   &result);

	if (queryResult != 0)
	{
		ereport(logLevel, (errcode(ERRCODE_CONNECTION_FAILURE),
						   errmsg("could not connect to %s:%d to get size of "
								  "table \"%s\"",
								  workerNodeName, workerNodePort,
								  get_rel_name(relationId))));

		return false;
	}

	List *sizeList = ReadFirstColumnAsText(result);
	if (list_length(sizeList) != 1)
	{
		PQclear(result);
		ClearResults(connection, failOnError);

		ereport(logLevel, (errcode(ERRCODE_CONNECTION_FAILURE),
						   errmsg("cannot parse size of table \"%s\" from %s:%d",
								  get_rel_name(relationId), workerNodeName,
								  workerNodePort)));

		return false;
	}

	StringInfo tableSizeStringInfo = (StringInfo) linitial(sizeList);
	char *tableSizeString = tableSizeStringInfo->data;

	if (strlen(tableSizeString) > 0)
	{
		*tableSize = SafeStringToUint64(tableSizeString);
	}
	else
	{
		/*
		 * This means the shard is moved or dropped while citus_total_relation_size is
		 * being executed. For this case we get an empty string as table size.
		 * We can take that as zero to prevent any unnecessary errors.
		 */
		*tableSize = 0;
	}

	PQclear(result);
	ClearResults(connection, failOnError);

	return true;
}


/*
 * GroupShardPlacementsForTableOnGroup accepts a relationId and a group and returns a list
 * of GroupShardPlacement's representing all of the placements for the table which reside
 * on the group.
 */
List *
GroupShardPlacementsForTableOnGroup(Oid relationId, int32 groupId)
{
	CitusTableCacheEntry *distTableCacheEntry = GetCitusTableCacheEntry(relationId);
	List *resultList = NIL;

	int shardIntervalArrayLength = distTableCacheEntry->shardIntervalArrayLength;

	for (int shardIndex = 0; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		GroupShardPlacement *placementArray =
			distTableCacheEntry->arrayOfPlacementArrays[shardIndex];
		int numberOfPlacements =
			distTableCacheEntry->arrayOfPlacementArrayLengths[shardIndex];

		for (int placementIndex = 0; placementIndex < numberOfPlacements;
			 placementIndex++)
		{
			if (placementArray[placementIndex].groupId == groupId)
			{
				GroupShardPlacement *placement = palloc0(sizeof(GroupShardPlacement));
				*placement = placementArray[placementIndex];
				resultList = lappend(resultList, placement);
			}
		}
	}

	return resultList;
}


/*
 * ShardIntervalsOnWorkerGroup accepts a WorkerNode and returns a list of the shard
 * intervals of the given table which are placed on the group the node is a part of.
 */
static List *
ShardIntervalsOnWorkerGroup(WorkerNode *workerNode, Oid relationId)
{
	CitusTableCacheEntry *distTableCacheEntry = GetCitusTableCacheEntry(relationId);
	List *shardIntervalList = NIL;
	int shardIntervalArrayLength = distTableCacheEntry->shardIntervalArrayLength;

	for (int shardIndex = 0; shardIndex < shardIntervalArrayLength; shardIndex++)
	{
		GroupShardPlacement *placementArray =
			distTableCacheEntry->arrayOfPlacementArrays[shardIndex];
		int numberOfPlacements =
			distTableCacheEntry->arrayOfPlacementArrayLengths[shardIndex];

		for (int placementIndex = 0; placementIndex < numberOfPlacements;
			 placementIndex++)
		{
			GroupShardPlacement *placement = &placementArray[placementIndex];

			if (placement->groupId == workerNode->groupId)
			{
				ShardInterval *cachedShardInterval =
					distTableCacheEntry->sortedShardIntervalArray[shardIndex];
				ShardInterval *shardInterval = CopyShardInterval(cachedShardInterval);
				shardIntervalList = lappend(shardIntervalList, shardInterval);
			}
		}
	}

	return shardIntervalList;
}


/*
 * GenerateSizeQueryOnMultiplePlacements generates a select size query to get
 * size of multiple tables. Note that, different size functions supported by PG
 * are also supported by this function changing the size query type given as the
 * last parameter to function. Depending on the sizeQueryType enum parameter, the
 * generated query will call one of the functions: pg_relation_size,
 * pg_total_relation_size, pg_table_size and cstore_table_size.
 * This function uses UDFs named worker_partitioned_*_size for partitioned tables,
 * if the parameter optimizePartitionCalculations is true. The UDF to be called is
 * determined by the parameter sizeQueryType.
 */
StringInfo
GenerateSizeQueryOnMultiplePlacements(List *shardIntervalList,
									  SizeQueryType sizeQueryType,
									  bool optimizePartitionCalculations)
{
	StringInfo selectQuery = makeStringInfo();

	List *partitionedShardNames = NIL;
	List *nonPartitionedShardNames = NIL;

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		if (optimizePartitionCalculations && PartitionTable(shardInterval->relationId))
		{
			/*
			 * Skip child tables of a partitioned table as they are already counted in
			 * worker_partitioned_*_size UDFs, if optimizePartitionCalculations is true.
			 * We don't expect this case to happen, since we don't send the child tables
			 * to this function. Because they are all eliminated in
			 * ColocatedNonPartitionShardIntervalList. Therefore we can't cover here with
			 * a test currently. This is added for possible future usages.
			 */
			continue;
		}
		uint64 shardId = shardInterval->shardId;
		Oid schemaId = get_rel_namespace(shardInterval->relationId);
		char *schemaName = get_namespace_name(schemaId);
		char *shardName = get_rel_name(shardInterval->relationId);
		AppendShardIdToName(&shardName, shardId);

		char *shardQualifiedName = quote_qualified_identifier(schemaName, shardName);
		char *quotedShardName = quote_literal_cstr(shardQualifiedName);

		/* for partitoned tables, we will call worker_partitioned_... size functions */
		if (optimizePartitionCalculations && PartitionedTable(shardInterval->relationId))
		{
			partitionedShardNames = lappend(partitionedShardNames, quotedShardName);
		}
		/* for non-partitioned tables, we will use Postgres' size functions */
		else
		{
			nonPartitionedShardNames = lappend(nonPartitionedShardNames, quotedShardName);
		}
	}

	/* SELECT SUM(worker_partitioned_...) FROM VALUES (...) */
	char *subqueryForPartitionedShards =
		GenerateSizeQueryForRelationNameList(partitionedShardNames,
											 GetWorkerPartitionedSizeUDFNameBySizeQueryType(
												 sizeQueryType));

	/* SELECT SUM(pg_..._size) FROM VALUES (...) */
	char *subqueryForNonPartitionedShards =
		GenerateSizeQueryForRelationNameList(nonPartitionedShardNames,
											 GetSizeQueryBySizeQueryType(sizeQueryType));

	appendStringInfo(selectQuery, "SELECT (%s) + (%s);",
					 subqueryForPartitionedShards, subqueryForNonPartitionedShards);

	elog(DEBUG4, "Size Query: %s", selectQuery->data);

	return selectQuery;
}


/*
 * GenerateSizeQueryForPartitionedShards generates and returns a query with a template:
 * SELECT SUM( <sizeFunction>(relid) ) FROM (VALUES (<shardName>), (<shardName>), ...) as q(relid)
 */
static char *
GenerateSizeQueryForRelationNameList(List *quotedShardNames, char *sizeFunction)
{
	if (list_length(quotedShardNames) == 0)
	{
		return "SELECT 0";
	}

	StringInfo selectQuery = makeStringInfo();

	appendStringInfo(selectQuery, "SELECT SUM(");
	appendStringInfo(selectQuery, sizeFunction, "relid");
	appendStringInfo(selectQuery, ") FROM (VALUES ");

	bool addComma = false;
	char *quotedShardName = NULL;
	foreach_ptr(quotedShardName, quotedShardNames)
	{
		if (addComma)
		{
			appendStringInfoString(selectQuery, ", ");
		}
		addComma = true;

		appendStringInfo(selectQuery, "(%s)", quotedShardName);
	}

	appendStringInfoString(selectQuery, ") as q(relid)");

	return selectQuery->data;
}


/*
 * GetWorkerPartitionedSizeUDFNameBySizeQueryType returns the corresponding worker
 * partitioned size query for given query type.
 * Errors out for an invalid query type.
 * Currently this function is only called with the type TOTAL_RELATION_SIZE.
 * The others are added for possible future usages. Since they are not used anywhere,
 * currently we can't cover them with tests.
 */
static char *
GetWorkerPartitionedSizeUDFNameBySizeQueryType(SizeQueryType sizeQueryType)
{
	switch (sizeQueryType)
	{
		case RELATION_SIZE:
		{
			return WORKER_PARTITIONED_RELATION_SIZE_FUNCTION;
		}

		case TOTAL_RELATION_SIZE:
		{
			return WORKER_PARTITIONED_RELATION_TOTAL_SIZE_FUNCTION;
		}

		case TABLE_SIZE:
		{
			return WORKER_PARTITIONED_TABLE_SIZE_FUNCTION;
		}

		default:
		{
			elog(ERROR, "Size query type couldn't be found.");
		}
	}
}


/*
 * GetSizeQueryBySizeQueryType returns the corresponding size query for given query type.
 * Errors out for an invalid query type.
 */
static char *
GetSizeQueryBySizeQueryType(SizeQueryType sizeQueryType)
{
	switch (sizeQueryType)
	{
		case RELATION_SIZE:
		{
			return PG_RELATION_SIZE_FUNCTION;
		}

		case TOTAL_RELATION_SIZE:
		{
			return PG_TOTAL_RELATION_SIZE_FUNCTION;
		}

		case TABLE_SIZE:
		{
			return PG_TABLE_SIZE_FUNCTION;
		}

		default:
		{
			elog(ERROR, "Size query type couldn't be found.");
		}
	}
}


/*
 * GenerateAllShardStatisticsQueryForNode generates a query that returns:
 * shard_id, shard_name, shard_size for all shard placements on the node
 */
static char *
GenerateAllShardStatisticsQueryForNode(WorkerNode *workerNode, List *citusTableIds)
{
	StringInfo allShardStatisticsQuery = makeStringInfo();
	bool insertedValues = false;

	appendStringInfoString(allShardStatisticsQuery, "SELECT shard_id, ");
	appendStringInfo(allShardStatisticsQuery, PG_TOTAL_RELATION_SIZE_FUNCTION,
					 "table_name");
	appendStringInfoString(allShardStatisticsQuery, " FROM (VALUES ");

	Oid relationId = InvalidOid;
	foreach_oid(relationId, citusTableIds)
	{
		/*
		 * Ensure the table still exists by trying to acquire a lock on it
		 * If function returns NULL, it means the table doesn't exist
		 * hence we should skip
		 */
		Relation relation = try_relation_open(relationId, AccessShareLock);
		if (relation != NULL)
		{
			List *shardIntervalsOnNode = ShardIntervalsOnWorkerGroup(workerNode,
																	 relationId);
			if (list_length(shardIntervalsOnNode) == 0)
			{
				relation_close(relation, AccessShareLock);
				continue;
			}
			char *shardIdNameValues =
				GenerateShardIdNameValuesForShardList(shardIntervalsOnNode,
													  !insertedValues);
			insertedValues = true;
			appendStringInfoString(allShardStatisticsQuery, shardIdNameValues);
			relation_close(relation, AccessShareLock);
		}
	}

	if (!insertedValues)
	{
		return "SELECT 0 AS shard_id, '' AS table_name LIMIT 0";
	}

	appendStringInfoString(allShardStatisticsQuery, ") t(shard_id, table_name) "
													"WHERE to_regclass(table_name) IS NOT NULL");
	return allShardStatisticsQuery->data;
}


/*
 * GenerateShardIdNameValuesForShardList generates a list of (shard_id, shard_name) values
 * for all shards in the list
 */
static char *
GenerateShardIdNameValuesForShardList(List *shardIntervalList, bool firstValue)
{
	StringInfo selectQuery = makeStringInfo();

	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		if (!firstValue)
		{
			appendStringInfoString(selectQuery, ", ");
		}
		firstValue = false;
		AppendShardIdNameValues(selectQuery, shardInterval);
	}

	return selectQuery->data;
}


/*
 * AppendShardIdNameValues appends (shard_id, shard_name) for shard
 */
static void
AppendShardIdNameValues(StringInfo selectQuery, ShardInterval *shardInterval)
{
	uint64 shardId = shardInterval->shardId;
	Oid schemaId = get_rel_namespace(shardInterval->relationId);
	char *schemaName = get_namespace_name(schemaId);
	char *shardName = get_rel_name(shardInterval->relationId);

	AppendShardIdToName(&shardName, shardId);

	char *shardQualifiedName = quote_qualified_identifier(schemaName, shardName);
	char *quotedShardName = quote_literal_cstr(shardQualifiedName);

	appendStringInfo(selectQuery, "(" UINT64_FORMAT ", %s)", shardId, quotedShardName);
}


/*
 * ErrorIfNotSuitableToGetSize determines whether the table is suitable to find
 * its' size with internal functions.
 */
static void
ErrorIfNotSuitableToGetSize(Oid relationId)
{
	if (!IsCitusTable(relationId))
	{
		char *relationName = get_rel_name(relationId);
		char *escapedQueryString = quote_literal_cstr(relationName);
		ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
						errmsg("cannot calculate the size because relation %s is not "
							   "distributed", escapedQueryString)));
	}
}


/*
 * CompareShardPlacementsByWorker compares two shard placements by their
 * worker node name and port.
 */
int
CompareShardPlacementsByWorker(const void *leftElement, const void *rightElement)
{
	const ShardPlacement *leftPlacement = *((const ShardPlacement **) leftElement);
	const ShardPlacement *rightPlacement = *((const ShardPlacement **) rightElement);

	int nodeNameCmp = strncmp(leftPlacement->nodeName, rightPlacement->nodeName,
							  WORKER_LENGTH);
	if (nodeNameCmp != 0)
	{
		return nodeNameCmp;
	}
	else if (leftPlacement->nodePort > rightPlacement->nodePort)
	{
		return 1;
	}
	else if (leftPlacement->nodePort < rightPlacement->nodePort)
	{
		return -1;
	}

	return 0;
}


/*
 * CompareShardPlacementsByGroupId compares two shard placements by their
 * group id.
 */
int
CompareShardPlacementsByGroupId(const void *leftElement, const void *rightElement)
{
	const ShardPlacement *leftPlacement = *((const ShardPlacement **) leftElement);
	const ShardPlacement *rightPlacement = *((const ShardPlacement **) rightElement);

	if (leftPlacement->groupId > rightPlacement->groupId)
	{
		return 1;
	}
	else if (leftPlacement->groupId < rightPlacement->groupId)
	{
		return -1;
	}
	else
	{
		return 0;
	}
}


/*
 * TableShardReplicationFactor returns the current replication factor of the
 * given relation by looking into shard placements. It errors out if there
 * are different number of shard placements for different shards. It also
 * errors out if the table does not have any shards.
 */
uint32
TableShardReplicationFactor(Oid relationId)
{
	uint32 replicationCount = 0;

	List *shardIntervalList = LoadShardIntervalList(relationId);
	ShardInterval *shardInterval = NULL;
	foreach_ptr(shardInterval, shardIntervalList)
	{
		uint64 shardId = shardInterval->shardId;

		List *shardPlacementList = ShardPlacementListSortedByWorker(shardId);
		uint32 shardPlacementCount = list_length(shardPlacementList);

		/*
		 * Get the replication count of the first shard in the list, and error
		 * out if there is a shard with different replication count.
		 */
		if (replicationCount == 0)
		{
			replicationCount = shardPlacementCount;
		}
		else if (replicationCount != shardPlacementCount)
		{
			char *relationName = get_rel_name(relationId);
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot find the replication factor of the "
								   "table %s", relationName),
							errdetail("The shard " UINT64_FORMAT
									  " has different shards replication counts from "
									  "other shards.", shardId)));
		}
	}

	/* error out if the table does not have any shards */
	if (replicationCount == 0)
	{
		char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cannot find the replication factor of the "
							   "table %s", relationName),
						errdetail("The table %s does not have any shards.",
								  relationName)));
	}

	return replicationCount;
}


/*
 * LoadShardIntervalList returns a list of shard intervals related for a given
 * distributed table. The function returns an empty list if no shards can be
 * found for the given relation.
 * Since LoadShardIntervalList relies on sortedShardIntervalArray, it returns
 * a shard interval list whose elements are sorted on shardminvalue. Shard intervals
 * with uninitialized shard min/max values are placed in the end of the list.
 */
List *
LoadShardIntervalList(Oid relationId)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
	List *shardList = NIL;

	for (int i = 0; i < cacheEntry->shardIntervalArrayLength; i++)
	{
		ShardInterval *newShardInterval =
			CopyShardInterval(cacheEntry->sortedShardIntervalArray[i]);
		shardList = lappend(shardList, newShardInterval);
	}

	return shardList;
}


/*
 * LoadUnsortedShardIntervalListViaCatalog returns a list of shard intervals related for a
 * given distributed table. The function returns an empty list if no shards can be found
 * for the given relation.
 *
 * This function does not use CitusTableCache and instead reads from catalog tables
 * directly.
 */
List *
LoadUnsortedShardIntervalListViaCatalog(Oid relationId)
{
	List *shardIntervalList = NIL;
	List *distShardTuples = LookupDistShardTuples(relationId);
	Relation distShardRelation = table_open(DistShardRelationId(), AccessShareLock);
	TupleDesc distShardTupleDesc = RelationGetDescr(distShardRelation);
	Oid intervalTypeId = InvalidOid;
	int32 intervalTypeMod = -1;

	char partitionMethod = PartitionMethodViaCatalog(relationId);
	Var *partitionColumn = PartitionColumnViaCatalog(relationId);
	GetIntervalTypeInfo(partitionMethod, partitionColumn, &intervalTypeId,
						&intervalTypeMod);

	HeapTuple distShardTuple = NULL;
	foreach_ptr(distShardTuple, distShardTuples)
	{
		ShardInterval *interval = TupleToShardInterval(distShardTuple,
													   distShardTupleDesc,
													   intervalTypeId,
													   intervalTypeMod);
		shardIntervalList = lappend(shardIntervalList, interval);
	}
	table_close(distShardRelation, AccessShareLock);

	return shardIntervalList;
}


/*
 * LoadShardIntervalWithLongestShardName is a utility function that returns
 * the shard interaval with the largest shardId for the given relationId. Note
 * that largest shardId implies longest shard name.
 */
ShardInterval *
LoadShardIntervalWithLongestShardName(Oid relationId)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
	int shardIntervalCount = cacheEntry->shardIntervalArrayLength;

	int maxShardIndex = shardIntervalCount - 1;
	uint64 largestShardId = INVALID_SHARD_ID;

	for (int shardIndex = 0; shardIndex <= maxShardIndex; ++shardIndex)
	{
		ShardInterval *currentShardInterval =
			cacheEntry->sortedShardIntervalArray[shardIndex];

		if (largestShardId < currentShardInterval->shardId)
		{
			largestShardId = currentShardInterval->shardId;
		}
	}

	return LoadShardInterval(largestShardId);
}


/*
 * ShardIntervalCount returns number of shard intervals for a given distributed table.
 * The function returns 0 if no shards can be found for the given relation id.
 */
int
ShardIntervalCount(Oid relationId)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);

	return cacheEntry->shardIntervalArrayLength;
}


/*
 * LoadShardList reads list of shards for given relationId from pg_dist_shard,
 * and returns the list of found shardIds.
 * Since LoadShardList relies on sortedShardIntervalArray, it returns a shard
 * list whose elements are sorted on shardminvalue. Shards with uninitialized
 * shard min/max values are placed in the end of the list.
 */
List *
LoadShardList(Oid relationId)
{
	CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
	List *shardList = NIL;

	for (int i = 0; i < cacheEntry->shardIntervalArrayLength; i++)
	{
		ShardInterval *currentShardInterval = cacheEntry->sortedShardIntervalArray[i];
		uint64 *shardIdPointer = AllocateUint64(currentShardInterval->shardId);

		shardList = lappend(shardList, shardIdPointer);
	}

	return shardList;
}


/* Allocates eight bytes, and copies given value's contents those bytes. */
static uint64 *
AllocateUint64(uint64 value)
{
	uint64 *allocatedValue = (uint64 *) palloc0(sizeof(uint64));
	Assert(sizeof(uint64) >= 8);

	(*allocatedValue) = value;

	return allocatedValue;
}


/*
 * CopyShardInterval creates a copy of the specified source ShardInterval.
 */
ShardInterval *
CopyShardInterval(ShardInterval *srcInterval)
{
	ShardInterval *destInterval = palloc0(sizeof(ShardInterval));

	destInterval->type = srcInterval->type;
	destInterval->relationId = srcInterval->relationId;
	destInterval->storageType = srcInterval->storageType;
	destInterval->valueTypeId = srcInterval->valueTypeId;
	destInterval->valueTypeLen = srcInterval->valueTypeLen;
	destInterval->valueByVal = srcInterval->valueByVal;
	destInterval->minValueExists = srcInterval->minValueExists;
	destInterval->maxValueExists = srcInterval->maxValueExists;
	destInterval->shardId = srcInterval->shardId;
	destInterval->shardIndex = srcInterval->shardIndex;

	destInterval->minValue = 0;
	if (destInterval->minValueExists)
	{
		destInterval->minValue = datumCopy(srcInterval->minValue,
										   srcInterval->valueByVal,
										   srcInterval->valueTypeLen);
	}

	destInterval->maxValue = 0;
	if (destInterval->maxValueExists)
	{
		destInterval->maxValue = datumCopy(srcInterval->maxValue,
										   srcInterval->valueByVal,
										   srcInterval->valueTypeLen);
	}

	return destInterval;
}


/*
 * ShardLength finds shard placements for the given shardId, extracts the length
 * of an active shard, and returns the shard's length. This function errors
 * out if we cannot find any active shard placements for the given shardId.
 */
uint64
ShardLength(uint64 shardId)
{
	uint64 shardLength = 0;

	List *shardPlacementList = ActiveShardPlacementList(shardId);
	if (shardPlacementList == NIL)
	{
		ereport(ERROR, (errmsg("could not find length of shard " UINT64_FORMAT, shardId),
						errdetail("Could not find any shard placements for the shard.")));
	}
	else
	{
		ShardPlacement *shardPlacement = (ShardPlacement *) linitial(shardPlacementList);
		shardLength = shardPlacement->shardLength;
	}

	return shardLength;
}


/*
 * NodeGroupHasShardPlacements returns whether any active shards are placed on the group
 */
bool
NodeGroupHasShardPlacements(int32 groupId)
{
	const int scanKeyCount = 1;
	const bool indexOK = false;

	ScanKeyData scanKey[1];

	Relation pgPlacement = table_open(DistPlacementRelationId(),
									  AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_placement_groupid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(groupId));

	SysScanDesc scanDescriptor = systable_beginscan(pgPlacement,
													DistPlacementGroupidIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	bool hasActivePlacements = HeapTupleIsValid(heapTuple);

	systable_endscan(scanDescriptor);
	table_close(pgPlacement, NoLock);

	return hasActivePlacements;
}


/*
 * IsActiveShardPlacement checks if the shard placement is labelled as
 * active, and that it is placed in an active worker.
 * Expects shard worker to not be NULL.
 */
bool
IsActiveShardPlacement(ShardPlacement *shardPlacement)
{
	WorkerNode *workerNode =
		FindWorkerNode(shardPlacement->nodeName, shardPlacement->nodePort);

	if (!workerNode)
	{
		ereport(ERROR, (errmsg("There is a shard placement on node %s:%d but "
							   "could not find the node.", shardPlacement->nodeName,
							   shardPlacement->nodePort)));
	}

	return workerNode->isActive;
}


/*
 * IsPlacementOnWorkerNode checks if the shard placement is for to the given
 * workenode.
 */
bool
IsPlacementOnWorkerNode(ShardPlacement *placement, WorkerNode *workerNode)
{
	if (strncmp(workerNode->workerName, placement->nodeName, WORKER_LENGTH) != 0)
	{
		return false;
	}
	return workerNode->workerPort == placement->nodePort;
}


/*
 * FilterShardPlacementList filters a list of shard placements based on a filter.
 * Keep only the shard for which the filter function returns true.
 */
List *
FilterShardPlacementList(List *shardPlacementList, bool (*filter)(ShardPlacement *))
{
	List *filteredShardPlacementList = NIL;
	ShardPlacement *shardPlacement = NULL;

	foreach_ptr(shardPlacement, shardPlacementList)
	{
		if (filter(shardPlacement))
		{
			filteredShardPlacementList = lappend(filteredShardPlacementList,
												 shardPlacement);
		}
	}

	return filteredShardPlacementList;
}


/*
 * FilterActiveShardPlacementListByNode filters a list of active shard placements based on given nodeName and nodePort.
 */
List *
FilterActiveShardPlacementListByNode(List *shardPlacementList, WorkerNode *workerNode)
{
	List *activeShardPlacementList = FilterShardPlacementList(shardPlacementList,
															  IsActiveShardPlacement);
	List *filteredShardPlacementList = NIL;
	ShardPlacement *shardPlacement = NULL;

	foreach_ptr(shardPlacement, activeShardPlacementList)
	{
		if (IsPlacementOnWorkerNode(shardPlacement, workerNode))
		{
			filteredShardPlacementList = lappend(filteredShardPlacementList,
												 shardPlacement);
		}
	}

	return filteredShardPlacementList;
}


/*
 * ActiveShardPlacementListOnGroup returns a list of active shard placements
 * that are sitting on group with groupId for given shardId.
 */
List *
ActiveShardPlacementListOnGroup(uint64 shardId, int32 groupId)
{
	List *activeShardPlacementListOnGroup = NIL;

	List *activePlacementList = ActiveShardPlacementList(shardId);
	ShardPlacement *shardPlacement = NULL;
	foreach_ptr(shardPlacement, activePlacementList)
	{
		if (shardPlacement->groupId == groupId)
		{
			activeShardPlacementListOnGroup = lappend(activeShardPlacementListOnGroup,
													  shardPlacement);
		}
	}

	return activeShardPlacementListOnGroup;
}


/*
 * ActiveShardPlacementList finds shard placements for the given shardId from
 * system catalogs, chooses placements that are in active state, and returns
 * these shard placements in a new list.
 */
List *
ActiveShardPlacementList(uint64 shardId)
{
	List *shardPlacementList = ShardPlacementList(shardId);

	List *activePlacementList = FilterShardPlacementList(shardPlacementList,
														 IsActiveShardPlacement);

	return SortList(activePlacementList, CompareShardPlacementsByWorker);
}


/*
 * ShardPlacementListSortedByWorker returns shard placements sorted by worker port.
 */
List *
ShardPlacementListSortedByWorker(uint64 shardId)
{
	List *shardPlacementList = ShardPlacementList(shardId);

	return SortList(shardPlacementList, CompareShardPlacementsByWorker);
}


/*
 * ActiveShardPlacement finds a shard placement for the given shardId from
 * system catalog, chooses a placement that is in active state and returns
 * that shard placement. If this function cannot find a healthy shard placement
 * and missingOk is set to false it errors out.
 */
ShardPlacement *
ActiveShardPlacement(uint64 shardId, bool missingOk)
{
	List *activePlacementList = ActiveShardPlacementList(shardId);
	ShardPlacement *shardPlacement = NULL;

	if (list_length(activePlacementList) == 0)
	{
		if (!missingOk)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("could not find any healthy placement for shard "
								   UINT64_FORMAT, shardId)));
		}

		return shardPlacement;
	}

	shardPlacement = (ShardPlacement *) linitial(activePlacementList);

	return shardPlacement;
}


/*
 * ActiveShardPlacementWorkerNode returns the worker node of the first placement of
 * a shard.
 */
WorkerNode *
ActiveShardPlacementWorkerNode(uint64 shardId)
{
	bool missingOK = false;

	List *sourcePlacementList = ActiveShardPlacementList(shardId);

	Assert(sourcePlacementList->length == 1);

	ShardPlacement *sourceShardPlacement = linitial(sourcePlacementList);
	WorkerNode *sourceShardToCopyNode = FindNodeWithNodeId(sourceShardPlacement->nodeId,
														   missingOK);

	return sourceShardToCopyNode;
}


/*
 * BuildShardPlacementList finds shard placements for the given shardId from
 * system catalogs, converts these placements to their in-memory
 * representation, and returns the converted shard placements in a new list.
 *
 * This probably only should be called from metadata_cache.c.  Resides here
 * because it shares code with other routines in this file.
 */
List *
BuildShardPlacementList(int64 shardId)
{
	List *shardPlacementList = NIL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;

	Relation pgPlacement = table_open(DistPlacementRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_placement_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	SysScanDesc scanDescriptor = systable_beginscan(pgPlacement,
													DistPlacementShardidIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgPlacement);

		GroupShardPlacement *placement =
			TupleToGroupShardPlacement(tupleDescriptor, heapTuple);

		shardPlacementList = lappend(shardPlacementList, placement);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgPlacement, NoLock);

	return shardPlacementList;
}


/*
 * BuildShardPlacementListForGroup finds shard placements for the given groupId
 * from system catalogs, converts these placements to their in-memory
 * representation, and returns the converted shard placements in a new list.
 */
List *
AllShardPlacementsOnNodeGroup(int32 groupId)
{
	List *shardPlacementList = NIL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;

	Relation pgPlacement = table_open(DistPlacementRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_placement_groupid,
				BTEqualStrategyNumber, F_INT4EQ, Int32GetDatum(groupId));

	SysScanDesc scanDescriptor = systable_beginscan(pgPlacement,
													DistPlacementGroupidIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgPlacement);

		GroupShardPlacement *placement =
			TupleToGroupShardPlacement(tupleDescriptor, heapTuple);

		shardPlacementList = lappend(shardPlacementList, placement);

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	table_close(pgPlacement, NoLock);

	return shardPlacementList;
}


/*
 * TupleToGroupShardPlacement takes in a heap tuple from pg_dist_placement,
 * and converts this tuple to in-memory struct. The function assumes the
 * caller already has locks on the tuple, and doesn't perform any locking.
 */
static GroupShardPlacement *
TupleToGroupShardPlacement(TupleDesc tupleDescriptor, HeapTuple heapTuple)
{
	bool isNullArray[Natts_pg_dist_placement];
	Datum datumArray[Natts_pg_dist_placement];

	if (HeapTupleHeaderGetNatts(heapTuple->t_data) != Natts_pg_dist_placement ||
		HeapTupleHasNulls(heapTuple))
	{
		ereport(ERROR, (errmsg("unexpected null in pg_dist_placement tuple")));
	}

	/*
	 * We use heap_deform_tuple() instead of heap_getattr() to expand tuple
	 * to contain missing values when ALTER TABLE ADD COLUMN happens.
	 */
	heap_deform_tuple(heapTuple, tupleDescriptor, datumArray, isNullArray);

	GroupShardPlacement *shardPlacement = CitusMakeNode(GroupShardPlacement);
	shardPlacement->placementId = DatumGetInt64(
		datumArray[Anum_pg_dist_placement_placementid - 1]);
	shardPlacement->shardId = DatumGetInt64(
		datumArray[Anum_pg_dist_placement_shardid - 1]);
	shardPlacement->shardLength = DatumGetInt64(
		datumArray[Anum_pg_dist_placement_shardlength - 1]);
	shardPlacement->groupId = DatumGetInt32(
		datumArray[Anum_pg_dist_placement_groupid - 1]);

	return shardPlacement;
}


/*
 * LookupTaskPlacementHostAndPort sets the nodename and nodeport for the given task placement
 * with a lookup.
 */
void
LookupTaskPlacementHostAndPort(ShardPlacement *taskPlacement, char **nodeName,
							   int *nodePort)
{
	if (IsDummyPlacement(taskPlacement))
	{
		/*
		 * If we create a dummy placement for the local node, it is possible
		 * that the entry doesn't exist in pg_dist_node, hence a lookup will fail.
		 * In that case we want to use the dummy placements values.
		 */
		*nodeName = taskPlacement->nodeName;
		*nodePort = taskPlacement->nodePort;
	}
	else
	{
		/*
		 * We want to lookup the node information again since it is possible that
		 * there were changes in pg_dist_node and we will get those invalidations
		 * in LookupNodeForGroup.
		 */
		WorkerNode *workerNode = LookupNodeForGroup(taskPlacement->groupId);
		*nodeName = workerNode->workerName;
		*nodePort = workerNode->workerPort;
	}
}


/*
 * IsDummyPlacement returns true if the given placement is a dummy placement.
 */
bool
IsDummyPlacement(ShardPlacement *taskPlacement)
{
	return taskPlacement->nodeId == LOCAL_NODE_ID;
}


/*
 * InsertShardRow opens the shard system catalog, and inserts a new row with the
 * given values into that system catalog. Note that we allow the user to pass in
 * null min/max values in case they are creating an empty shard.
 */
void
InsertShardRow(Oid relationId, uint64 shardId, char storageType,
			   text *shardMinValue, text *shardMaxValue)
{
	Datum values[Natts_pg_dist_shard];
	bool isNulls[Natts_pg_dist_shard];

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_dist_shard_logicalrelid - 1] = ObjectIdGetDatum(relationId);
	values[Anum_pg_dist_shard_shardid - 1] = Int64GetDatum(shardId);
	values[Anum_pg_dist_shard_shardstorage - 1] = CharGetDatum(storageType);

	/* dropped shardalias column must also be set; it is still part of the tuple */
	isNulls[Anum_pg_dist_shard_shardalias_DROPPED - 1] = true;

	/* check if shard min/max values are null */
	if (shardMinValue != NULL && shardMaxValue != NULL)
	{
		values[Anum_pg_dist_shard_shardminvalue - 1] = PointerGetDatum(shardMinValue);
		values[Anum_pg_dist_shard_shardmaxvalue - 1] = PointerGetDatum(shardMaxValue);
	}
	else
	{
		isNulls[Anum_pg_dist_shard_shardminvalue - 1] = true;
		isNulls[Anum_pg_dist_shard_shardmaxvalue - 1] = true;
	}

	/* open shard relation and insert new tuple */
	Relation pgDistShard = table_open(DistShardRelationId(), RowExclusiveLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistShard);
	HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistShard, heapTuple);

	/* invalidate previous cache entry and close relation */
	CitusInvalidateRelcacheByRelid(relationId);

	CommandCounterIncrement();
	table_close(pgDistShard, NoLock);
}


/*
 * InsertShardPlacementRow opens the shard placement system catalog, and inserts
 * a new row with the given values into that system catalog. If placementId is
 * INVALID_PLACEMENT_ID, a new placement id will be assigned.Then, returns the
 * placement id of the added shard placement.
 */
uint64
InsertShardPlacementRow(uint64 shardId, uint64 placementId,
						uint64 shardLength, int32 groupId)
{
	Datum values[Natts_pg_dist_placement];
	bool isNulls[Natts_pg_dist_placement];

	/* form new shard placement tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	if (placementId == INVALID_PLACEMENT_ID)
	{
		placementId = master_get_new_placementid(NULL);
	}
	values[Anum_pg_dist_placement_placementid - 1] = Int64GetDatum(placementId);
	values[Anum_pg_dist_placement_shardid - 1] = Int64GetDatum(shardId);
	values[Anum_pg_dist_placement_shardstate - 1] = Int32GetDatum(1);
	values[Anum_pg_dist_placement_shardlength - 1] = Int64GetDatum(shardLength);
	values[Anum_pg_dist_placement_groupid - 1] = Int32GetDatum(groupId);

	/* open shard placement relation and insert new tuple */
	Relation pgDistPlacement = table_open(DistPlacementRelationId(), RowExclusiveLock);

	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPlacement);
	HeapTuple heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	CatalogTupleInsert(pgDistPlacement, heapTuple);

	CitusInvalidateRelcacheByShardId(shardId);

	CommandCounterIncrement();
	table_close(pgDistPlacement, NoLock);

	return placementId;
}


/*
 * InsertIntoPgDistPartition inserts a new tuple into pg_dist_partition.
 */
void
InsertIntoPgDistPartition(Oid relationId, char distributionMethod,
						  Var *distributionColumn, uint32 colocationId,
						  char replicationModel, bool autoConverted)
{
	char *distributionColumnString = NULL;

	Datum newValues[Natts_pg_dist_partition];
	bool newNulls[Natts_pg_dist_partition];

	/* open system catalog and insert new tuple */
	Relation pgDistPartition = table_open(DistPartitionRelationId(), RowExclusiveLock);

	/* form new tuple for pg_dist_partition */
	memset(newValues, 0, sizeof(newValues));
	memset(newNulls, false, sizeof(newNulls));

	newValues[Anum_pg_dist_partition_logicalrelid - 1] =
		ObjectIdGetDatum(relationId);
	newValues[Anum_pg_dist_partition_partmethod - 1] =
		CharGetDatum(distributionMethod);
	newValues[Anum_pg_dist_partition_colocationid - 1] = UInt32GetDatum(colocationId);
	newValues[Anum_pg_dist_partition_repmodel - 1] = CharGetDatum(replicationModel);
	newValues[Anum_pg_dist_partition_autoconverted - 1] = BoolGetDatum(autoConverted);

	/* set partkey column to NULL for reference tables */
	if (distributionMethod != DISTRIBUTE_BY_NONE)
	{
		distributionColumnString = nodeToString((Node *) distributionColumn);

		newValues[Anum_pg_dist_partition_partkey - 1] =
			CStringGetTextDatum(distributionColumnString);
	}
	else
	{
		newValues[Anum_pg_dist_partition_partkey - 1] = PointerGetDatum(NULL);
		newNulls[Anum_pg_dist_partition_partkey - 1] = true;
	}

	HeapTuple newTuple = heap_form_tuple(RelationGetDescr(pgDistPartition), newValues,
										 newNulls);

	/* finally insert tuple, build index entries & register cache invalidation */
	CatalogTupleInsert(pgDistPartition, newTuple);

	CitusInvalidateRelcacheByRelid(relationId);

	RecordDistributedRelationDependencies(relationId);

	CommandCounterIncrement();
	table_close(pgDistPartition, NoLock);
}


/*
 * RecordDistributedRelationDependencies creates the dependency entries
 * necessary for a distributed relation in addition to the preexisting ones
 * for a normal relation.
 *
 * We create one dependency from the (now distributed) relation to the citus
 * extension to prevent the extension from being dropped while distributed
 * tables exist. Furthermore a dependency from pg_dist_partition's
 * distribution clause to the underlying columns is created, but it's marked
 * as being owned by the relation itself. That means the entire table can be
 * dropped, but the column itself can't. Neither can the type of the
 * distribution column be changed (c.f. ATExecAlterColumnType).
 */
static void
RecordDistributedRelationDependencies(Oid distributedRelationId)
{
	ObjectAddress relationAddr = { 0, 0, 0 };
	ObjectAddress citusExtensionAddr = { 0, 0, 0 };

	relationAddr.classId = RelationRelationId;
	relationAddr.objectId = distributedRelationId;
	relationAddr.objectSubId = 0;

	citusExtensionAddr.classId = ExtensionRelationId;
	citusExtensionAddr.objectId = get_extension_oid("citus", false);
	citusExtensionAddr.objectSubId = 0;

	/* dependency from table entry to extension */
	recordDependencyOn(&relationAddr, &citusExtensionAddr, DEPENDENCY_NORMAL);
}


/*
 * DeletePartitionRow removes the row from pg_dist_partition where the logicalrelid
 * field equals to distributedRelationId. Then, the function invalidates the
 * metadata cache.
 */
void
DeletePartitionRow(Oid distributedRelationId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	Relation pgDistPartition = table_open(DistPartitionRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_logicalrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(distributedRelationId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPartition, InvalidOid, false,
													NULL,
													scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for partition %d",
							   distributedRelationId)));
	}

	simple_heap_delete(pgDistPartition, &heapTuple->t_self);

	systable_endscan(scanDescriptor);

	/* invalidate the cache */
	CitusInvalidateRelcacheByRelid(distributedRelationId);

	/* increment the counter so that next command can see the row */
	CommandCounterIncrement();

	table_close(pgDistPartition, NoLock);
}


/*
 * DeleteShardRow opens the shard system catalog, finds the unique row that has
 * the given shardId, and deletes this row.
 */
void
DeleteShardRow(uint64 shardId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;

	Relation pgDistShard = table_open(DistShardRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_shard_shardid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(shardId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistShard,
													DistShardShardidIndexId(), indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard "
							   UINT64_FORMAT, shardId)));
	}

	Form_pg_dist_shard pgDistShardForm = (Form_pg_dist_shard) GETSTRUCT(heapTuple);
	Oid distributedRelationId = pgDistShardForm->logicalrelid;

	simple_heap_delete(pgDistShard, &heapTuple->t_self);

	systable_endscan(scanDescriptor);

	/* invalidate previous cache entry */
	CitusInvalidateRelcacheByRelid(distributedRelationId);

	CommandCounterIncrement();
	table_close(pgDistShard, NoLock);
}


/*
 * DeleteShardPlacementRow opens the shard placement system catalog, finds the placement
 * with the given placementId, and deletes it.
 */
void
DeleteShardPlacementRow(uint64 placementId)
{
	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	bool indexOK = true;
	bool isNull = false;

	Relation pgDistPlacement = table_open(DistPlacementRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPlacement);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_placement_placementid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(placementId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPlacement,
													DistPlacementPlacementidIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (heapTuple == NULL)
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard placement "
							   INT64_FORMAT, placementId)));
	}

	uint64 shardId = heap_getattr(heapTuple, Anum_pg_dist_placement_shardid,
								  tupleDescriptor, &isNull);
	if (HeapTupleHeaderGetNatts(heapTuple->t_data) != Natts_pg_dist_placement ||
		HeapTupleHasNulls(heapTuple))
	{
		ereport(ERROR, (errmsg("unexpected null in pg_dist_placement tuple")));
	}

	simple_heap_delete(pgDistPlacement, &heapTuple->t_self);
	systable_endscan(scanDescriptor);

	CitusInvalidateRelcacheByShardId(shardId);

	CommandCounterIncrement();
	table_close(pgDistPlacement, NoLock);
}


/*
 * UpdatePlacementGroupId sets the groupId for the placement identified
 * by placementId.
 */
void
UpdatePlacementGroupId(uint64 placementId, int groupId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	Datum values[Natts_pg_dist_placement];
	bool isnull[Natts_pg_dist_placement];
	bool replace[Natts_pg_dist_placement];
	bool colIsNull = false;

	Relation pgDistPlacement = table_open(DistPlacementRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPlacement);
	ScanKeyInit(&scanKey[0], Anum_pg_dist_placement_placementid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(placementId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPlacement,
													DistPlacementPlacementidIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for shard placement "
							   UINT64_FORMAT,
							   placementId)));
	}

	memset(replace, 0, sizeof(replace));

	values[Anum_pg_dist_placement_groupid - 1] = Int32GetDatum(groupId);
	isnull[Anum_pg_dist_placement_groupid - 1] = false;
	replace[Anum_pg_dist_placement_groupid - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);

	CatalogTupleUpdate(pgDistPlacement, &heapTuple->t_self, heapTuple);

	uint64 shardId = DatumGetInt64(heap_getattr(heapTuple,
												Anum_pg_dist_placement_shardid,
												tupleDescriptor, &colIsNull));
	Assert(!colIsNull);
	CitusInvalidateRelcacheByShardId(shardId);

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	table_close(pgDistPlacement, NoLock);
}


/*
 * UpdatePgDistPartitionAutoConverted sets the autoConverted for the partition identified
 * by citusTableId.
 */
void
UpdatePgDistPartitionAutoConverted(Oid citusTableId, bool autoConverted)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	Datum values[Natts_pg_dist_partition];
	bool isnull[Natts_pg_dist_partition];
	bool replace[Natts_pg_dist_partition];

	Relation pgDistPartition = table_open(DistPartitionRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);
	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_logicalrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(citusTableId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPartition,
													DistPartitionLogicalRelidIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for citus table with oid: %u",
							   citusTableId)));
	}

	memset(replace, 0, sizeof(replace));

	values[Anum_pg_dist_partition_autoconverted - 1] = BoolGetDatum(autoConverted);
	isnull[Anum_pg_dist_partition_autoconverted - 1] = false;
	replace[Anum_pg_dist_partition_autoconverted - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);

	CatalogTupleUpdate(pgDistPartition, &heapTuple->t_self, heapTuple);

	CitusInvalidateRelcacheByRelid(citusTableId);

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	table_close(pgDistPartition, NoLock);
}


/*
 * UpdateDistributionColumnGlobally sets the distribution column and colocation ID
 * for a table in pg_dist_partition on all nodes
 */
void
UpdateDistributionColumnGlobally(Oid relationId, char distributionMethod,
								 Var *distributionColumn, int colocationId)
{
	UpdateDistributionColumn(relationId, distributionMethod, distributionColumn,
							 colocationId);

	if (ShouldSyncTableMetadata(relationId))
	{
		/* we use delete+insert because syncing uses specialized RPCs */
		char *deleteMetadataCommand = DistributionDeleteMetadataCommand(relationId);
		SendCommandToWorkersWithMetadata(deleteMetadataCommand);

		/* pick up the new metadata (updated above) */
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
		char *insertMetadataCommand = DistributionCreateCommand(cacheEntry);
		SendCommandToWorkersWithMetadata(insertMetadataCommand);
	}
}


/*
 * UpdateDistributionColumn sets the distribution column and colocation ID for a table
 * in pg_dist_partition.
 */
void
UpdateDistributionColumn(Oid relationId, char distributionMethod, Var *distributionColumn,
						 int colocationId)
{
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	Datum values[Natts_pg_dist_partition];
	bool isnull[Natts_pg_dist_partition];
	bool replace[Natts_pg_dist_partition];

	Relation pgDistPartition = table_open(DistPartitionRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);
	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_logicalrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relationId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPartition,
													DistPartitionLogicalRelidIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for citus table with oid: %u",
							   relationId)));
	}

	memset(replace, 0, sizeof(replace));

	replace[Anum_pg_dist_partition_partmethod - 1] = true;
	values[Anum_pg_dist_partition_partmethod - 1] = CharGetDatum(distributionMethod);
	isnull[Anum_pg_dist_partition_partmethod - 1] = false;

	replace[Anum_pg_dist_partition_colocationid - 1] = true;
	values[Anum_pg_dist_partition_colocationid - 1] = UInt32GetDatum(colocationId);
	isnull[Anum_pg_dist_partition_colocationid - 1] = false;

	replace[Anum_pg_dist_partition_autoconverted - 1] = true;
	values[Anum_pg_dist_partition_autoconverted - 1] = BoolGetDatum(false);
	isnull[Anum_pg_dist_partition_autoconverted - 1] = false;

	char *distributionColumnString = nodeToString((Node *) distributionColumn);

	replace[Anum_pg_dist_partition_partkey - 1] = true;
	values[Anum_pg_dist_partition_partkey - 1] =
		CStringGetTextDatum(distributionColumnString);
	isnull[Anum_pg_dist_partition_partkey - 1] = false;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);

	CatalogTupleUpdate(pgDistPartition, &heapTuple->t_self, heapTuple);

	CitusInvalidateRelcacheByRelid(relationId);
	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	table_close(pgDistPartition, NoLock);
}


/*
 * UpdateNoneDistTableMetadataGlobally globally updates pg_dist_partition for
 * given none-distributed table.
 */
void
UpdateNoneDistTableMetadataGlobally(Oid relationId, char replicationModel,
									uint32 colocationId, bool autoConverted)
{
	UpdateNoneDistTableMetadata(relationId, replicationModel,
								colocationId, autoConverted);

	if (ShouldSyncTableMetadata(relationId))
	{
		char *metadataCommand =
			UpdateNoneDistTableMetadataCommand(relationId,
											   replicationModel,
											   colocationId,
											   autoConverted);
		SendCommandToWorkersWithMetadata(metadataCommand);
	}
}


/*
 * UpdateNoneDistTableMetadata locally updates pg_dist_partition for given
 * none-distributed table.
 */
void
UpdateNoneDistTableMetadata(Oid relationId, char replicationModel, uint32 colocationId,
							bool autoConverted)
{
	if (HasDistributionKey(relationId))
	{
		ereport(ERROR, (errmsg("cannot update metadata for a distributed "
							   "table that has a distribution column")));
	}

	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	bool indexOK = true;
	Datum values[Natts_pg_dist_partition];
	bool isnull[Natts_pg_dist_partition];
	bool replace[Natts_pg_dist_partition];

	Relation pgDistPartition = table_open(DistPartitionRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistPartition);
	ScanKeyInit(&scanKey[0], Anum_pg_dist_partition_logicalrelid,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(relationId));

	SysScanDesc scanDescriptor = systable_beginscan(pgDistPartition,
													DistPartitionLogicalRelidIndexId(),
													indexOK,
													NULL, scanKeyCount, scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find valid entry for Citus table with oid: %u",
							   relationId)));
	}

	memset(replace, 0, sizeof(replace));

	values[Anum_pg_dist_partition_colocationid - 1] = UInt32GetDatum(colocationId);
	isnull[Anum_pg_dist_partition_colocationid - 1] = false;
	replace[Anum_pg_dist_partition_colocationid - 1] = true;

	values[Anum_pg_dist_partition_repmodel - 1] = CharGetDatum(replicationModel);
	isnull[Anum_pg_dist_partition_repmodel - 1] = false;
	replace[Anum_pg_dist_partition_repmodel - 1] = true;

	values[Anum_pg_dist_partition_autoconverted - 1] = BoolGetDatum(autoConverted);
	isnull[Anum_pg_dist_partition_autoconverted - 1] = false;
	replace[Anum_pg_dist_partition_autoconverted - 1] = true;

	heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull, replace);

	CatalogTupleUpdate(pgDistPartition, &heapTuple->t_self, heapTuple);

	CitusInvalidateRelcacheByRelid(relationId);
	CommandCounterIncrement();

	systable_endscan(scanDescriptor);
	table_close(pgDistPartition, NoLock);
}


/*
 * Check that the current user has `mode` permissions on relationId, error out
 * if not. Superusers always have such permissions.
 */
void
EnsureTablePermissions(Oid relationId, AclMode mode)
{
	AclResult aclresult = pg_class_aclcheck(relationId, GetUserId(), mode);

	if (aclresult != ACLCHECK_OK)
	{
		aclcheck_error(aclresult, OBJECT_TABLE, get_rel_name(relationId));
	}
}


/*
 * Check that the current user has owner rights to relationId, error out if
 * not. Superusers are regarded as owners.
 */
void
EnsureTableOwner(Oid relationId)
{
	if (!object_ownercheck(RelationRelationId, relationId, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relationId));
	}
}


/*
 * Check that the current user has owner rights to schemaId, error out if
 * not. Superusers are regarded as owners.
 */
void
EnsureSchemaOwner(Oid schemaId)
{
	if (!object_ownercheck(NamespaceRelationId, schemaId, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_SCHEMA,
					   get_namespace_name(schemaId));
	}
}


/*
 * Check that the current user has owner rights to functionId, error out if
 * not. Superusers are regarded as owners. Functions and procedures are
 * treated equally.
 */
void
EnsureFunctionOwner(Oid functionId)
{
	if (!object_ownercheck(ProcedureRelationId, functionId, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_FUNCTION,
					   get_func_name(functionId));
	}
}


/*
 * EnsureHashDistributedTable error out if the given relation is not a hash distributed table
 * with the given message.
 */
void
EnsureHashDistributedTable(Oid relationId)
{
	if (!IsCitusTableType(relationId, HASH_DISTRIBUTED))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("relation %s should be a "
							   "hash distributed table", get_rel_name(relationId))));
	}
}


/*
 * EnsureHashOrSingleShardDistributedTable error out if the given relation is not a
 * hash or single shard distributed table with the given message.
 */
void
EnsureHashOrSingleShardDistributedTable(Oid relationId)
{
	if (!IsCitusTableType(relationId, HASH_DISTRIBUTED) &&
		!IsCitusTableType(relationId, SINGLE_SHARD_DISTRIBUTED))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("relation %s should be a "
							   "hash or single shard distributed table",
							   get_rel_name(relationId))));
	}
}


/*
 * EnsureSuperUser check that the current user is a superuser and errors out if not.
 */
void
EnsureSuperUser(void)
{
	if (!superuser())
	{
		ereport(ERROR, (errmsg("operation is not allowed"),
						errhint("Run the command with a superuser.")));
	}
}


Oid
TableOwnerOid(Oid relationId)
{
	HeapTuple tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));
	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_TABLE),
						errmsg("relation with OID %u does not exist", relationId)));
	}

	Oid userId = ((Form_pg_class) GETSTRUCT(tuple))->relowner;

	ReleaseSysCache(tuple);
	return userId;
}


/*
 * Return a table's owner as a string.
 */
char *
TableOwner(Oid relationId)
{
	return GetUserNameFromId(TableOwnerOid(relationId), false);
}


/*
 * IsForeignTable takes a relation id and returns true if it's a foreign table.
 * Returns false otherwise.
 */
bool
IsForeignTable(Oid relationId)
{
	return get_rel_relkind(relationId) == RELKIND_FOREIGN_TABLE;
}


/*
 * HasRunnableBackgroundTask looks in the catalog if there are any tasks that can be run.
 * For a task to be able to run the following conditions apply:
 *  - Task is in Running state. This could happen when a Background Tasks Queue Monitor
 *    had crashed or is otherwise restarted. To recover from such a failure tasks in
 *    Running state are deemed Runnable.
 *  - Task is in Runnable state with either _no_ value set in not_before, or a value that
 *    has currently passed. If the not_before field is set to a time in the future the
 *    task is currently not ready to be started.
 */
bool
HasRunnableBackgroundTask()
{
	Relation pgDistBackgroundTasks =
		table_open(DistBackgroundTaskRelationId(), AccessShareLock);

	/* find any job in states listed here */
	BackgroundTaskStatus taskStatus[] = {
		BACKGROUND_TASK_STATUS_RUNNABLE,
		BACKGROUND_TASK_STATUS_RUNNING
	};

	bool hasScheduledTask = false;
	for (int i = 0; !hasScheduledTask && i < lengthof(taskStatus); i++)
	{
		const int scanKeyCount = 1;
		ScanKeyData scanKey[1] = { 0 };
		const bool indexOK = true;

		/* pg_dist_background_task.status == taskStatus[i] */
		ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_status,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(BackgroundTaskStatusOid(taskStatus[i])));

		SysScanDesc scanDescriptor =
			systable_beginscan(pgDistBackgroundTasks,
							   DistBackgroundTaskStatusTaskIdIndexId(),
							   indexOK, NULL, scanKeyCount,
							   scanKey);

		HeapTuple taskTuple = NULL;
		while (HeapTupleIsValid(taskTuple = systable_getnext(scanDescriptor)))
		{
			TupleDesc tupleDescriptor = RelationGetDescr(pgDistBackgroundTasks);
			BackgroundTask *task = DeformBackgroundTaskHeapTuple(tupleDescriptor,
																 taskTuple);

			if (task->not_before && *(task->not_before) > GetCurrentTimestamp())
			{
				continue;
			}

			hasScheduledTask = true;
			break;
		}

		systable_endscan(scanDescriptor);
	}

	table_close(pgDistBackgroundTasks, NoLock);

	return hasScheduledTask;
}


/*
 * BackgroundJobStatusByOid returns the C enum representation of a BackgroundJobsStatus
 * based on the Oid of the SQL enum value.
 */
BackgroundJobStatus
BackgroundJobStatusByOid(Oid enumOid)
{
	if (enumOid == CitusJobStatusScheduledId())
	{
		return BACKGROUND_JOB_STATUS_SCHEDULED;
	}
	else if (enumOid == CitusJobStatusRunningId())
	{
		return BACKGROUND_JOB_STATUS_RUNNING;
	}
	else if (enumOid == CitusJobStatusFinishedId())
	{
		return BACKGROUND_JOB_STATUS_FINISHED;
	}
	else if (enumOid == CitusJobStatusCancelledId())
	{
		return BACKGROUND_JOB_STATUS_CANCELLED;
	}
	else if (enumOid == CitusJobStatusFailingId())
	{
		return BACKGROUND_JOB_STATUS_FAILING;
	}
	else if (enumOid == CitusJobStatusFailedId())
	{
		return BACKGROUND_JOB_STATUS_FAILED;
	}
	else if (enumOid == CitusJobStatusCancellingId())
	{
		return BACKGROUND_JOB_STATUS_CANCELLING;
	}
	elog(ERROR, "unknown enum value for citus_job_status");
}


/*
 * BackgroundTaskStatusByOid returns the C enum representation of a BackgroundTaskStatus
 * based on the Oid of the SQL enum value.
 */
BackgroundTaskStatus
BackgroundTaskStatusByOid(Oid enumOid)
{
	if (enumOid == CitusTaskStatusDoneId())
	{
		return BACKGROUND_TASK_STATUS_DONE;
	}
	else if (enumOid == CitusTaskStatusRunnableId())
	{
		return BACKGROUND_TASK_STATUS_RUNNABLE;
	}
	else if (enumOid == CitusTaskStatusRunningId())
	{
		return BACKGROUND_TASK_STATUS_RUNNING;
	}
	else if (enumOid == CitusTaskStatusErrorId())
	{
		return BACKGROUND_TASK_STATUS_ERROR;
	}
	else if (enumOid == CitusTaskStatusUnscheduledId())
	{
		return BACKGROUND_TASK_STATUS_UNSCHEDULED;
	}
	else if (enumOid == CitusTaskStatusBlockedId())
	{
		return BACKGROUND_TASK_STATUS_BLOCKED;
	}
	else if (enumOid == CitusTaskStatusCancelledId())
	{
		return BACKGROUND_TASK_STATUS_CANCELLED;
	}
	else if (enumOid == CitusTaskStatusCancellingId())
	{
		return BACKGROUND_TASK_STATUS_CANCELLING;
	}
	ereport(ERROR, (errmsg("unknown enum value for citus_task_status")));
}


/*
 * IsBackgroundJobStatusTerminal is a predicate returning if the BackgroundJobStatus
 * passed is a terminal state of the Background Job state machine.
 *
 * For a Job to be in it's terminal state, all tasks from that job should also be in their
 * terminal state.
 */
bool
IsBackgroundJobStatusTerminal(BackgroundJobStatus status)
{
	switch (status)
	{
		case BACKGROUND_JOB_STATUS_CANCELLED:
		case BACKGROUND_JOB_STATUS_FAILED:
		case BACKGROUND_JOB_STATUS_FINISHED:
		{
			return true;
		}

		case BACKGROUND_JOB_STATUS_CANCELLING:
		case BACKGROUND_JOB_STATUS_FAILING:
		case BACKGROUND_JOB_STATUS_RUNNING:
		case BACKGROUND_JOB_STATUS_SCHEDULED:
		{
			return false;
		}

			/* no default to make sure we explicitly add every state here */
	}
	elog(ERROR, "unknown BackgroundJobStatus");
}


/*
 * IsBackgroundTaskStatusTerminal is a predicate returning if the BackgroundTaskStatus
 * passed is a terminal state of the Background Task state machine.
 */
bool
IsBackgroundTaskStatusTerminal(BackgroundTaskStatus status)
{
	switch (status)
	{
		case BACKGROUND_TASK_STATUS_CANCELLED:
		case BACKGROUND_TASK_STATUS_DONE:
		case BACKGROUND_TASK_STATUS_ERROR:
		case BACKGROUND_TASK_STATUS_UNSCHEDULED:
		{
			return true;
		}

		case BACKGROUND_TASK_STATUS_BLOCKED:
		case BACKGROUND_TASK_STATUS_CANCELLING:
		case BACKGROUND_TASK_STATUS_RUNNABLE:
		case BACKGROUND_TASK_STATUS_RUNNING:
		{
			return false;
		}

			/* no default to make sure we explicitly add every state here */
	}
	elog(ERROR, "unknown BackgroundTaskStatus");
}


/*
 * BackgroundJobStatusOid returns the Oid corresponding to SQL enum value corresponding to
 * the BackgroundJobStatus.
 */
Oid
BackgroundJobStatusOid(BackgroundJobStatus status)
{
	switch (status)
	{
		case BACKGROUND_JOB_STATUS_SCHEDULED:
		{
			return CitusJobStatusScheduledId();
		}

		case BACKGROUND_JOB_STATUS_RUNNING:
		{
			return CitusJobStatusRunningId();
		}

		case BACKGROUND_JOB_STATUS_CANCELLING:
		{
			return CitusJobStatusCancellingId();
		}

		case BACKGROUND_JOB_STATUS_FINISHED:
		{
			return CitusJobStatusFinishedId();
		}

		case BACKGROUND_JOB_STATUS_CANCELLED:
		{
			return CitusJobStatusCancelledId();
		}

		case BACKGROUND_JOB_STATUS_FAILING:
		{
			return CitusJobStatusFailingId();
		}

		case BACKGROUND_JOB_STATUS_FAILED:
		{
			return CitusJobStatusFailedId();
		}
	}

	elog(ERROR, "unknown BackgroundJobStatus");
}


/*
 * BackgroundTaskStatusOid returns the Oid corresponding to SQL enum value corresponding to
 * the BackgroundTaskStatus.
 */
Oid
BackgroundTaskStatusOid(BackgroundTaskStatus status)
{
	switch (status)
	{
		case BACKGROUND_TASK_STATUS_BLOCKED:
		{
			return CitusTaskStatusBlockedId();
		}

		case BACKGROUND_TASK_STATUS_RUNNABLE:
		{
			return CitusTaskStatusRunnableId();
		}

		case BACKGROUND_TASK_STATUS_RUNNING:
		{
			return CitusTaskStatusRunningId();
		}

		case BACKGROUND_TASK_STATUS_DONE:
		{
			return CitusTaskStatusDoneId();
		}

		case BACKGROUND_TASK_STATUS_ERROR:
		{
			return CitusTaskStatusErrorId();
		}

		case BACKGROUND_TASK_STATUS_UNSCHEDULED:
		{
			return CitusTaskStatusUnscheduledId();
		}

		case BACKGROUND_TASK_STATUS_CANCELLED:
		{
			return CitusTaskStatusCancelledId();
		}

		case BACKGROUND_TASK_STATUS_CANCELLING:
		{
			return CitusTaskStatusCancellingId();
		}
	}

	elog(ERROR, "unknown BackgroundTaskStatus");
	return InvalidOid;
}


/*
 * GetNextBackgroundJobsJobId reads and increments the SQL sequence associated with the
 * background job's job_id. After incrementing the counter it returns the counter back to
 * the caller.
 *
 * The return value is typically used to insert new jobs into the catalog.
 */
static int64
GetNextBackgroundJobsJobId(void)
{
	return DatumGetInt64(nextval_internal(DistBackgroundJobJobIdSequenceId(), false));
}


/*
 * GetNextBackgroundTaskTaskId reads and increments the SQL sequence associated with the
 * background job tasks' task_id. After incrementing the counter it returns the counter
 * back to the caller.
 *
 * The return value is typically used to insert new tasks into the catalog.
 */
static int64
GetNextBackgroundTaskTaskId(void)
{
	return DatumGetInt64(nextval_internal(DistBackgroundTaskTaskIdSequenceId(), false));
}


/*
 * HasNonTerminalJobOfType returns true if there is a job of a given type that is not in
 * its terminal state.
 *
 * Some jobs would want a single instance to be able to run at once. Before submitting a
 * new job if could see if there is a job of their type already executing.
 *
 * If a job is found the options jobIdOut is populated with the jobId.
 */
bool
HasNonTerminalJobOfType(const char *jobType, int64 *jobIdOut)
{
	Relation pgDistBackgroundJob =
		table_open(DistBackgroundJobRelationId(), AccessShareLock);

	/* find any job in states listed here */
	BackgroundJobStatus jobStatus[] = {
		BACKGROUND_JOB_STATUS_RUNNING,
		BACKGROUND_JOB_STATUS_CANCELLING,
		BACKGROUND_JOB_STATUS_FAILING,
		BACKGROUND_JOB_STATUS_SCHEDULED
	};

	NameData jobTypeName = { 0 };
	namestrcpy(&jobTypeName, jobType);

	bool foundJob = false;
	for (int i = 0; !foundJob && i < lengthof(jobStatus); i++)
	{
		ScanKeyData scanKey[2] = { 0 };
		const bool indexOK = true;

		/* pg_dist_background_job.status == jobStatus[i] */
		ScanKeyInit(&scanKey[0], Anum_pg_dist_background_job_state,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(BackgroundJobStatusOid(jobStatus[i])));

		/* pg_dist_background_job.job_type == jobType */
		ScanKeyInit(&scanKey[1], Anum_pg_dist_background_job_job_type,
					BTEqualStrategyNumber, F_NAMEEQ,
					NameGetDatum(&jobTypeName));

		SysScanDesc scanDescriptor =
			systable_beginscan(pgDistBackgroundJob,
							   InvalidOid,     /* TODO use an actual index here */
							   indexOK, NULL, lengthof(scanKey), scanKey);

		HeapTuple taskTuple = NULL;
		if (HeapTupleIsValid(taskTuple = systable_getnext(scanDescriptor)))
		{
			foundJob = true;

			if (jobIdOut)
			{
				Datum values[Natts_pg_dist_background_job] = { 0 };
				bool isnull[Natts_pg_dist_background_job] = { 0 };

				TupleDesc tupleDesc = RelationGetDescr(pgDistBackgroundJob);
				heap_deform_tuple(taskTuple, tupleDesc, values, isnull);

				*jobIdOut = DatumGetInt64(values[Anum_pg_dist_background_job_job_id - 1]);
			}
		}

		systable_endscan(scanDescriptor);
	}

	table_close(pgDistBackgroundJob, NoLock);

	return foundJob;
}


/*
 * CreateBackgroundJob is a helper function to insert a new Background Job into Citus'
 * catalog. After inserting the new job's metadataa into the catalog it returns the job_id
 * assigned to the new job. This is typically used to associate new tasks with the newly
 * created job.
 */
int64
CreateBackgroundJob(const char *jobType, const char *description)
{
	Relation pgDistBackgroundJobs =
		table_open(DistBackgroundJobRelationId(), RowExclusiveLock);

	/* insert new job */
	Datum values[Natts_pg_dist_background_job] = { 0 };
	bool isnull[Natts_pg_dist_background_job] = { 0 };
	memset(isnull, true, sizeof(isnull));

	int64 jobId = GetNextBackgroundJobsJobId();

	InitFieldValue(Anum_pg_dist_background_job_job_id, values, isnull,
				   Int64GetDatum(jobId));

	InitFieldValue(Anum_pg_dist_background_job_state, values, isnull,
				   ObjectIdGetDatum(CitusJobStatusScheduledId()));

	if (jobType)
	{
		NameData jobTypeName = { 0 };
		namestrcpy(&jobTypeName, jobType);
		InitFieldValue(Anum_pg_dist_background_job_job_type, values, isnull,
					   NameGetDatum(&jobTypeName));
	}

	if (description)
	{
		InitFieldText(Anum_pg_dist_background_job_description, values, isnull,
					  description);
	}

	HeapTuple newTuple = heap_form_tuple(RelationGetDescr(pgDistBackgroundJobs),
										 values, isnull);
	CatalogTupleInsert(pgDistBackgroundJobs, newTuple);

	CommandCounterIncrement();

	table_close(pgDistBackgroundJobs, NoLock);

	return jobId;
}


/*
 * ScheduleBackgroundTask creates a new background task to be executed in the background.
 *
 * The new task is associated with an existing job based on it's id.
 *
 * Optionally the new task can depend on separate tasks associated with the same job. When
 * a new task is created with dependencies on previous tasks we assume this task is
 * blocked on its depending tasks.
 */
BackgroundTask *
ScheduleBackgroundTask(int64 jobId, Oid owner, char *command, int dependingTaskCount,
					   int64 dependingTaskIds[], int nodesInvolvedCount, int32
					   nodesInvolved[])
{
	BackgroundTask *task = NULL;

	Relation pgDistBackgroundJob =
		table_open(DistBackgroundJobRelationId(), ExclusiveLock);
	Relation pgDistBackgroundTask =
		table_open(DistBackgroundTaskRelationId(), ExclusiveLock);
	Relation pgDistbackgroundTasksDepend = NULL;
	if (dependingTaskCount > 0)
	{
		pgDistbackgroundTasksDepend =
			table_open(DistBackgroundTaskDependRelationId(), ExclusiveLock);
	}

	/* 1. verify job exist */
	{
		ScanKeyData scanKey[1] = { 0 };
		bool indexOK = true;

		/* pg_dist_background_job.job_id == $jobId */
		ScanKeyInit(&scanKey[0], Anum_pg_dist_background_job_job_id,
					BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(jobId));

		SysScanDesc scanDescriptor =
			systable_beginscan(pgDistBackgroundJob,
							   DistBackgroundJobPKeyIndexId(),
							   indexOK, NULL, lengthof(scanKey), scanKey);

		HeapTuple jobTuple = systable_getnext(scanDescriptor);
		if (!HeapTupleIsValid(jobTuple))
		{
			ereport(ERROR, (errmsg("job for newly created task does not exist.")));
		}

		systable_endscan(scanDescriptor);
	}

	/* 2. insert new task */
	{
		Datum values[Natts_pg_dist_background_task] = { 0 };
		bool nulls[Natts_pg_dist_background_task] = { 0 };

		memset(nulls, true, sizeof(nulls));

		int64 taskId = GetNextBackgroundTaskTaskId();

		values[Anum_pg_dist_background_task_job_id - 1] = Int64GetDatum(jobId);
		nulls[Anum_pg_dist_background_task_job_id - 1] = false;

		values[Anum_pg_dist_background_task_task_id - 1] = Int64GetDatum(taskId);
		nulls[Anum_pg_dist_background_task_task_id - 1] = false;

		values[Anum_pg_dist_background_task_owner - 1] = ObjectIdGetDatum(owner);
		nulls[Anum_pg_dist_background_task_owner - 1] = false;

		Oid statusOid = InvalidOid;
		if (dependingTaskCount == 0)
		{
			statusOid = CitusTaskStatusRunnableId();
		}
		else
		{
			statusOid = CitusTaskStatusBlockedId();
		}
		values[Anum_pg_dist_background_task_status - 1] = ObjectIdGetDatum(statusOid);
		nulls[Anum_pg_dist_background_task_status - 1] = false;

		values[Anum_pg_dist_background_task_command - 1] = CStringGetTextDatum(command);
		nulls[Anum_pg_dist_background_task_command - 1] = false;

		values[Anum_pg_dist_background_task_message - 1] = CStringGetTextDatum("");
		nulls[Anum_pg_dist_background_task_message - 1] = false;

		values[Anum_pg_dist_background_task_nodes_involved - 1] =
			IntArrayToDatum(nodesInvolvedCount, nodesInvolved);
		nulls[Anum_pg_dist_background_task_nodes_involved - 1] = (nodesInvolvedCount ==
																  0);

		HeapTuple newTuple = heap_form_tuple(RelationGetDescr(pgDistBackgroundTask),
											 values, nulls);
		CatalogTupleInsert(pgDistBackgroundTask, newTuple);

		task = palloc0(sizeof(BackgroundTask));
		task->taskid = taskId;
		task->status = BACKGROUND_TASK_STATUS_RUNNABLE;
		task->command = pstrdup(command);
	}

	/* 3. insert dependencies into catalog */
	{
		for (int i = 0; i < dependingTaskCount; i++)
		{
			/* 3.1 after verifying the task exists for this job */
			{
				ScanKeyData scanKey[2] = { 0 };
				bool indexOK = true;

				/* pg_dist_background_task.job_id == $jobId */
				ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_job_id,
							BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(jobId));

				/* pg_dist_background_task.task_id == $taskId */
				ScanKeyInit(&scanKey[1], Anum_pg_dist_background_task_task_id,
							BTEqualStrategyNumber, F_INT8EQ,
							Int64GetDatum(dependingTaskIds[i]));

				SysScanDesc scanDescriptor =
					systable_beginscan(pgDistBackgroundTask,
									   DistBackgroundTaskJobIdTaskIdIndexId(),
									   indexOK, NULL, lengthof(scanKey), scanKey);

				HeapTuple taskTuple = systable_getnext(scanDescriptor);
				if (!HeapTupleIsValid(taskTuple))
				{
					ereport(ERROR, (errmsg("depending task for newly scheduled task does "
										   "not exist")));
				}

				systable_endscan(scanDescriptor);
			}

			Assert(pgDistbackgroundTasksDepend != NULL);

			Datum values[Natts_pg_dist_background_task_depend] = { 0 };
			bool nulls[Natts_pg_dist_background_task_depend] = { 0 };
			memset(nulls, true, sizeof(nulls));

			values[Anum_pg_dist_background_task_depend_job_id - 1] =
				Int64GetDatum(jobId);
			nulls[Anum_pg_dist_background_task_depend_job_id - 1] = false;

			values[Anum_pg_dist_background_task_depend_task_id - 1] =
				Int64GetDatum(task->taskid);
			nulls[Anum_pg_dist_background_task_depend_task_id - 1] = false;

			values[Anum_pg_dist_background_task_depend_depends_on - 1] =
				Int64GetDatum(dependingTaskIds[i]);
			nulls[Anum_pg_dist_background_task_depend_depends_on - 1] = false;

			HeapTuple newTuple = heap_form_tuple(
				RelationGetDescr(pgDistbackgroundTasksDepend), values, nulls);
			CatalogTupleInsert(pgDistbackgroundTasksDepend, newTuple);
		}
	}

	if (pgDistbackgroundTasksDepend)
	{
		table_close(pgDistbackgroundTasksDepend, NoLock);
	}
	table_close(pgDistBackgroundTask, NoLock);
	table_close(pgDistBackgroundJob, NoLock);

	CommandCounterIncrement();

	return task;
}


/*
 * ResetRunningBackgroundTasks finds all tasks currently in Running state and resets their
 * state back to runnable.
 *
 * While marking running tasks as runnable we check if the task might still be locked and
 * the pid is managed by our current postmaster. If both are the case we terminate the
 * backend. This will make sure that if a task was still running after a monitor crash or
 * restart we stop the executor before we start a new one.
 *
 * Any pid associated with the running tasks will be cleared back to the NULL value.
 */
void
ResetRunningBackgroundTasks(void)
{
	const int scanKeyCount = 1;
	ScanKeyData scanKey[1];
	const bool indexOK = true;

	Relation pgDistBackgroundTasks =
		table_open(DistBackgroundTaskRelationId(), ExclusiveLock);

	/* pg_dist_background_task.status == 'running' */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_status,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(CitusTaskStatusRunningId()));

	SysScanDesc scanDescriptor =
		systable_beginscan(pgDistBackgroundTasks,
						   DistBackgroundTaskStatusTaskIdIndexId(),
						   indexOK, NULL, scanKeyCount,
						   scanKey);

	HeapTuple taskTuple = NULL;
	List *taskIdsToWait = NIL;
	while (HeapTupleIsValid(taskTuple = systable_getnext(scanDescriptor)))
	{
		Datum values[Natts_pg_dist_background_task] = { 0 };
		bool isnull[Natts_pg_dist_background_task] = { 0 };
		bool replace[Natts_pg_dist_background_task] = { 0 };

		TupleDesc tupleDescriptor = RelationGetDescr(pgDistBackgroundTasks);
		heap_deform_tuple(taskTuple, tupleDescriptor, values, isnull);

		values[Anum_pg_dist_background_task_status - 1] =
			ObjectIdGetDatum(CitusTaskStatusRunnableId());
		isnull[Anum_pg_dist_background_task_status - 1] = false;
		replace[Anum_pg_dist_background_task_status - 1] = true;

		/* if there is a pid we need to signal the backend to stop */
		if (!isnull[Anum_pg_dist_background_task_pid - 1])
		{
			/*
			 * Before signalling the pid we check if the task lock is held, otherwise we
			 * might cancel an arbitrary postgres backend
			 */

			int64 taskId =
				DatumGetInt64(values[Anum_pg_dist_background_task_task_id - 1]);

			/* No need to release lock, will get unlocked once our changes commit */
			LOCKTAG locktag = { 0 };
			SET_LOCKTAG_BACKGROUND_TASK(locktag, taskId);
			const bool sessionLock = false;
			const bool dontWait = true;
			LockAcquireResult locked = LockAcquire(&locktag, AccessExclusiveLock,
												   sessionLock, dontWait);
			if (locked == LOCKACQUIRE_NOT_AVAIL)
			{
				/*
				 * There is still an executor holding the lock, needs a SIGTERM.
				 */
				Datum pidDatum = values[Anum_pg_dist_background_task_pid - 1];
				const Datum timeoutDatum = Int64GetDatum(0);
				Datum signalSuccessDatum = DirectFunctionCall2(pg_terminate_backend,
															   pidDatum, timeoutDatum);
				bool signalSuccess = DatumGetBool(signalSuccessDatum);
				if (!signalSuccess)
				{
					/*
					 * We run this backend as superuser, any failure will probably cause
					 * long delays waiting on the task lock before we can commit.
					 */
					ereport(WARNING,
							(errmsg("could not send signal to process %d: %m",
									DatumGetInt32(pidDatum)),
							 errdetail("failing to signal an old executor could cause "
									   "delays starting the background task monitor")));
				}

				/*
				 * Since we didn't already acquire the lock here we need to wait on this
				 * lock before committing the change to the catalog. However, we first
				 * want to signal all backends before waiting on the lock, hence we keep a
				 * list for later
				 */
				int64 *taskIdTarget = palloc0(sizeof(int64));
				*taskIdTarget = taskId;
				taskIdsToWait = lappend(taskIdsToWait, taskIdTarget);
			}
		}

		values[Anum_pg_dist_background_task_pid - 1] = InvalidOid;
		isnull[Anum_pg_dist_background_task_pid - 1] = true;
		replace[Anum_pg_dist_background_task_pid - 1] = true;

		taskTuple = heap_modify_tuple(taskTuple, tupleDescriptor, values, isnull,
									  replace);

		CatalogTupleUpdate(pgDistBackgroundTasks, &taskTuple->t_self, taskTuple);
	}

	if (list_length(taskIdsToWait) > 0)
	{
		ereport(LOG, (errmsg("waiting till all tasks release their lock before "
							 "continuing with the background task monitor")));

		/* there are tasks that need to release their lock before we can continue */
		int64 *taskId = NULL;
		foreach_ptr(taskId, taskIdsToWait)
		{
			LOCKTAG locktag = { 0 };
			SET_LOCKTAG_BACKGROUND_TASK(locktag, *taskId);
			const bool sessionLock = false;
			const bool dontWait = false;
			(void) LockAcquire(&locktag, AccessExclusiveLock, sessionLock, dontWait);
		}
	}

	CommandCounterIncrement();

	systable_endscan(scanDescriptor);

	table_close(pgDistBackgroundTasks, NoLock);
}


/*
 * DeformBackgroundJobHeapTuple pareses a HeapTuple from pg_dist_background_job into its
 * inmemory representation. This can be used while scanning a heap to quickly get access
 * to all fields of a Job.
 */
static BackgroundJob *
DeformBackgroundJobHeapTuple(TupleDesc tupleDescriptor, HeapTuple jobTuple)
{
	Datum values[Natts_pg_dist_background_job] = { 0 };
	bool nulls[Natts_pg_dist_background_job] = { 0 };
	heap_deform_tuple(jobTuple, tupleDescriptor, values, nulls);

	BackgroundJob *job = palloc0(sizeof(BackgroundJob));
	job->jobid = DatumGetInt64(values[Anum_pg_dist_background_job_job_id - 1]);
	job->state = BackgroundJobStatusByOid(
		DatumGetObjectId(values[Anum_pg_dist_background_job_state - 1]));

	if (!nulls[Anum_pg_dist_background_job_job_type - 1])
	{
		Name jobTypeName = DatumGetName(values[Anum_pg_dist_background_job_job_type -
											   1]);
		job->jobType = pstrdup(NameStr(*jobTypeName));
	}

	if (!nulls[Anum_pg_dist_background_job_description - 1])
	{
		job->description = text_to_cstring(
			DatumGetTextP(values[Anum_pg_dist_background_job_description - 1]));
	}

	if (!nulls[Anum_pg_dist_background_job_started_at - 1])
	{
		TimestampTz startedAt =
			DatumGetTimestampTz(values[Anum_pg_dist_background_job_started_at - 1]);
		SET_NULLABLE_FIELD(job, started_at, startedAt);
	}

	if (!nulls[Anum_pg_dist_background_job_finished_at - 1])
	{
		TimestampTz finishedAt =
			DatumGetTimestampTz(values[Anum_pg_dist_background_job_finished_at - 1]);
		SET_NULLABLE_FIELD(job, finished_at, finishedAt);
	}

	return job;
}


/*
 * DeformBackgroundTaskHeapTuple parses a HeapTuple from pg_dist_background_task into its
 * inmemory representation. This can be used while scanning a heap to quickly get access
 * to all fields of a Task.
 */
static BackgroundTask *
DeformBackgroundTaskHeapTuple(TupleDesc tupleDescriptor, HeapTuple taskTuple)
{
	Datum values[Natts_pg_dist_background_task] = { 0 };
	bool nulls[Natts_pg_dist_background_task] = { 0 };
	heap_deform_tuple(taskTuple, tupleDescriptor, values, nulls);

	BackgroundTask *task = palloc0(sizeof(BackgroundTask));
	task->jobid = DatumGetInt64(values[Anum_pg_dist_background_task_job_id - 1]);
	task->taskid = DatumGetInt64(values[Anum_pg_dist_background_task_task_id - 1]);
	task->owner = DatumGetObjectId(values[Anum_pg_dist_background_task_owner - 1]);
	if (!nulls[Anum_pg_dist_background_task_pid - 1])
	{
		int32 pid = DatumGetInt32(values[Anum_pg_dist_background_task_pid - 1]);
		SET_NULLABLE_FIELD(task, pid, pid);
	}
	task->status = BackgroundTaskStatusByOid(
		DatumGetObjectId(values[Anum_pg_dist_background_task_status - 1]));

	task->command = text_to_cstring(
		DatumGetTextP(values[Anum_pg_dist_background_task_command - 1]));

	if (!nulls[Anum_pg_dist_background_task_retry_count - 1])
	{
		int32 retryCount =
			DatumGetInt32(values[Anum_pg_dist_background_task_retry_count - 1]);
		SET_NULLABLE_FIELD(task, retry_count, retryCount);
	}

	if (!nulls[Anum_pg_dist_background_task_not_before - 1])
	{
		TimestampTz notBefore =
			DatumGetTimestampTz(values[Anum_pg_dist_background_task_not_before - 1]);
		SET_NULLABLE_FIELD(task, not_before, notBefore);
	}

	if (!nulls[Anum_pg_dist_background_task_message - 1])
	{
		task->message =
			TextDatumGetCString(values[Anum_pg_dist_background_task_message - 1]);
	}

	if (!nulls[Anum_pg_dist_background_task_nodes_involved - 1])
	{
		ArrayType *nodesInvolvedArrayObject =
			DatumGetArrayTypeP(values[Anum_pg_dist_background_task_nodes_involved - 1]);
		task->nodesInvolved = IntegerArrayTypeToList(nodesInvolvedArrayObject);
	}

	return task;
}


/*
 * BackgroundTaskHasUmnetDependencies checks if a task from the given job has any unmet
 * dependencies. An unmet dependency is a Task that the task in question depends on and
 * has not reached its Done state.
 */
static bool
BackgroundTaskHasUmnetDependencies(int64 jobId, int64 taskId)
{
	bool hasUnmetDependency = false;

	Relation pgDistBackgroundTasksDepend =
		table_open(DistBackgroundTaskDependRelationId(), AccessShareLock);

	ScanKeyData scanKey[2] = { 0 };
	bool indexOK = true;

	/* pg_catalog.pg_dist_background_task_depend.job_id = jobId */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_depend_job_id,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(jobId));

	/* pg_catalog.pg_dist_background_task_depend.task_id = $taskId */
	ScanKeyInit(&scanKey[1], Anum_pg_dist_background_task_depend_task_id,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(taskId));

	SysScanDesc scanDescriptor =
		systable_beginscan(pgDistBackgroundTasksDepend,
						   DistBackgroundTaskDependTaskIdIndexId(),
						   indexOK, NULL, lengthof(scanKey),
						   scanKey);

	HeapTuple dependTuple = NULL;
	while (HeapTupleIsValid(dependTuple = systable_getnext(scanDescriptor)))
	{
		Form_pg_dist_background_task_depend depends =
			(Form_pg_dist_background_task_depend) GETSTRUCT(dependTuple);

		BackgroundTask *dependingJob = GetBackgroundTaskByTaskId(depends->depends_on);

		/*
		 * Only when the status of all depending jobs is done we clear this job and say
		 * that is has no unmet dependencies.
		 */
		if (dependingJob->status == BACKGROUND_TASK_STATUS_DONE)
		{
			continue;
		}

		hasUnmetDependency = true;
		break;
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistBackgroundTasksDepend, AccessShareLock);

	return hasUnmetDependency;
}


/*
 * BackgroundTaskReadyToRun checks if a task is ready to run. This consists of two checks
 *  - the task has no unmet dependencies
 *  - the task either has no not_before value set, or the not_before time has passed.
 *
 * Due to the costs of checking we check them in reverse order, but conceptually they
 * should be thought of in the above order.
 */
static bool
BackgroundTaskReadyToRun(BackgroundTask *task)
{
	if (task->not_before)
	{
		if (*(task->not_before) > GetCurrentTimestamp())
		{
			/* task should not yet be run */
			return false;
		}
	}

	if (BackgroundTaskHasUmnetDependencies(task->jobid, task->taskid))
	{
		return false;
	}

	return true;
}


/*
 * GetRunnableBackgroundTask returns the first candidate for a task to be run. When a task
 * is returned it has been checked for all the preconditions to hold.
 *
 * That means, if there is no task returned the background worker should close and let the
 * maintenance daemon start a new background tasks queue monitor once task become
 * available.
 */
BackgroundTask *
GetRunnableBackgroundTask(void)
{
	Relation pgDistBackgroundTasks =
		table_open(DistBackgroundTaskRelationId(), ExclusiveLock);

	BackgroundTaskStatus taskStatus[] = {
		BACKGROUND_TASK_STATUS_RUNNABLE
	};

	BackgroundTask *task = NULL;
	for (int i = 0; !task && i < sizeof(taskStatus) / sizeof(taskStatus[0]); i++)
	{
		const int scanKeyCount = 1;
		ScanKeyData scanKey[1] = { 0 };
		const bool indexOK = true;

		/* pg_dist_background_task.status == taskStatus[i] */
		ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_status,
					BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(
						BackgroundTaskStatusOid(taskStatus[i])));

		SysScanDesc scanDescriptor =
			systable_beginscan(pgDistBackgroundTasks,
							   DistBackgroundTaskStatusTaskIdIndexId(),
							   indexOK, NULL, scanKeyCount,
							   scanKey);

		HeapTuple taskTuple = NULL;
		TupleDesc tupleDescriptor = RelationGetDescr(pgDistBackgroundTasks);
		while (HeapTupleIsValid(taskTuple = systable_getnext(scanDescriptor)))
		{
			task = DeformBackgroundTaskHeapTuple(tupleDescriptor, taskTuple);
			if (BackgroundTaskReadyToRun(task) &&
				IncrementParallelTaskCountForNodesInvolved(task))
			{
				/* found task, close table and return */
				break;
			}
			task = NULL;
		}

		systable_endscan(scanDescriptor);
	}

	table_close(pgDistBackgroundTasks, NoLock);

	return task;
}


/*
 * GetBackgroundJobByJobId loads a BackgroundJob from the catalog into memory. Return's a
 * null pointer if no job exist with the given JobId.
 */
BackgroundJob *
GetBackgroundJobByJobId(int64 jobId)
{
	ScanKeyData scanKey[1] = { 0 };
	bool indexOK = true;

	Relation pgDistBackgroundJobs =
		table_open(DistBackgroundJobRelationId(), AccessShareLock);

	/* pg_dist_background_task.job_id == $jobId */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_background_job_job_id,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(jobId));

	SysScanDesc scanDescriptor =
		systable_beginscan(pgDistBackgroundJobs, DistBackgroundJobPKeyIndexId(),
						   indexOK, NULL, lengthof(scanKey), scanKey);

	HeapTuple taskTuple = systable_getnext(scanDescriptor);
	BackgroundJob *job = NULL;
	if (HeapTupleIsValid(taskTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgDistBackgroundJobs);
		job = DeformBackgroundJobHeapTuple(tupleDescriptor, taskTuple);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistBackgroundJobs, AccessShareLock);

	return job;
}


/*
 * GetBackgroundTaskByTaskId loads a BackgroundTask from the catalog into memory. Return's
 * a null pointer if no job exist with the given JobId and TaskId.
 */
BackgroundTask *
GetBackgroundTaskByTaskId(int64 taskId)
{
	ScanKeyData scanKey[1] = { 0 };
	bool indexOK = true;

	Relation pgDistBackgroundTasks =
		table_open(DistBackgroundTaskRelationId(), AccessShareLock);

	/* pg_dist_background_task.task_id == $taskId */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_task_id,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(taskId));

	SysScanDesc scanDescriptor =
		systable_beginscan(pgDistBackgroundTasks,
						   DistBackgroundTaskPKeyIndexId(),
						   indexOK, NULL, lengthof(scanKey), scanKey);

	HeapTuple taskTuple = systable_getnext(scanDescriptor);
	BackgroundTask *task = NULL;
	if (HeapTupleIsValid(taskTuple))
	{
		TupleDesc tupleDescriptor = RelationGetDescr(pgDistBackgroundTasks);
		task = DeformBackgroundTaskHeapTuple(tupleDescriptor, taskTuple);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistBackgroundTasks, AccessShareLock);

	return task;
}


typedef struct JobTaskStatusCounts
{
	int blocked;
	int runnable;
	int running;
	int done;
	int error;
	int unscheduled;
	int cancelled;
	int cancelling;
} JobTaskStatusCounts;


/*
 * JobTasksStatusCount scans all tasks associated with the provided job and count's the
 * number of tasks that are tracked in each state. Effectively grouping and counting the
 * tasks by their state.
 */
static JobTaskStatusCounts
JobTasksStatusCount(int64 jobId)
{
	Relation pgDistBackgroundTasks =
		table_open(DistBackgroundTaskRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistBackgroundTasks);

	ScanKeyData scanKey[1] = { 0 };
	const bool indexOK = true;

	/* WHERE job_id = $task->jobId */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_job_id,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(jobId));

	SysScanDesc scanDescriptor =
		systable_beginscan(pgDistBackgroundTasks,
						   DistBackgroundTaskJobIdTaskIdIndexId(),
						   indexOK, NULL, lengthof(scanKey), scanKey);

	JobTaskStatusCounts counts = { 0 };
	HeapTuple heapTuple = NULL;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Datum values[Natts_pg_dist_background_task] = { 0 };
		bool isnull[Natts_pg_dist_background_task] = { 0 };

		heap_deform_tuple(heapTuple, tupleDescriptor, values, isnull);

		Oid statusOid = DatumGetObjectId(values[Anum_pg_dist_background_task_status -
												1]);
		BackgroundTaskStatus status = BackgroundTaskStatusByOid(statusOid);

		switch (status)
		{
			case BACKGROUND_TASK_STATUS_BLOCKED:
			{
				counts.blocked++;
				break;
			}

			case BACKGROUND_TASK_STATUS_RUNNABLE:
			{
				counts.runnable++;
				break;
			}

			case BACKGROUND_TASK_STATUS_RUNNING:
			{
				counts.running++;
				break;
			}

			case BACKGROUND_TASK_STATUS_DONE:
			{
				counts.done++;
				break;
			}

			case BACKGROUND_TASK_STATUS_ERROR:
			{
				counts.error++;
				break;
			}

			case BACKGROUND_TASK_STATUS_UNSCHEDULED:
			{
				counts.unscheduled++;
				break;
			}

			case BACKGROUND_TASK_STATUS_CANCELLED:
			{
				counts.cancelled++;
				break;
			}

			case BACKGROUND_TASK_STATUS_CANCELLING:
			{
				counts.cancelling++;
				break;
			}

			default:
			{
				elog(ERROR, "unknown state in pg_dist_background_task");
			}
		}
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistBackgroundTasks, NoLock);

	return counts;
}


/*
 * SetFieldValue populates values, isnull, replace according to the newValue passed,
 * returning if the value has been updated or not. The replace argument can be omitted if
 * we are simply initializing a field.
 *
 * suggested use would be:
 *   bool updated = false;
 *   updated |= SetFieldValue(Anum_...._...., isnull, replace, values, newValue);
 *   updated |= SetFieldText(Anum_...._...., isnull, replace, values, "hello world");
 *   updated |= SetFieldNull(Anum_...._...., isnull, replace, values);
 *
 * Only if updated is set in the end the tuple has to be updated in the catalog.
 */
static bool
SetFieldValue(int attno, Datum values[], bool isnull[], bool replace[], Datum newValue)
{
	int idx = attno - 1;
	bool updated = false;

	if (!isnull[idx] && newValue == values[idx])
	{
		return updated;
	}

	values[idx] = newValue;
	isnull[idx] = false;
	updated = true;

	if (replace)
	{
		replace[idx] = true;
	}
	return updated;
}


/*
 * SetFieldText populates values, isnull, replace according to the newValue passed,
 * returning if the value has been updated or not. The replace argument can be omitted if
 * we are simply initializing a field.
 *
 * suggested use would be:
 *   bool updated = false;
 *   updated |= SetFieldValue(Anum_...._...., isnull, replace, values, newValue);
 *   updated |= SetFieldText(Anum_...._...., isnull, replace, values, "hello world");
 *   updated |= SetFieldNull(Anum_...._...., isnull, replace, values);
 *
 * Only if updated is set in the end the tuple has to be updated in the catalog.
 */
static bool
SetFieldText(int attno, Datum values[], bool isnull[], bool replace[],
			 const char *newValue)
{
	int idx = attno - 1;
	bool updated = false;

	if (!isnull[idx])
	{
		char *oldText = TextDatumGetCString(values[idx]);
		if (strcmp(oldText, newValue) == 0)
		{
			return updated;
		}
	}

	values[idx] = CStringGetTextDatum(newValue);
	isnull[idx] = false;
	updated = true;

	if (replace)
	{
		replace[idx] = true;
	}
	return updated;
}


/*
 * SetFieldNull populates values, isnull and replace according to a null value,
 * returning if the value has been updated or not. The replace argument can be omitted if
 * we are simply initializing a field.
 *
 * suggested use would be:
 *   bool updated = false;
 *   updated |= SetFieldValue(Anum_...._...., isnull, replace, values, newValue);
 *   updated |= SetFieldText(Anum_...._...., isnull, replace, values, "hello world");
 *   updated |= SetFieldNull(Anum_...._...., isnull, replace, values);
 *
 * Only if updated is set in the end the tuple has to be updated in the catalog.
 */
static bool
SetFieldNull(int attno, Datum values[], bool isnull[], bool replace[])
{
	int idx = attno - 1;
	bool updated = false;

	if (isnull[idx])
	{
		return updated;
	}

	isnull[idx] = true;
	values[idx] = InvalidOid;
	updated = true;

	if (replace)
	{
		replace[idx] = true;
	}
	return updated;
}


/*
 * UpdateBackgroundJob updates the job's metadata based on the most recent status of all
 * its associated tasks.
 *
 * Since the state of a job is a function of the state of all associated tasks this
 * function projects the tasks states into the job's state.
 *
 * When Citus makes a change to any of the tasks associated with the job it should call
 * this function to correctly project the task updates onto the jobs metadata.
 */
void
UpdateBackgroundJob(int64 jobId)
{
	JobTaskStatusCounts counts = JobTasksStatusCount(jobId);
	BackgroundJobStatus status = BACKGROUND_JOB_STATUS_RUNNING;

	if (counts.cancelling > 0)
	{
		status = BACKGROUND_JOB_STATUS_CANCELLING;
	}
	else if (counts.cancelled > 0)
	{
		status = BACKGROUND_JOB_STATUS_CANCELLED;
	}
	else if (counts.blocked + counts.runnable + counts.running + counts.error +
			 counts.unscheduled == 0)
	{
		/* all tasks are done, job is finished */
		status = BACKGROUND_JOB_STATUS_FINISHED;
	}
	else if (counts.error + counts.unscheduled > 0)
	{
		/* we are either failing, or failed */
		if (counts.blocked + counts.runnable + counts.running > 0)
		{
			/* failing, as there are still tasks to be run */
			status = BACKGROUND_JOB_STATUS_FAILING;
		}
		else
		{
			status = BACKGROUND_JOB_STATUS_FAILED;
		}
	}
	else if (counts.blocked + counts.runnable + counts.running > 0)
	{
		status = BACKGROUND_JOB_STATUS_RUNNING;
	}
	else
	{
		return;
	}

	Relation pgDistBackgroundJobs =
		table_open(DistBackgroundJobRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistBackgroundJobs);

	ScanKeyData scanKey[1] = { 0 };
	const bool indexOK = true;

	/* WHERE job_id = $task->jobId */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_background_job_job_id,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(jobId));

	SysScanDesc scanDescriptor =
		systable_beginscan(pgDistBackgroundJobs,
						   DistBackgroundJobPKeyIndexId(),
						   indexOK, NULL, lengthof(scanKey), scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find background jobs entry for job_id: "
							   UINT64_FORMAT, jobId)));
	}

	Datum values[Natts_pg_dist_background_task] = { 0 };
	bool isnull[Natts_pg_dist_background_task] = { 0 };
	bool replace[Natts_pg_dist_background_task] = { 0 };

	heap_deform_tuple(heapTuple, tupleDescriptor, values, isnull);

	bool updated = false;

	Oid stateOid = BackgroundJobStatusOid(status);
	updated |= SetFieldValue(Anum_pg_dist_background_job_state, values, isnull, replace,
							 ObjectIdGetDatum(stateOid));

	if (status == BACKGROUND_JOB_STATUS_RUNNING)
	{
		if (isnull[Anum_pg_dist_background_job_started_at - 1])
		{
			/* first time status has been updated and was running, updating started_at */
			TimestampTz startedAt = GetCurrentTimestamp();
			updated |= SetFieldValue(Anum_pg_dist_background_job_started_at, values,
									 isnull, replace, TimestampTzGetDatum(startedAt));
		}
	}

	if (IsBackgroundJobStatusTerminal(status))
	{
		if (isnull[Anum_pg_dist_background_job_finished_at - 1])
		{
			/* didn't have a finished at time just yet, updating to now */
			TimestampTz finishedAt = GetCurrentTimestamp();
			updated |= SetFieldValue(Anum_pg_dist_background_job_finished_at, values,
									 isnull, replace, TimestampTzGetDatum(finishedAt));
		}
	}

	if (updated)
	{
		heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull,
									  replace);

		CatalogTupleUpdate(pgDistBackgroundJobs, &heapTuple->t_self, heapTuple);

		CommandCounterIncrement();
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistBackgroundJobs, NoLock);
}


/*
 * UpdateBackgroundTask updates the catalog entry for the passed task, preventing an
 * actual update when the inmemory representation is the same as the one stored in the
 * catalog.
 */
void
UpdateBackgroundTask(BackgroundTask *task)
{
	Relation pgDistBackgroundTasks =
		table_open(DistBackgroundTaskRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistBackgroundTasks);

	ScanKeyData scanKey[1] = { 0 };
	const bool indexOK = true;

	/* WHERE task_id = $task->taskid */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_task_id,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(task->taskid));

	SysScanDesc scanDescriptor =
		systable_beginscan(pgDistBackgroundTasks,
						   DistBackgroundTaskPKeyIndexId(),
						   indexOK, NULL, lengthof(scanKey), scanKey);

	HeapTuple heapTuple = systable_getnext(scanDescriptor);
	if (!HeapTupleIsValid(heapTuple))
	{
		ereport(ERROR, (errmsg("could not find background task entry for :"
							   "job_id/task_id: " UINT64_FORMAT "/" UINT64_FORMAT,
							   task->jobid, task->taskid)));
	}

	Datum values[Natts_pg_dist_background_task] = { 0 };
	bool isnull[Natts_pg_dist_background_task] = { 0 };
	bool replace[Natts_pg_dist_background_task] = { 0 };

	heap_deform_tuple(heapTuple, tupleDescriptor, values, isnull);

	bool updated = false;

	updated |= SetFieldValue(Anum_pg_dist_background_task_owner, values, isnull, replace,
							 task->owner);

	if (task->pid)
	{
		updated |= SetFieldValue(Anum_pg_dist_background_task_pid, values, isnull,
								 replace, Int32GetDatum(*task->pid));
	}
	else
	{
		updated |= SetFieldNull(Anum_pg_dist_background_task_pid, values, isnull,
								replace);
	}

	Oid statusOid = ObjectIdGetDatum(BackgroundTaskStatusOid(task->status));
	updated |= SetFieldValue(Anum_pg_dist_background_task_status, values, isnull, replace,
							 statusOid);

	if (task->retry_count)
	{
		updated |= SetFieldValue(Anum_pg_dist_background_task_retry_count, values, isnull,
								 replace, Int32GetDatum(*task->retry_count));
	}
	else
	{
		updated |= SetFieldNull(Anum_pg_dist_background_task_retry_count, values, isnull,
								replace);
	}

	if (task->not_before)
	{
		updated |= SetFieldValue(Anum_pg_dist_background_task_not_before, values, isnull,
								 replace, TimestampTzGetDatum(*task->not_before));
	}
	else
	{
		updated |= SetFieldNull(Anum_pg_dist_background_task_not_before, values, isnull,
								replace);
	}

	if (task->message)
	{
		updated |= SetFieldText(Anum_pg_dist_background_task_message, values, isnull,
								replace, task->message);
	}
	else
	{
		updated |= SetFieldNull(Anum_pg_dist_background_task_message, values, isnull,
								replace);
	}

	if (updated)
	{
		heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull,
									  replace);

		CatalogTupleUpdate(pgDistBackgroundTasks, &heapTuple->t_self, heapTuple);

		CommandCounterIncrement();
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistBackgroundTasks, NoLock);
}


/*
 * GetDependantTasks returns a list of taskId's containing all tasks depending on the task
 * passed via its arguments.
 *
 * Becasue tasks are int64 we allocate and return a List of int64 pointers.
 */
static List *
GetDependantTasks(int64 jobId, int64 taskId)
{
	Relation pgDistBackgroundTasksDepends =
		table_open(DistBackgroundTaskDependRelationId(), RowExclusiveLock);

	ScanKeyData scanKey[2] = { 0 };
	const bool indexOK = true;

	/* pg_dist_background_task_depend.job_id = $jobId */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_depend_job_id,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(jobId));

	/* pg_dist_background_task_depend.depends_on = $taskId */
	ScanKeyInit(&scanKey[1], Anum_pg_dist_background_task_depend_depends_on,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(taskId));

	SysScanDesc scanDescriptor =
		systable_beginscan(pgDistBackgroundTasksDepends,
						   DistBackgroundTaskDependDependsOnIndexId(),
						   indexOK,
						   NULL, lengthof(scanKey), scanKey);

	List *dependantTasks = NIL;
	HeapTuple heapTuple = NULL;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Form_pg_dist_background_task_depend depend =
			(Form_pg_dist_background_task_depend) GETSTRUCT(heapTuple);

		int64 *dTaskId = palloc0(sizeof(int64));
		*dTaskId = depend->task_id;

		dependantTasks = lappend(dependantTasks, dTaskId);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistBackgroundTasksDepends, NoLock);

	return dependantTasks;
}


/*
 * CancelTasksForJob cancels all tasks associated with a job that are not currently
 * running and are not already in their terminal state. Canceling these tasks consist of
 * updating the status of the task in the catalog.
 *
 * For all other tasks, namely the ones that are currently running, it returns the list of
 * Pid's of the tasks running. These backends should be signalled for cancellation.
 *
 * Since we are either signalling or changing the status of a task we perform appropriate
 * permission checks. This currently includes the exact same checks pg_cancel_backend
 * would perform.
 */
List *
CancelTasksForJob(int64 jobid)
{
	Relation pgDistBackgroundTasks =
		table_open(DistBackgroundTaskRelationId(), ExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistBackgroundTasks);

	ScanKeyData scanKey[1] = { 0 };

	/* WHERE jobId = $jobid */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_job_id,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(jobid));

	const bool indexOK = true;
	SysScanDesc scanDescriptor = systable_beginscan(pgDistBackgroundTasks,
													DistBackgroundTaskJobIdTaskIdIndexId(),
													indexOK, NULL,
													lengthof(scanKey), scanKey);

	List *runningTaskPids = NIL;
	HeapTuple taskTuple = NULL;
	while (HeapTupleIsValid(taskTuple = systable_getnext(scanDescriptor)))
	{
		Datum values[Natts_pg_dist_background_task] = { 0 };
		bool nulls[Natts_pg_dist_background_task] = { 0 };
		bool replace[Natts_pg_dist_background_task] = { 0 };
		heap_deform_tuple(taskTuple, tupleDescriptor, values, nulls);

		Oid statusOid =
			DatumGetObjectId(values[Anum_pg_dist_background_task_status - 1]);
		BackgroundTaskStatus status = BackgroundTaskStatusByOid(statusOid);

		if (IsBackgroundTaskStatusTerminal(status))
		{
			continue;
		}

		/* make sure the current user has the rights to cancel this task */
		Oid taskOwner = DatumGetObjectId(values[Anum_pg_dist_background_task_owner - 1]);
		if (superuser_arg(taskOwner) && !superuser())
		{
			/* must be a superuser to cancel tasks owned by superuser */
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("must be a superuser to cancel superuser tasks")));
		}
		else if (!has_privs_of_role(GetUserId(), taskOwner) &&
				 !has_privs_of_role(GetUserId(), ROLE_PG_SIGNAL_BACKEND))
		{
			/* user doesn't have the permissions to cancel this job */
			ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("must be a member of the role whose task is being "
								   "canceled or member of pg_signal_backend")));
		}

		BackgroundTaskStatus newStatus = BACKGROUND_TASK_STATUS_CANCELLED;
		if (status == BACKGROUND_TASK_STATUS_RUNNING)
		{
			if (!nulls[Anum_pg_dist_background_task_pid - 1])
			{
				int32 pid = DatumGetInt32(values[Anum_pg_dist_background_task_pid - 1]);
				runningTaskPids = lappend_int(runningTaskPids, pid);
				newStatus = BACKGROUND_TASK_STATUS_CANCELLING;
			}
		}

		/* update task to new status */
		nulls[Anum_pg_dist_background_task_status - 1] = false;
		values[Anum_pg_dist_background_task_status - 1] = ObjectIdGetDatum(
			BackgroundTaskStatusOid(newStatus));
		replace[Anum_pg_dist_background_task_status - 1] = true;

		taskTuple = heap_modify_tuple(taskTuple, tupleDescriptor, values, nulls,
									  replace);
		CatalogTupleUpdate(pgDistBackgroundTasks, &taskTuple->t_self, taskTuple);
	}

	systable_endscan(scanDescriptor);
	table_close(pgDistBackgroundTasks, NoLock);

	CommandCounterIncrement();

	return runningTaskPids;
}


/*
 * UnscheduleDependentTasks follows the dependency tree of the provided task recursively
 * to unschedule any task depending on the current task.
 *
 * This is useful to unschedule any task that can never run because it will never satisfy
 * the unmet dependency constraint.
 */
void
UnscheduleDependentTasks(BackgroundTask *task)
{
	Relation pgDistBackgroundTasks =
		table_open(DistBackgroundTaskRelationId(), RowExclusiveLock);
	TupleDesc tupleDescriptor = RelationGetDescr(pgDistBackgroundTasks);

	List *dependantTasks = GetDependantTasks(task->jobid, task->taskid);
	while (list_length(dependantTasks) > 0)
	{
		/* pop last item from stack */
		int64 cTaskId = *(int64 *) llast(dependantTasks);
		dependantTasks = list_delete_last(dependantTasks);

		/* push new dependant tasks on to stack */
		dependantTasks = list_concat(dependantTasks,
									 GetDependantTasks(task->jobid, cTaskId));

		/* unschedule current task */
		{
			ScanKeyData scanKey[1] = { 0 };

			/* WHERE taskId = dependentTask->taskId */
			ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_task_id,
						BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(cTaskId));
			const bool indexOK = true;
			SysScanDesc scanDescriptor = systable_beginscan(pgDistBackgroundTasks,
															DistBackgroundTaskPKeyIndexId(),
															indexOK, NULL,
															lengthof(scanKey), scanKey);

			HeapTuple heapTuple = systable_getnext(scanDescriptor);
			if (!HeapTupleIsValid(heapTuple))
			{
				ereport(ERROR, (errmsg("could not find background task entry for "
									   "task_id: " UINT64_FORMAT, cTaskId)));
			}

			Datum values[Natts_pg_dist_background_task] = { 0 };
			bool isnull[Natts_pg_dist_background_task] = { 0 };
			bool replace[Natts_pg_dist_background_task] = { 0 };

			values[Anum_pg_dist_background_task_status - 1] =
				ObjectIdGetDatum(CitusTaskStatusUnscheduledId());
			isnull[Anum_pg_dist_background_task_status - 1] = false;
			replace[Anum_pg_dist_background_task_status - 1] = true;

			heapTuple = heap_modify_tuple(heapTuple, tupleDescriptor, values, isnull,
										  replace);
			CatalogTupleUpdate(pgDistBackgroundTasks, &heapTuple->t_self, heapTuple);

			systable_endscan(scanDescriptor);
		}
	}

	CommandCounterIncrement();

	table_close(pgDistBackgroundTasks, NoLock);
}


/*
 * UnblockDependingBackgroundTasks unblocks any depending task that now satisfies the
 * constraaint that it doesn't have unmet dependencies anymore. For this to be done we
 * will find all tasks depending on the current task. Per found task we check if it has
 * any unmet dependencies. If no tasks are found that would block the execution of this
 * task we transition the task to Runnable state.
 */
void
UnblockDependingBackgroundTasks(BackgroundTask *task)
{
	Relation pgDistBackgroundTasksDepend =
		table_open(DistBackgroundTaskDependRelationId(), RowExclusiveLock);

	ScanKeyData scanKey[2] = { 0 };

	/* WHERE jobId = $jobId */
	ScanKeyInit(&scanKey[0], Anum_pg_dist_background_task_depend_job_id,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(task->jobid));

	/* WHERE depends_on = $taskId */
	ScanKeyInit(&scanKey[1], Anum_pg_dist_background_task_depend_depends_on,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(task->taskid));
	const bool indexOK = true;
	SysScanDesc scanDescriptor = systable_beginscan(
		pgDistBackgroundTasksDepend, DistBackgroundTaskDependDependsOnIndexId(), indexOK,
		NULL, lengthof(scanKey), scanKey);

	HeapTuple heapTuple = NULL;
	while (HeapTupleIsValid(heapTuple = systable_getnext(scanDescriptor)))
	{
		Form_pg_dist_background_task_depend depend =
			(Form_pg_dist_background_task_depend) GETSTRUCT(heapTuple);

		if (!BackgroundTaskHasUmnetDependencies(task->jobid, depend->task_id))
		{
			/*
			 * The task does not have any unmet dependencies anymore and should become
			 * runnable
			 */

			BackgroundTask *unblockedTask = GetBackgroundTaskByTaskId(depend->task_id);
			if (unblockedTask->status == BACKGROUND_TASK_STATUS_CANCELLED)
			{
				continue;
			}

			Assert(unblockedTask->status == BACKGROUND_TASK_STATUS_BLOCKED);
			unblockedTask->status = BACKGROUND_TASK_STATUS_RUNNABLE;
			UpdateBackgroundTask(unblockedTask);
		}
	}

	systable_endscan(scanDescriptor);

	table_close(pgDistBackgroundTasksDepend, NoLock);
}
