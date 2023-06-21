#include "postgres.h"
#include "libpq-fe.h"
#include "executor/spi.h"
#include "distributed/lock_graph.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_logical_replication.h"
#include "postmaster/postmaster.h"
#include "distributed/distribution_column.h"
#include "utils/builtins.h"
#include "distributed/shard_split.h"
#include "utils/lsyscache.h"
#include "distributed/listutils.h"
#include "distributed/metadata_utility.h"

PG_FUNCTION_INFO_V1(citus_auto_shard_split_start);

uint64 MaxShardSize = 102400;
double TenantFrequency = 0.3;

/*
 * Struct to store all the information related to
 * a shard.
 */
typedef struct ShardInfoData
{
	int64 shardSize;
	int64 shardMinValue;
	int64 shardMaxValue;
	int64 shardId;
	int32 nodeId;
	char *tableName;
	char *distributionColumn;
	char *dataType;
	char *shardName;
	Oid tableId;
	Oid distributionColumnId;
}ShardInfoData;
typedef ShardInfoData *ShardInfo;

void ErrorOnConcurrentOperation(void);
StringInfo GetShardSplitQuery(ShardInfo shardinfo, List *splitPoints,
							  char *shardSplitMode);
void ExecuteSplitBackgroundJob(int64 jobId, ShardInfo shardinfo, List *splitPoints,
							   char *shardSplitMode);
int64 ExecuteAvgHashQuery(ShardInfo shardinfo);
List * FindShardSplitPoints(ShardInfo shardinfo);
void ScheduleShardSplit(ShardInfo shardinfo, char *shardSplitMode);

/*
 * It throws an error if a concurrent automatic shard split or Rebalance operation is happening.
 */
void
ErrorOnConcurrentOperation()
{
	int64 jobId = 0;
	if (HasNonTerminalJobOfType("rebalance", &jobId))
	{
		ereport(ERROR, (
					errmsg("A rebalance is already running as job %ld", jobId),
					errdetail("A rebalance was already scheduled as background job"),
					errhint("To monitor progress, run: SELECT * FROM "
							"citus_rebalance_status();")));
	}
	if (HasNonTerminalJobOfType("Automatic Shard Split", &jobId))
	{
		ereport(ERROR, (
					errmsg("An automatic shard split is already running as job %ld",
						   jobId),
					errdetail(
						"An automatic shard split was already scheduled as background job")));
	}
}


/*
 * For a given SplitPoints , it creates the SQL query for the Shard Splitting
 */
StringInfo
GetShardSplitQuery(ShardInfo shardinfo, List *splitPoints, char *shardSplitMode)
{
	StringInfo splitQuery = makeStringInfo();

	int64 length = list_length(splitPoints);
	appendStringInfo(splitQuery, "SELECT citus_split_shard_by_split_points(%ld, ARRAY[",
					 shardinfo->shardId);

	int32 splitpoint = 0;
	int index = 0;
	foreach_int(splitpoint, splitPoints)
	{
		appendStringInfo(splitQuery, "'%d'", splitpoint);

		if (index < length - 1)
		{
			appendStringInfoString(splitQuery, ",");
		}

		index++;
	}

/*All the shards after the split will be belonging to the same node */
	appendStringInfo(splitQuery, "], ARRAY[");

	for (int i = 0; i < length; i++)
	{
		appendStringInfo(splitQuery, "%d,", shardinfo->nodeId);
	}
	appendStringInfo(splitQuery, "%d], %s)", shardinfo->nodeId, quote_literal_cstr(
						 shardSplitMode));

	return splitQuery;
}


/*
 * It creates a background job for citus_split_shard_by_split_points and executes it in background.
 */
void
ExecuteSplitBackgroundJob(int64 jobId, ShardInfo shardinfo, List *splitPoints,
						  char *shardSplitMode)
{
	StringInfo splitQuery = makeStringInfo();
	splitQuery = GetShardSplitQuery(shardinfo, splitPoints, shardSplitMode);
	/* ereport(LOG, (errmsg(splitQuery->data))); */
	int32 nodesInvolved[] = { shardinfo->nodeId };
	Oid superUserId = CitusExtensionOwner();
	BackgroundTask *task = ScheduleBackgroundTask(jobId, superUserId, splitQuery->data, 0,
												  NULL, 1, nodesInvolved);
}


/*
 * It executes a query to find the average hash value in a shard considering rows with a limit of 10GB .
 * If there exists a hash value it is returned otherwise shardminvalue-1 is returned.
 */
int64
ExecuteAvgHashQuery(ShardInfo shardinfo)
{
	StringInfo AvgHashQuery = makeStringInfo();
	uint64 tableSize = 0;
	bool check = DistributedTableSize(shardinfo->tableId, TOTAL_RELATION_SIZE, true,
									  &tableSize);
	appendStringInfo(AvgHashQuery, "SELECT avg(h)::int,count(*)"
								   " FROM (SELECT worker_hash(%s) h FROM %s TABLESAMPLE SYSTEM(least(10, 100*10000000000/%lu))"
								   " WHERE worker_hash(%s)>=%ld AND worker_hash(%s)<=%ld) s",
					 shardinfo->distributionColumn, shardinfo->tableName,
					 tableSize,
					 shardinfo->distributionColumn, shardinfo->shardMinValue,
					 shardinfo->distributionColumn, shardinfo->shardMaxValue
					 );
	ereport(LOG, errmsg("%s", AvgHashQuery->data));
	SPI_connect();
	SPI_exec(AvgHashQuery->data, 0);
	SPITupleTable *tupletable = SPI_tuptable;
	HeapTuple tuple = tupletable->vals[0];
	bool isnull;
	Datum average = SPI_getbinval(tuple, tupletable->tupdesc, 1, &isnull);
	int64 isResultNull = 1;
	if (!isnull)
	{
		isResultNull = 0;
	}
	SPI_freetuptable(tupletable);
	SPI_finish();

	if (isResultNull == 0)
	{
		return DatumGetInt64(average);
	}
	else
	{
		return shardinfo->shardMinValue - 1;
	}
}


/*
 * This function executes a query and then decides whether a shard is subjected for isolation or average hash 2 way split.
 * If a tenant is found splitpoints for isolation is returned otherwise average hash value is returned.
 */
List *
FindShardSplitPoints(ShardInfo shardinfo)
{
	StringInfo CommonValueQuery = makeStringInfo();

	/*
	 * The inner query for extracting the tenant value having frequency > TenantFrequency is executed on
	 * every shard of the table and outer query gives the shardid and tenant values as the
	 * output. Tenant Frequency is a GUC here.
	 */
	appendStringInfo(CommonValueQuery,
					 "SELECT shardid , unnest(result::%s[]) from run_command_on_shards(%s,$$SELECT array_agg(val)"
					 " FROM pg_stats s , unnest(most_common_vals::text::%s[],most_common_freqs) as res(val,freq)"
					 " WHERE tablename = %s AND attname = %s AND schemaname = %s AND freq > %f $$)"
					 " WHERE result <> '' AND shardid = %ld;",
					 shardinfo->dataType, quote_literal_cstr(shardinfo->tableName),
					 shardinfo->dataType,
					 quote_literal_cstr(shardinfo->shardName),
					 quote_literal_cstr(shardinfo->distributionColumn),
					 quote_literal_cstr(get_namespace_name(get_rel_namespace(
															   shardinfo->tableId))),
					 TenantFrequency,
					 shardinfo->shardId);

	ereport(LOG, errmsg("%s", CommonValueQuery->data));
	List *splitPoints = NULL;

	/* Saving the current memory context*/
	MemoryContext originalContext = CurrentMemoryContext;

	SPI_connect();
	SPI_exec(CommonValueQuery->data, 0);

	/*Saving the SPI memory context for switching*/
	MemoryContext spiContext = CurrentMemoryContext;

	int64 rowCount = SPI_processed;
	int64 average;
	int32 hashedValue;

	ereport(LOG, errmsg("%ld", rowCount));

	if (rowCount > 0)
	{
		SPITupleTable *tupletable = SPI_tuptable;
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(shardinfo->tableId);
		for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			HeapTuple tuple = tupletable->vals[rowIndex];
			char *commonValue = SPI_getvalue(tuple, tupletable->tupdesc, 2);
			ereport(LOG, errmsg("%s", commonValue));
			Datum tenantIdDatum = StringToDatum(commonValue,
												shardinfo->distributionColumnId);
			Datum hashedValueDatum = FunctionCall1Coll(cacheEntry->hashFunction,
													   cacheEntry->partitionColumn->
													   varcollid,
													   tenantIdDatum);
			hashedValue = DatumGetInt32(hashedValueDatum);
			ereport(LOG, errmsg("%d", hashedValue));

			/*Switching the memory context to store the unique SplitPoints in a list*/

			MemoryContextSwitchTo(originalContext);
			if (hashedValue == shardinfo->shardMinValue)
			{
				splitPoints = list_append_unique_int(splitPoints, hashedValue);
			}
			else if (hashedValue == shardinfo->shardMaxValue)
			{
				splitPoints = list_append_unique_int(splitPoints, hashedValue - 1);
			}
			else
			{
				splitPoints = list_append_unique_int(splitPoints, hashedValue - 1);
				splitPoints = list_append_unique_int(splitPoints, hashedValue);
			}
			MemoryContextSwitchTo(spiContext);
		}
		SPI_freetuptable(tupletable);
		list_sort(splitPoints, list_int_cmp);
	}
	else
	{
		average = ExecuteAvgHashQuery(shardinfo);
		ereport(LOG, errmsg("%ld", average));
		MemoryContextSwitchTo(originalContext);
		if (shardinfo->shardMinValue <= average)
		{
			splitPoints = lappend_int(splitPoints, average);
		}
	}

	SPI_finish();
	return splitPoints;
}


/*
 * This function calculates the split points of the shard to
 * split and then executes the background job for the shard split.
 */
void
ScheduleShardSplit(ShardInfo shardinfo, char *shardSplitMode)
{
	List *splitPoints = FindShardSplitPoints(shardinfo);
	if (list_length(splitPoints) > 0)
	{
		int64 jobId = CreateBackgroundJob("Automatic Shard Split",
										  "Split using SplitPoints List");
		ereport(LOG, errmsg("%s", GetShardSplitQuery(shardinfo, splitPoints,
													 shardSplitMode)->data));
		ExecuteSplitBackgroundJob(jobId, shardinfo, splitPoints, shardSplitMode);
	}
	else
	{
		ereport(LOG, errmsg("No Splitpoints for shard split"));
	}
}


/*
 * citus_auto_shard_split_start finds shards whose colocation group has total size greater than threshold
 * and from that extracts the shard with the maximum size . It isolates the shards if the shard has common
 * tenant values otherwise it 2 way splits it on the basis of average hash value.
 *
 */
Datum
citus_auto_shard_split_start(PG_FUNCTION_ARGS)
{
	ErrorOnConcurrentOperation();
	StringInfo query = makeStringInfo();

	/* This query is written to group the shards on the basis of colocation id and shardminvalue and get the groups whose sum of shardsize
	 * are greater than a threshold and than extract the shard in them which has the maximum size. So for that first pg_dist_shard and citus_shards are joined followed by the joining of pg_dist_node
	 * and citus_shards and finally joined by the table obtained by the grouping of colocation id and shardminvalue and shardsize exceeding the threshold.*/

	appendStringInfo(
		query,
		" SELECT cs.shardid,pd.shardminvalue,pd.shardmaxvalue,cs.shard_size,pn.nodeid,ct.distribution_column,ct.table_name,cs.shard_name,(SELECT relname FROM pg_class WHERE oid = ct.table_name)"
		" FROM pg_catalog.pg_dist_shard pd JOIN pg_catalog.citus_shards cs ON pd.shardid = cs.shardid JOIN pg_catalog.pg_dist_node pn ON cs.nodename = pn.nodename AND cs.nodeport= pn.nodeport"
		" JOIN"
		" ( select shardid , max_size from (SELECT distinct first_value(shardid) OVER w as shardid, sum(shard_size) OVER (PARTITION BY colocation_id, shardminvalue) as total_sum, max(shard_size) OVER w as max_size"
		" FROM citus_shards cs JOIN pg_dist_shard ps USING(shardid)"
		" WINDOW w AS (PARTITION BY colocation_id, shardminvalue ORDER BY shard_size DESC) )as t where total_sum >= %lu )"
		" AS max_sizes ON cs.shardid=max_sizes.shardid AND cs.shard_size = max_sizes.max_size JOIN citus_tables ct ON cs.table_name = ct.table_name AND pd.shardminvalue <> pd.shardmaxvalue AND pd.shardminvalue <> ''",
		MaxShardSize * 1024
		);

	ereport(LOG, errmsg("%s", query->data));
	Oid shardTransferModeOid = PG_GETARG_OID(0);
	Datum enumLabelDatum = DirectFunctionCall1(enum_out, shardTransferModeOid);
	char *shardSplitMode = DatumGetCString(enumLabelDatum);
	ereport(LOG, errmsg("%s", shardSplitMode));
	if (SPI_connect() != SPI_OK_CONNECT)
	{
		elog(ERROR, "SPI_connect to the query failed");
	}
	if (SPI_exec(query->data, 0) != SPI_OK_SELECT)
	{
		elog(ERROR, "SPI_exec for the execution failed");
	}

	SPITupleTable *tupletable = SPI_tuptable;
	int rowCount = SPI_processed;
	bool isnull;

	for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		ShardInfoData shardinfo;
		HeapTuple tuple = tupletable->vals[rowIndex];

		Datum shardIdDatum = SPI_getbinval(tuple, tupletable->tupdesc, 1, &isnull);
		shardinfo.shardId = DatumGetInt64(shardIdDatum);

		Datum shardSizeDatum = SPI_getbinval(tuple, tupletable->tupdesc, 4, &isnull);
		shardinfo.shardSize = DatumGetInt64(shardSizeDatum);

		Datum nodeIdDatum = SPI_getbinval(tuple, tupletable->tupdesc, 5, &isnull);
		shardinfo.nodeId = DatumGetInt32(nodeIdDatum);

		char *shardMinVal = SPI_getvalue(tuple, tupletable->tupdesc, 2);
		shardinfo.shardMinValue = strtoi64(shardMinVal, NULL, 10);

		char *shardMaxVal = SPI_getvalue(tuple, tupletable->tupdesc, 3);
		shardinfo.shardMaxValue = strtoi64(shardMaxVal, NULL, 10);

		shardinfo.distributionColumn = SPI_getvalue(tuple, tupletable->tupdesc, 6);
		shardinfo.tableName = SPI_getvalue(tuple, tupletable->tupdesc, 7);

		shardinfo.shardName = SPI_getvalue(tuple, tupletable->tupdesc, 9);
		AppendShardIdToName(&shardinfo.shardName, shardinfo.shardId);

		Datum tableIdDatum = SPI_getbinval(tuple, tupletable->tupdesc, 7, &isnull);
		shardinfo.tableId = DatumGetObjectId(tableIdDatum);
		shardinfo.distributionColumnId = ColumnTypeIdForRelationColumnName(
			shardinfo.tableId,
			shardinfo.
			distributionColumn);
		shardinfo.dataType = format_type_be(shardinfo.distributionColumnId);

		ScheduleShardSplit(&shardinfo, shardSplitMode);
		ereport(LOG, (errmsg(
						  "Shard ID: %ld,ShardMinValue: %ld, ShardMaxValue: %ld , totalSize: %ld , nodeId: %d",
						  shardinfo.shardId, shardinfo.shardMinValue,
						  shardinfo.shardMaxValue,
						  shardinfo.shardSize, shardinfo.nodeId)));
	}

	SPI_freetuptable(tupletable);
	SPI_finish();

	PG_RETURN_VOID();
}
