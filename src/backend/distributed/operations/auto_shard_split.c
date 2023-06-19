#include "postgres.h"
#include "libpq-fe.h"
#include "executor/spi.h"
#include "distributed/lock_graph.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/multi_server_executor.h"
#include "distributed/pg_dist_rebalance_strategy.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/tuplestore.h"
#include "distributed/utils/array_type.h"
#include "distributed/worker_protocol.h"
#include "nodes/pg_list.h"
#include "postmaster/postmaster.h"
#include "distributed/distribution_column.h"
#include "utils/builtins.h"
#include "distributed/shard_split.h"


PG_FUNCTION_INFO_V1(citus_auto_shard_split_start);

int MaxShardSize = 104857600;

/*
 * Struct to store all the information related to
 * a shard.
 */
typedef struct ShardInfoData
{
	int64 shardsize;
	int64 shardminvalue;
	int64 shardmaxvalue;
	int64 shardid;
	int64 nodeid;
	char *tablename;
	char *distributionColumn;
	char *datatype;
	char *shardname;
	Oid tableId;
	Oid distributionColumnId;
}ShardInfoData;
typedef ShardInfoData *ShardInfo;


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
 * For a given SplitPoints , it creates the SQL query for the shard Splitting
 */
StringInfo
GetShardSplitQuery(ShardInfo shardinfo, List *SplitPoints)
{
	StringInfo splitQuery = makeStringInfo();

	int64 length = list_length(SplitPoints);
	appendStringInfo(splitQuery, "SELECT citus_split_shard_by_split_points(%ld, ARRAY[",
					 shardinfo->shardid);

	for (int i = 0; i < length - 1; i++)
	{
		appendStringInfo(splitQuery, "'%ld',", DatumGetInt64(list_nth(SplitPoints, i)));
	}
	appendStringInfo(splitQuery, "'%ld'], ARRAY[", DatumGetInt64(list_nth(SplitPoints,
																		  length - 1)));

	for (int i = 0; i < length; i++)
	{
		appendStringInfo(splitQuery, "%ld,", shardinfo->nodeid);
	}
	appendStringInfo(splitQuery, "%ld], 'block_writes')", shardinfo->nodeid);

	return splitQuery;
}


/*
 * It creates a background job for citus_split_shard_by_split_points and executes it in background.
 */
void
ExecuteSplitBackgroundJob(int64 jobid, ShardInfo shardinfo, List *SplitPoints)
{
	StringInfo splitQuery = makeStringInfo();
	splitQuery = GetShardSplitQuery(shardinfo, SplitPoints);
	ereport(LOG, (errmsg(splitQuery->data)));
	int32 nodesInvolved[1];
	nodesInvolved[0] = shardinfo->nodeid;
	Oid superUserId = CitusExtensionOwner();
	BackgroundTask *task = ScheduleBackgroundTask(jobid, superUserId, splitQuery->data, 0,
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
	appendStringInfo(AvgHashQuery, "SELECT avg(h)::int,count(*)"
								   " FROM (SELECT worker_hash(%s) h FROM %s TABLESAMPLE SYSTEM(least(10, 100*10000000000/citus_total_relation_size(%s)))"
								   " WHERE worker_hash(%s)>=%ld AND worker_hash(%s)<=%ld) s",
					 shardinfo->distributionColumn, shardinfo->tablename,
					 quote_literal_cstr(shardinfo->tablename),
					 shardinfo->distributionColumn, shardinfo->shardminvalue,
					 shardinfo->distributionColumn, shardinfo->shardmaxvalue
					 );
	ereport(LOG, errmsg("%s", AvgHashQuery->data));
	SPI_connect();
	SPI_exec(AvgHashQuery->data, 0);
	SPITupleTable *tupletable = SPI_tuptable;
	HeapTuple tuple = tupletable->vals[0];
	bool isnull;
	Datum average = SPI_getbinval(tuple, tupletable->tupdesc, 1, &isnull);
	int64 IsResultNull = 1;
	if (!isnull)
	{
		IsResultNull = 0;
	}
	SPI_freetuptable(tupletable);
	SPI_finish();

	if (IsResultNull == 0)
	{
		return DatumGetInt64(average);
	}
	else
	{
		return shardinfo->shardminvalue - 1;
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
	 * The inner query for extracting the tenant value having frequency > 0.3 is executed on
	 * every shard of the table and outer query gives the shardid and tenant values as the
	 * output.
	 */
	appendStringInfo(CommonValueQuery,
					 "SELECT shardid , unnest(result::%s[]) from run_command_on_shards(%s,$$SELECT array_agg(val)"
					 " FROM pg_stats s , unnest(most_common_vals::text::%s[],most_common_freqs) as res(val,freq)"
					 " WHERE tablename = %s AND attname = %s AND freq > 0.2 $$)"
					 " WHERE result <> '' AND shardid = %ld;",
					 shardinfo->datatype, quote_literal_cstr(shardinfo->tablename),
					 shardinfo->datatype,
					 quote_literal_cstr(shardinfo->shardname),
					 quote_literal_cstr(shardinfo->distributionColumn),
					 shardinfo->shardid);

	ereport(LOG, errmsg("%s", CommonValueQuery->data));
	List *SplitPoints = NULL;
	MemoryContext originalContext = CurrentMemoryContext;
	SPI_connect();
	SPI_exec(CommonValueQuery->data, 0);
	MemoryContext spiContext = CurrentMemoryContext;
	int64 rowCount = SPI_processed;
	int64 average, hashedValue;
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
			ereport(LOG, errmsg("%ld", hashedValue));
			MemoryContextSwitchTo(originalContext);
			if (hashedValue == shardinfo->shardminvalue)
			{
				SplitPoints = lappend(SplitPoints, Int64GetDatum(hashedValue));
			}
			else if (hashedValue == shardinfo->shardmaxvalue)
			{
				SplitPoints = lappend(SplitPoints, Int64GetDatum(hashedValue - 1));
			}
			else
			{
				SplitPoints = lappend(SplitPoints, Int64GetDatum(hashedValue - 1));
				SplitPoints = lappend(SplitPoints, Int64GetDatum(hashedValue));
			}
			MemoryContextSwitchTo(spiContext);
		}
		SPI_freetuptable(tupletable);
	}
	else
	{
		average = ExecuteAvgHashQuery(shardinfo);
		ereport(LOG, errmsg("%ld", average));
	}

	SPI_finish();

	if (rowCount > 0)
	{
		list_sort(SplitPoints, list_int_cmp);
	}
	else
	{
		if (shardinfo->shardminvalue <= average)
		{
			SplitPoints = lappend(SplitPoints, Int64GetDatum(average));
		}
	}

	return SplitPoints;
}


/*
 * This function calculates the split points of the shard to
 * split and then executes the background job for the shard split.
 */
void
ScheduleShardSplit(ShardInfo shardinfo)
{
	List *SplitPoints = FindShardSplitPoints(shardinfo);
	if (list_length(SplitPoints) > 0)
	{
		ErrorOnConcurrentOperation();
		int64 jobId = CreateBackgroundJob("Automatic Shard Split",
										  "Split using SplitPoints List");
		ExecuteSplitBackgroundJob(jobId, shardinfo, SplitPoints);
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
	StringInfo query = makeStringInfo();

	/* This query is written to group the shards on the basis of colocation id and shardminvalue and get the groups whose sum of shardsize
	 * are greater than a threshold and than extract the shard in them which has the maximum size. So for that first pg_dist_shard and citus_shards are joined followed by the joining of pg_dist_node
	 * and citus_shards and finally joined by the table obtained by the grouping of colocation id and shardminvalue and shardsize exceeding the threshold.*/

	appendStringInfo(
		query,
		" SELECT cs.shardid,pd.shardminvalue,pd.shardmaxvalue,cs.shard_size,pn.nodeid,ct.distribution_column,ct.table_name,cs.shard_name,(SELECT relname FROM pg_class WHERE oid = (ct.table_name::regclass)::oid) "
		" FROM pg_catalog.pg_dist_shard pd JOIN pg_catalog.citus_shards cs ON pd.shardid = cs.shardid JOIN pg_catalog.pg_dist_node pn ON cs.nodename = pn.nodename AND cs.nodeport= pn.nodeport"
		" JOIN"
		" ( select shardid , max_size from (SELECT distinct first_value(shardid) OVER w as shardid, sum(shard_size) OVER (PARTITION BY colocation_id, shardminvalue) as total_sum, max(shard_size) OVER w as max_size"
		" FROM citus_shards cs JOIN pg_dist_shard ps USING(shardid)"
		" WINDOW w AS (PARTITION BY colocation_id, shardminvalue ORDER BY shard_size DESC) )as t where total_sum >= %ld )"
		" AS max_sizes ON cs.shardid=max_sizes.shardid AND cs.shard_size = max_sizes.max_size JOIN citus_tables ct ON cs.table_name = ct.table_name AND pd.shardminvalue <> pd.shardmaxvalue AND pd.shardminvalue <> ''",
		MaxShardSize
		);

	ereport(LOG, errmsg("%s", query->data));

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

		Datum shardId = SPI_getbinval(tuple, tupletable->tupdesc, 1, &isnull);
		shardinfo.shardid = DatumGetInt64(shardId);

		Datum shardSize = SPI_getbinval(tuple, tupletable->tupdesc, 4, &isnull);
		shardinfo.shardsize = DatumGetInt64(shardSize);

		Datum nodeId = SPI_getbinval(tuple, tupletable->tupdesc, 5, &isnull);
		shardinfo.nodeid = DatumGetInt64(nodeId);

		char *shardMinVal = SPI_getvalue(tuple, tupletable->tupdesc, 2);
		shardinfo.shardminvalue = strtoi64(shardMinVal, NULL, 10);

		char *shardMaxVal = SPI_getvalue(tuple, tupletable->tupdesc, 3);
		shardinfo.shardmaxvalue = strtoi64(shardMaxVal, NULL, 10);

		shardinfo.distributionColumn = SPI_getvalue(tuple, tupletable->tupdesc, 6);
		shardinfo.tablename = SPI_getvalue(tuple, tupletable->tupdesc, 7);

		StringInfo shardnameQuery = makeStringInfo();
		appendStringInfo(shardnameQuery, "%s_%ld", SPI_getvalue(tuple,
																tupletable->tupdesc, 9),
						 shardinfo.shardid);
		shardinfo.shardname = shardnameQuery->data;

		Datum tableIdDatum = SPI_getbinval(tuple, tupletable->tupdesc, 7, &isnull);
		shardinfo.tableId = DatumGetObjectId(tableIdDatum);
		shardinfo.distributionColumnId = ColumnTypeIdForRelationColumnName(
			shardinfo.tableId,
			shardinfo.
			distributionColumn);
		shardinfo.datatype = format_type_be(shardinfo.distributionColumnId);

		char *shardSplitMode;

		ScheduleShardSplit(&shardinfo);
		ereport(LOG, (errmsg(
						  "Shard ID: %ld,ShardMinValue: %ld, ShardMaxValue: %ld , totalSize: %ld , nodeId: %ld",
						  shardinfo.shardid, shardinfo.shardminvalue,
						  shardinfo.shardmaxvalue,
						  shardinfo.shardsize, shardinfo.nodeid)));
	}

	SPI_freetuptable(tupletable);
	SPI_finish();

	PG_RETURN_VOID();
}
