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


PG_FUNCTION_INFO_V1(citus_auto_shard_split_start); 


typedef struct ShardInfoData
{
    int64 shardsize;
    int64 shardminvalue;
    int64 shardmaxvalue;
    int64 shardid ;
    int64 nodeid ;

}ShardInfoData;

typedef ShardInfoData *shardInfo;

StringInfo 
GetSplitQuery(int64 shardminvalue , int64 shardmaxvalue , int64 nodeid , int64 shardid)
{

    int64 midpoint = shardminvalue + ((shardmaxvalue - shardminvalue) >> 1);
    StringInfo query = makeStringInfo();
    appendStringInfo(query,"SELECT citus_split_shard_by_split_points(%ld, ARRAY['%ld'], ARRAY[%ld,%ld], 'block_writes')",
        shardid, midpoint, nodeid, nodeid);      
    return query;

}

void
ExecuteBackgroundJob(int64 jobid ,int64 shardminvalue ,int64 shardmaxvalue ,int64 nodeid, int64 shardid)
{
        StringInfo splitquery = NULL;
        splitquery = GetSplitQuery(shardminvalue,shardmaxvalue,nodeid,shardid);
        ereport(LOG, (errmsg(splitquery->data))); 
        int32 nodesInvolved[1] ;
        nodesInvolved[0]= nodeid;
		Oid superUserId = CitusExtensionOwner();
		BackgroundTask *task = ScheduleBackgroundTask(jobid, superUserId, splitquery->data, 0,
													  NULL, 1, nodesInvolved);


}

Datum
citus_auto_shard_split_start(PG_FUNCTION_ARGS)
{

       StringInfo query = makeStringInfo();

    /* This query is written to group the shards on the basis of colocation id and shardminvalue and get the shards with maximum shardsize
     which is greater than a threshold . So for that first pg_dist_shard and citus_shards are joined followed by the joining of pg_dist_node
     and citus_shards and finally joined by the table obtained by the grouping of colocation id and shardminvalue and shardsize exceeding the threshold.*/

    appendStringInfoString(
		query,
		" SELECT cs.shardid,pd.shardminvalue,pd.shardmaxvalue,cs.shard_size,pn.nodeid"
        " FROM pg_catalog.pg_dist_shard pd JOIN pg_catalog.citus_shards cs ON pd.shardid = cs.shardid JOIN pg_catalog.pg_dist_node pn ON cs.nodename = pn.nodename AND cs.nodeport = pn.nodeport"
        " JOIN ("
        " SELECT cs.colocation_id,pd.shardminvalue,MAX(cs.shard_size) AS max_shard_size"
        " FROM pg_catalog.citus_shards cs JOIN pg_catalog.pg_dist_shard pd ON cs.shardid = pd.shardid"
        " GROUP BY cs.colocation_id,pd.shardminvalue"
        " HAVING MAX(cs.shard_size) > 20000"
        " )AS max_sizes ON cs.colocation_id = max_sizes.colocation_id AND pd.shardminvalue = max_sizes.shardminvalue AND cs.shard_size = max_sizes.max_shard_size"

    );

    if(SPI_connect() != SPI_OK_CONNECT)
    {
        elog(ERROR,"SPI_connect to the query failed");
    }
    if(SPI_exec(query->data,0) != SPI_OK_SELECT)
    {
        elog(ERROR,"SPI_exec for the execution failed");
    }

    SPITupleTable *tupletable = SPI_tuptable;
    int rowCount = SPI_processed;
    int64 jobId = CreateBackgroundJob("auto split", "Automatic Split Shards having Size greater than threshold");
    bool isnull;
    
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{  
        shardInfo shardinfo = (shardInfo)palloc(sizeof(ShardInfoData));
        HeapTuple tuple = tupletable->vals[rowIndex];

        Datum shardId = SPI_getbinval(tuple,tupletable->tupdesc,1,&isnull);
        shardinfo->shardid = DatumGetInt64(shardId);

		Datum shardSize = SPI_getbinval(tuple,tupletable->tupdesc,4,&isnull);
        shardinfo->shardsize = DatumGetInt64(shardSize);

		Datum nodeId = SPI_getbinval(tuple,tupletable->tupdesc,5,&isnull);
        shardinfo->nodeid = DatumGetInt64(nodeId);

        char *shardMinVal = SPI_getvalue(tuple,tupletable->tupdesc,2);
        shardinfo->shardminvalue = strtoi64(shardMinVal,NULL,10);

        char *shardMaxVal = SPI_getvalue(tuple,tupletable->tupdesc,3);
        shardinfo->shardmaxvalue = strtoi64(shardMaxVal,NULL,10);

        ereport(LOG, (errmsg("Shard ID: %ld,ShardMinValue: %ld, ShardMaxValue: %ld , totalSize: %ld , nodeId: %ld", shardinfo->shardid,shardinfo->shardminvalue,shardinfo->shardmaxvalue,shardinfo->shardsize,shardinfo->nodeid)));

        ExecuteBackgroundJob(jobId,shardinfo->shardminvalue,shardinfo->shardmaxvalue,shardinfo->nodeid,shardinfo->shardid);  

        
	}
    SPI_freetuptable(tupletable);
    SPI_finish();
    
    PG_RETURN_VOID();

}