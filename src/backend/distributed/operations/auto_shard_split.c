#include "postgres.h"
#include "libpq-fe.h"
#include "executor/spi.h"
#include <math.h>
#include "distributed/pg_version_constants.h"
#include "access/htup_details.h"
#include "access/genam.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "commands/dbcommands.h"
#include "commands/sequence.h"
#include "distributed/argutils.h"
#include "distributed/background_jobs.h"
#include "distributed/citus_safe_lib.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/connection_management.h"
#include "distributed/listutils.h"
#include "distributed/lock_graph.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_logical_replication.h"
#include "distributed/multi_progress.h"
#include "distributed/multi_server_executor.h"
#include "distributed/pg_dist_rebalance_strategy.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/reference_table_utils.h"
#include "distributed/remote_commands.h"
#include "distributed/resource_lock.h"
#include "distributed/shard_rebalancer.h"
#include "distributed/shard_cleaner.h"
#include "distributed/shard_transfer.h"
#include "distributed/tuplestore.h"
#include "distributed/utils/array_type.h"
#include "distributed/worker_protocol.h"
#include "nodes/pg_list.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/syscache.h"
#include "common/hashfn.h"
#include "utils/varlena.h"
#include "utils/guc_tables.h"
#include "distributed/commands/utility_hook.h"


#define MAX_SHARD_SIZE  20000
PG_FUNCTION_INFO_V1(citus_auto_shard_split_start); 

StringInfo GetSplitQuery(int64 shardminvalue , int64 shardmaxvalue , int64 nodeid , int64 shardid){

    int64 midpoint = shardminvalue + ((shardmaxvalue - shardminvalue) >> 1);
    StringInfo query = makeStringInfo();
    appendStringInfo(query,"SELECT citus_split_shard_by_split_points(%ld, ARRAY['%ld'], ARRAY[%ld, %ld], 'block_writes')",
        shardid, midpoint, nodeid, nodeid);      
    return query;

}


void CitusGetShardData(){
    

    StringInfo query = makeStringInfo();
    bool isnull;

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

    if(SPI_connect() != SPI_OK_CONNECT){
        elog(ERROR,"SPI_connect to the query failed");
    }
    if(SPI_exec(query->data,0) != SPI_OK_SELECT){
        elog(ERROR,"SPI_exec for the execution failed");
    }

    SPITupleTable *tupletable = SPI_tuptable;
    int rowCount = SPI_processed;
    int64** shardInfo = (int64**)palloc(rowCount * sizeof(int64*));
    for (int i = 0; i < rowCount; i++) {
        shardInfo[i] = (int64*)palloc(5 * sizeof(int64));
    }
    
    int64 jobId = CreateBackgroundJob("auto split", "Automatic Split Shards having Size greater than threshold");

    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
        HeapTuple tuple = tupletable->vals[rowIndex];

        Datum shardId = SPI_getbinval(tuple,tupletable->tupdesc,1,&isnull);
        shardInfo[rowIndex][0]= DatumGetInt64(shardId);

		Datum shardSize = SPI_getbinval(tuple,tupletable->tupdesc,4,&isnull);
        shardInfo[rowIndex][1]= DatumGetInt64(shardSize);

		Datum nodeId = SPI_getbinval(tuple,tupletable->tupdesc,5,&isnull);
        shardInfo[rowIndex][2]= DatumGetInt64(nodeId);

        char *shardMinVal = SPI_getvalue(tuple,tupletable->tupdesc,2);
        shardInfo[rowIndex][3]= strtoi64(shardMinVal,NULL,10);

        char *shardMaxVal = SPI_getvalue(tuple,tupletable->tupdesc,3);
        shardInfo[rowIndex][4]= strtoi64(shardMaxVal,NULL,10);

        ereport(LOG, (errmsg("Shard ID: %ld,ShardMinValue: %ld, ShardMaxValue: %ld , totalSize: %ld , nodeId: %ld", shardInfo[rowIndex][0],shardInfo[rowIndex][3],shardInfo[rowIndex][4],shardInfo[rowIndex][1],shardInfo[rowIndex][2])));
           

        
	}

    SPI_freetuptable(tupletable);

    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
    {   
        StringInfo splitquery = NULL;
        splitquery = GetSplitQuery(shardInfo[rowIndex][3],shardInfo[rowIndex][4],shardInfo[rowIndex][2],shardInfo[rowIndex][0]);
        ereport(LOG, (errmsg(splitquery->data))); 
        StringInfoData buf = { 0 };
	    initStringInfo(&buf);
        appendStringInfo(&buf, splitquery);
        int32 nodesInvolved[] = { 0 };
		Oid superUserId = CitusExtensionOwner();
		BackgroundTask *task = ScheduleBackgroundTask(jobId, superUserId, buf.data, 0,
													  NULL, 0, nodesInvolved);    
        
    }

   
    SPI_finish();
    
}

Datum
citus_auto_shard_split_start(PG_FUNCTION_ARGS){
    CitusGetShardData();
    PG_RETURN_VOID();

}