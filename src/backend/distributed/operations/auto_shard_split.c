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


PG_FUNCTION_INFO_V1(citus_auto_shard_split_start); 

int MaxShardSize = 104857600;

typedef struct ShardInfoData
{
    int64 shardsize;
    int64 shardminvalue;
    int64 shardmaxvalue;
    int64 shardid ;
    int64 nodeid ;
    char *tablename;
    char *distributionColumn;
    char *datatype;

}ShardInfoData;
typedef ShardInfoData *ShardInfo;



StringInfo 
GetShardSplitQuery(ShardInfo shardinfo, int64 averagehash)
{
    StringInfo hashsplitquery = makeStringInfo();
    appendStringInfo(hashsplitquery,"SELECT citus_split_shard_by_split_points(%ld, ARRAY['%ld'], ARRAY[%ld,%ld], 'block_writes')",
        shardinfo->shardid, averagehash, shardinfo->nodeid, shardinfo->nodeid);      
    return hashsplitquery;
}



int64
ExecuteAvgHashQuery(ShardInfo shardinfo )
{

    StringInfo AvgHashQuery = makeStringInfo();
    appendStringInfo(AvgHashQuery, "SELECT avg(h)::int,count(*)"
    " FROM (SELECT worker_hash(%s) h FROM %s tablesample system(10)"
    " WHERE worker_hash(%s)>=%ld AND worker_hash(%s)<=%ld) s"
    , shardinfo->distributionColumn, shardinfo->tablename , shardinfo->distributionColumn , shardinfo->shardminvalue , shardinfo->distributionColumn , shardinfo->shardmaxvalue
    );
    ereport(LOG,errmsg("%s",AvgHashQuery->data));
    SPI_connect();
    SPI_exec(AvgHashQuery->data,0);
    SPITupleTable *tupletable = SPI_tuptable;
    HeapTuple tuple = tupletable->vals[0];
    bool isnull;
    Datum average = SPI_getbinval(tuple,tupletable->tupdesc,1,&isnull);
    int64 IsResultNull = 1;
    if(!isnull){
        IsResultNull=0;
    }
    SPI_freetuptable(tupletable);
    SPI_finish();

    if(IsResultNull==0){
        return DatumGetInt64(average);
    }else{
        return shardinfo->shardminvalue-1;
    }



}

void
ExecuteSplitBackgroundJob(int64 jobid, ShardInfo shardinfo , int64 averagehash)
{
    StringInfo splitquery = makeStringInfo();      
    splitquery = GetShardSplitQuery(shardinfo, averagehash);
    ereport(LOG, (errmsg(splitquery->data))); 
    int32 nodesInvolved[1] ;
    nodesInvolved[0]= shardinfo->nodeid;
    Oid superUserId = CitusExtensionOwner();
    BackgroundTask *task = ScheduleBackgroundTask(jobid, superUserId, splitquery->data, 0,
                                                    NULL, 1, nodesInvolved);


}


void 
ExecuteTenantIsolationBackgroundJob(char* tablename , char *commonValue , int64 nodeid , int64 jobid)
{
    StringInfo tenantIsolationQuery = makeStringInfo();
    appendStringInfo(tenantIsolationQuery,"SELECT isolate_tenant_to_new_shard('%s', '%s' , 'CASCADE', shard_transfer_mode => 'block_writes')",tablename ,commonValue);
    ereport(LOG,errmsg("%s",tenantIsolationQuery->data));
    int32 nodesInvolved[1] ;
    nodesInvolved[0]= nodeid;
	Oid superUserId = CitusExtensionOwner();
	BackgroundTask *task = ScheduleBackgroundTask(jobid, superUserId, tenantIsolationQuery->data, 0,
													  NULL, 1, nodesInvolved);


}

void 
ScheduleShardSplit(ShardInfo shardinfo)
{
    StringInfo CommonValueQuery = makeStringInfo();
    appendStringInfo(CommonValueQuery , "SELECT shardid , unnest(result::%s[]) from run_command_on_shards('%s',$$SELECT array_agg(val)"
    " FROM pg_stats s , unnest(most_common_vals::text::%s[],most_common_freqs) as res(val,freq)"
    " WHERE tablename = %%1$L AND attname = '%s' AND freq > 0.3 $$)"
    " WHERE result <> '' AND shardid = %ld;",shardinfo->datatype,shardinfo->tablename,shardinfo->datatype,shardinfo->distributionColumn,shardinfo->shardid);

    ereport(LOG,errmsg("%s",CommonValueQuery->data));
    SPI_connect();
    SPI_exec(CommonValueQuery->data,0);
    int64 rowCount = SPI_processed;
    ereport(LOG,errmsg("%ld",rowCount));

    if(rowCount>0){
        SPITupleTable *tupletable = SPI_tuptable;
        int64 jobId = CreateBackgroundJob("tenant isolation", "Isolating a tenant to new Shard");    
        for(int rowIndex = 0 ; rowIndex<rowCount ; rowIndex++ ){

            HeapTuple tuple = tupletable->vals[rowIndex];
            char* commonValue = SPI_getvalue(tuple,tupletable->tupdesc,2);
            ereport(LOG,errmsg("%s",commonValue));
            if(shardinfo->shardminvalue != shardinfo->shardmaxvalue){
             ExecuteTenantIsolationBackgroundJob(shardinfo->tablename,commonValue,shardinfo->nodeid,jobId);
            }

        }
        SPI_freetuptable(tupletable);

    }else{

        int64 jobId = CreateBackgroundJob("Average Hash Split", "Split using an average hash value");
        int64 average = ExecuteAvgHashQuery(shardinfo);
        ereport(LOG,errmsg("%ld",average));   
        if(average>=shardinfo->shardminvalue){     
           ExecuteSplitBackgroundJob(jobId, shardinfo, average);
        }
        

    }
    
    SPI_finish();
    
}

Datum
citus_auto_shard_split_start(PG_FUNCTION_ARGS)
{   
 
    StringInfo query = makeStringInfo();

    /* This query is written to group the shards on the basis of colocation id and shardminvalue and get the groups whose sum of shardsize
     are greater than a threshold and than extract the shard in them which has the maximum size. So for that first pg_dist_shard and citus_shards are joined followed by the joining of pg_dist_node
     and citus_shards and finally joined by the table obtained by the grouping of colocation id and shardminvalue and shardsize exceeding the threshold.*/

    appendStringInfo(
		query,
		" SELECT cs.shardid,pd.shardminvalue,pd.shardmaxvalue,cs.shard_size,pn.nodeid,ct.distribution_column,ct.table_name"
        " FROM pg_catalog.pg_dist_shard pd JOIN pg_catalog.citus_shards cs ON pd.shardid = cs.shardid JOIN pg_catalog.pg_dist_node pn ON cs.nodename = pn.nodename AND cs.nodeport= pn.nodeport" 
        " JOIN"
        " ( select shardid , max_size from (SELECT distinct first_value(shardid) OVER w as shardid, sum(shard_size) OVER w as sum_size, max(shard_size) OVER w as max_size"
        " FROM citus_shards cs JOIN pg_dist_shard ps USING(shardid)"
        " WINDOW w AS (PARTITION BY colocation_id, shardminvalue ORDER BY shard_size DESC) )as t where sum_size > %ld )"
        " AS max_sizes ON cs.shardid=max_sizes.shardid AND cs.shard_size = max_sizes.max_size JOIN citus_tables ct ON cs.table_name = ct.table_name"
        , MaxShardSize
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
    bool isnull;
    
    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{   

        ShardInfoData shardinfo ;
        HeapTuple tuple = tupletable->vals[rowIndex];

        Datum shardId = SPI_getbinval(tuple,tupletable->tupdesc,1,&isnull);
        shardinfo.shardid = DatumGetInt64(shardId);

		Datum shardSize = SPI_getbinval(tuple,tupletable->tupdesc,4,&isnull);
        shardinfo.shardsize = DatumGetInt64(shardSize);

		Datum nodeId = SPI_getbinval(tuple,tupletable->tupdesc,5,&isnull);
        shardinfo.nodeid = DatumGetInt64(nodeId);

        char *shardMinVal = SPI_getvalue(tuple,tupletable->tupdesc,2);
        shardinfo.shardminvalue = strtoi64(shardMinVal,NULL,10);

        char *shardMaxVal = SPI_getvalue(tuple,tupletable->tupdesc,3);
        shardinfo.shardmaxvalue = strtoi64(shardMaxVal,NULL,10);

        shardinfo.distributionColumn = SPI_getvalue(tuple,tupletable->tupdesc,6);
        shardinfo.tablename = SPI_getvalue(tuple,tupletable->tupdesc,7);

        Datum tableIdDatum = SPI_getbinval(tuple,tupletable->tupdesc,7,&isnull);
        Oid tableId = DatumGetObjectId(tableIdDatum);
        Oid distributionColumnId = ColumnTypeIdForRelationColumnName(tableId, shardinfo.distributionColumn);
        shardinfo.datatype = format_type_be(distributionColumnId);


         ScheduleShardSplit(&shardinfo);
        ereport(LOG, (errmsg("Shard ID: %ld,ShardMinValue: %ld, ShardMaxValue: %ld , totalSize: %ld , nodeId: %ld", shardinfo.shardid,shardinfo.shardminvalue,shardinfo.shardmaxvalue,shardinfo.shardsize,shardinfo.nodeid)));

 
        
	}
   
    SPI_freetuptable(tupletable);
    SPI_finish();
 
    PG_RETURN_VOID();

}