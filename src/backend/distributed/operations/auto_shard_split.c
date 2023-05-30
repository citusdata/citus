#include "postgres.h"
#include "libpq-fe.h"
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
#include "distributed/enterprise.h"
#include "distributed/hash_helpers.h"
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

#define MAX_SHARD_SIZE  1
PG_FUNCTION_INFO_V1(citus_auto_shard_split_start); 

void Citus_Table_List(void)
{
	Assert(CitusHasBeenLoaded() && CheckCitusVersion(WARNING));

	/* first, we need to iterate over pg_dist_partition */
	List *citusTableIdList = CitusTableTypeIdList(DISTRIBUTED_TABLE);

	Oid relationId = InvalidOid;
	foreach_oid(relationId, citusTableIdList)
	{
		CitusTableCacheEntry *cacheEntry = GetCitusTableCacheEntry(relationId);
        Oid tempRelationId = cacheEntry->relationId;
        List *tuplesFromEachDistributedTable =LookupDistShardTuples(tempRelationId);
        HeapTuple tempTuple;

        ListCell* cell;
        foreach(cell, tuplesFromEachDistributedTable) {
            tempTuple = lfirst(cell);
            Form_pg_dist_shard distShard = (Form_pg_dist_shard)GETSTRUCT(tempTuple);
            ShardInterval *shardInterval = LoadShardInterval(distShard->shardid);
            int32 shardMinValue = DatumGetInt32(shardInterval->minValue);
            int32 shardMaxValue = DatumGetInt32(shardInterval->maxValue);

            ereport(LOG, (errmsg("Shard ID: %d,ShardMinValue: %d, ShardMaxValue: %d", distShard->shardid,shardMinValue,shardMaxValue)));
        }
        
       
        
    }
}

void Citus_get_Data_by_view(){
    bool missingOk = false;
    int connectionFlags = 0;
    WorkerNode *workerNode = FindNodeWithNodeId(1, missingOk);
    MultiConnection *connection = GetNodeConnection(connectionFlags,
													workerNode->workerName,
													workerNode->workerPort);

    StringInfo query = makeStringInfo();
    appendStringInfoString(
		query,
		" SELECT s.*, c.shard_size, n.nodeid, c.nodeport"
		" FROM pg_dist_shard s JOIN citus_shards c ON s.shardid=c.shardid"
        " JOIN pg_dist_node n ON c.nodeport=n.nodeport"
    );

    PGresult *result = NULL;
	int queryResult = ExecuteOptionalRemoteCommand(connection, query->data, &result);

    int rowCount = PQntuples(result);
	int colCount = PQnfields(result);

    for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
	{
		char *shardIdString = PQgetvalue(result, rowIndex, 1);
		uint64 shardId = strtou64(shardIdString, NULL, 10);

		char *sizeString = PQgetvalue(result, rowIndex, 5);
		uint64 totalSize = strtou64(sizeString, NULL, 10);

        char *nodeIdString = PQgetvalue(result, rowIndex, 6);
        uint64 nodeId = strtou64(nodeIdString, NULL, 10);

        char *minValueString = PQgetvalue(result, rowIndex, 3);
		uint64 shardMinValue = strtou64(minValueString, NULL, 10);

        char *maxValueString= PQgetvalue(result, rowIndex, 4);
		uint64 shardMaxValue = strtou64(maxValueString, NULL, 10);

        ereport(LOG, (errmsg("Shard ID: %d,ShardMinValue: %d, ShardMaxValue: %d , totalSize: %d , nodeId: %d", shardId,shardMinValue,shardMaxValue,totalSize,nodeId)));

	}


}

Datum
citus_auto_shard_split_start(PG_FUNCTION_ARGS){
   // Citus_Table_List();
    Citus_get_Data_by_view();
    PG_RETURN_VOID();

}