CREATE OR REPLACE FUNCTION wait_until_metadata_sync(timeout INTEGER DEFAULT 15000)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';

-- set sync intervals to less than 15s so wait_until_metadata_sync never times out
ALTER SYSTEM SET citus.metadata_sync_interval TO 3000;
ALTER SYSTEM SET citus.metadata_sync_retry_interval TO 500;
SELECT pg_reload_conf();

-- Verifies pg_dist_node and pg_dist_palcement in the given worker matches the ones in coordinator
CREATE OR REPLACE FUNCTION verify_metadata(hostname TEXT, port INTEGER, master_port INTEGER DEFAULT 57636)
    RETURNS BOOLEAN
    LANGUAGE sql
    AS $$
SELECT wait_until_metadata_sync();
WITH dist_node_summary AS (
    SELECT 'SELECT jsonb_agg(ROW(nodeid, groupid, nodename, nodeport, isactive) ORDER BY nodeid) FROM  pg_dist_node' as query
), dist_node_check AS (
    SELECT count(distinct result) = 1 AS matches
    FROM dist_node_summary CROSS JOIN LATERAL
        master_run_on_worker(ARRAY[hostname, 'localhost'], ARRAY[port, master_port],
                            ARRAY[dist_node_summary.query, dist_node_summary.query],
                            false)
), dist_placement_summary AS (
    SELECT 'SELECT jsonb_agg(pg_dist_placement ORDER BY shardid) FROM pg_dist_placement)' AS query
), dist_placement_check AS (
    SELECT count(distinct result) = 1 AS matches
    FROM dist_placement_summary CROSS JOIN LATERAL
        master_run_on_worker(ARRAY[hostname, 'localhost'], ARRAY[port, master_port],
                            ARRAY[dist_placement_summary.query, dist_placement_summary.query],
                            false)
)
SELECT dist_node_check.matches AND dist_placement_check.matches
FROM dist_node_check CROSS JOIN dist_placement_check
$$;


-- partition_task_list_results tests the internal PartitionTasklistResults function
CREATE OR REPLACE FUNCTION pg_catalog.partition_task_list_results(resultIdPrefix text,
                                                                  query text,
                                                                  target_table regclass,
                                                                  binaryFormat bool DEFAULT true)
    RETURNS TABLE(resultId text,
                  nodeId int,
                  rowCount bigint,
                  targetShardId bigint,
                  targetShardIndex int)
    LANGUAGE C STRICT VOLATILE
    AS 'citus', $$partition_task_list_results$$;
