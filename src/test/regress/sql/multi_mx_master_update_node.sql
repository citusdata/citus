-- Test creation of mx tables and metadata syncing

SELECT nextval('pg_catalog.pg_dist_placement_placementid_seq') AS last_placement_id
\gset
SELECT nextval('pg_catalog.pg_dist_groupid_seq') AS last_group_id \gset
SELECT nextval('pg_catalog.pg_dist_node_nodeid_seq') AS last_node_id \gset
SELECT nextval('pg_catalog.pg_dist_colocationid_seq') AS last_colocation_id \gset
SELECT nextval('pg_catalog.pg_dist_shardid_seq') AS last_shard_id \gset


SET citus.replication_model TO streaming;
SET citus.shard_count TO 8;
SET citus.shard_replication_factor TO 1;

-- set sync intervals to less than 15s so wait_until_metadata_sync never times out
ALTER SYSTEM SET citus.metadata_sync_interval TO 3000;
ALTER SYSTEM SET citus.metadata_sync_retry_interval TO 500;
SELECT pg_reload_conf();

CREATE FUNCTION wait_until_metadata_sync(timeout INTEGER DEFAULT 15000)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';

-- Verifies pg_dist_node and pg_dist_palcement in the given worker matches the ones in coordinator
CREATE FUNCTION verify_metadata(hostname TEXT, port INTEGER, master_port INTEGER DEFAULT 57636)
    RETURNS BOOLEAN
    LANGUAGE sql
    AS $$
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

-- Simulates a readonly node by setting default_transaction_read_only. 
CREATE FUNCTION mark_node_readonly(hostname TEXT, port INTEGER, isreadonly BOOLEAN)
    RETURNS TEXT
    LANGUAGE sql
    AS $$
    SELECT master_run_on_worker(ARRAY[hostname], ARRAY[port],
           ARRAY['ALTER SYSTEM SET default_transaction_read_only TO ' || isreadonly::TEXT], false);
    SELECT result FROM
        master_run_on_worker(ARRAY[hostname], ARRAY[port],
                             ARRAY['SELECT pg_reload_conf()'], false);
$$;

-- add a node to the cluster
SELECT master_add_node('localhost', :worker_1_port) As nodeid_1 \gset
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;

-- create couple of tables
CREATE TABLE ref_table(a int primary key);
SELECT create_reference_table('ref_table');

CREATE TABLE dist_table_1(a int primary key, b int references ref_table(a));
SELECT create_distributed_table('dist_table_1', 'a');

-- update the node
SELECT 1 FROM master_update_node((SELECT nodeid FROM pg_dist_node),
                                 'localhost', :worker_2_port);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;

-- start syncing metadata to the node
SELECT 1 FROM start_metadata_sync_to_node('localhost', :worker_2_port);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;

--------------------------------------------------------------------------
-- Test that maintenance daemon syncs after master_update_node
--------------------------------------------------------------------------

-- Update the node again. We do this as epeatable read, so we just see the
-- changes by master_update_node(). This is to avoid inconsistent results
-- if the maintenance daemon does the metadata sync too fast.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', :worker_1_port);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
END;

-- wait until maintenance daemon does the next metadata sync, and then
-- check if metadata is synced again
SELECT wait_until_metadata_sync();
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node;

SELECT verify_metadata('localhost', :worker_1_port);

-- Update the node to a non-existent node. This is to simulate updating to
-- a unwriteable node.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', 12345);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
END;

-- maintenace daemon metadata sync should fail, because node is still unwriteable.
SELECT wait_until_metadata_sync();
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node;

-- update it back to :worker_1_port, now metadata should be synced
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', :worker_1_port);
SELECT wait_until_metadata_sync();
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node;

--------------------------------------------------------------------------
-- Test updating a node when another node is in readonly-mode
--------------------------------------------------------------------------

SELECT FROM master_add_node('localhost', :worker_2_port) AS nodeid_2 \gset
SELECT 1 FROM start_metadata_sync_to_node('localhost', :worker_2_port);

-- Create a table with shards on both nodes
CREATE TABLE dist_table_2(a int);
SELECT create_distributed_table('dist_table_2', 'a');
INSERT INTO dist_table_2 SELECT i FROM generate_series(1, 100) i;

SELECT mark_node_readonly('localhost', :worker_2_port, TRUE);

-- Now updating the other node should try syncing to worker 2, but instead of
-- failure, it should just warn and mark the readonly node as not synced.
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', 12345);
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node ORDER BY nodeid;

-- worker_2 is out of sync, so further updates aren't sent to it and
-- we shouldn't see the warnings.
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', 23456);
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node ORDER BY nodeid;

-- Make the node writeable.
SELECT mark_node_readonly('localhost', :worker_2_port, FALSE);
SELECT wait_until_metadata_sync();

-- Mark the node readonly again, so the following master_update_node warns
SELECT mark_node_readonly('localhost', :worker_2_port, TRUE);

-- Revert the nodeport of worker 1, metadata propagation to worker 2 should
-- still fail, but after the failure, we should still be able to read from
-- worker 2 in the same transaction!
BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', :worker_1_port);
SELECT count(*) FROM dist_table_2;
END;

SELECT wait_until_metadata_sync();

-- Make the node writeable.
SELECT mark_node_readonly('localhost', :worker_2_port, FALSE);
SELECT wait_until_metadata_sync();

SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', :worker_1_port);
SELECT verify_metadata('localhost', :worker_1_port),
       verify_metadata('localhost', :worker_2_port);

--------------------------------------------------------------------------
-- Test that master_update_node rolls back properly
--------------------------------------------------------------------------
BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, 'localhost', 12345);
ROLLBACK;

SELECT verify_metadata('localhost', :worker_1_port),
       verify_metadata('localhost', :worker_2_port);

-- cleanup
DROP TABLE dist_table_1, ref_table, dist_table_2;
TRUNCATE pg_dist_colocation;
SELECT count(*) FROM (SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node) t;
ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART :last_group_id;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART :last_node_id;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART :last_colocation_id;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART :last_placement_id;
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART :last_shard_id;

RESET citus.shard_count;
RESET citus.shard_replication_factor;
RESET citus.replication_model;
