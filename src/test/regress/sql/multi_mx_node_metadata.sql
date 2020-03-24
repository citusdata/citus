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
SELECT master_add_node(:'worker_1_host', :worker_1_port) As nodeid_1 \gset
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;

-- create couple of tables
CREATE TABLE ref_table(a int primary key);
SELECT create_reference_table('ref_table');

CREATE TABLE dist_table_1(a int primary key, b int references ref_table(a));
SELECT create_distributed_table('dist_table_1', 'a');

-- update the node
SELECT 1 FROM master_update_node((SELECT nodeid FROM pg_dist_node),
                                 :'worker_2_host', :worker_2_port);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;

-- start syncing metadata to the node
SELECT 1 FROM start_metadata_sync_to_node(:'worker_2_host', :worker_2_port);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;

--------------------------------------------------------------------------
-- Test that maintenance daemon syncs after master_update_node
--------------------------------------------------------------------------

-- Update the node again. We do this as epeatable read, so we just see the
-- changes by master_update_node(). This is to avoid inconsistent results
-- if the maintenance daemon does the metadata sync too fast.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
SELECT 1 FROM master_update_node(:nodeid_1, :'worker_1_host', :worker_1_port);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
END;

-- wait until maintenance daemon does the next metadata sync, and then
-- check if metadata is synced again
SELECT wait_until_metadata_sync();
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node;

SELECT verify_metadata(:'worker_1_host', :worker_1_port);

-- Update the node to a non-existent node. This is to simulate updating to
-- a unwriteable node.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
SELECT 1 FROM master_update_node(:nodeid_1, :'worker_1_host', 12345);
SELECT nodeid, nodename, nodeport, hasmetadata, metadatasynced FROM pg_dist_node;
END;

-- maintenace daemon metadata sync should fail, because node is still unwriteable.
SELECT wait_until_metadata_sync();
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node;

-- update it back to :worker_1_port, now metadata should be synced
SELECT 1 FROM master_update_node(:nodeid_1, :'worker_1_host', :worker_1_port);
SELECT wait_until_metadata_sync();
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node;

--------------------------------------------------------------------------
-- Test updating a node when another node is in readonly-mode
--------------------------------------------------------------------------

SELECT master_add_node(:'worker_2_host', :worker_2_port) AS nodeid_2 \gset
SELECT 1 FROM start_metadata_sync_to_node(:'worker_2_host', :worker_2_port);

-- Create a table with shards on both nodes
CREATE TABLE dist_table_2(a int);
SELECT create_distributed_table('dist_table_2', 'a');
INSERT INTO dist_table_2 SELECT i FROM generate_series(1, 100) i;

SELECT mark_node_readonly(:'worker_2_host', :worker_2_port, TRUE);

-- Now updating the other node will mark worker 2 as not synced.
BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, :'worker_1_host', 12345);
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node ORDER BY nodeid;
COMMIT;

-- worker_2 is out of sync, so further updates aren't sent to it and
-- we shouldn't see the warnings.
SELECT 1 FROM master_update_node(:nodeid_1, :'worker_1_host', 23456);
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node ORDER BY nodeid;

-- Make the node writeable.
SELECT mark_node_readonly(:'worker_2_host', :worker_2_port, FALSE);
SELECT wait_until_metadata_sync();

-- Mark the node readonly again, so the following master_update_node warns
SELECT mark_node_readonly(:'worker_2_host', :worker_2_port, TRUE);

-- Revert the nodeport of worker 1.
BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, :'worker_1_host', :worker_1_port);
SELECT count(*) FROM dist_table_2;
END;

SELECT wait_until_metadata_sync();

-- Make the node writeable.
SELECT mark_node_readonly(:'worker_2_host', :worker_2_port, FALSE);
SELECT wait_until_metadata_sync();

SELECT 1 FROM master_update_node(:nodeid_1, :'worker_1_host', :worker_1_port);
SELECT verify_metadata(:'worker_1_host', :worker_1_port),
       verify_metadata(:'worker_2_host', :worker_2_port);

--------------------------------------------------------------------------
-- Test that master_update_node rolls back properly
--------------------------------------------------------------------------
BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, :'worker_1_host', 12345);
ROLLBACK;

SELECT verify_metadata(:'worker_1_host', :worker_1_port),
       verify_metadata(:'worker_2_host', :worker_2_port);

--------------------------------------------------------------------------
-- Test that master_update_node can appear in a prepared transaction.
--------------------------------------------------------------------------
BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, :'worker_1_host', 12345);
PREPARE TRANSACTION 'tx01';
COMMIT PREPARED 'tx01';

SELECT wait_until_metadata_sync();
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node ORDER BY nodeid;

BEGIN;
SELECT 1 FROM master_update_node(:nodeid_1, :'worker_1_host', :worker_1_port);
PREPARE TRANSACTION 'tx01';
COMMIT PREPARED 'tx01';

SELECT wait_until_metadata_sync();
SELECT nodeid, hasmetadata, metadatasynced FROM pg_dist_node ORDER BY nodeid;

SELECT verify_metadata(:'worker_1_host', :worker_1_port),
       verify_metadata(:'worker_2_host', :worker_2_port);

--------------------------------------------------------------------------
-- Test that changes in isactive is propagated to the metadata nodes
--------------------------------------------------------------------------
-- Don't drop the reference table so it has shards on the nodes being disabled
DROP TABLE dist_table_1, dist_table_2;

SELECT 1 FROM master_disable_node(:'worker_2_host', :worker_2_port);
SELECT verify_metadata(:'worker_1_host', :worker_1_port);

SELECT 1 FROM master_activate_node(:'worker_2_host', :worker_2_port);
SELECT verify_metadata(:'worker_1_host', :worker_1_port);

------------------------------------------------------------------------------------
-- Test master_disable_node() when the node that is being disabled is actually down
------------------------------------------------------------------------------------
SELECT master_update_node(:nodeid_2, :'worker_2_host', 1);
SELECT wait_until_metadata_sync();

-- set metadatasynced so we try porpagating metadata changes
UPDATE pg_dist_node SET metadatasynced = TRUE WHERE nodeid IN (:nodeid_1, :nodeid_2);

-- should error out
SELECT 1 FROM master_disable_node('localhost', 1);

-- try again after stopping metadata sync
SELECT stop_metadata_sync_to_node('localhost', 1);
SELECT 1 FROM master_disable_node('localhost', 1);

SELECT verify_metadata(:'worker_1_host', :worker_1_port);

SELECT master_update_node(:nodeid_2, :'worker_2_host', :worker_2_port);
SELECT wait_until_metadata_sync();

SELECT 1 FROM master_activate_node(:'worker_2_host', :worker_2_port);
SELECT verify_metadata(:'worker_1_host', :worker_1_port);


------------------------------------------------------------------------------------
-- Test master_disable_node() when the other node is down
------------------------------------------------------------------------------------
-- node 1 is down.
SELECT master_update_node(:nodeid_1, :'worker_1_host', 1);
SELECT wait_until_metadata_sync();

-- set metadatasynced so we try porpagating metadata changes
UPDATE pg_dist_node SET metadatasynced = TRUE WHERE nodeid IN (:nodeid_1, :nodeid_2);

-- should error out
SELECT 1 FROM master_disable_node(:'worker_2_host', :worker_2_port);

-- try again after stopping metadata sync
SELECT stop_metadata_sync_to_node('localhost', 1);
SELECT 1 FROM master_disable_node(:'worker_2_host', :worker_2_port);

-- bring up node 1
SELECT master_update_node(:nodeid_1, :'worker_1_host', :worker_1_port);
SELECT wait_until_metadata_sync();

SELECT 1 FROM master_activate_node(:'worker_2_host', :worker_2_port);

SELECT verify_metadata(:'worker_1_host', :worker_1_port);

-- cleanup
DROP TABLE ref_table;
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
