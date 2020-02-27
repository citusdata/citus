-- when using distributed functions in mx for function call delegation you could define
-- functions that have references to distributed tables in their signature or declare
-- blocks.
-- This has caused issues with adding nodes to the cluster as functions will be
-- distributed to the new node before shards or metadata gets synced to the new now,
-- causing referenced tables to not be available during dependency distribution time,
SET citus.next_shard_id TO 20060000;
CREATE SCHEMA function_table_reference;
SET search_path TO function_table_reference;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO streaming;

SELECT start_metadata_sync_to_node(:'worker_1_host', :worker_1_port);
SELECT start_metadata_sync_to_node(:'worker_2_host', :worker_2_port);

-- SET citus.log_remote_commands TO on;
-- SET client_min_messages TO log;

-- remove worker 2, so we can add it after we have created some functions that caused
-- problems
SELECT master_remove_node(:'worker_2_host', :worker_2_port);

-- reproduction case as described in #3378
CREATE TABLE zoop_table (x int, y int);
SELECT create_distributed_table('zoop_table','x');

-- Create a function that refers to the distributed table
CREATE OR REPLACE FUNCTION zoop(a int)
    RETURNS int
    LANGUAGE plpgsql

    -- setting the search path makes the table name resolve on the worker during initial
    -- distribution
    SET search_path FROM CURRENT
AS $$
DECLARE
    b zoop_table.x%TYPE := 3;
BEGIN
    return a + b;
END;
$$;
SELECT create_distributed_function('zoop(int)', '$1');

-- now add the worker back, this triggers function distribution which should not fail.
SELECT 1 FROM master_add_node(:'worker_2_host', :worker_2_port);
SELECT public.wait_until_metadata_sync();


-- clean up after testing
DROP SCHEMA function_table_reference CASCADE;

-- make sure the worker is added at the end irregardless of anything failing to not make
-- subsequent tests fail as well. All artifacts created during this test should have been
-- dropped by the drop cascade above.
SELECT 1 FROM master_add_node(:'worker_2_host', :worker_2_port);
