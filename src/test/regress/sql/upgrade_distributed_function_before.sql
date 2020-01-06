CREATE SCHEMA upgrade_distributed_function_before;
SET search_path TO upgrade_distributed_function_before, public;
SET citus.replication_model TO streaming;
SET citus.shard_replication_factor TO 1;

CREATE TABLE t1 (a int PRIMARY KEY, b int);
SELECT create_distributed_table('t1','a');
INSERT INTO t1 VALUES (11), (12);

-- create a very simple distributed function colocated with the table
CREATE FUNCTION count_values(input int) RETURNS int AS
$$
    DECLARE
       cnt int := 0;
    BEGIN
        SELECT count(*) INTO cnt FROM upgrade_distributed_function_before.t1 WHERE a = $1;
        RETURN cnt;
    END;
$$ LANGUAGE plpgsql;
SELECT create_distributed_function('count_values(int)', '$1', colocate_with:='t1');

-- make sure that the metadata synced before running the queries
SELECT wait_until_metadata_sync();
SELECT bool_and(metadatasynced) FROM pg_dist_node WHERE isactive AND noderole = 'primary';
SET client_min_messages TO DEBUG1;

SELECT count_values(11);
SELECT count_values(12);
