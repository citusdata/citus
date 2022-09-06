CREATE SCHEMA adaptive_executor;
SET search_path TO adaptive_executor;

CREATE TABLE test (x int, y int);

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 801009000;
SELECT create_distributed_table('test','x');
-- Add 1 row to each shard
SELECT get_shard_id_for_distribution_column('test', 1);
INSERT INTO test VALUES (1,2);
SELECT get_shard_id_for_distribution_column('test', 3);
INSERT INTO test VALUES (3,2);
SELECT get_shard_id_for_distribution_column('test', 6);
INSERT INTO test VALUES (8,2);
SELECT get_shard_id_for_distribution_column('test', 11);
INSERT INTO test VALUES (11,2);

-- Set a very high slow start to avoid opening parallel connections
SET citus.executor_slow_start_interval TO '60s';
SET citus.max_adaptive_executor_pool_size TO 2;

BEGIN;
SELECT count(*) FROM test a JOIN (SELECT x, pg_sleep(0.1) FROM test) b USING (x);
SELECT sum(result::bigint) FROM run_command_on_workers($$
  SELECT count(*) FROM pg_stat_activity
  WHERE pid <> pg_backend_pid() AND query LIKE '%8010090%'
$$);
END;

-- SELECT takes longer than slow start interval, should open multiple connections
SET citus.executor_slow_start_interval TO '10ms';

BEGIN;
SELECT count(*) FROM test a JOIN (SELECT x, pg_sleep(0.2) FROM test) b USING (x);
SELECT sum(result::bigint) FROM run_command_on_workers($$
  SELECT count(*) FROM pg_stat_activity
  WHERE pid <> pg_backend_pid() AND query LIKE '%8010090%'
$$);
END;

CREATE OR REPLACE FUNCTION select_for_update()
RETURNS void
AS $$
DECLARE
    my int;
BEGIN
    SELECT y INTO my FROM test WHERE x = 1 FOR UPDATE;
END;
$$ LANGUAGE plpgsql;

-- so that we can prove that we open a transaction block by logging below:
-- "NOTICE:  issuing BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;..."
SET citus.log_remote_commands TO on;

SELECT select_for_update();

SET citus.log_remote_commands TO off;

DROP SCHEMA adaptive_executor CASCADE;
