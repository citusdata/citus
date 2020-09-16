CREATE SCHEMA insert_select_connection_leak;
SET search_path TO 'insert_select_connection_leak';

SET citus.next_shard_id TO 4213581;
SET citus.shard_count TO 64;

CREATE OR REPLACE FUNCTION
worker_connection_count(nodeport int)
RETURNS int AS $$
  SELECT result::int - 1 FROM
  run_command_on_workers($Q$select count(*) from pg_stat_activity where backend_type = 'client backend';$Q$)
  WHERE nodeport = nodeport
$$ LANGUAGE SQL;

CREATE TABLE source_table(a int, b int);
SELECT create_distributed_table('source_table', 'a');

CREATE TABLE target_table(a numeric, b int not null);
SELECT create_distributed_table('target_table', 'a');

INSERT INTO source_table SELECT i, 2 * i FROM generate_series(1, 100) i;

EXPLAIN (costs off) INSERT INTO target_table SELECT * FROM source_table;

SELECT worker_connection_count(:worker_1_port) AS pre_xact_worker_1_connections,
       worker_connection_count(:worker_2_port) AS pre_xact_worker_2_connections \gset

BEGIN;
INSERT INTO target_table SELECT * FROM source_table;
SELECT worker_connection_count(:worker_1_port) AS worker_1_connections,
       worker_connection_count(:worker_2_port) AS worker_2_connections \gset
INSERT INTO target_table SELECT * FROM source_table;
INSERT INTO target_table SELECT * FROM source_table;
INSERT INTO target_table SELECT * FROM source_table;
INSERT INTO target_table SELECT * FROM source_table;
SELECT worker_connection_count(:worker_1_port) - :worker_1_connections AS leaked_worker_1_connections,
       worker_connection_count(:worker_2_port) - :worker_2_connections AS leaked_worker_2_connections;
END;

SELECT worker_connection_count(:worker_1_port) - :pre_xact_worker_1_connections AS leaked_worker_1_connections,
       worker_connection_count(:worker_2_port) - :pre_xact_worker_2_connections AS leaked_worker_2_connections;

-- ROLLBACK
BEGIN;
INSERT INTO target_table SELECT * FROM source_table;
INSERT INTO target_table SELECT * FROM source_table;
ROLLBACK;

SELECT worker_connection_count(:worker_1_port) - :pre_xact_worker_1_connections AS leaked_worker_1_connections,
       worker_connection_count(:worker_2_port) - :pre_xact_worker_2_connections AS leaked_worker_2_connections;

\set VERBOSITY TERSE

-- Error on constraint failure
BEGIN;
INSERT INTO target_table SELECT * FROM source_table;
SELECT worker_connection_count(:worker_1_port) AS worker_1_connections,
       worker_connection_count(:worker_2_port) AS worker_2_connections \gset
SAVEPOINT s1;
INSERT INTO target_table SELECT a, CASE WHEN a < 50 THEN b ELSE null END  FROM source_table;
ROLLBACK TO SAVEPOINT s1;
SELECT worker_connection_count(:worker_1_port) - :worker_1_connections AS leaked_worker_1_connections,
       worker_connection_count(:worker_2_port) - :worker_2_connections AS leaked_worker_2_connections;
END;

SELECT worker_connection_count(:worker_1_port) - :pre_xact_worker_1_connections AS leaked_worker_1_connections,
       worker_connection_count(:worker_2_port) - :pre_xact_worker_2_connections AS leaked_worker_2_connections;

SET client_min_messages TO WARNING;
DROP SCHEMA insert_select_connection_leak CASCADE;
