--
-- failure_task_tracker.sql
--
CREATE SCHEMA IF NOT EXISTS failure_task_tracker;
SET SEARCH_PATH = failure_task_tracker;
SET citus.shard_count TO 2;
SET citus.next_shard_id TO 1990000;
SET citus.shard_replication_factor TO 1;

SELECT pg_backend_pid() as pid \gset
SELECT citus.mitmproxy('conn.allow()');

CREATE TABLE t1 (
  id int PRIMARY KEY,
  data int
);

CREATE TABLE t2 (
  id  int PRIMARY KEY,
  ref_id int REFERENCES t1 (id)
);

SELECT create_distributed_table('t1', 'id');
SELECT create_distributed_table('t2', 'id');


INSERT INTO t1 SELECT x, x FROM generate_series(1,101) AS f(x);
INSERT INTO t2 SELECT x, x+1 FROM generate_series(1,100) AS f(x);
-- ADD TESTS HERE

SELECT citus.clear_network_traffic();
SELECT t1_id, data, t2_id FROM t1 NATURAL JOIN t2;
SELECT citus.dump_network_traffic();

DROP SCHEMA failure_task_tracker CASCADE;
