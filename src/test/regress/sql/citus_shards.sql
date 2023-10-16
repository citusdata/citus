CREATE SCHEMA citus_shards;
SET search_path TO citus_shards;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 99456900;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 456900;

CREATE TABLE t1 (i int);
SELECT create_distributed_table('t1', 'i');
CREATE TABLE "t with space" (i int);
SELECT create_distributed_table('"t with space"', 'i');
INSERT INTO t1 SELECT generate_series(1, 100);
INSERT INTO "t with space" SELECT generate_series(1, 1000);
SELECT * FROM citus_shards;

SET client_min_messages TO WARNING;
DROP SCHEMA citus_shards CASCADE;
