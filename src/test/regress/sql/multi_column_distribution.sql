CREATE SCHEMA multi_column_distribution;
SET search_path TO multi_column_distribution;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 27905500;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 27905500;

create table t(id int, a int);
select create_distributed_table('t', ARRAY['id'], colocate_with := 'none');
select * from pg_dist_partition WHERE NOT (logicalrelid::text LIKE '%.%');

create table t2(id int, a int);
select create_distributed_table('t2', ARRAY['id', 'a'], colocate_with := 'none');
select * from pg_dist_partition WHERE NOT (logicalrelid::text LIKE '%.%');

SET client_min_messages TO WARNING;
DROP SCHEMA multi_column_distribution CASCADE;
