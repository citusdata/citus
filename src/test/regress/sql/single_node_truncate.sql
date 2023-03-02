CREATE SCHEMA single_node_truncate;
SET search_path TO single_node_truncate;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 91630500;

-- helper view that prints out local table names and sizes in the schema
CREATE VIEW table_sizes AS
SELECT
  c.relname as name,
  pg_catalog.pg_table_size(c.oid) > 0 as has_data
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
      AND n.nspname = 'single_node_truncate'
ORDER BY 1;


-- test truncating reference tables
CREATE TABLE ref(id int UNIQUE, data int);
INSERT INTO ref SELECT x,x FROM generate_series(1,10000) x;
SELECT create_reference_table('ref');

CREATE TABLE citus_local(id int, ref_id int REFERENCES ref(id));
INSERT INTO citus_local SELECT x,x FROM generate_series(1,10000) x;

-- verify that shell tables for citus local tables are empty
SELECT * FROM table_sizes;

-- verify that this UDF is noop on Citus local tables
SELECT truncate_local_data_after_distributing_table('citus_local');
SELECT * FROM table_sizes;

-- test that we allow cascading truncates to citus local tables
BEGIN;
SELECT truncate_local_data_after_distributing_table('ref');
SELECT * FROM table_sizes;
ROLLBACK;

-- test that we allow distributing tables that have foreign keys to reference tables
CREATE TABLE dist(id int, ref_id int REFERENCES ref(id));
INSERT INTO dist SELECT x,x FROM generate_series(1,10000) x;
SELECT create_distributed_table('dist','id');

-- the following should truncate ref, dist and citus_local
BEGIN;
SELECT truncate_local_data_after_distributing_table('ref');
SELECT * FROM table_sizes;
ROLLBACK;

-- the following should truncate dist table only
BEGIN;
SELECT truncate_local_data_after_distributing_table('dist');
SELECT * FROM table_sizes;
ROLLBACK;

DROP TABLE ref, dist, citus_local;
DROP VIEW table_sizes;
DROP SCHEMA single_node_truncate CASCADE;

-- Remove the coordinator
SELECT 1 FROM master_remove_node('localhost', :master_port);
-- restart nodeid sequence so that multi_cluster_management still has the same
-- nodeids
ALTER SEQUENCE pg_dist_node_nodeid_seq RESTART 1;
