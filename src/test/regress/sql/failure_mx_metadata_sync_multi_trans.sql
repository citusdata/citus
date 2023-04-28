--
-- failure_mx_metadata_sync_multi_trans.sql
--
CREATE SCHEMA IF NOT EXISTS mx_metadata_sync_multi_trans;
SET SEARCH_PATH = mx_metadata_sync_multi_trans;
SET citus.shard_count TO 2;
SET citus.next_shard_id TO 16000000;
SET citus.shard_replication_factor TO 1;
SET citus.metadata_sync_mode TO 'nontransactional';

SELECT pg_backend_pid() as pid \gset
SELECT citus.mitmproxy('conn.allow()');

\set VERBOSITY terse
SET client_min_messages TO ERROR;

-- Create roles
CREATE ROLE foo1;
CREATE ROLE foo2;

-- Create collation
CREATE COLLATION german_phonebook (provider = icu, locale = 'de-u-co-phonebk');

-- Create type
CREATE TYPE pair_type AS (a int, b int);

-- Create function
CREATE FUNCTION one_as_result() RETURNS INT LANGUAGE SQL AS
$$
  SELECT 1;
$$;

-- Create text search dictionary
CREATE TEXT SEARCH DICTIONARY my_german_dict (
    template = snowball,
    language = german,
    stopwords = german
);

-- Create text search config
CREATE TEXT SEARCH CONFIGURATION my_ts_config ( parser = default );
ALTER TEXT SEARCH CONFIGURATION my_ts_config ALTER MAPPING FOR asciiword WITH my_german_dict;

-- Create sequence
CREATE SEQUENCE seq;

-- Create colocated distributed tables
CREATE TABLE dist1 (id int PRIMARY KEY default nextval('seq'), col int default (one_as_result()), myserial serial, phone text COLLATE german_phonebook, initials pair_type);
CREATE SEQUENCE seq_owned OWNED BY dist1.id;
CREATE INDEX dist1_search_phone_idx ON dist1 USING gin (to_tsvector('my_ts_config'::regconfig, (COALESCE(phone, ''::text))::text));
SELECT create_distributed_table('dist1', 'id');
INSERT INTO dist1 SELECT i FROM generate_series(1,100) i;

CREATE TABLE dist2 (id int PRIMARY KEY default nextval('seq'));
SELECT create_distributed_table('dist2', 'id');
INSERT INTO dist2 SELECT i FROM generate_series(1,100) i;

-- Create a reference table
CREATE TABLE ref (id int UNIQUE);
SELECT create_reference_table('ref');
INSERT INTO ref SELECT i FROM generate_series(1,100) i;

-- Create local tables
CREATE TABLE loc1 (id int PRIMARY KEY);
INSERT INTO loc1 SELECT i FROM generate_series(1,100) i;

CREATE TABLE loc2 (id int REFERENCES loc1(id));
INSERT INTO loc2 SELECT i FROM generate_series(1,100) i;

-- Create publication
CREATE PUBLICATION pub_all;

-- citus_set_coordinator_host with wrong port
SELECT citus_set_coordinator_host('localhost', 9999);
-- citus_set_coordinator_host with correct port
SELECT citus_set_coordinator_host('localhost', :master_port);
-- show coordinator port is correct on all workers
SELECT * FROM run_command_on_workers($$SELECT row(nodename,nodeport) FROM pg_dist_node WHERE groupid = 0$$);
SELECT citus_add_local_table_to_metadata('loc1', cascade_via_foreign_keys => true);

-- Create partitioned distributed table
CREATE TABLE orders (
    id bigint,
    order_time timestamp without time zone NOT NULL,
    region_id bigint NOT NULL
)
PARTITION BY RANGE (order_time);

SELECT create_time_partitions(
  table_name         := 'orders',
  partition_interval := '1 day',
  start_from        := '2020-01-01',
  end_at             := '2020-01-11'
);
SELECT create_distributed_table('orders', 'region_id');

-- Initially turn metadata sync to worker2 off because we'll ingest errors to start/stop metadata sync operations
SELECT stop_metadata_sync_to_node('localhost', :worker_2_proxy_port);
SELECT isactive, metadatasynced, hasmetadata FROM pg_dist_node WHERE nodeport=:worker_2_proxy_port;

-- Failure to send local group id
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE pg_dist_local_group SET groupid").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE pg_dist_local_group SET groupid").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to drop node metadata
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_node").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_node").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to send node metadata
SELECT citus.mitmproxy('conn.onQuery(query="INSERT INTO pg_dist_node").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="INSERT INTO pg_dist_node").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to drop sequence dependency for all tables
SELECT citus.mitmproxy('conn.onQuery(query="SELECT pg_catalog.worker_drop_sequence_dependency.*FROM pg_dist_partition").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT pg_catalog.worker_drop_sequence_dependency.*FROM pg_dist_partition").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to drop shell table
SELECT citus.mitmproxy('conn.onQuery(query="CALL pg_catalog.worker_drop_all_shell_tables").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="CALL pg_catalog.worker_drop_all_shell_tables").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to delete all pg_dist_partition metadata
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_partition").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_partition").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to delete all pg_dist_shard metadata
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_shard").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_shard").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to delete all pg_dist_placement metadata
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_placement").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_dist_placement").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to delete all pg_dist_object metadata
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_catalog.pg_dist_object").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_catalog.pg_dist_object").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to delete all pg_dist_colocation metadata
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_catalog.pg_dist_colocation").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="DELETE FROM pg_catalog.pg_dist_colocation").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to alter or create role
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_create_or_alter_role").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_create_or_alter_role").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to set database owner
SELECT citus.mitmproxy('conn.onQuery(query="ALTER DATABASE.*OWNER TO").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="ALTER DATABASE.*OWNER TO").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create schema
SELECT citus.mitmproxy('conn.onQuery(query="CREATE SCHEMA IF NOT EXISTS mx_metadata_sync_multi_trans AUTHORIZATION").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="CREATE SCHEMA IF NOT EXISTS mx_metadata_sync_multi_trans AUTHORIZATION").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create collation
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_create_or_replace_object.*CREATE COLLATION mx_metadata_sync_multi_trans.german_phonebook").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_create_or_replace_object.*CREATE COLLATION mx_metadata_sync_multi_trans.german_phonebook").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create function
SELECT citus.mitmproxy('conn.onQuery(query="CREATE OR REPLACE FUNCTION mx_metadata_sync_multi_trans.one_as_result").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="CREATE OR REPLACE FUNCTION mx_metadata_sync_multi_trans.one_as_result").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create text search dictionary
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_create_or_replace_object.*my_german_dict").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_create_or_replace_object.*my_german_dict").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create text search config
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_create_or_replace_object.*my_ts_config").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_create_or_replace_object.*my_ts_config").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create type
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_create_or_replace_object.*pair_type").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_create_or_replace_object.*pair_type").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create publication
SELECT citus.mitmproxy('conn.onQuery(query="CREATE PUBLICATION.*pub_all").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="CREATE PUBLICATION.*pub_all").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create sequence
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_apply_sequence_command").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT worker_apply_sequence_command").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to drop sequence dependency for distributed table
SELECT citus.mitmproxy('conn.onQuery(query="SELECT pg_catalog.worker_drop_sequence_dependency.*mx_metadata_sync_multi_trans.dist1").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT pg_catalog.worker_drop_sequence_dependency.*mx_metadata_sync_multi_trans.dist1").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to drop distributed table if exists
SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS mx_metadata_sync_multi_trans.dist1").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS mx_metadata_sync_multi_trans.dist1").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create distributed table
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE mx_metadata_sync_multi_trans.dist1").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE mx_metadata_sync_multi_trans.dist1").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to record sequence dependency for table
SELECT citus.mitmproxy('conn.onQuery(query="SELECT pg_catalog.worker_record_sequence_dependency").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT pg_catalog.worker_record_sequence_dependency").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create index for table
SELECT citus.mitmproxy('conn.onQuery(query="CREATE INDEX dist1_search_phone_idx ON mx_metadata_sync_multi_trans.dist1 USING gin").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="CREATE INDEX dist1_search_phone_idx ON mx_metadata_sync_multi_trans.dist1 USING gin").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create reference table
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE mx_metadata_sync_multi_trans.ref").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE mx_metadata_sync_multi_trans.ref").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create local table
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE mx_metadata_sync_multi_trans.loc1").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE mx_metadata_sync_multi_trans.loc1").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create distributed partitioned table
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE mx_metadata_sync_multi_trans.orders").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE mx_metadata_sync_multi_trans.orders").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to create distributed partition table
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE mx_metadata_sync_multi_trans.orders_p2020_01_05").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE mx_metadata_sync_multi_trans.orders_p2020_01_05").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to attach partition
SELECT citus.mitmproxy('conn.onQuery(query="ALTER TABLE mx_metadata_sync_multi_trans.orders ATTACH PARTITION mx_metadata_sync_multi_trans.orders_p2020_01_05").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="ALTER TABLE mx_metadata_sync_multi_trans.orders ATTACH PARTITION mx_metadata_sync_multi_trans.orders_p2020_01_05").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to add partition metadata
SELECT citus.mitmproxy('conn.onQuery(query="SELECT citus_internal_add_partition_metadata").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT citus_internal_add_partition_metadata").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to add shard metadata
SELECT citus.mitmproxy('conn.onQuery(query="SELECT citus_internal_add_shard_metadata").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT citus_internal_add_shard_metadata").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to add placement metadata
SELECT citus.mitmproxy('conn.onQuery(query="SELECT citus_internal_add_placement_metadata").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT citus_internal_add_placement_metadata").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to add colocation metadata
SELECT citus.mitmproxy('conn.onQuery(query="SELECT pg_catalog.citus_internal_add_colocation_metadata").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT pg_catalog.citus_internal_add_colocation_metadata").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to add distributed object metadata
SELECT citus.mitmproxy('conn.onQuery(query="SELECT citus_internal_add_object_metadata").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="SELECT citus_internal_add_object_metadata").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to mark function as distributed
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*one_as_result").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*one_as_result").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to mark collation as distributed
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*german_phonebook").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*german_phonebook").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to mark text search dictionary as distributed
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*my_german_dict").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*my_german_dict").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to mark text search configuration as distributed
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*my_ts_config").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*my_ts_config").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to mark type as distributed
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*pair_type").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*pair_type").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to mark sequence as distributed
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*seq_owned").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*seq_owned").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to mark publication as distributed
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*pub_all").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="WITH distributed_object_data.*pub_all").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to set isactive to true
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE pg_dist_node SET isactive = TRUE").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE pg_dist_node SET isactive = TRUE").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to set metadatasynced to true
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE pg_dist_node SET metadatasynced = TRUE").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE pg_dist_node SET metadatasynced = TRUE").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Failure to set hasmetadata to true
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE pg_dist_node SET hasmetadata = TRUE").cancel(' || :pid || ')');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE pg_dist_node SET hasmetadata = TRUE").kill()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Show node metadata info on coordinator after failures
SELECT * FROM pg_dist_node ORDER BY nodeport;

-- Show that we can still query the node from coordinator
SELECT COUNT(*) FROM dist1;

-- Verify that the value 103 belongs to a shard at the node to which we failed to sync metadata
SELECT 103 AS failed_node_val \gset
SELECT nodeid AS failed_nodeid FROM pg_dist_node WHERE metadatasynced = false \gset
SELECT get_shard_id_for_distribution_column('dist1', :failed_node_val) AS shardid \gset
SELECT groupid = :failed_nodeid FROM pg_dist_placement WHERE shardid = :shardid;

-- Show that we can still insert into a shard at the node from coordinator
INSERT INTO dist1 VALUES (:failed_node_val);

-- Show that we can still update a shard at the node from coordinator
UPDATE dist1 SET id = :failed_node_val WHERE id = :failed_node_val;

-- Show that we can still delete from a shard at the node from coordinator
DELETE FROM dist1 WHERE id = :failed_node_val;

-- Show that DDL would still propagate to the node
CREATE SCHEMA dummy;
SELECT * FROM run_command_on_workers($$SELECT nspname FROM pg_namespace WHERE nspname = 'dummy'$$);

-- Successfully activate the node after many failures
SELECT citus.mitmproxy('conn.allow()');
SELECT citus_activate_node('localhost', :worker_2_proxy_port);
-- Activate the node once more to verify it works again with already synced metadata
SELECT citus_activate_node('localhost', :worker_2_proxy_port);

-- Show node metadata info on worker2 and coordinator after success
\c - - - :worker_2_port
SELECT * FROM pg_dist_node ORDER BY nodeport;
\c - - - :master_port
SELECT * FROM pg_dist_node ORDER BY nodeport;
SELECT citus.mitmproxy('conn.allow()');

RESET citus.metadata_sync_mode;
DROP PUBLICATION pub_all;
DROP SCHEMA dummy;
DROP SCHEMA mx_metadata_sync_multi_trans CASCADE;
DROP ROLE foo1;
DROP ROLE foo2;
SELECT citus_remove_node('localhost', :master_port);
ALTER SEQUENCE pg_dist_node_nodeid_seq RESTART 3;
ALTER SEQUENCE pg_dist_groupid_seq RESTART 3;
