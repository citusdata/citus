CREATE SCHEMA start_stop_metadata_sync;
SET search_path TO "start_stop_metadata_sync";
SET citus.next_shard_id TO 980000;
SET client_min_messages TO WARNING;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE TABLE distributed_table_1(col int unique);
CREATE TABLE distributed_table_2(col int unique);
CREATE TABLE reference_table_1(col int unique);
CREATE TABLE reference_table_2(col int unique);
CREATE TABLE local_table(col int unique);

ALTER TABLE distributed_table_1 ADD CONSTRAINT fkey_1 FOREIGN KEY (col) REFERENCES distributed_table_2(col);
ALTER TABLE distributed_table_2 ADD CONSTRAINT fkey_1 FOREIGN KEY (col) REFERENCES reference_table_1(col);
ALTER TABLE reference_table_1 ADD CONSTRAINT fkey_1 FOREIGN KEY (col) REFERENCES reference_table_2(col);
ALTER TABLE local_table ADD CONSTRAINT fkey_1 FOREIGN KEY (col) REFERENCES reference_table_1(col);

SELECT create_reference_table('reference_table_2');
SELECT create_reference_table('reference_table_1');
SELECT create_distributed_table('distributed_table_2', 'col');
SELECT create_distributed_table('distributed_table_1', 'col');

SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

\c - - - :worker_1_port
SELECT count(*) > 0 FROM pg_dist_node;
SELECT count(*) > 0 FROM pg_dist_shard;
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'distributed_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'reference_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
\c - - - :master_port
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT count(*) > 0 FROM pg_dist_node;
SELECT count(*) > 0 FROM pg_dist_shard;
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'distributed_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'reference_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
\c - - - :worker_1_port
SELECT count(*) > 0 FROM pg_dist_node;
SELECT count(*) > 0 FROM pg_dist_shard;
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'distributed_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'reference_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
\c - - - :master_port

--cleanup
DROP SCHEMA start_stop_metadata_sync CASCADE;
