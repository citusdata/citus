CREATE SCHEMA start_stop_metadata_sync;
SET search_path TO "start_stop_metadata_sync";
SET citus.next_shard_id TO 980000;
SET client_min_messages TO WARNING;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

-- create a custom type for testing with a distributed table
CREATE TYPE tt2 AS ENUM ('a', 'b');

-- create test tables
CREATE TABLE distributed_table_1(col int unique, b tt2);
CREATE TABLE "distributed_table_2'! ?._"(col int unique);
CREATE TABLE distributed_table_3(col int);
CREATE TABLE distributed_table_4(a int UNIQUE NOT NULL, b int, c int);
CREATE TABLE reference_table_1(col int unique);
CREATE TABLE reference_table_2(col int unique);
CREATE TABLE local_table(col int unique);

-- create a fkey graph: dist -> dist -> ref1 <- local  && ref1 -> ref2
ALTER TABLE distributed_table_1 ADD CONSTRAINT fkey_1 FOREIGN KEY (col) REFERENCES "distributed_table_2'! ?._"(col);
ALTER TABLE "distributed_table_2'! ?._" ADD CONSTRAINT fkey_1 FOREIGN KEY (col) REFERENCES reference_table_1(col);
ALTER TABLE reference_table_1 ADD CONSTRAINT fkey_1 FOREIGN KEY (col) REFERENCES reference_table_2(col);
ALTER TABLE local_table ADD CONSTRAINT fkey_1 FOREIGN KEY (col) REFERENCES reference_table_1(col);

SELECT create_reference_table('reference_table_2');
SELECT create_reference_table('reference_table_1');
SELECT create_distributed_table('"distributed_table_2''! ?._"', 'col');
SELECT create_distributed_table('distributed_table_1', 'col');
SELECT create_distributed_table('distributed_table_3', 'col');
SELECT create_distributed_table('distributed_table_4', 'a');

CREATE INDEX ind1 ON distributed_table_4(a);
CREATE INDEX ind2 ON distributed_table_4(b);
CREATE INDEX ind3 ON distributed_table_4(a, b);

CREATE STATISTICS stat ON a,b FROM distributed_table_4;

-- create views to make sure that they'll continue working after stop_sync
INSERT INTO distributed_table_3 VALUES (1);
CREATE VIEW test_view AS SELECT COUNT(*) FROM distributed_table_3;
CREATE MATERIALIZED VIEW test_matview AS SELECT COUNT(*) FROM distributed_table_3;

ALTER TABLE distributed_table_4 DROP COLUMN c;

SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

\c - - - :worker_1_port
SET search_path TO "start_stop_metadata_sync";
\d+ distributed_table_4
SELECT * FROM distributed_table_1;
CREATE VIEW test_view AS SELECT COUNT(*) FROM distributed_table_3;
CREATE MATERIALIZED VIEW test_matview AS SELECT COUNT(*) FROM distributed_table_3;
SELECT * FROM test_view;
SELECT * FROM test_matview;
SELECT count(*) > 0 FROM pg_dist_node;
SELECT count(*) > 0 FROM pg_dist_shard;
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'distributed_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'reference_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
\c - - - :master_port
SET search_path TO "start_stop_metadata_sync";
SELECT * FROM distributed_table_1;
ALTER TABLE distributed_table_4 DROP COLUMN b;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
\d+ distributed_table_4
SELECT * FROM test_view;
SELECT * FROM test_matview;
SELECT count(*) > 0 FROM pg_dist_node;
SELECT count(*) > 0 FROM pg_dist_shard;
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'distributed_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'reference_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
\c - - - :worker_1_port
SET search_path TO "start_stop_metadata_sync";
-- these should not be found because of stop_metadata_sync_to_node
SELECT * FROM distributed_table_1;
\d+ distributed_table_4
SELECT * FROM test_view;
SELECT * FROM test_matview;

SELECT count(*) > 0 FROM pg_dist_node;
SELECT count(*) > 0 FROM pg_dist_shard;
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'distributed_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'reference_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
\c - - - :master_port
SET search_path TO "start_stop_metadata_sync";
SELECT * FROM distributed_table_1;

--cleanup
DROP SCHEMA start_stop_metadata_sync CASCADE;
