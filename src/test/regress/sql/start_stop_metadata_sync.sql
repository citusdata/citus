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

-- test for hybrid partitioned table (columnar+heap)
CREATE TABLE events(ts timestamptz, i int, n numeric, s text)
  PARTITION BY RANGE (ts);

CREATE TABLE events_2021_jan PARTITION OF events
  FOR VALUES FROM ('2021-01-01') TO ('2021-02-01');

CREATE TABLE events_2021_feb PARTITION OF events
  FOR VALUES FROM ('2021-02-01') TO ('2021-03-01');

INSERT INTO events SELECT
    '2021-01-01'::timestamptz + '0.45 seconds'::interval * g,
    g,
    g*pi(),
    'number: ' || g::text
    FROM generate_series(1,1000) g;

VACUUM (FREEZE, ANALYZE) events_2021_feb;

SELECT create_distributed_table('events', 'ts');

SELECT alter_table_set_access_method('events_2021_jan', 'columnar');

VACUUM (FREEZE, ANALYZE) events_2021_jan;

-- this should fail
BEGIN;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
ROLLBACK;

-- sync metadata
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

\c - - - :worker_1_port
SET search_path TO "start_stop_metadata_sync";
SELECT * FROM distributed_table_1;
CREATE VIEW test_view AS SELECT COUNT(*) FROM distributed_table_3;
CREATE MATERIALIZED VIEW test_matview AS SELECT COUNT(*) FROM distributed_table_3;
SELECT * FROM test_view;
SELECT * FROM test_matview;
SELECT * FROM pg_dist_partition WHERE logicalrelid::text LIKE 'events%' ORDER BY logicalrelid::text;
SELECT count(*) > 0 FROM pg_dist_node;
SELECT count(*) > 0 FROM pg_dist_shard;
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'distributed_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'reference_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
\c - - - :master_port
SET search_path TO "start_stop_metadata_sync";
SELECT * FROM distributed_table_1;
ALTER TABLE distributed_table_4 DROP COLUMN b;

-- this should fail
BEGIN;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
ROLLBACK;

SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT * FROM test_view;
SELECT * FROM test_matview;
SELECT count(*) > 0 FROM pg_dist_node;
SELECT count(*) > 0 FROM pg_dist_shard;
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'distributed_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'reference_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
\c - - - :worker_1_port
SET search_path TO "start_stop_metadata_sync";

SELECT count(*) > 0 FROM pg_dist_node;
SELECT count(*) > 0 FROM pg_dist_shard;
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'distributed_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'reference_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');

\c - - - :master_port
SET search_path TO "start_stop_metadata_sync";
SELECT * FROM distributed_table_1;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

\c - - - :worker_1_port
SELECT count(*) > 0 FROM pg_dist_node;
SELECT count(*) > 0 FROM pg_dist_shard;
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'distributed_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');
SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'reference_table__' AND relnamespace IN (SELECT oid FROM pg_namespace WHERE nspname = 'start_stop_metadata_sync');

\c - - - :master_port
SET search_path TO "start_stop_metadata_sync";

-- cleanup
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);
SET client_min_messages TO WARNING;
DROP SCHEMA start_stop_metadata_sync CASCADE;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);
