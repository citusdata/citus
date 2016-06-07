--
-- MULTI_INDEX_STATEMENTS
--

-- Check that we can run CREATE INDEX and DROP INDEX statements on distributed
-- tables.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 640000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 640000;


--
-- CREATE TEST TABLES
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 102080;

CREATE TABLE index_test_range(a int, b int, c int);
SELECT master_create_distributed_table('index_test_range', 'a', 'range');
SELECT master_create_empty_shard('index_test_range');
SELECT master_create_empty_shard('index_test_range');

CREATE TABLE index_test_hash(a int, b int, c int);
SELECT master_create_distributed_table('index_test_hash', 'a', 'hash');
SELECT master_create_worker_shards('index_test_hash', 8, 2);

CREATE TABLE index_test_append(a int, b int, c int);
SELECT master_create_distributed_table('index_test_append', 'a', 'append');
SELECT master_create_empty_shard('index_test_append');
SELECT master_create_empty_shard('index_test_append');

--
-- CREATE INDEX
--

-- Verify that we can create different types of indexes

CREATE INDEX lineitem_orderkey_index ON lineitem (l_orderkey);

CREATE INDEX lineitem_partkey_desc_index ON lineitem (l_partkey DESC);

CREATE INDEX lineitem_partial_index ON lineitem (l_shipdate)
	WHERE l_shipdate < '1995-01-01';

SET client_min_messages = ERROR; -- avoid version dependant warning about WAL
CREATE INDEX lineitem_orderkey_hash_index ON lineitem USING hash (l_partkey);
CREATE UNIQUE INDEX index_test_range_index_a ON index_test_range(a);
CREATE UNIQUE INDEX index_test_range_index_a_b ON index_test_range(a,b);
CREATE UNIQUE INDEX index_test_hash_index_a ON index_test_hash(a);
CREATE UNIQUE INDEX index_test_hash_index_a_b ON index_test_hash(a,b);
RESET client_min_messages;

-- Verify that all indexes got created on the master node and one of the workers
SELECT * FROM pg_indexes WHERE tablename = 'lineitem' or tablename like 'index_test_%' ORDER BY indexname;
\c - - - :worker_1_port
SELECT count(*) FROM pg_indexes WHERE tablename = (SELECT relname FROM pg_class WHERE relname LIKE 'lineitem%' ORDER BY relname LIMIT 1);
SELECT count(*) FROM pg_indexes WHERE tablename LIKE 'index_test_hash%';
SELECT count(*) FROM pg_indexes WHERE tablename LIKE 'index_test_range%';
SELECT count(*) FROM pg_indexes WHERE tablename LIKE 'index_test_append%';
\c - - - :master_port

-- Verify that we error out on unsupported statement types

CREATE INDEX CONCURRENTLY try_index ON lineitem (l_orderkey);
CREATE UNIQUE INDEX try_index ON lineitem (l_orderkey);
CREATE INDEX try_index ON lineitem (l_orderkey) TABLESPACE newtablespace;

CREATE UNIQUE INDEX try_unique_range_index ON index_test_range(b);
CREATE UNIQUE INDEX try_unique_hash_index ON index_test_hash(b);
CREATE UNIQUE INDEX try_unique_append_index ON index_test_append(b);
CREATE UNIQUE INDEX try_unique_append_index ON index_test_append(a);
CREATE UNIQUE INDEX try_unique_append_index_a_b ON index_test_append(a,b);

-- Verify that we error out in case of postgres errors on supported statement
-- types.

CREATE INDEX lineitem_orderkey_index ON lineitem (l_orderkey);
CREATE INDEX try_index ON lineitem USING gist (l_orderkey);
CREATE INDEX try_index ON lineitem (non_existent_column);
CREATE INDEX ON lineitem (l_orderkey);

-- Verify that none of failed indexes got created on the master node
SELECT * FROM pg_indexes WHERE tablename = 'lineitem' or tablename like 'index_test_%' ORDER BY indexname;

--
-- DROP INDEX
--

-- Verify that we can't drop multiple indexes in a single command
DROP INDEX lineitem_orderkey_index, lineitem_partial_index;

-- Verify that we error out on the CONCURRENTLY clause
DROP INDEX CONCURRENTLY lineitem_orderkey_index;

-- Verify that we can succesfully drop indexes
DROP INDEX lineitem_orderkey_index;
DROP INDEX lineitem_partkey_desc_index;
DROP INDEX lineitem_partial_index;

-- Verify that we handle if exists statements correctly

DROP INDEX non_existent_index;
DROP INDEX IF EXISTS non_existent_index;

DROP INDEX IF EXISTS lineitem_orderkey_hash_index;
DROP INDEX lineitem_orderkey_hash_index;

DROP INDEX index_test_range_index_a;
DROP INDEX index_test_range_index_a_b;
DROP INDEX index_test_hash_index_a;
DROP INDEX index_test_hash_index_a_b;

-- Verify that all the indexes are dropped from the master and one worker node.
-- As there's a primary key, so exclude those from this check.
SELECT indrelid::regclass, indexrelid::regclass FROM pg_index WHERE indrelid = (SELECT relname FROM pg_class WHERE relname LIKE 'lineitem%' ORDER BY relname LIMIT 1)::regclass AND NOT indisprimary AND indexrelid::regclass::text NOT LIKE 'lineitem_time_index%';
SELECT * FROM pg_indexes WHERE tablename LIKE 'index_test_%' ORDER BY indexname;
\c - - - :worker_1_port
SELECT indrelid::regclass, indexrelid::regclass FROM pg_index WHERE indrelid = (SELECT relname FROM pg_class WHERE relname LIKE 'lineitem%' ORDER BY relname LIMIT 1)::regclass AND NOT indisprimary AND indexrelid::regclass::text NOT LIKE 'lineitem_time_index%';
SELECT * FROM pg_indexes WHERE tablename LIKE 'index_test_%' ORDER BY indexname;
\c - - - :master_port

-- Drop created tables
DROP TABLE index_test_range;
DROP TABLE index_test_hash;
DROP TABLE index_test_append;
