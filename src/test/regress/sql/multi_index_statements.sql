--
-- MULTI_INDEX_STATEMENTS
--

-- Check that we can run CREATE INDEX and DROP INDEX statements on distributed
-- tables.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 640000;


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

CREATE INDEX lineitem_colref_index ON lineitem (record_ne(lineitem.*, NULL));

SET client_min_messages = ERROR; -- avoid version dependant warning about WAL
CREATE INDEX lineitem_orderkey_hash_index ON lineitem USING hash (l_partkey);
CREATE UNIQUE INDEX index_test_range_index_a ON index_test_range(a);
CREATE UNIQUE INDEX index_test_range_index_a_b ON index_test_range(a,b);
CREATE UNIQUE INDEX index_test_hash_index_a ON index_test_hash(a);
CREATE UNIQUE INDEX index_test_hash_index_a_b ON index_test_hash(a,b);
CREATE UNIQUE INDEX index_test_hash_index_a_b_partial ON index_test_hash(a,b) WHERE c IS NOT NULL;
CREATE UNIQUE INDEX index_test_range_index_a_b_partial ON index_test_range(a,b) WHERE c IS NOT NULL;
RESET client_min_messages;

-- Verify that we handle if not exists statements correctly
CREATE INDEX lineitem_orderkey_index on lineitem(l_orderkey);
CREATE INDEX IF NOT EXISTS lineitem_orderkey_index on lineitem(l_orderkey);
CREATE INDEX IF NOT EXISTS lineitem_orderkey_index_new on lineitem(l_orderkey);

-- Verify if not exists behavior with an index with same name on a different table
CREATE INDEX lineitem_orderkey_index on index_test_hash(a);
CREATE INDEX IF NOT EXISTS lineitem_orderkey_index on index_test_hash(a);

-- Verify that we can create indexes concurrently
CREATE INDEX CONCURRENTLY lineitem_concurrently_index ON lineitem (l_orderkey);

-- Verify that all indexes got created on the master node and one of the workers
SELECT * FROM pg_indexes WHERE tablename = 'lineitem' or tablename like 'index_test_%' ORDER BY indexname;
\c - - - :worker_1_port
SELECT count(*) FROM pg_indexes WHERE tablename = (SELECT relname FROM pg_class WHERE relname LIKE 'lineitem%' ORDER BY relname LIMIT 1);
SELECT count(*) FROM pg_indexes WHERE tablename LIKE 'index_test_hash%';
SELECT count(*) FROM pg_indexes WHERE tablename LIKE 'index_test_range%';
SELECT count(*) FROM pg_indexes WHERE tablename LIKE 'index_test_append%';
\c - - - :master_port

-- Verify that we error out on unsupported statement types

CREATE UNIQUE INDEX try_index ON lineitem (l_orderkey);
CREATE INDEX try_index ON lineitem (l_orderkey) TABLESPACE newtablespace;

CREATE UNIQUE INDEX try_unique_range_index ON index_test_range(b);
CREATE UNIQUE INDEX try_unique_range_index_partial ON index_test_range(b) WHERE c IS NOT NULL;
CREATE UNIQUE INDEX try_unique_hash_index ON index_test_hash(b);
CREATE UNIQUE INDEX try_unique_hash_index_partial ON index_test_hash(b) WHERE c IS NOT NULL;
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

-- Verify that we can succesfully drop indexes
DROP INDEX lineitem_orderkey_index;
DROP INDEX lineitem_orderkey_index_new;
DROP INDEX lineitem_partkey_desc_index;
DROP INDEX lineitem_partial_index;
DROP INDEX lineitem_colref_index;

-- Verify that we handle if exists statements correctly

DROP INDEX non_existent_index;
DROP INDEX IF EXISTS non_existent_index;

DROP INDEX IF EXISTS lineitem_orderkey_hash_index;
DROP INDEX lineitem_orderkey_hash_index;

DROP INDEX index_test_range_index_a;
DROP INDEX index_test_range_index_a_b;
DROP INDEX index_test_range_index_a_b_partial;
DROP INDEX index_test_hash_index_a;
DROP INDEX index_test_hash_index_a_b;
DROP INDEX index_test_hash_index_a_b_partial;

-- Verify that we can drop indexes concurrently
DROP INDEX CONCURRENTLY lineitem_concurrently_index;

-- Verify that all the indexes are dropped from the master and one worker node.
-- As there's a primary key, so exclude those from this check.
SELECT indrelid::regclass, indexrelid::regclass FROM pg_index WHERE indrelid = (SELECT relname FROM pg_class WHERE relname LIKE 'lineitem%' ORDER BY relname LIMIT 1)::regclass AND NOT indisprimary AND indexrelid::regclass::text NOT LIKE 'lineitem_time_index%';
SELECT * FROM pg_indexes WHERE tablename LIKE 'index_test_%' ORDER BY indexname;
\c - - - :worker_1_port
SELECT indrelid::regclass, indexrelid::regclass FROM pg_index WHERE indrelid = (SELECT relname FROM pg_class WHERE relname LIKE 'lineitem%' ORDER BY relname LIMIT 1)::regclass AND NOT indisprimary AND indexrelid::regclass::text NOT LIKE 'lineitem_time_index%';
SELECT * FROM pg_indexes WHERE tablename LIKE 'index_test_%' ORDER BY indexname;

-- create index that will conflict with master operations
CREATE INDEX CONCURRENTLY ith_b_idx_102089 ON index_test_hash_102089(b);

\c - - - :master_port

-- should fail because worker index already exists
CREATE INDEX CONCURRENTLY ith_b_idx ON index_test_hash(b);

-- the failure results in an INVALID index
SELECT indisvalid AS "Index Valid?" FROM pg_index WHERE indexrelid='ith_b_idx'::regclass;

-- we can clean it up and recreate with an DROP IF EXISTS
DROP INDEX CONCURRENTLY IF EXISTS ith_b_idx;
CREATE INDEX CONCURRENTLY ith_b_idx ON index_test_hash(b);
SELECT indisvalid AS "Index Valid?" FROM pg_index WHERE indexrelid='ith_b_idx'::regclass;

\c - - - :worker_1_port

-- now drop shard index to test partial master DROP failure
DROP INDEX CONCURRENTLY ith_b_idx_102089;

\c - - - :master_port
DROP INDEX CONCURRENTLY ith_b_idx;

-- the failure results in an INVALID index
SELECT indisvalid AS "Index Valid?" FROM pg_index WHERE indexrelid='ith_b_idx'::regclass;

-- final clean up
DROP INDEX CONCURRENTLY IF EXISTS ith_b_idx;

-- Drop created tables
DROP TABLE index_test_range;
DROP TABLE index_test_hash;
DROP TABLE index_test_append;
