--
-- MULTI_INDEX_STATEMENTS
--

-- Check that we can run CREATE INDEX and DROP INDEX statements on distributed
-- tables. We increase the logging verbosity to verify that commands are
-- propagated to all worker shards.

SET client_min_messages TO DEBUG2;

--
-- CREATE INDEX
--

-- Verify that we can create different types of indexes

CREATE INDEX lineitem_orderkey_index ON lineitem (l_orderkey);

CREATE INDEX lineitem_partkey_desc_index ON lineitem (l_partkey DESC);

CREATE INDEX lineitem_partial_index ON lineitem (l_shipdate)
	WHERE l_shipdate < '1995-01-01';

CREATE INDEX lineitem_orderkey_hash_index ON lineitem USING hash (l_partkey);

-- Verify that all indexes got created on the master node
SELECT * FROM pg_indexes WHERE tablename = 'lineitem' ORDER BY indexname;

-- Verify that we error out on unsupported statement types

CREATE INDEX CONCURRENTLY try_index ON lineitem (l_orderkey);
CREATE UNIQUE INDEX try_index ON lineitem (l_orderkey);
CREATE INDEX try_index ON lineitem (l_orderkey) TABLESPACE newtablespace;

-- Verify that we error out in case of postgres errors on supported statement
-- types.

CREATE INDEX lineitem_orderkey_index ON lineitem (l_orderkey);
CREATE INDEX try_index ON lineitem USING gist (l_orderkey);
CREATE INDEX try_index ON lineitem (non_existent_column);

-- Verify that none of failed indexes got created on the master node
SELECT * FROM pg_indexes WHERE tablename = 'lineitem' ORDER BY indexname;

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

-- Verify that all the indexes are also dropped from the master node
SELECT * FROM pg_indexes WHERE tablename = 'lineitem' ORDER BY indexname;

