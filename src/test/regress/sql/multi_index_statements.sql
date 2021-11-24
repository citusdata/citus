--
-- MULTI_INDEX_STATEMENTS
--

-- Check that we can run CREATE INDEX and DROP INDEX statements on distributed
-- tables.

--
-- CREATE TEST TABLES
--

CREATE SCHEMA multi_index_statements;
CREATE SCHEMA multi_index_statements_2;
SET search_path TO multi_index_statements;

SET citus.next_shard_id TO 102080;

CREATE TABLE index_test_range(a int, b int, c int);
SELECT create_distributed_table('index_test_range', 'a', 'range');
SELECT master_create_empty_shard('index_test_range');
SELECT master_create_empty_shard('index_test_range');

SET citus.shard_count TO 8;
SET citus.shard_replication_factor TO 2;
CREATE TABLE index_test_hash(a int, b int, c int);
SELECT create_distributed_table('index_test_hash', 'a', 'hash');

CREATE TABLE index_test_append(a int, b int, c int);
SELECT create_distributed_table('index_test_append', 'a', 'append');
SELECT master_create_empty_shard('index_test_append');
SELECT master_create_empty_shard('index_test_append');

--
-- CREATE INDEX
--

-- Verify that we can create different types of indexes

CREATE INDEX lineitem_orderkey_index ON public.lineitem (l_orderkey);

CREATE INDEX lineitem_partkey_desc_index ON public.lineitem (l_partkey DESC);

CREATE INDEX lineitem_partial_index ON public.lineitem (l_shipdate)
	WHERE l_shipdate < '1995-01-01';

CREATE INDEX lineitem_colref_index ON public.lineitem (record_ne(lineitem.*, NULL));

SET client_min_messages = ERROR; -- avoid version dependent warning about WAL
CREATE INDEX lineitem_orderkey_hash_index ON public.lineitem USING hash (l_partkey);
CREATE UNIQUE INDEX index_test_range_index_a ON index_test_range(a);
CREATE UNIQUE INDEX index_test_range_index_a_b ON index_test_range(a,b);
CREATE UNIQUE INDEX index_test_hash_index_a ON index_test_hash(a);
CREATE UNIQUE INDEX index_test_hash_index_a_b ON index_test_hash(a,b);
CREATE UNIQUE INDEX index_test_hash_index_a_b_partial ON index_test_hash(a,b) WHERE c IS NOT NULL;
CREATE UNIQUE INDEX index_test_range_index_a_b_partial ON index_test_range(a,b) WHERE c IS NOT NULL;
CREATE UNIQUE INDEX index_test_hash_index_a_b_c ON index_test_hash(a) INCLUDE (b,c);
RESET client_min_messages;


-- Verify that we can create expression indexes and be robust to different schemas
CREATE OR REPLACE FUNCTION value_plus_one(a int)
RETURNS int IMMUTABLE AS $$
BEGIN
	RETURN a + 1;
END;
$$ LANGUAGE plpgsql;
SELECT create_distributed_function('value_plus_one(int)');

CREATE OR REPLACE FUNCTION multi_index_statements_2.value_plus_one(a int)
RETURNS int IMMUTABLE AS $$
BEGIN
	RETURN a + 1;
END;
$$ LANGUAGE plpgsql;
SELECT create_distributed_function('multi_index_statements_2.value_plus_one(int)');

CREATE INDEX ON index_test_hash ((value_plus_one(b)));
CREATE INDEX ON index_test_hash ((multi_index_statements.value_plus_one(b)));
CREATE INDEX ON index_test_hash ((multi_index_statements_2.value_plus_one(b)));

-- Verify that we handle if not exists statements correctly
CREATE INDEX lineitem_orderkey_index on public.lineitem(l_orderkey);
CREATE INDEX IF NOT EXISTS lineitem_orderkey_index on public.lineitem(l_orderkey);
CREATE INDEX IF NOT EXISTS lineitem_orderkey_index_new on public.lineitem(l_orderkey);

-- Verify if not exists behavior with an index with same name on a different table
CREATE INDEX lineitem_orderkey_index on public.nation(n_nationkey);
CREATE INDEX IF NOT EXISTS lineitem_orderkey_index on public.nation(n_nationkey);

-- Verify that we can create indexes concurrently
CREATE INDEX CONCURRENTLY lineitem_concurrently_index ON public.lineitem (l_orderkey);

-- Verify that no-name local CREATE INDEX CONCURRENTLY works
CREATE TABLE local_table (id integer, name text);
CREATE INDEX CONCURRENTLY ON local_table(id);

-- Verify that we warn out on CLUSTER command for distributed tables and no parameter
CLUSTER index_test_hash USING index_test_hash_index_a;
CLUSTER;

-- Vefify we don't warn out on CLUSTER command for local tables
CREATE INDEX CONCURRENTLY local_table_index ON local_table(id);
CLUSTER local_table USING local_table_index;

DROP TABLE local_table;

-- Verify that all indexes got created on the master node and one of the workers
SELECT * FROM pg_indexes WHERE tablename = 'lineitem' or tablename like 'index_test_%' ORDER BY indexname;
\c - - - :worker_1_port
SELECT count(*) FROM pg_indexes WHERE tablename = (SELECT relname FROM pg_class WHERE relname LIKE 'lineitem_%' ORDER BY relname LIMIT 1);
SELECT count(*) FROM pg_indexes WHERE tablename LIKE 'index_test_hash_%';
SELECT count(*) FROM pg_indexes WHERE tablename LIKE 'index_test_range_%';
SELECT count(*) FROM pg_indexes WHERE tablename LIKE 'index_test_append_%';
\c - - - :master_port
SET search_path TO multi_index_statements, public;

-- Verify that we error out on unsupported statement types

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

-- show that we support indexes without names
CREATE INDEX ON lineitem (l_orderkey);
CREATE UNIQUE INDEX ON index_test_hash(a);
CREATE INDEX CONCURRENTLY ON lineitem USING hash (l_shipdate);

-- Verify that none of failed indexes got created on the master node
SELECT * FROM pg_indexes WHERE tablename = 'lineitem' or tablename like 'index_test_%' ORDER BY indexname;

--
-- REINDEX
--

REINDEX INDEX lineitem_orderkey_index;
REINDEX TABLE lineitem;
REINDEX SCHEMA public;
REINDEX DATABASE regression;
REINDEX SYSTEM regression;

--
-- DROP INDEX
--

-- Verify that we can't drop multiple indexes in a single command
DROP INDEX lineitem_orderkey_index, lineitem_partial_index;

-- Verify that we can succesfully drop indexes
DROP INDEX lineitem_partkey_desc_index;
DROP INDEX lineitem_partial_index;
DROP INDEX lineitem_colref_index;

-- Verify that we can drop distributed indexes with local indexes
CREATE TABLE local_table(a int, b int);
CREATE INDEX local_index ON local_table(a);
CREATE INDEX local_index2 ON local_table(b);
DROP INDEX lineitem_orderkey_index, local_index;
DROP INDEX IF EXISTS lineitem_orderkey_index_new, local_index2, non_existing_index;

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
SELECT indrelid::regclass, indexrelid::regclass FROM pg_index WHERE indrelid = (SELECT relname FROM pg_class WHERE relname LIKE 'lineitem%' ORDER BY relname LIMIT 1)::regclass AND NOT indisprimary AND indexrelid::regclass::text NOT LIKE 'lineitem_time_index%' ORDER BY 1,2;
SELECT * FROM pg_indexes WHERE tablename LIKE 'index_test_%' ORDER BY indexname;
\c - - - :worker_1_port
SELECT indrelid::regclass, indexrelid::regclass FROM pg_index WHERE indrelid = (SELECT relname FROM pg_class WHERE relname LIKE 'lineitem%' ORDER BY relname LIMIT 1)::regclass AND NOT indisprimary AND indexrelid::regclass::text NOT LIKE 'lineitem_time_index%' ORDER BY 1,2;
SELECT * FROM pg_indexes WHERE tablename LIKE 'index_test_%' ORDER BY indexname;

-- create index that will conflict with master operations
CREATE INDEX CONCURRENTLY ith_b_idx_102089 ON multi_index_statements.index_test_hash_102089(b);

\c - - - :master_port
SET search_path TO multi_index_statements;

-- should fail because worker index already exists
CREATE INDEX CONCURRENTLY ith_b_idx ON index_test_hash(b);

-- the failure results in an INVALID index
SELECT indisvalid AS "Index Valid?" FROM pg_index WHERE indexrelid='ith_b_idx'::regclass;

-- we can clean it up and recreate with an DROP IF EXISTS
DROP INDEX CONCURRENTLY IF EXISTS ith_b_idx;
CREATE INDEX CONCURRENTLY ith_b_idx ON index_test_hash(b);
SELECT indisvalid AS "Index Valid?" FROM pg_index WHERE indexrelid='ith_b_idx'::regclass;

\c - - - :worker_1_port
SET search_path TO multi_index_statements;

-- now drop shard index to test partial master DROP failure
DROP INDEX CONCURRENTLY ith_b_idx_102089;

\c - - - :master_port
SET search_path TO multi_index_statements;
SET citus.next_shard_id TO 103080;

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

-- the following tests are intended to show that
-- Citus does not get into self-deadlocks because
-- of long index names. So, make sure that we have
-- enough remote connections to trigger the case
SET citus.force_max_query_parallelization TO ON;

CREATE TABLE test_index_creation1
(
    tenant_id integer NOT NULL,
    timeperiod timestamp without time zone NOT NULL,
    field1 integer NOT NULL,
    inserted_utc timestamp without time zone NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, timeperiod)
) PARTITION BY RANGE (timeperiod);

select create_distributed_table('test_index_creation1', 'tenant_id');


-- should be able to create short named indexes in parallel
-- as there are no partitions even if the index name is too long
SET client_min_messages TO DEBUG1;
CREATE INDEX ix_test_index_creation1_ix_test_index_creation1_ix_test_index_creation1_ix_test_index_creation1_ix_test_index_creation1
    ON test_index_creation1 USING btree
    (tenant_id, timeperiod);
RESET client_min_messages;

CREATE TABLE test_index_creation1_p2020_09_26 PARTITION OF test_index_creation1 FOR VALUES FROM ('2020-09-26 00:00:00') TO ('2020-09-27 00:00:00');
CREATE TABLE test_index_creation1_p2020_09_27 PARTITION OF test_index_creation1 FOR VALUES FROM ('2020-09-27 00:00:00') TO ('2020-09-28 00:00:00');

-- should switch to sequential execution as the index name on the partition is
-- longer than 63
SET client_min_messages TO DEBUG1;
CREATE INDEX ix_test_index_creation2
    ON test_index_creation1 USING btree
    (tenant_id, timeperiod);

-- same test with schema qualified
SET search_path TO public;
CREATE INDEX ix_test_index_creation3
    ON multi_index_statements.test_index_creation1 USING btree
    (tenant_id, timeperiod);
SET search_path TO multi_index_statements;

-- we cannot switch to sequential execution
-- after a parallel query
BEGIN;
	SELECT count(*) FROM test_index_creation1;
	CREATE INDEX ix_test_index_creation4
	ON test_index_creation1 USING btree
	(tenant_id, timeperiod);
ROLLBACK;

-- try inside a sequential block
BEGIN;
	SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
	SELECT count(*) FROM test_index_creation1;
	CREATE INDEX ix_test_index_creation4
	ON test_index_creation1 USING btree
	(tenant_id, timeperiod);
ROLLBACK;

-- should be able to create indexes with INCLUDE/WHERE
CREATE INDEX ix_test_index_creation5 ON test_index_creation1
	USING btree(tenant_id, timeperiod)
	INCLUDE (field1) WHERE (tenant_id = 100);

CREATE UNIQUE INDEX ix_test_index_creation6 ON test_index_creation1
	USING btree(tenant_id, timeperiod);

-- should be able to create short named indexes in parallel
-- as  the table/index name is short
CREATE INDEX f1
    ON test_index_creation1 USING btree
    (field1);

-- should be able to create index only for parent on both
-- coordinator and worker nodes
CREATE INDEX parent_index
    ON ONLY test_index_creation1 USING btree
    (field1);

-- show that we have parent index only on the parent table not on the partitions
SELECT count(*) FROM pg_index WHERE indrelid::regclass::text = 'test_index_creation1' AND indexrelid::regclass::text = 'parent_index';
SELECT count(*) FROM pg_index WHERE indrelid::regclass::text LIKE 'test_index_creation1_p2020%' AND indexrelid::regclass::text LIKE 'parent_index%';

\c - - - :worker_1_port
SET search_path TO multi_index_statements;

-- show that we have parent index_* only on the parent shards not on the partition shards
SELECT count(*) FROM pg_index WHERE indrelid::regclass::text LIKE 'test_index_creation1_%' AND indexrelid::regclass::text LIKE 'parent_index%';
SELECT count(*) FROM pg_index WHERE indrelid::regclass::text LIKE 'test_index_creation1_p2020%' AND indexrelid::regclass::text LIKE 'parent_index%';

\c - - - :master_port
SET search_path TO multi_index_statements;

-- attach child index of a partition to parent index of the partitioned table
CREATE INDEX child_index
    ON test_index_creation1_p2020_09_26 USING btree
    (field1);

ALTER INDEX parent_index ATTACH PARTITION child_index;

-- show that child index inherits from parent index which means it is attached to it
SELECT count(*) FROM pg_inherits WHERE inhrelid::regclass::text = 'child_index' AND inhparent::regclass::text = 'parent_index';

\c - - - :worker_1_port
SET search_path TO multi_index_statements;

-- show that child indices of partition shards also inherit from parent indices of parent shards
SELECT count(*) FROM pg_inherits WHERE inhrelid::regclass::text LIKE 'child_index\_%' AND inhparent::regclass::text LIKE 'parent_index\_%';

\c - - - :master_port
SET search_path TO multi_index_statements;

-- verify error check for partitioned index
ALTER INDEX parent_index SET TABLESPACE foo;

-- drop parent index and show that child index will also be dropped
DROP INDEX parent_index;
SELECT count(*) FROM pg_index where indexrelid::regclass::text = 'child_index';

-- show that having a foreign key to reference table causes sequential execution mode
-- with ALTER INDEX ... ATTACH PARTITION

CREATE TABLE index_creation_reference_table (id int primary key);
SELECT create_reference_table('index_creation_reference_table');
ALTER TABLE test_index_creation1 ADD CONSTRAINT foreign_key_to_ref_table
    FOREIGN KEY (tenant_id)
    REFERENCES index_creation_reference_table (id);

CREATE INDEX parent_index ON ONLY test_index_creation1 USING btree (field1);
CREATE INDEX child_index ON test_index_creation1_p2020_09_26 USING btree (field1);

BEGIN;
    show citus.multi_shard_modify_mode;
    ALTER INDEX parent_index ATTACH PARTITION child_index;
    show citus.multi_shard_modify_mode;
ROLLBACK;

DROP TABLE index_creation_reference_table CASCADE;

SELECT
'CREATE TABLE distributed_table(' ||
string_Agg('col' || x::text || ' int,', ' ') ||
' last_column int)'
FROM
generate_Series(1, 32) x;
\gexec

SELECT create_distributed_table('distributed_table', 'last_column');

-- try to use all 33 columns to create the index
-- show that we error out as postgres would do
SELECT
'CREATE INDEX ON distributed_table(' ||
string_Agg('col' || x::text || ',', ' ') ||
' last_column)'
FROM
generate_Series(1, 32) x;
\gexec

-- show that we generate different default index names
-- for the indexes with same parameters on the same relation
CREATE INDEX ON distributed_table(last_column);
CREATE INDEX ON distributed_table(last_column);
SELECT indexrelid::regclass FROM pg_index WHERE indrelid='distributed_table'::regclass ORDER BY indexrelid;

-- test CREATE INDEX in plpgsql to verify that we don't break parse tree
CREATE OR REPLACE FUNCTION create_index_in_plpgsql()
RETURNS VOID AS
$BODY$
BEGIN
    CREATE INDEX ON distributed_table(last_column);
END;
$BODY$ LANGUAGE plpgsql;

-- hide plpgsql messages as they differ across pg versions
\set VERBOSITY terse

SELECT create_index_in_plpgsql();
SELECT create_index_in_plpgsql();
SELECT create_index_in_plpgsql();
SELECT indexrelid::regclass FROM pg_index WHERE indrelid='distributed_table'::regclass ORDER BY indexrelid;

SET citus.force_max_query_parallelization TO OFF;

SET client_min_messages TO ERROR;
DROP INDEX f1;
DROP INDEX ix_test_index_creation2;
DROP INDEX ix_test_index_creation1_ix_test_index_creation1_ix_test_index_creation1_ix_test_index_creation1_ix_test_index_creation1;
DROP INDEX CONCURRENTLY ith_b_idx;

-- the failure results in an INVALID index
SELECT indisvalid AS "Index Valid?" FROM pg_index WHERE indexrelid='ith_b_idx'::regclass;

-- final clean up
DROP INDEX CONCURRENTLY IF EXISTS ith_b_idx;

DROP SCHEMA multi_index_statements CASCADE;
DROP SCHEMA multi_index_statements_2 CASCADE;
