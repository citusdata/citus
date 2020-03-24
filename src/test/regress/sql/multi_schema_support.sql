--
-- MULTI_SCHEMA_SUPPORT
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1190000;

-- create schema to test schema support
CREATE SCHEMA test_schema_support;


-- test master_append_table_to_shard with schema
-- create local table to append
CREATE TABLE public.nation_local(
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);

\copy public.nation_local FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

CREATE TABLE test_schema_support.nation_append(
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);
SELECT master_create_distributed_table('test_schema_support.nation_append', 'n_nationkey', 'append');
SELECT master_create_empty_shard('test_schema_support.nation_append');

-- append table to shard
SELECT master_append_table_to_shard(1190000, 'public.nation_local', 'localhost', :master_port);

-- verify table actually appended to shard
SELECT COUNT(*) FROM test_schema_support.nation_append;

-- test with shard name contains special characters
CREATE TABLE test_schema_support."nation._'append" (
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152));

SELECT master_create_distributed_table('test_schema_support."nation._''append"', 'n_nationkey', 'append');
SELECT master_create_empty_shard('test_schema_support."nation._''append"');

SELECT master_append_table_to_shard(1190001, 'nation_local', 'localhost', :master_port);

-- verify table actually appended to shard
SELECT COUNT(*) FROM test_schema_support."nation._'append";

-- test master_append_table_to_shard with schema with search_path is set
SET search_path TO test_schema_support;

SELECT master_append_table_to_shard(1190000, 'public.nation_local', 'localhost', :master_port);

-- verify table actually appended to shard
SELECT COUNT(*) FROM nation_append;

-- test with search_path is set and shard name contains special characters
SELECT master_append_table_to_shard(1190001, 'nation_local', 'localhost', :master_port);

-- verify table actually appended to shard
SELECT COUNT(*) FROM "nation._'append";


-- test shard creation on append(by data loading) and hash distributed(with UDF) tables
-- when search_path is set
SET search_path TO test_schema_support;

-- create shard with COPY on append distributed table
CREATE TABLE nation_append_search_path(
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);
SELECT master_create_distributed_table('nation_append_search_path', 'n_nationkey', 'append');

\copy nation_append_search_path FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

-- create shard with master_create_worker_shards
CREATE TABLE test_schema_support.nation_hash(
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);
SELECT master_create_distributed_table('test_schema_support.nation_hash', 'n_nationkey', 'hash');
SELECT master_create_worker_shards('test_schema_support.nation_hash', 4, 2);


-- test cursors
SET search_path TO public;
BEGIN;
DECLARE test_cursor CURSOR FOR
    SELECT *
        FROM test_schema_support.nation_append
        WHERE n_nationkey = 1;
FETCH test_cursor;
FETCH test_cursor;
FETCH BACKWARD test_cursor;
END;

-- test with search_path is set
SET search_path TO test_schema_support;
BEGIN;
DECLARE test_cursor CURSOR FOR
    SELECT *
        FROM nation_append
        WHERE n_nationkey = 1;
FETCH test_cursor;
FETCH test_cursor;
FETCH BACKWARD test_cursor;
END;


-- test inserting to table in different schema
SET search_path TO public;

INSERT INTO test_schema_support.nation_hash(n_nationkey, n_name, n_regionkey) VALUES (6, 'FRANCE', 3);

-- verify insertion
SELECT * FROM test_schema_support.nation_hash WHERE n_nationkey = 6;

-- test with search_path is set
SET search_path TO test_schema_support;

INSERT INTO nation_hash(n_nationkey, n_name, n_regionkey) VALUES (7, 'GERMANY', 3);

-- verify insertion
SELECT * FROM nation_hash WHERE n_nationkey = 7;


-- test UDFs with schemas
SET search_path TO public;

\copy test_schema_support.nation_hash FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

-- create UDF in master node
CREATE OR REPLACE FUNCTION dummyFunction(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

-- create UDF in worker node 1
\c - - :public_worker_1_host :worker_1_port
CREATE OR REPLACE FUNCTION dummyFunction(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

-- create UDF in worker node 2
\c - - :public_worker_2_host :worker_2_port
CREATE OR REPLACE FUNCTION dummyFunction(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

\c - - :master_host :master_port

-- UDF in public, table in a schema other than public, search_path is not set
SELECT dummyFunction(n_nationkey) FROM test_schema_support.nation_hash GROUP BY 1 ORDER BY 1;

-- UDF in public, table in a schema other than public, search_path is set
SET search_path TO test_schema_support;
SELECT public.dummyFunction(n_nationkey) FROM test_schema_support.nation_hash GROUP BY 1 ORDER BY 1;

-- create UDF in master node in schema
SET search_path TO test_schema_support;
CREATE OR REPLACE FUNCTION dummyFunction2(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

-- create UDF in worker node 1 in schema
\c - - :public_worker_1_host :worker_1_port
SET search_path TO test_schema_support;
CREATE OR REPLACE FUNCTION dummyFunction2(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

-- create UDF in worker node 2 in schema
\c - - :public_worker_2_host :worker_2_port
SET search_path TO test_schema_support;
CREATE OR REPLACE FUNCTION dummyFunction2(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

\c - - :master_host :master_port

-- UDF in schema, table in a schema other than public, search_path is not set
SET search_path TO public;
SELECT test_schema_support.dummyFunction2(n_nationkey) FROM test_schema_support.nation_hash  GROUP BY 1 ORDER BY 1;

-- UDF in schema, table in a schema other than public, search_path is set
SET search_path TO test_schema_support;
SELECT dummyFunction2(n_nationkey) FROM nation_hash  GROUP BY 1 ORDER BY 1;


-- test operators with schema
SET search_path TO public;

-- create operator in master
CREATE OPERATOR test_schema_support.=== (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===,
    NEGATOR = !==,
    HASHES, MERGES
);

-- create operator in worker node 1
\c - - :public_worker_1_host :worker_1_port
CREATE OPERATOR test_schema_support.=== (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===,
    NEGATOR = !==,
    HASHES, MERGES
);

-- create operator in worker node 2
\c - - :public_worker_2_host :worker_2_port
CREATE OPERATOR test_schema_support.=== (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===,
    NEGATOR = !==,
    HASHES, MERGES
);

\c - - :master_host :master_port

-- test with search_path is not set
SELECT * FROM test_schema_support.nation_hash  WHERE n_nationkey OPERATOR(test_schema_support.===) 1;

-- test with search_path is set
SET search_path TO test_schema_support;
SELECT * FROM nation_hash  WHERE n_nationkey OPERATOR(===) 1;


-- test with multi-shard DML
SET search_path TO public;
UPDATE test_schema_support.nation_hash SET n_regionkey = n_regionkey + 1;

--verify modification
SELECT * FROM test_schema_support.nation_hash ORDER BY 1,2,3,4;

--test with search_path is set
SET search_path TO test_schema_support;
UPDATE nation_hash SET n_regionkey = n_regionkey + 1;

--verify modification
SELECT * FROM nation_hash ORDER BY 1,2,3,4;


--test COLLATION with schema
SET search_path TO public;
SELECT quote_ident(current_setting('lc_collate')) as current_locale \gset
CREATE COLLATION test_schema_support.english (LOCALE = :current_locale);

\c - - :master_host :master_port

CREATE TABLE test_schema_support.nation_hash_collation(
    n_nationkey integer not null,
    n_name char(25) not null COLLATE test_schema_support.english,
    n_regionkey integer not null,
    n_comment varchar(152)
);
SELECT master_get_table_ddl_events('test_schema_support.nation_hash_collation') ORDER BY 1;
SELECT master_create_distributed_table('test_schema_support.nation_hash_collation', 'n_nationkey', 'hash');
SELECT master_create_worker_shards('test_schema_support.nation_hash_collation', 4, 2);

\copy test_schema_support.nation_hash_collation FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

SELECT * FROM test_schema_support.nation_hash_collation ORDER BY 1,2,3,4;
SELECT n_comment FROM test_schema_support.nation_hash_collation ORDER BY n_comment COLLATE test_schema_support.english;

--test with search_path is set
SET search_path TO test_schema_support;
CREATE TABLE nation_hash_collation_search_path(
    n_nationkey integer not null,
    n_name char(25) not null COLLATE english,
    n_regionkey integer not null,
    n_comment varchar(152)
);
SELECT master_create_distributed_table('nation_hash_collation_search_path', 'n_nationkey', 'hash');
SELECT master_create_worker_shards('nation_hash_collation_search_path', 4, 2);

\copy nation_hash_collation_search_path FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

SELECT * FROM nation_hash_collation_search_path ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC;
SELECT n_comment FROM nation_hash_collation_search_path ORDER BY n_comment COLLATE english;

--test composite types with schema
SET search_path TO public;
CREATE TYPE test_schema_support.new_composite_type as (key1 text, key2 text);

CREATE TABLE test_schema_support.nation_hash_composite_types(
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152),
    test_col test_schema_support.new_composite_type
);
SELECT master_create_distributed_table('test_schema_support.nation_hash_composite_types', 'n_nationkey', 'hash');
SELECT master_create_worker_shards('test_schema_support.nation_hash_composite_types', 4, 2);

-- insert some data to verify composite type queries
\copy test_schema_support.nation_hash_composite_types FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai|(a,a)
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon|(a,b)
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special |(a,c)
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold|(a,d)
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d|(a,e)
5|ETHIOPIA|0|ven packages wake quickly. regu|(a,f)
\.

SELECT * FROM test_schema_support.nation_hash_composite_types WHERE test_col = '(a,a)'::test_schema_support.new_composite_type;

--test with search_path is set
SET search_path TO test_schema_support;
SELECT * FROM nation_hash_composite_types WHERE test_col = '(a,a)'::new_composite_type;


-- test ALTER TABLE ADD/DROP queries with schemas
SET search_path TO public;
ALTER TABLE test_schema_support.nation_hash ADD COLUMN new_col INT;

-- verify column is added
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='test_schema_support.nation_hash'::regclass;
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='test_schema_support.nation_hash_1190003'::regclass;
\c - - :master_host :master_port

ALTER TABLE test_schema_support.nation_hash DROP COLUMN IF EXISTS non_existent_column;
ALTER TABLE test_schema_support.nation_hash DROP COLUMN IF EXISTS new_col;

-- verify column is dropped
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='test_schema_support.nation_hash'::regclass;
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='test_schema_support.nation_hash_1190003'::regclass;
\c - - :master_host :master_port

--test with search_path is set
SET search_path TO test_schema_support;
ALTER TABLE nation_hash ADD COLUMN new_col INT;

-- verify column is added
SELECT "Column", "Type", "Modifiers" FROM public.table_desc WHERE relid='test_schema_support.nation_hash'::regclass;
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM public.table_desc WHERE relid='test_schema_support.nation_hash_1190003'::regclass;
\c - - :master_host :master_port

SET search_path TO test_schema_support;
ALTER TABLE nation_hash DROP COLUMN IF EXISTS non_existent_column;
ALTER TABLE nation_hash DROP COLUMN IF EXISTS new_col;

-- verify column is dropped
SELECT "Column", "Type", "Modifiers" FROM public.table_desc WHERE relid='test_schema_support.nation_hash'::regclass;
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM public.table_desc WHERE relid='test_schema_support.nation_hash_1190003'::regclass;
\c - - :master_host :master_port


-- test CREATE/DROP INDEX with schemas
SET search_path TO public;

-- CREATE index
CREATE INDEX index1 ON test_schema_support.nation_hash(n_name);

--verify INDEX is created
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'test_schema_support.index1'::regclass;
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'test_schema_support.index1_1190003'::regclass;
\c - - :master_host :master_port

-- DROP index
DROP INDEX test_schema_support.index1;

--verify INDEX is dropped
\d test_schema_support.index1
\c - - :public_worker_1_host :worker_1_port
\d test_schema_support.index1_1190003
\c - - :master_host :master_port

--test with search_path is set
SET search_path TO test_schema_support;

-- CREATE index
CREATE INDEX index1 ON nation_hash(n_name);

--verify INDEX is created
SELECT "Column", "Type", "Definition" FROM public.index_attrs WHERE
    relid = 'test_schema_support.index1'::regclass;
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'test_schema_support.index1_1190003'::regclass;
\c - - :master_host :master_port

-- DROP index
SET search_path TO test_schema_support;
DROP INDEX index1;

--verify INDEX is dropped
\d test_schema_support.index1
\c - - :public_worker_1_host :worker_1_port
\d test_schema_support.index1_1190003
\c - - :master_host :master_port


-- test master_copy_shard_placement with schemas
SET search_path TO public;

-- mark shard as inactive
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 1190000 and nodeport = :worker_1_port;
SELECT master_copy_shard_placement(1190000, :'worker_2_host', :worker_2_port, :'worker_1_host', :worker_1_port);

-- verify shardstate
SELECT shardstate, nodename, nodeport FROM pg_dist_shard_placement WHERE shardid = 1190000 ORDER BY nodeport;


--test with search_path is set
SET search_path TO test_schema_support;

-- mark shard as inactive
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 1190000 and nodeport = :worker_1_port;
SELECT master_copy_shard_placement(1190000, :'worker_2_host', :worker_2_port, :'worker_1_host', :worker_1_port);

-- verify shardstate
SELECT shardstate, nodename, nodeport FROM pg_dist_shard_placement WHERE shardid = 1190000 ORDER BY nodeport;


-- test master_apply_delete_command with schemas
SET search_path TO public;
SELECT master_apply_delete_command('DELETE FROM test_schema_support.nation_append') ;

-- verify shard is dropped
\c - - :public_worker_1_host :worker_1_port
\d test_schema_support.nation_append_119*

\c - - :master_host :master_port

-- test with search_path is set
SET search_path TO test_schema_support;

\copy nation_append FROM STDIN with delimiter '|';
0|ALGERIA|0| haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

SELECT master_apply_delete_command('DELETE FROM nation_append') ;

-- verify shard is dropped
\c - - :public_worker_1_host :worker_1_port
\d test_schema_support.nation_append_119*

\c - - :master_host :master_port

-- check joins of tables which are in schemas other than public
-- we create new tables with replication factor of 1
-- so that we guarantee to have repartitions when necessary

-- create necessary objects and load data to them
CREATE SCHEMA test_schema_support_join_1;
CREATE SCHEMA test_schema_support_join_2;
SET citus.shard_count to 4;
SET citus.shard_replication_factor to 1;

CREATE TABLE test_schema_support_join_1.nation_hash (
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152));

CREATE TABLE test_schema_support_join_1.nation_hash_2 (
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152));

CREATE TABLE test_schema_support_join_2.nation_hash (
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152));

SELECT create_distributed_table('test_schema_support_join_1.nation_hash', 'n_nationkey');

\copy test_schema_support_join_1.nation_hash FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

SELECT create_distributed_table('test_schema_support_join_1.nation_hash_2', 'n_nationkey');

\copy test_schema_support_join_1.nation_hash_2 FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

SELECT create_distributed_table('test_schema_support_join_2.nation_hash', 'n_nationkey');

\copy test_schema_support_join_2.nation_hash FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

-- check when search_path is public,
-- join of two tables which are in different schemas,
-- join on partition column
SET search_path TO public;
SELECT
    count (*)
FROM
    test_schema_support_join_1.nation_hash n1, test_schema_support_join_2.nation_hash n2
WHERE
    n1.n_nationkey = n2.n_nationkey;

-- check when search_path is different than public,
-- join of two tables which are in different schemas,
-- join on partition column
SET search_path TO test_schema_support_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, test_schema_support_join_2.nation_hash n2
WHERE
    n1.n_nationkey = n2.n_nationkey;

-- check when search_path is public,
-- join of two tables which are in same schemas,
-- join on partition column
SET search_path TO public;
SELECT
    count (*)
FROM
    test_schema_support_join_1.nation_hash n1, test_schema_support_join_1.nation_hash_2 n2
WHERE
    n1.n_nationkey = n2.n_nationkey;

-- check when search_path is different than public,
-- join of two tables which are in same schemas,
-- join on partition column
SET search_path TO test_schema_support_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, nation_hash_2 n2
WHERE
    n1.n_nationkey = n2.n_nationkey;

-- single repartition joins
SET citus.task_executor_type TO "task-tracker";

-- check when search_path is public,
-- join of two tables which are in different schemas,
-- join on partition column and non-partition column
SET search_path TO public;
SELECT
    count (*)
FROM
    test_schema_support_join_1.nation_hash n1, test_schema_support_join_2.nation_hash n2
WHERE
    n1.n_nationkey = n2.n_regionkey;

-- check when search_path is different than public,
-- join of two tables which are in different schemas,
-- join on partition column and non-partition column
SET search_path TO test_schema_support_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, test_schema_support_join_2.nation_hash n2
WHERE
    n1.n_nationkey = n2.n_regionkey;

-- check when search_path is different than public,
-- join of two tables which are in same schemas,
-- join on partition column and non-partition column
SET search_path TO test_schema_support_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, nation_hash_2 n2
WHERE
    n1.n_nationkey = n2.n_regionkey;

-- hash repartition joins

-- check when search_path is public,
-- join of two tables which are in different schemas,
-- join on non-partition column
SET search_path TO public;
SELECT
    count (*)
FROM
    test_schema_support_join_1.nation_hash n1, test_schema_support_join_2.nation_hash n2
WHERE
    n1.n_regionkey = n2.n_regionkey;

-- check when search_path is different than public,
-- join of two tables which are in different schemas,
-- join on non-partition column
SET search_path TO test_schema_support_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, test_schema_support_join_2.nation_hash n2
WHERE
    n1.n_regionkey = n2.n_regionkey;

-- check when search_path is different than public,
-- join of two tables which are in same schemas,
-- join on non-partition column
SET search_path TO test_schema_support_join_1;
SELECT
    count (*)
FROM
    nation_hash n1, nation_hash_2 n2
WHERE
    n1.n_regionkey = n2.n_regionkey;

-- set task_executor back to adaptive
SET citus.task_executor_type TO "adaptive";


-- test ALTER TABLE SET SCHEMA
SET search_path TO public;

CREATE SCHEMA old_schema;
CREATE TABLE old_schema.table_set_schema(id int);
SELECT create_distributed_table('old_schema.table_set_schema', 'id');
CREATE SCHEMA new_schema;

SELECT objid::oid::regnamespace as "Distributed Schemas"
    FROM citus.pg_dist_object
    WHERE objid::oid::regnamespace IN ('old_schema', 'new_schema');
\c - - :public_worker_1_host :worker_1_port
SELECT table_schema AS "Shards' Schema"
    FROM information_schema.tables
    WHERE table_name LIKE 'table\_set\_schema\_%' AND
          table_schema IN ('old_schema', 'new_schema', 'public')
    GROUP BY table_schema;
\c - - :master_host :master_port

ALTER TABLE old_schema.table_set_schema SET SCHEMA new_schema;

SELECT objid::oid::regnamespace as "Distributed Schemas"
    FROM citus.pg_dist_object
    WHERE objid::oid::regnamespace IN ('old_schema', 'new_schema');
\c - - :public_worker_1_host :worker_1_port
SELECT table_schema AS "Shards' Schema"
    FROM information_schema.tables
    WHERE table_name LIKE 'table\_set\_schema\_%' AND
          table_schema IN ('old_schema', 'new_schema', 'public')
    GROUP BY table_schema;
\c - - :master_host :master_port
SELECT * FROM new_schema.table_set_schema;

DROP SCHEMA old_schema CASCADE;
DROP SCHEMA new_schema CASCADE;


-- test ALTER TABLE SET SCHEMA from public
CREATE TABLE table_set_schema(id int);
SELECT create_distributed_table('table_set_schema', 'id');
CREATE SCHEMA new_schema;

SELECT objid::oid::regnamespace as "Distributed Schemas"
    FROM citus.pg_dist_object
    WHERE objid='new_schema'::regnamespace::oid;
\c - - :public_worker_1_host :worker_1_port
SELECT table_schema AS "Shards' Schema"
    FROM information_schema.tables
    WHERE table_name LIKE 'table\_set\_schema\_%' AND
          table_schema IN ('new_schema', 'public')
    GROUP BY table_schema;
\c - - :master_host :master_port

ALTER TABLE table_set_schema SET SCHEMA new_schema;

SELECT objid::oid::regnamespace as "Distributed Schemas"
    FROM citus.pg_dist_object
    WHERE objid='new_schema'::regnamespace::oid;
\c - - :public_worker_1_host :worker_1_port
SELECT table_schema AS "Shards' Schema"
    FROM information_schema.tables
    WHERE table_name LIKE 'table\_set\_schema\_%' AND
          table_schema IN ('new_schema', 'public')
    GROUP BY table_schema;
\c - - :master_host :master_port
SELECT * FROM new_schema.table_set_schema;

DROP SCHEMA new_schema CASCADE;


-- test ALTER TABLE SET SCHEMA when a search path is set
CREATE SCHEMA old_schema;
CREATE TABLE old_schema.table_set_schema(id int);
SELECT create_distributed_table('old_schema.table_set_schema', 'id');
CREATE TABLE table_set_schema(id int);
SELECT create_distributed_table('table_set_schema', 'id');
CREATE SCHEMA new_schema;

SELECT objid::oid::regnamespace as "Distributed Schemas"
    FROM citus.pg_dist_object
    WHERE objid::oid::regnamespace IN ('old_schema', 'new_schema');
\c - - :public_worker_1_host :worker_1_port
SELECT table_schema AS "Shards' Schema", COUNT(*) AS "Counts"
    FROM information_schema.tables
    WHERE table_name LIKE 'table\_set\_schema\_%' AND
          table_schema IN ('old_schema', 'new_schema', 'public')
    GROUP BY table_schema;
\c - - :master_host :master_port

SET search_path TO old_schema;
ALTER TABLE table_set_schema SET SCHEMA new_schema;

SELECT objid::oid::regnamespace as "Distributed Schemas"
    FROM citus.pg_dist_object
    WHERE objid::oid::regnamespace IN ('old_schema', 'new_schema');
\c - - :public_worker_1_host :worker_1_port
SELECT table_schema AS "Shards' Schema", COUNT(*) AS "Counts"
    FROM information_schema.tables
    WHERE table_name LIKE 'table\_set\_schema\_%' AND
          table_schema IN ('old_schema', 'new_schema', 'public')
    GROUP BY table_schema;
\c - - :master_host :master_port
SELECT * FROM new_schema.table_set_schema;

SET search_path to public;
DROP SCHEMA old_schema CASCADE;
DROP SCHEMA new_schema CASCADE;
DROP TABLE table_set_schema;


-- test ALTER TABLE SET SCHEMA with nonexisting schemas and table
-- expect all to give error
CREATE SCHEMA existing_schema;
CREATE SCHEMA another_existing_schema;
CREATE TABLE existing_schema.table_set_schema(id int);
SELECT create_distributed_table('existing_schema.table_set_schema', 'id');
ALTER TABLE non_existent_schema.table_set_schema SET SCHEMA another_existing_schema;
ALTER TABLE non_existent_schema.non_existent_table SET SCHEMA another_existing_schema;
ALTER TABLE non_existent_schema.table_set_schema SET SCHEMA another_non_existent_schema;
ALTER TABLE non_existent_schema.non_existent_table SET SCHEMA another_non_existent_schema;
ALTER TABLE existing_schema.non_existent_table SET SCHEMA another_existing_schema;
ALTER TABLE existing_schema.non_existent_table SET SCHEMA non_existent_schema;
ALTER TABLE existing_schema.table_set_schema SET SCHEMA non_existent_schema;
DROP SCHEMA existing_schema, another_existing_schema CASCADE;


-- test ALTER TABLE SET SCHEMA with interesting names
CREATE SCHEMA "cItuS.T E E N'sSchema";
CREATE SCHEMA "citus-teen's scnd schm.";

CREATE TABLE "cItuS.T E E N'sSchema"."be$t''t*ble" (id int);

SELECT create_distributed_table('"cItuS.T E E N''sSchema"."be$t''''t*ble"', 'id');

ALTER TABLE "cItuS.T E E N'sSchema"."be$t''t*ble" SET SCHEMA "citus-teen's scnd schm.";

\c - - :public_worker_1_host :worker_1_port
SELECT table_schema AS "Shards' Schema"
    FROM information_schema.tables
    WHERE table_name LIKE 'be$t''''t*ble%'
    GROUP BY table_schema;
\c - - :master_host :master_port

SELECT * FROM "citus-teen's scnd schm."."be$t''t*ble";

DROP SCHEMA "cItuS.T E E N'sSchema", "citus-teen's scnd schm." CASCADE;


-- test schema propagation with user other than current user
SELECT run_command_on_coordinator_and_workers('CREATE USER "test-user"');
SELECT run_command_on_coordinator_and_workers('GRANT ALL ON DATABASE postgres to "test-user"');

CREATE SCHEMA schema_with_user AUTHORIZATION "test-user";
CREATE TABLE schema_with_user.test_table(column1 int);
SELECT create_reference_table('schema_with_user.test_table');

-- verify that owner of the created schema is test-user
\c - - :public_worker_1_host :worker_1_port
\dn schema_with_user

\c - - :master_host :master_port

-- we do not use run_command_on_coordinator_and_workers here because when there is CASCADE, it causes deadlock
DROP OWNED BY "test-user" CASCADE;
SELECT run_command_on_workers('DROP OWNED BY "test-user" CASCADE');
SELECT run_command_on_coordinator_and_workers('DROP USER "test-user"');

DROP FUNCTION run_command_on_coordinator_and_workers(p_sql text);

-- test run_command_on_* UDFs with schema
CREATE SCHEMA run_test_schema;
CREATE TABLE run_test_schema.test_table(id int);
SELECT create_distributed_table('run_test_schema.test_table','id');

-- randomly insert data to evaluate below UDFs better
INSERT INTO run_test_schema.test_table VALUES(1);
INSERT INTO run_test_schema.test_table VALUES(7);
INSERT INTO run_test_schema.test_table VALUES(9);

-- try UDFs which call shard_name as a subroutine
SELECT sum(result::int) FROM run_command_on_placements('run_test_schema.test_table','SELECT pg_table_size(''%s'')');
SELECT sum(result::int) FROM run_command_on_shards('run_test_schema.test_table','SELECT pg_table_size(''%s'')');

-- test capital letters on both table and schema names
SET citus.task_executor_type to "adaptive";
-- create schema with weird names
CREATE SCHEMA "CiTuS.TeeN";
CREATE SCHEMA "CiTUS.TEEN2";

-- create table with weird names
CREATE TABLE "CiTuS.TeeN"."TeeNTabLE.1!?!"(id int, "TeNANt_Id" int);
CREATE TABLE "CiTUS.TEEN2"."CAPITAL_TABLE"(i int, j int);

-- create distributed table with weird names
SELECT create_distributed_table('"CiTuS.TeeN"."TeeNTabLE.1!?!"', 'TeNANt_Id');
SELECT create_distributed_table('"CiTUS.TEEN2"."CAPITAL_TABLE"', 'i');

-- truncate tables with weird names
INSERT INTO "CiTuS.TeeN"."TeeNTabLE.1!?!" VALUES(1, 1);
INSERT INTO "CiTUS.TEEN2"."CAPITAL_TABLE" VALUES(0, 1);
TRUNCATE "CiTuS.TeeN"."TeeNTabLE.1!?!", "CiTUS.TEEN2"."CAPITAL_TABLE";
SELECT count(*) FROM "CiTUS.TEEN2"."CAPITAL_TABLE";

-- insert into table with weird names
INSERT INTO "CiTuS.TeeN"."TeeNTabLE.1!?!" VALUES(1, 1),(1, 0),(0, 1),(2, 3),(3, 2),(4, 4);
INSERT INTO "CiTUS.TEEN2"."CAPITAL_TABLE" VALUES(0, 1),(1, 0),(2, 1),(4, 3),(3, 2),(4, 4);

-- join on tables with weird names
SELECT *
FROM "CiTuS.TeeN"."TeeNTabLE.1!?!", "CiTUS.TEEN2"."CAPITAL_TABLE"
WHERE "CiTUS.TEEN2"."CAPITAL_TABLE".i = "CiTuS.TeeN"."TeeNTabLE.1!?!"."TeNANt_Id"
ORDER BY 1,2,3,4;

-- add group by, having, order by clauses
SELECT *
FROM "CiTuS.TeeN"."TeeNTabLE.1!?!", "CiTUS.TEEN2"."CAPITAL_TABLE"
WHERE "CiTUS.TEEN2"."CAPITAL_TABLE".i = "CiTuS.TeeN"."TeeNTabLE.1!?!"."TeNANt_Id"
GROUP BY "TeNANt_Id", id, i, j
HAVING "TeNANt_Id" > 0 AND j >= id ORDER BY "TeNANt_Id";

SELECT *
FROM "CiTuS.TeeN"."TeeNTabLE.1!?!" join "CiTUS.TEEN2"."CAPITAL_TABLE" on
("CiTUS.TEEN2"."CAPITAL_TABLE".i = "CiTuS.TeeN"."TeeNTabLE.1!?!"."TeNANt_Id")
GROUP BY "TeNANt_Id", id, i, j
HAVING "TeNANt_Id" > 0 AND j >= id
ORDER BY 1,2,3,4;

-- run with CTEs
WITH "cTE" AS (
  SELECT *
  FROM "CiTuS.TeeN"."TeeNTabLE.1!?!"
)
SELECT * FROM "cTE" join "CiTUS.TEEN2"."CAPITAL_TABLE" on
("cTE"."TeNANt_Id" = "CiTUS.TEEN2"."CAPITAL_TABLE".i)
GROUP BY "TeNANt_Id", id, i, j
HAVING "TeNANt_Id" > 0 AND j >= id
ORDER BY 1,2,3,4;

SET search_path to "CiTuS.TeeN";
-- and subqueries
SELECT *
FROM (
      SELECT *
      FROM "TeeNTabLE.1!?!"
      ) "cTE"
join "CiTUS.TEEN2"."CAPITAL_TABLE" on
("cTE"."TeNANt_Id" = "CiTUS.TEEN2"."CAPITAL_TABLE".i)
GROUP BY "TeNANt_Id", id, i, j
HAVING "TeNANt_Id" > 0 AND j >= id
ORDER BY 1,2,3,4;

SET search_path to default;
-- Some DDL
ALTER TABLE "CiTuS.TeeN"."TeeNTabLE.1!?!" ADD COLUMN "NEW_TeeN:COl" text;

-- Some DML
DELETE FROM "CiTuS.TeeN"."TeeNTabLE.1!?!" WHERE "TeNANt_Id"=1;

-- Some more DDL
ALTER TABLE "CiTuS.TeeN"."TeeNTabLE.1!?!" ADD CONSTRAINT "ConsNAmE<>" PRIMARY KEY ("TeNANt_Id");

-- Clean up the created schema
DROP SCHEMA run_test_schema CASCADE;
DROP SCHEMA test_schema_support_join_1 CASCADE;
DROP SCHEMA test_schema_support_join_2 CASCADE;
DROP SCHEMA "CiTuS.TeeN" CASCADE;
DROP SCHEMA "CiTUS.TEEN2" CASCADE;
