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
\c - - - :worker_1_port
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
\c - - - :worker_2_port
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

\c - - - :master_port

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
\c - - - :worker_1_port
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
\c - - - :worker_2_port
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

\c - - - :master_port

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
\c - - - :worker_1_port
CREATE OPERATOR test_schema_support.=== (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===,
    NEGATOR = !==,
    HASHES, MERGES
);

-- create operator in worker node 2
\c - - - :worker_2_port
CREATE OPERATOR test_schema_support.=== (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===,
    NEGATOR = !==,
    HASHES, MERGES
);

\c - - - :master_port

-- test with search_path is not set
SELECT * FROM test_schema_support.nation_hash  WHERE n_nationkey OPERATOR(test_schema_support.===) 1;

-- test with search_path is set
SET search_path TO test_schema_support;
SELECT * FROM nation_hash  WHERE n_nationkey OPERATOR(===) 1;


-- test with master_modify_multiple_shards
SET search_path TO public;
SELECT master_modify_multiple_shards('UPDATE test_schema_support.nation_hash SET n_regionkey = n_regionkey + 1');

--verify master_modify_multiple_shards
SELECT * FROM test_schema_support.nation_hash;

--test with search_path is set
SET search_path TO test_schema_support;
SELECT master_modify_multiple_shards('UPDATE nation_hash SET n_regionkey = n_regionkey + 1');

--verify master_modify_multiple_shards
SELECT * FROM nation_hash;


--test COLLATION with schema
SET search_path TO public;
CREATE COLLATION test_schema_support.english FROM "en_US";

-- create COLLATION in worker node 1 in schema
\c - - - :worker_1_port
CREATE COLLATION test_schema_support.english FROM "en_US";

-- create COLLATION in worker node 2 in schema
\c - - - :worker_2_port
CREATE COLLATION test_schema_support.english FROM "en_US";

\c - - - :master_port

CREATE TABLE test_schema_support.nation_hash_collation(
    n_nationkey integer not null,
    n_name char(25) not null COLLATE test_schema_support.english,
    n_regionkey integer not null,
    n_comment varchar(152)
);
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

SELECT * FROM test_schema_support.nation_hash_collation;
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

SELECT * FROM nation_hash_collation_search_path;
SELECT n_comment FROM nation_hash_collation_search_path ORDER BY n_comment COLLATE english;

--test composite types with schema
SET search_path TO public;
CREATE TYPE test_schema_support.new_composite_type as (key1 text, key2 text);

-- create type in worker node 1 in schema
\c - - - :worker_1_port
CREATE TYPE test_schema_support.new_composite_type as (key1 text, key2 text);

-- create type in worker node 2 in schema
\c - - - :worker_2_port
CREATE TYPE test_schema_support.new_composite_type as (key1 text, key2 text);

\c - - - :master_port
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
\d test_schema_support.nation_hash;
\c - - - :worker_1_port
\d test_schema_support.nation_hash_1190003;
\c - - - :master_port

ALTER TABLE test_schema_support.nation_hash DROP COLUMN IF EXISTS non_existent_column;
ALTER TABLE test_schema_support.nation_hash DROP COLUMN IF EXISTS new_col;

-- verify column is dropped
\d test_schema_support.nation_hash;
\c - - - :worker_1_port
\d test_schema_support.nation_hash_1190003;
\c - - - :master_port

--test with search_path is set
SET search_path TO test_schema_support;
ALTER TABLE nation_hash ADD COLUMN new_col INT;

-- verify column is added
\d test_schema_support.nation_hash;
\c - - - :worker_1_port
\d test_schema_support.nation_hash_1190003;
\c - - - :master_port

SET search_path TO test_schema_support;
ALTER TABLE nation_hash DROP COLUMN IF EXISTS non_existent_column;
ALTER TABLE nation_hash DROP COLUMN IF EXISTS new_col;

-- verify column is dropped
\d test_schema_support.nation_hash;
\c - - - :worker_1_port
\d test_schema_support.nation_hash_1190003;
\c - - - :master_port


-- test CREATE/DROP INDEX with schemas
SET search_path TO public;

-- CREATE index
CREATE INDEX index1 ON test_schema_support.nation_hash(n_name);

--verify INDEX is created
\d test_schema_support.nation_hash;
\c - - - :worker_1_port
\d test_schema_support.nation_hash_1190003;
\c - - - :master_port

-- DROP index
DROP INDEX test_schema_support.index1;

--verify INDEX is dropped
\d test_schema_support.nation_hash;
\c - - - :worker_1_port
\d test_schema_support.nation_hash_1190003;
\c - - - :master_port

--test with search_path is set
SET search_path TO test_schema_support;

-- CREATE index
CREATE INDEX index1 ON nation_hash(n_name);

--verify INDEX is created
\d test_schema_support.nation_hash;
\c - - - :worker_1_port
\d test_schema_support.nation_hash_1190003;
\c - - - :master_port

-- DROP index
SET search_path TO test_schema_support;
DROP INDEX index1;

--verify INDEX is dropped
\d test_schema_support.nation_hash;
\c - - - :worker_1_port
\d test_schema_support.nation_hash_1190003;
\c - - - :master_port


-- test master_copy_shard_placement with schemas
SET search_path TO public;

-- mark shard as inactive
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 1190000 and nodeport = :worker_1_port;
SELECT master_copy_shard_placement(1190000, 'localhost', :worker_2_port, 'localhost', :worker_1_port);

-- verify shardstate
SELECT shardstate, nodename, nodeport FROM pg_dist_shard_placement WHERE shardid = 1190000 ORDER BY nodeport;


--test with search_path is set
SET search_path TO test_schema_support;

-- mark shard as inactive
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 1190000 and nodeport = :worker_1_port;
SELECT master_copy_shard_placement(1190000, 'localhost', :worker_2_port, 'localhost', :worker_1_port);

-- verify shardstate
SELECT shardstate, nodename, nodeport FROM pg_dist_shard_placement WHERE shardid = 1190000 ORDER BY nodeport;


-- test master_apply_delete_command with schemas
SET search_path TO public;
SELECT master_apply_delete_command('DELETE FROM test_schema_support.nation_append') ;

-- verify shard is dropped
\c - - - :worker_1_port
\d test_schema_support.nation_append_119*

\c - - - :master_port

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
\c - - - :worker_1_port
\d test_schema_support.nation_append_119*

\c - - - :master_port


-- check joins of tables which are in schemas other than public
-- we create new tables with replication factor of 1
-- so that we guarantee to have repartitions when necessary

-- create necessary objects and load data to them
CREATE SCHEMA test_schema_support_join_1;
CREATE SCHEMA test_schema_support_join_2;

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

SELECT master_create_distributed_table('test_schema_support_join_1.nation_hash', 'n_nationkey', 'hash');
SELECT master_create_worker_shards('test_schema_support_join_1.nation_hash', 4, 1);

\copy test_schema_support_join_1.nation_hash FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special 
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

SELECT master_create_distributed_table('test_schema_support_join_1.nation_hash_2', 'n_nationkey', 'hash');
SELECT master_create_worker_shards('test_schema_support_join_1.nation_hash_2', 4, 1);

\copy test_schema_support_join_1.nation_hash_2 FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special 
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

SELECT master_create_distributed_table('test_schema_support_join_2.nation_hash', 'n_nationkey', 'hash');
SELECT master_create_worker_shards('test_schema_support_join_2.nation_hash', 4, 1);

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

-- set task_executor back to real-time
SET citus.task_executor_type TO "real-time";


-- test ALTER TABLE SET SCHEMA
-- we expect that it will warn out
SET search_path TO public;
ALTER TABLE test_schema_support.nation_hash SET SCHEMA public;

-- we will use this function in next test
CREATE FUNCTION run_command_on_coordinator_and_workers(p_sql text)
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
     EXECUTE p_sql;
     PERFORM run_command_on_workers(p_sql);
END;$$;

-- test schema propagation with user other than current user
SELECT run_command_on_coordinator_and_workers('CREATE USER "test-user"');
SELECT run_command_on_coordinator_and_workers('GRANT ALL ON DATABASE postgres to "test-user"');

CREATE SCHEMA schema_with_user AUTHORIZATION "test-user";
CREATE TABLE schema_with_user.test_table(column1 int);
SELECT create_reference_table('schema_with_user.test_table');

-- verify that owner of the created schema is test-user
\c - - - :worker_1_port
\dn schema_with_user

\c - - - :master_port
-- we do not use run_command_on_coordinator_and_workers here because when there is CASCADE, it causes deadlock
DROP OWNED BY "test-user" CASCADE;
SELECT run_command_on_workers('DROP OWNED BY "test-user" CASCADE');
SELECT run_command_on_coordinator_and_workers('DROP USER "test-user"');

DROP FUNCTION run_command_on_coordinator_and_workers(p_sql text);
