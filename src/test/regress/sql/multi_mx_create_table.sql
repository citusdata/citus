--
-- MULTI_MX_CREATE_TABLE
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1220000;

SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
SELECT start_metadata_sync_to_node('localhost', :worker_2_port);

-- create schema to test schema support
CREATE SCHEMA citus_mx_test_schema;
CREATE SCHEMA citus_mx_test_schema_join_1;
CREATE SCHEMA citus_mx_test_schema_join_2;

-- create UDFs that we're going to use in our tests
SET search_path TO public;
CREATE OR REPLACE FUNCTION simpleTestFunction(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

SET search_path TO citus_mx_test_schema;
CREATE OR REPLACE FUNCTION simpleTestFunction2(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE FUNCTION public.immutable_append_mx(old_values int[], new_value int)
RETURNS int[] AS $$ SELECT old_values || new_value $$ LANGUAGE SQL IMMUTABLE;

CREATE OPERATOR citus_mx_test_schema.=== (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===,
    NEGATOR = !==,
    HASHES, MERGES
);

SET search_path TO public;
CREATE COLLATION citus_mx_test_schema.english FROM "en_US";

CREATE TYPE citus_mx_test_schema.new_composite_type as (key1 text, key2 text);
CREATE TYPE order_side_mx AS ENUM ('buy', 'sell');

-- now create required stuff in the worker 1
\c - - - :worker_1_port

-- create schema to test schema support
CREATE SCHEMA citus_mx_test_schema;
CREATE SCHEMA citus_mx_test_schema_join_1;
CREATE SCHEMA citus_mx_test_schema_join_2;

-- create UDFs in worker node
CREATE OR REPLACE FUNCTION simpleTestFunction(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

SET search_path TO citus_mx_test_schema;
CREATE OR REPLACE FUNCTION simpleTestFunction2(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE FUNCTION public.immutable_append_mx(old_values int[], new_value int)
RETURNS int[] AS $$ SELECT old_values || new_value $$ LANGUAGE SQL IMMUTABLE;

-- create operator
CREATE OPERATOR citus_mx_test_schema.=== (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===,
    NEGATOR = !==,
    HASHES, MERGES
);

SET search_path TO public;
CREATE COLLATION citus_mx_test_schema.english FROM "en_US";

SET search_path TO public;
CREATE TYPE citus_mx_test_schema.new_composite_type as (key1 text, key2 text);
CREATE TYPE order_side_mx AS ENUM ('buy', 'sell');

-- now create required stuff in the worker 2
\c - - - :worker_2_port

-- create schema to test schema support
CREATE SCHEMA citus_mx_test_schema;
CREATE SCHEMA citus_mx_test_schema_join_1;
CREATE SCHEMA citus_mx_test_schema_join_2;


-- create UDF
CREATE OR REPLACE FUNCTION simpleTestFunction(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

SET search_path TO citus_mx_test_schema;
CREATE OR REPLACE FUNCTION simpleTestFunction2(theValue integer)
    RETURNS text AS
$$
DECLARE
    strresult text;
BEGIN
    RETURN theValue * 3 / 2 + 1;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE FUNCTION public.immutable_append_mx(old_values int[], new_value int)
RETURNS int[] AS $$ SELECT old_values || new_value $$ LANGUAGE SQL IMMUTABLE;

-- create operator
CREATE OPERATOR citus_mx_test_schema.=== (
    LEFTARG = int,
    RIGHTARG = int,
    PROCEDURE = int4eq,
    COMMUTATOR = ===,
    NEGATOR = !==,
    HASHES, MERGES
);


SET search_path TO public;
CREATE COLLATION citus_mx_test_schema.english FROM "en_US";

SET search_path TO public;
CREATE TYPE citus_mx_test_schema.new_composite_type as (key1 text, key2 text);
CREATE TYPE order_side_mx AS ENUM ('buy', 'sell');

-- connect back to the master, and do some more tests
\c - - - :master_port

SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO streaming;
SET search_path TO public;

CREATE TABLE nation_hash(
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);

SET citus.shard_count TO 16;
SELECT create_distributed_table('nation_hash', 'n_nationkey');

SET search_path TO citus_mx_test_schema;

-- create mx tables that we're going to use for our tests
CREATE TABLE citus_mx_test_schema.nation_hash(
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);

SELECT create_distributed_table('nation_hash', 'n_nationkey');

CREATE TABLE citus_mx_test_schema_join_1.nation_hash (
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152));

SET citus.shard_count TO 4;
SELECT create_distributed_table('citus_mx_test_schema_join_1.nation_hash', 'n_nationkey');

CREATE TABLE citus_mx_test_schema_join_1.nation_hash_2 (
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152));

SELECT create_distributed_table('citus_mx_test_schema_join_1.nation_hash_2', 'n_nationkey');

SET search_path TO citus_mx_test_schema_join_2;
CREATE TABLE nation_hash (
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152));

SELECT create_distributed_table('nation_hash', 'n_nationkey');

SET search_path TO citus_mx_test_schema;
CREATE TABLE nation_hash_collation_search_path(
    n_nationkey integer not null,
    n_name char(25) not null COLLATE english,
    n_regionkey integer not null,
    n_comment varchar(152)
);
SELECT create_distributed_table('nation_hash_collation_search_path', 'n_nationkey');

\COPY nation_hash_collation_search_path FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special 
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

CREATE TABLE citus_mx_test_schema.nation_hash_composite_types(
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152),
    test_col citus_mx_test_schema.new_composite_type
);

SELECT create_distributed_table('citus_mx_test_schema.nation_hash_composite_types', 'n_nationkey');

-- insert some data to verify composite type queries
\COPY citus_mx_test_schema.nation_hash_composite_types FROM STDIN with delimiter '|';
0|ALGERIA|0|haggle. carefully final deposits detect slyly agai|(a,a)
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon|(a,b)
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special |(a,c)
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold|(a,d)
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d|(a,e)
5|ETHIOPIA|0|ven packages wake quickly. regu|(a,f)
\.

-- now create tpch tables 
-- Create new table definitions for use in testing in distributed planning and
-- execution functionality. Also create indexes to boost performance.
SET search_path TO public;

CREATE TABLE lineitem_mx (
    l_orderkey bigint not null,
    l_partkey integer not null,
    l_suppkey integer not null,
    l_linenumber integer not null,
    l_quantity decimal(15, 2) not null,
    l_extendedprice decimal(15, 2) not null,
    l_discount decimal(15, 2) not null,
    l_tax decimal(15, 2) not null,
    l_returnflag char(1) not null,
    l_linestatus char(1) not null,
    l_shipdate date not null,
    l_commitdate date not null,
    l_receiptdate date not null,
    l_shipinstruct char(25) not null,
    l_shipmode char(10) not null,
    l_comment varchar(44) not null,
    PRIMARY KEY(l_orderkey, l_linenumber) );

SET citus.shard_count TO 16;
SELECT create_distributed_table('lineitem_mx', 'l_orderkey');

CREATE INDEX lineitem_mx_time_index ON lineitem_mx (l_shipdate);

CREATE TABLE orders_mx (
    o_orderkey bigint not null,
    o_custkey integer not null,
    o_orderstatus char(1) not null,
    o_totalprice decimal(15,2) not null,
    o_orderdate date not null,
    o_orderpriority char(15) not null,
    o_clerk char(15) not null,
    o_shippriority integer not null,
    o_comment varchar(79) not null,
    PRIMARY KEY(o_orderkey) );
SELECT create_distributed_table('orders_mx', 'o_orderkey');

CREATE TABLE customer_mx (
    c_custkey integer not null,
    c_name varchar(25) not null,
    c_address varchar(40) not null,
    c_nationkey integer not null,
    c_phone char(15) not null,
    c_acctbal decimal(15,2) not null,
    c_mktsegment char(10) not null,
    c_comment varchar(117) not null);

SET citus.shard_count TO 1;
SELECT create_reference_table('customer_mx');

CREATE TABLE nation_mx (
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152));

SELECT create_reference_table('nation_mx');

CREATE TABLE part_mx (
    p_partkey integer not null,
    p_name varchar(55) not null,
    p_mfgr char(25) not null,
    p_brand char(10) not null,
    p_type varchar(25) not null,
    p_size integer not null,
    p_container char(10) not null,
    p_retailprice decimal(15,2) not null,
    p_comment varchar(23) not null);

SELECT create_reference_table('part_mx');

CREATE TABLE supplier_mx
(
    s_suppkey integer not null,
    s_name char(25) not null,
    s_address varchar(40) not null,
    s_nationkey integer,
    s_phone char(15) not null,
    s_acctbal decimal(15,2) not null,
    s_comment varchar(101) not null
);

SELECT create_reference_table('supplier_mx');

-- Create test table for ddl
CREATE TABLE mx_ddl_table (
    key int primary key,
    value int
);

SET citus.shard_count TO 4;
SELECT create_distributed_table('mx_ddl_table', 'key', 'hash');

-- Load some test data
COPY mx_ddl_table (key, value) FROM STDIN WITH (FORMAT 'csv');
1,10
2,11
3,21
4,37
5,60
6,100
10,200
11,230
\.

-- test table for modifications
CREATE TABLE limit_orders_mx (
    id bigint PRIMARY KEY,
    symbol text NOT NULL,
    bidder_id bigint NOT NULL,
    placed_at timestamp NOT NULL,
    kind order_side_mx NOT NULL,
    limit_price decimal NOT NULL DEFAULT 0.00 CHECK (limit_price >= 0.00)
);

SET citus.shard_count TO 2;
SELECT create_distributed_table('limit_orders_mx', 'id');

-- test table for modifications
CREATE TABLE multiple_hash_mx (
    category text NOT NULL,
    data text NOT NULL
);

SELECT create_distributed_table('multiple_hash_mx', 'category');

SET citus.shard_count TO 4;
CREATE TABLE app_analytics_events_mx (id bigserial, app_id integer, name text);
SELECT create_distributed_table('app_analytics_events_mx', 'app_id');


CREATE TABLE researchers_mx (
    id bigint NOT NULL,
    lab_id int NOT NULL,
    name text NOT NULL
);

SET citus.shard_count TO 2;
SELECT create_distributed_table('researchers_mx', 'lab_id');

CREATE TABLE labs_mx (
    id bigint NOT NULL,
    name text NOT NULL
);

SET citus.shard_count TO 1;
SELECT create_distributed_table('labs_mx', 'id');

-- now, for some special failures...
CREATE TABLE objects_mx (
    id bigint PRIMARY KEY,
    name text NOT NULL
);

SELECT create_distributed_table('objects_mx', 'id', 'hash');

CREATE TABLE articles_hash_mx (
    id bigint NOT NULL,
    author_id bigint NOT NULL,
    title varchar(20) NOT NULL,
    word_count integer
);

-- this table is used in router executor tests
CREATE TABLE articles_single_shard_hash_mx (LIKE articles_hash_mx);

SET citus.shard_count TO 2;
SELECT create_distributed_table('articles_hash_mx', 'author_id');

SET citus.shard_count TO 1;
SELECT create_distributed_table('articles_single_shard_hash_mx', 'author_id');

SET citus.shard_count TO 4;
CREATE TABLE company_employees_mx (company_id int, employee_id int, manager_id int); 
SELECT create_distributed_table('company_employees_mx', 'company_id');

WITH shard_counts AS (
	SELECT logicalrelid, count(*) AS shard_count FROM pg_dist_shard GROUP BY logicalrelid
	)
SELECT logicalrelid, colocationid, shard_count, partmethod, repmodel 
FROM pg_dist_partition NATURAL JOIN shard_counts 
ORDER BY colocationid, logicalrelid;
