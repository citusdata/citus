--
-- MULTI_SCHEMA_SUPPORT
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1190000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1190000;

-- test master_append_table_to_shard with schema
CREATE SCHEMA test_schema_support;

CREATE TABLE test_schema_support.nation_append(
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);
SELECT master_create_distributed_table('test_schema_support.nation_append', 'n_nationkey', 'append');

SELECT master_create_empty_shard('test_schema_support.nation_append');

-- create table to append
CREATE TABLE public.nation_local(
    n_nationkey integer not null,
    n_name char(25) not null,
    n_regionkey integer not null,
    n_comment varchar(152)
);

\COPY public.nation_local FROM STDIN with delimiter '|';
0|ALGERIA|0| haggle. carefully final deposits detect slyly agai
1|ARGENTINA|1|al foxes promise slyly according to the regular accounts. bold requests alon
2|BRAZIL|1|y alongside of the pending deposits. carefully special packages are about the ironic forges. slyly special 
3|CANADA|1|eas hang ironic, silent packages. slyly regular packages are furiously over the tithes. fluffily bold
4|EGYPT|4|y above the carefully unusual theodolites. final dugouts are quickly across the furiously regular d
5|ETHIOPIA|0|ven packages wake quickly. regu
\.

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

-- test with search_path is set
SET search_path TO test_schema_support;

SELECT master_append_table_to_shard(1190000, 'public.nation_local', 'localhost', :master_port);

-- verify table actually appended to shard
SELECT COUNT(*) FROM nation_append;

-- test with search_path is set and shard name contains special characters
SELECT master_append_table_to_shard(1190001, 'nation_local', 'localhost', :master_port);

-- verify table actually appended to shard
SELECT COUNT(*) FROM "nation._'append";
