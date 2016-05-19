--
-- MULTI_FDW_STORAGE_TYPE
--

-- Create two tables one regular and one foreign, then check whether
-- shardstorage is correct

-- explicitly set shard id
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 400000;

-- create regular table
CREATE TABLE people (
	id bigint not null,
	firstname char(10) not null,
	lastname char(10) not null,
	age integer not null);

-- create distributed table
SELECT master_create_distributed_table('people', 'id', 'append');

-- create worker shards
SELECT master_create_empty_shard('people');

-- check shardstorage
SELECT shardstorage FROM pg_dist_shard WHERE shardid = 400000;

-- create foreign table
CREATE FOREIGN TABLE people_foreign (
	id bigint not null,
	firstname char(10) not null,
	lastname char(10) not null,
	age integer not null)
SERVER file_server 
OPTIONS (format 'text', filename '', delimiter '|', null '');

-- create distributed table
SELECT master_create_distributed_table('people_foreign', 'id', 'append');

-- create worker shards
SELECT master_create_empty_shard('people_foreign');

-- check shardstorage
SELECT shardstorage FROM pg_dist_shard WHERE shardid = 400001;
