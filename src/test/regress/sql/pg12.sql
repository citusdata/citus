SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 11 AS server_version_above_eleven
\gset
\if :server_version_above_eleven
\else
\q
\endif

SET citus.shard_replication_factor to 1;
SET citus.next_shard_id TO 60000;
SET citus.next_placement_id TO 60000;

create schema test_pg12;
set search_path to test_pg12;

CREATE FUNCTION blackhole_am_handler(internal)
RETURNS table_am_handler
AS 'citus'
LANGUAGE C;
CREATE ACCESS METHOD blackhole_am TYPE TABLE HANDLER blackhole_am_handler;

create table test_am(id int, val int) using blackhole_am;
insert into test_am values (1, 1);
-- Custom table access methods should be rejected
select create_distributed_table('test_am','id');

-- Test generated columns
-- val1 after val2 to test https://github.com/citusdata/citus/issues/3538
create table gen1 (
	id int,
	val2 int GENERATED ALWAYS AS (val1 + 2) STORED,
	val1 int
);
create table gen2 (
	id int,
	val1 int,
	val2 int GENERATED ALWAYS AS (val1 + 2) STORED
);

insert into gen1 (id, val1) values (1,4),(3,6),(5,2),(7,2);
insert into gen2 (id, val1) values (1,4),(3,6),(5,2),(7,2);

select create_distributed_table('gen1', 'id');
select create_distributed_table('gen2', 'val2');

copy gen1 to :'temp_dir''pg12_copy_test_generated';

insert into gen1 (id, val1) values (2,4),(4,6),(6,2),(8,2);
insert into gen2 (id, val1) values (2,4),(4,6),(6,2),(8,2);

select * from gen1 order by 1,2,3;
select * from gen2 order by 1,2,3;

truncate gen1;
copy gen1 from :'temp_dir''pg12_copy_test_generated';
select * from gen1 order by 1,2,3;

-- Test new VACUUM/ANALYZE options
analyze (skip_locked) gen1;
vacuum (skip_locked) gen1;
vacuum (truncate 0) gen1;
vacuum (index_cleanup 1) gen1;

-- COPY FROM
create table cptest (id int, val int);
select create_distributed_table('cptest', 'id');
copy cptest from STDIN with csv where val < 4;
1,6
2,3
3,2
4,9
5,4
\.
select sum(id), sum(val) from cptest;

-- CTE materialized/not materialized
CREATE TABLE single_hash_repartition_first (id int, sum int, avg float);
CREATE TABLE single_hash_repartition_second (id int primary key, sum int, avg float);

SELECT create_distributed_table('single_hash_repartition_first', 'id');
SELECT create_distributed_table('single_hash_repartition_second', 'id');

INSERT INTO single_hash_repartition_first
SELECT i, i * 3, i * 0.3
FROM generate_series(0, 100) i;

INSERT INTO single_hash_repartition_second
SELECT i * 2, i * 5, i * 0.6
FROM generate_series(0, 100) i;

-- a sample router query with NOT MATERIALIZED
-- which pushes down the filters to the CTE
SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
WITH cte1 AS NOT MATERIALIZED
(
	SELECT id
	FROM single_hash_repartition_first t1
)
SELECT count(*)
FROM cte1, single_hash_repartition_second
WHERE cte1.id = single_hash_repartition_second.id AND single_hash_repartition_second.id = 45;
$Q$);

-- same query, without NOT MATERIALIZED, which is already default
-- which pushes down the filters to the CTE
SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
WITH cte1 AS
(
	SELECT id
	FROM single_hash_repartition_first t1
)
SELECT count(*)
FROM cte1, single_hash_repartition_second
WHERE cte1.id = single_hash_repartition_second.id AND single_hash_repartition_second.id = 45;
$Q$);

-- same query with MATERIALIZED
-- which prevents pushing down filters to the CTE,
-- thus becoming a real-time query
SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
WITH cte1 AS MATERIALIZED
(
	SELECT id
	FROM single_hash_repartition_first t1
)
SELECT count(*)
FROM cte1, single_hash_repartition_second
WHERE cte1.id = single_hash_repartition_second.id AND single_hash_repartition_second.id = 45;
$Q$);

-- similar query with MATERIALIZED
-- now manually have the same filter in the CTE
-- thus becoming a router query again
SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
WITH cte1 AS MATERIALIZED
(
	SELECT id
	FROM single_hash_repartition_first t1
	WHERE id = 45
)
SELECT count(*)
FROM cte1, single_hash_repartition_second
WHERE cte1.id = single_hash_repartition_second.id AND single_hash_repartition_second.id = 45;
$Q$);

-- now, have a real-time query without MATERIALIZED
-- these are sanitiy checks, because all of the CTEs are recursively
-- planned and there is no benefit that Citus can have
SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
WITH cte1 AS MATERIALIZED
(
	SELECT id
	FROM single_hash_repartition_first t1
	WHERE sum = 45
)
SELECT count(*)
FROM cte1, single_hash_repartition_second
WHERE cte1.id = single_hash_repartition_second.id AND single_hash_repartition_second.sum = 45;
$Q$);

SELECT public.coordinator_plan($Q$
EXPLAIN (COSTS OFF)
WITH cte1 AS NOT MATERIALIZED
(
	SELECT id
	FROM single_hash_repartition_first t1
	WHERE sum = 45
)
SELECT count(*)
FROM cte1, single_hash_repartition_second
WHERE cte1.id = single_hash_repartition_second.id AND single_hash_repartition_second.sum = 45;
$Q$);

-- Foreign keys to partition tables
CREATE TABLE collections_list (
	key bigint,
	collection_id integer,
	value numeric,
	PRIMARY KEY(key, collection_id)
) PARTITION BY LIST (collection_id);

CREATE TABLE collections_list_0
	PARTITION OF collections_list (key, collection_id, value)
	FOR VALUES IN ( 0 );
CREATE TABLE collections_list_1
	PARTITION OF collections_list (key, collection_id, value)
	FOR VALUES IN ( 1 );

CREATE TABLE collection_users
	(used_id integer, collection_id integer, key bigint);

ALTER TABLE collection_users
    ADD CONSTRAINT collection_users_fkey FOREIGN KEY (key, collection_id) REFERENCES collections_list (key, collection_id);

-- sanity check for postgres
INSERT INTO collections_list VALUES (1, 0, '1.1');
INSERT INTO collection_users VALUES (1, 0, 1);

-- should fail because of fkey
INSERT INTO collection_users VALUES (1, 1000, 1);

SELECT create_distributed_table('collections_list', 'key');
SELECT create_distributed_table('collection_users', 'key');

-- should still fail because of fkey
INSERT INTO collection_users VALUES (1, 1000, 1);

-- whereas new record with partition should go through
INSERT INTO collections_list VALUES (2, 1, '1.2');
INSERT INTO collection_users VALUES (5, 1, 2);

-- AND CHAIN
CREATE TABLE test (x int, y int);
INSERT INTO test (x,y) SELECT i,i*3 from generate_series(1, 100) i;

SELECT create_distributed_table('test', 'x');

-- single shard queries with CHAIN
BEGIN;
UPDATE test SET y = 15 WHERE x = 1;
COMMIT AND CHAIN;
SELECT * FROM test WHERE x = 1;
COMMIT;

BEGIN;
UPDATE test SET y = 20 WHERE x = 1;
ROLLBACK AND CHAIN;
SELECT * FROM test WHERE x = 1;
COMMIT;

-- multi shard queries with CHAIN
BEGIN;
UPDATE test SET y = 25;
COMMIT AND CHAIN;
SELECT DISTINCT y FROM test;
COMMIT;

BEGIN;
UPDATE test SET y = 30;
ROLLBACK AND CHAIN;
SELECT DISTINCT y FROM test;
COMMIT;

-- does read only carry over?

BEGIN READ ONLY;
COMMIT AND CHAIN;
UPDATE test SET y = 35;
COMMIT;
SELECT DISTINCT y FROM test;

BEGIN READ ONLY;
ROLLBACK AND CHAIN;
UPDATE test SET y = 40;
COMMIT;
SELECT DISTINCT y FROM test;

-- non deterministic collations
CREATE COLLATION test_pg12.case_insensitive (
	provider = icu,
	locale = 'und-u-ks-level2',
	deterministic = false
);

CREATE TABLE col_test (
	id int,
	val text collate case_insensitive
);

insert into col_test values
	(1, 'asdF'), (2, 'vAlue'), (3, 'asDF');

-- Hash distribution of non deterministic collations are unsupported
select create_distributed_table('col_test', 'val');
select create_distributed_table('col_test', 'id');

insert into col_test values
	(4, 'vALue'), (5, 'AsDf'), (6, 'value');

select count(*)
from col_test
where val = 'asdf';

\set VERBOSITY terse
drop schema test_pg12 cascade;
\set VERBOSITY default

SET citus.shard_replication_factor to 2;

