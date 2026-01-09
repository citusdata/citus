-- Test the fix for https://github.com/citusdata/citus/issues/8243

-- Fix empty list for worker subquery tasks when the query has no
-- ditribtued table but at least one reference table or schema
-- sharded table

SET citus.next_shard_id TO 580000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor to 1;

CREATE SCHEMA issue_8243;
SET search_path TO issue_8243;

-- DDL for the test; we need some schema sharded tables
CREATE SCHEMA schmshrd;
SELECT citus_schema_distribute('schmshrd');
CREATE TABLE schmshrd.t1(id bigserial PRIMARY KEY, name text, val int);
CREATE TABLE schmshrd.t2(id bigserial PRIMARY KEY, name text, val int);
CREATE TABLE schmshrd.t3(id bigserial PRIMARY KEY, name text, val int);

-- and some reference tables
CREATE TABLE ref1(id bigserial PRIMARY KEY, name text);
CREATE TABLE ref2(id bigserial PRIMARY KEY, name text);
CREATE TABLE ref3(id bigserial PRIMARY KEY, name text);

SELECT create_reference_table('ref1');
SELECT create_reference_table('ref2');
SELECT create_reference_table('ref3');

-- and distributed tables
CREATE TABLE dist1_8243(id bigserial PRIMARY KEY, name text, val int);
CREATE TABLE dist2_8243(id bigserial PRIMARY KEY, name text, val int);

SELECT create_distributed_table('dist1_8243', 'id');
SELECT create_distributed_table('dist2_8243', 'id');

-- Test some queries that would previously end up with empty subquery tasks on workers.
-- Query characteristics:
-- - no distributed tables
-- - at least one reference table or schema sharded table
-- - some property that prevents a router plan (e.g. nextval() on a citus table in select targets)
-- - worker subquery task(s) needed; the Postgres plan has a subquery scan node

-- Test 1: schema shareded table only; exactly 1 task in the query plan.
EXPLAIN (verbose, costs off)
SELECT nextval('schmshrd.t1_id_seq'::regclass) AS id, name
FROM (select name from schmshrd.t2 group by name) sub ;

-- Test 2: bunch of schema sharded tables; still expect 1 task
EXPLAIN (verbose, costs off)
SELECT nextval('schmshrd.t1_id_seq'::regclass) AS id, sub.name
FROM (select t1.name as name from schmshrd.t1 t1, schmshrd.t2 t2, schmshrd.t3 t3 where t1.id = t2.id  and t3.name = t2.name group by t1.name) sub ;

-- Test 3: reference table only; exactly 1 task in the query plan.
EXPLAIN (verbose, costs off)
SELECT nextval('ref1_id_seq'::regclass) AS id, name
FROM (select name from ref2 group by name) sub ;

-- Test 4: bunch of reference tables; still expect 1 task
EXPLAIN (verbose, costs off)
SELECT nextval('ref1_id_seq'::regclass) AS id, sub.name
FROM (select r1.name as name from ref1 r1, ref2 r2, ref3 r3 where r1.id = r2.id  and r3.name = r2.name group by r1.name) sub ;

-- Test 5: mix of schema sharded and reference tables; exactly 1 task in the query plan.
EXPLAIN (verbose, costs off)
SELECT nextval('schmshrd.t1_id_seq'::regclass) AS id	, sub.name
FROM (select t1.name as name from schmshrd.t1 t1, ref1 r1 where t1.id = r1.id and r1.id IN (select id from ref3) group by t1.name) sub ;

-- Test 6: sanity tests - the fix does not interfere with outer join between reference and distributed table
-- where a restriction prunes out shard index 0 of the distributed table.

-- Plan has 3 tasks
EXPLAIN (verbose, costs off)
SELECT  x1.name, dist1_8243.val
FROM ref2 x1 left outer join dist1_8243 using (id)
WHERE dist1_8243.id IN (1, 10001, 999989);

-- Plan has 2 tasks
EXPLAIN (verbose, costs off)
SELECT  x1.name, dist2_8243.val
FROM ref3 x1 left outer join dist2_8243  using (id)
WHERE dist2_8243.id IN (10001, 999989);

-- Test 7: failing query from https://github.com/citusdata/citus/issues/8243
SET search_path TO schmshrd;
INSERT INTO t2 (name) VALUES ('user1'), ('user2'), ('user3'), ('user1'), ('user2'), ('user1');
INSERT INTO t1 (name) SELECT name FROM (SELECT name FROM t2 GROUP BY name) sub;
SELECT id, name FROM t1 ORDER BY id;

-- and for reference tables
SET search_path TO issue_8243;
INSERT INTO ref2 (name) VALUES ('user1'), ('user2'), ('user3'), ('user1'), ('user2'), ('user1');
INSERT INTO ref1 (name) SELECT name FROM (SELECT name FROM ref2 GROUP BY name) sub;
SELECT id, name FROM ref1 ORDER BY id;

--- clean up:
SET client_min_messages TO WARNING;
DROP SCHEMA schmshrd CASCADE;
DROP SCHEMA issue_8243 CASCADE;

RESET citus.next_shard_id;
RESET citus.shard_count;
RESET citus.shard_replication_factor;

