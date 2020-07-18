CREATE SCHEMA adaptive_executor;
SET search_path TO adaptive_executor;

SET citus.shard_replication_factor to 1;
SET citus.enable_repartition_joins TO true;

CREATE TABLE ab(a int, b int);
SELECT create_distributed_table('ab', 'a');
INSERT INTO ab SELECT *,* FROM generate_series(1,10);

SELECT COUNT(*) FROM ab k, ab l
WHERE k.a = l.b;

SELECT COUNT(*) FROM ab k, ab l, ab m, ab t
WHERE k.a = l.b AND k.a = m.b AND t.b = l.a;

SELECT count(*) FROM (SELECT k.a FROM ab k, ab l WHERE k.a = l.b) first, (SELECT * FROM ab) second WHERE first.a = second.b;

BEGIN;
SELECT count(*) FROM (SELECT k.a FROM ab k, ab l WHERE k.a = l.b) first, (SELECT * FROM ab) second WHERE first.a = second.b;
SELECT count(*) FROM (SELECT k.a FROM ab k, ab l WHERE k.a = l.b) first, (SELECT * FROM ab) second WHERE first.a = second.b;
SELECT count(*) FROM (SELECT k.a FROM ab k, ab l WHERE k.a = l.b) first, (SELECT * FROM ab) second WHERE first.a = second.b;
ROLLBACK;

BEGIN;
INSERT INTO ab values(1, 2);
-- DDL happened before repartition query in a transaction block, so this should error.
SELECT count(*) FROM (SELECT k.a FROM ab k, ab l WHERE k.a = l.b) first, (SELECT * FROM ab) second WHERE first.a = second.b;
ROLLBACK;

SET citus.enable_single_hash_repartition_joins TO ON;

CREATE TABLE single_hash_repartition_first (id int, sum int, avg float);
CREATE TABLE single_hash_repartition_second (id int, sum int, avg float);
CREATE TABLE ref_table (id int, sum int, avg float);


SELECT create_distributed_table('single_hash_repartition_first', 'id');
SELECT create_distributed_table('single_hash_repartition_second', 'id');
SELECT create_reference_table('ref_table');


-- single hash repartition after bcast joins
EXPLAIN (COSTS OFF)
SELECT
	count(*)
FROM
	ref_table r1, single_hash_repartition_second t1, single_hash_repartition_first t2
WHERE
	r1.id = t1.id AND t2.sum = t1.id;

-- a more complicated join order, first colocated join, later single hash repartition join
EXPLAIN (COSTS OFF)
SELECT
	count(*)
FROM
	single_hash_repartition_first t1, single_hash_repartition_first t2, single_hash_repartition_second t3
WHERE
	t1.id = t2.id AND t1.sum = t3.id;

SET citus.enable_single_hash_repartition_joins TO OFF;

DROP SCHEMA adaptive_executor CASCADE;
