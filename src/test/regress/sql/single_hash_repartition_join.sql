--
-- MULTI_SINGLE_HASH_REPARTITION_JOIN
--

CREATE SCHEMA single_hash_repartition;

SET search_path TO 'single_hash_repartition';
SET citus.enable_single_hash_repartition_joins TO ON;

CREATE TABLE single_hash_repartition_first (id int, sum int, avg float);
CREATE TABLE single_hash_repartition_second (id int, sum int, avg float);
CREATE TABLE ref_table (id int, sum int, avg float);

SET citus.shard_count TO 4;

SELECT create_distributed_table('single_hash_repartition_first', 'id');
SELECT create_distributed_table('single_hash_repartition_second', 'id');
SELECT create_reference_table('ref_table');

SET citus.log_multi_join_order TO ON;
SET client_min_messages TO DEBUG2;

-- a very basic single hash re-partitioning example
EXPLAIN SELECT
	count(*)
FROM
	single_hash_repartition_first t1, single_hash_repartition_second t2
WHERE
	t1.id = t2.sum;

-- the same query with the orders of the tables have changed
EXPLAIN SELECT
	count(*)
FROM
	single_hash_repartition_second t1, single_hash_repartition_first t2
WHERE
	t2.sum = t1.id;

-- single hash repartition after bcast joins
EXPLAIN SELECT
	count(*)
FROM
	ref_table r1, single_hash_repartition_second t1, single_hash_repartition_first t2
WHERE
	r1.id = t1.id AND t2.sum = t1.id;

-- a more complicated join order, first colocated join, later single hash repartition join
EXPLAIN SELECT
	count(*)
FROM
	single_hash_repartition_first t1, single_hash_repartition_first t2, single_hash_repartition_second t3
WHERE
	t1.id = t2.id AND t1.sum = t3.id;


-- a more complicated join order, first hash-repartition join, later single hash repartition join
EXPLAIN SELECT
	count(*)
FROM
	single_hash_repartition_first t1, single_hash_repartition_first t2, single_hash_repartition_second t3
WHERE
	t1.sum = t2.sum AND t1.sum = t3.id;

-- single hash repartitioning is not supported between different column types
EXPLAIN SELECT
	count(*)
FROM
	single_hash_repartition_first t1, single_hash_repartition_first t2, single_hash_repartition_second t3
WHERE
	t1.id = t2.id AND t1.avg = t3.id;

-- single repartition query in CTE
-- should work fine
EXPLAIN WITH cte1 AS
(
	SELECT
		t1.id * t2.avg as data
	FROM
		single_hash_repartition_first t1, single_hash_repartition_second t2
	WHERE
		t1.id = t2.sum
	AND t1.sum > 5000
	ORDER BY 1 DESC
	LIMIT 50
)
SELECT
	count(*)
FROM
	cte1, single_hash_repartition_first
WHERE
	cte1.data > single_hash_repartition_first.id;


-- two single repartitions
EXPLAIN SELECT
	count(*)
FROM
	single_hash_repartition_first t1, single_hash_repartition_second t2, single_hash_repartition_second t3
WHERE
	t1.id = t2.sum AND t2.sum = t3.id;


-- two single repartitions again, but this
-- time the columns of the second join is reverted
EXPLAIN SELECT
	avg(t1.avg + t2.avg)
FROM
	single_hash_repartition_first t1, single_hash_repartition_second t2, single_hash_repartition_second t3
WHERE
	t1.id = t2.sum AND t2.id = t3.sum
LIMIT 10;

-- lets try one more thing, show that non uniform shard distribution doesn't
-- end up with single hash repartitioning

 UPDATE pg_dist_shard SET shardmaxvalue = shardmaxvalue::int - 1 WHERE logicalrelid IN ('single_hash_repartition_first'::regclass);

-- the following queries should also be a single hash repartition queries
-- note that since we've manually updated the metadata without changing the
-- the corresponding data, the results of the query would be wrong
EXPLAIN SELECT
	count(*)
FROM
	single_hash_repartition_first t1, single_hash_repartition_second t2
WHERE
	t1.id = t2.sum;

-- the following queries should also be a single hash repartition queries
-- note that since we've manually updated the metadata without changing the
-- the corresponding data, the results of the query would be wrong
EXPLAIN SELECT
	count(*)
FROM
	single_hash_repartition_first t1, single_hash_repartition_second t2
WHERE
	t1.sum = t2.id;

RESET client_min_messages;

RESET search_path;
DROP SCHEMA single_hash_repartition CASCADE;
SET citus.enable_single_hash_repartition_joins TO OFF;

