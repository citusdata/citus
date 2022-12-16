
SET citus.next_shard_id TO 310000;


SET citus.log_multi_join_order to true;
SET client_min_messages TO LOG;

CREATE TABLE multi_outer_join_left
(
	l_custkey integer not null,
	l_name varchar(25) not null,
	l_address varchar(40) not null,
	l_nationkey integer not null,
	l_phone char(15) not null,
	l_acctbal decimal(15,2) not null,
	l_mktsegment char(10) not null,
	l_comment varchar(117) not null
);
SELECT create_distributed_table('multi_outer_join_left', 'l_custkey', 'hash');

CREATE TABLE multi_outer_join_right
(
	r_custkey integer not null,
	r_name varchar(25) not null,
	r_address varchar(40) not null,
	r_nationkey integer not null,
	r_phone char(15) not null,
	r_acctbal decimal(15,2) not null,
	r_mktsegment char(10) not null,
	r_comment varchar(117) not null
);
SELECT create_distributed_table('multi_outer_join_right', 'r_custkey', 'hash');

CREATE TABLE multi_outer_join_right_reference
(
	r_custkey integer not null,
	r_name varchar(25) not null,
	r_address varchar(40) not null,
	r_nationkey integer not null,
	r_phone char(15) not null,
	r_acctbal decimal(15,2) not null,
	r_mktsegment char(10) not null,
	r_comment varchar(117) not null
);
SELECT create_reference_table('multi_outer_join_right_reference');

CREATE TABLE multi_outer_join_third
(
	t_custkey integer not null,
	t_name varchar(25) not null,
	t_address varchar(40) not null,
	t_nationkey integer not null,
	t_phone char(15) not null,
	t_acctbal decimal(15,2) not null,
	t_mktsegment char(10) not null,
	t_comment varchar(117) not null
);
SELECT create_distributed_table('multi_outer_join_third', 't_custkey', 'hash');

CREATE TABLE multi_outer_join_third_reference
(
	t_custkey integer not null,
	t_name varchar(25) not null,
	t_address varchar(40) not null,
	t_nationkey integer not null,
	t_phone char(15) not null,
	t_acctbal decimal(15,2) not null,
	t_mktsegment char(10) not null,
	t_comment varchar(117) not null
);
SELECT create_reference_table('multi_outer_join_third_reference');
\set customer_1_10_data :abs_srcdir '/data/customer-1-10.data'
\set customer_11_20_data :abs_srcdir '/data/customer-11-20.data'
\set customer_1_15_data :abs_srcdir '/data/customer-1-15.data'
\set client_side_copy_command '\\copy multi_outer_join_left FROM ' :'customer_1_10_data' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy multi_outer_join_left FROM ' :'customer_11_20_data' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy multi_outer_join_right FROM ' :'customer_1_15_data' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy multi_outer_join_right_reference FROM ' :'customer_1_15_data' ' with delimiter '''|''';'
:client_side_copy_command

-- Make sure we do not crash if one table has no shards
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_third b ON (l_custkey = t_custkey);

SELECT
	min(t_custkey), max(t_custkey)
FROM
	multi_outer_join_third a LEFT JOIN multi_outer_join_right_reference b ON (r_custkey = t_custkey);

-- Third table is a single shard table with all data
\set customer_1_30_data :abs_srcdir '/data/customer-1-30.data'
\set client_side_copy_command '\\copy multi_outer_join_third FROM ' :'customer_1_30_data' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy multi_outer_join_third_reference FROM ' :'customer_1_30_data' ' with delimiter '''|''';'
:client_side_copy_command

-- Regular outer join should return results for all rows
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right_reference b ON (l_custkey = r_custkey);

-- Since this is a broadcast join, we should be able to join on any key
SELECT
	count(*)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right_reference b ON (l_nationkey = r_nationkey);


-- Anti-join should return customers for which there is no row in the right table
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right_reference b ON (l_custkey = r_custkey)
WHERE
	r_custkey IS NULL;


-- Partial anti-join with specific value
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right_reference b ON (l_custkey = r_custkey)
WHERE
	r_custkey IS NULL OR r_custkey = 5;


-- This query is an INNER JOIN in disguise since there cannot be NULL results
-- Added extra filter to make query not router plannable
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right_reference b ON (l_custkey = r_custkey)
WHERE
	r_custkey = 5 or r_custkey > 15;


-- Apply a filter before the join
SELECT
	count(l_custkey), count(r_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right_reference b
	ON (l_custkey = r_custkey AND r_custkey = 5);

-- Apply a filter before the join (no matches right)
SELECT
	count(l_custkey), count(r_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right_reference b
	ON (l_custkey = r_custkey AND r_custkey = -1 /* nonexistant */);

-- Apply a filter before the join (no matches left)
SELECT
	count(l_custkey), count(r_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right_reference b
	ON (l_custkey = r_custkey AND l_custkey = -1 /* nonexistant */);

-- Right join should be disallowed in this case
SELECT
	min(r_custkey), max(r_custkey)
FROM
	multi_outer_join_left a RIGHT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);


-- Reverse right join should be same as left join
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_right_reference a RIGHT JOIN multi_outer_join_left b ON (l_custkey = r_custkey);


-- Turn the right table into a large table
\set customer_21_30_data :abs_srcdir '/data/customer-21-30.data'
\set client_side_copy_command '\\copy multi_outer_join_right FROM ' :'customer_21_30_data' ' with delimiter '''|''';'
:client_side_copy_command


-- Shards do not have 1-1 matching. We should error here.
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

-- empty tables
TRUNCATE multi_outer_join_left;
TRUNCATE multi_outer_join_right;

-- reload shards with 1-1 matching
\set customer_subset_11_20_data :abs_srcdir '/data/customer-subset-11-20.data'
\set client_side_copy_command '\\copy multi_outer_join_left FROM ' :'customer_subset_11_20_data' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy multi_outer_join_left FROM ' :'customer_21_30_data' ' with delimiter '''|''';'
:client_side_copy_command

\set customer_subset_21_30_data :abs_srcdir '/data/customer-subset-21-30.data'
\set client_side_copy_command '\\copy multi_outer_join_right FROM ' :'customer_11_20_data' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy multi_outer_join_right FROM ' :'customer_subset_21_30_data' ' with delimiter '''|''';'
:client_side_copy_command

-- multi_outer_join_third is a single shard table
-- Regular left join should work as expected
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);


-- Since we cannot broadcast or re-partition, joining on a different key should error out
SELECT
	count(*)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_nationkey = r_nationkey);


-- Anti-join should return customers for which there is no row in the right table
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey)
WHERE
	r_custkey IS NULL;


-- Partial anti-join with specific value (5, 11-15)
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey)
WHERE
	r_custkey IS NULL OR r_custkey = 15;


-- This query is an INNER JOIN in disguise since there cannot be NULL results (21)
-- Added extra filter to make query not router plannable
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey)
WHERE
	r_custkey = 21 or r_custkey < 10;


-- Apply a filter before the join
SELECT
	count(l_custkey), count(r_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b
	ON (l_custkey = r_custkey AND r_custkey = 21);


-- Right join should be allowed in this case
SELECT
	min(r_custkey), max(r_custkey)
FROM
	multi_outer_join_left a RIGHT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);


-- Reverse right join should be same as left join
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_right a RIGHT JOIN multi_outer_join_left b ON (l_custkey = r_custkey);


-- Mix of outer joins on partition column
SELECT
	l1.l_custkey
FROM
	multi_outer_join_left l1
	LEFT JOIN multi_outer_join_right r1 ON (l1.l_custkey = r1.r_custkey)
	LEFT JOIN multi_outer_join_right r2 ON (l1.l_custkey  = r2.r_custkey)
	RIGHT JOIN multi_outer_join_left l2 ON (r2.r_custkey = l2.l_custkey)
ORDER BY 1
LIMIT 1;

-- add an anti-join
SELECT
	l1.l_custkey
FROM
	multi_outer_join_left l1
	LEFT JOIN multi_outer_join_right r1 ON (l1.l_custkey = r1.r_custkey)
	LEFT JOIN multi_outer_join_right r2 ON (l1.l_custkey  = r2.r_custkey)
	RIGHT JOIN multi_outer_join_left l2 ON (r2.r_custkey = l2.l_custkey)
WHERE
	r1.r_custkey is NULL
ORDER BY 1
LIMIT 1;

-- Three way join 2-2-1 (local + broadcast join) should work
SELECT
	l_custkey, r_custkey, t_custkey
FROM
	multi_outer_join_left l1
	LEFT JOIN multi_outer_join_right r1 ON (l1.l_custkey = r1.r_custkey)
	LEFT JOIN multi_outer_join_third_reference t1 ON (r1.r_custkey  = t1.t_custkey)
ORDER BY l_custkey, r_custkey, t_custkey;

-- Right join with single shard right most table should work
SELECT
	l_custkey, r_custkey, t_custkey
FROM
	multi_outer_join_left l1
	LEFT JOIN multi_outer_join_right r1 ON (l1.l_custkey = r1.r_custkey)
	RIGHT JOIN multi_outer_join_third_reference t1 ON (r1.r_custkey  = t1.t_custkey)
ORDER BY l_custkey, r_custkey, t_custkey;

-- Right join with single shard left most table should work
SELECT
	t_custkey, r_custkey, l_custkey
FROM
	multi_outer_join_third_reference t1
	RIGHT JOIN multi_outer_join_right r1 ON (t1.t_custkey = r1.r_custkey)
	LEFT JOIN multi_outer_join_left l1 ON (r1.r_custkey  = l1.l_custkey)
ORDER BY t_custkey, r_custkey, l_custkey;

-- Make it anti-join, should display values with l_custkey is null
SELECT
	t_custkey, r_custkey, l_custkey
FROM
	multi_outer_join_third_reference t1
	RIGHT JOIN multi_outer_join_right r1 ON (t1.t_custkey = r1.r_custkey)
	LEFT JOIN multi_outer_join_left l1 ON (r1.r_custkey  = l1.l_custkey)
WHERE
	l_custkey is NULL
ORDER BY t_custkey, r_custkey, l_custkey;

-- Cascading right join with single shard left most table
SELECT
	t_custkey, r_custkey, l_custkey
FROM
	multi_outer_join_third_reference t1
	RIGHT JOIN multi_outer_join_right r1 ON (t1.t_custkey = r1.r_custkey)
	RIGHT JOIN multi_outer_join_left l1 ON (r1.r_custkey  = l1.l_custkey)
ORDER BY 1,2,3;

-- full outer join should work between reference tables
SELECT
	t_custkey, r_custkey
FROM
	(SELECT * FROM multi_outer_join_third_reference r1
	 FULL JOIN multi_outer_join_right_reference r2 ON (r1.t_custkey = r2.r_custkey)
	) AS foo
	INNER JOIN multi_outer_join_right USING (r_custkey)
ORDER BY 1,2;

-- full outer join should work with 1-1 matched shards
SELECT
	l_custkey, r_custkey
FROM
	multi_outer_join_left l1
	FULL JOIN multi_outer_join_right r1 ON (l1.l_custkey = r1.r_custkey)
ORDER BY 1 DESC, 2 DESC;

-- full outer join + anti (right) should work with 1-1 matched shards
SELECT
	l_custkey, r_custkey
FROM
	multi_outer_join_left l1
	FULL JOIN multi_outer_join_right r1 ON (l1.l_custkey = r1.r_custkey)
WHERE
	r_custkey is NULL
ORDER BY 1 DESC, 2 DESC;

-- full outer join + anti (left) should work with 1-1 matched shards
SELECT
	l_custkey, r_custkey
FROM
	multi_outer_join_left l1
	FULL JOIN multi_outer_join_right r1 ON (l1.l_custkey = r1.r_custkey)
WHERE
	l_custkey is NULL
ORDER BY 1 DESC, 2 DESC;

-- full outer join + anti (both) should work with 1-1 matched shards
SELECT
	l_custkey, r_custkey
FROM
	multi_outer_join_left l1
	FULL JOIN multi_outer_join_right r1 ON (l1.l_custkey = r1.r_custkey)
WHERE
	l_custkey is NULL or r_custkey is NULL
ORDER BY 1 DESC, 2 DESC;

-- full outer join should error out for mismatched shards
SELECT
	l_custkey, t_custkey
FROM
	multi_outer_join_left l1
	FULL JOIN multi_outer_join_third t1 ON (l1.l_custkey = t1.t_custkey)
ORDER BY 1 DESC, 2 DESC;

-- inner join  + single shard left join should work
SELECT
	l_custkey, r_custkey, t_custkey
FROM
	multi_outer_join_left l1
	INNER JOIN multi_outer_join_right r1 ON (l1.l_custkey = r1.r_custkey)
	LEFT JOIN multi_outer_join_third_reference t1 ON (r1.r_custkey  = t1.t_custkey)
ORDER BY 1 DESC, 2 DESC, 3 DESC;

-- inner (broadcast) join  + 2 shards left (local) join should work
SELECT
	l_custkey, t_custkey, r_custkey
FROM
	multi_outer_join_left l1
	INNER JOIN multi_outer_join_third_reference t1 ON (l1.l_custkey = t1.t_custkey)
	LEFT JOIN multi_outer_join_right r1 ON (l1.l_custkey  = r1.r_custkey)
ORDER BY 1 DESC, 2 DESC, 3 DESC;

-- inner (local) join  + 2 shards left (dual partition) join
SELECT
	t_custkey, l_custkey, r_custkey
FROM
	multi_outer_join_third_reference t1
	INNER JOIN multi_outer_join_left l1 ON (l1.l_custkey = t1.t_custkey)
	LEFT JOIN multi_outer_join_right r1 ON (l1.l_custkey  = r1.r_custkey)
ORDER BY
    t_custkey, l_custkey, r_custkey;

-- inner (local) join  + 2 shards left (dual partition) join
SELECT
	l_custkey, t_custkey, r_custkey
FROM
	multi_outer_join_left l1
	INNER JOIN multi_outer_join_third_reference t1 ON (l1.l_custkey = t1.t_custkey)
	LEFT JOIN multi_outer_join_right r1 ON (l1.l_custkey  = r1.r_custkey)
ORDER BY 1 DESC, 2 DESC, 3 DESC;


-- inner (broadcast) join  + 2 shards left (local) + anti join should work
SELECT
	l_custkey, t_custkey, r_custkey
FROM
	multi_outer_join_left l1
	INNER JOIN multi_outer_join_third_reference t1 ON (l1.l_custkey = t1.t_custkey)
	LEFT JOIN multi_outer_join_right r1 ON (l1.l_custkey  = r1.r_custkey)
WHERE
	r_custkey is NULL
ORDER BY 1 DESC, 2 DESC, 3 DESC;

-- Test joinExpr aliases by performing an outer-join.
SELECT
	t_custkey
FROM
	(multi_outer_join_right r1
	LEFT OUTER JOIN multi_outer_join_left l1 ON (l1.l_custkey = r1.r_custkey)) AS
    test(c_custkey, c_nationkey)
    INNER JOIN multi_outer_join_third_reference t1 ON (test.c_custkey = t1.t_custkey)
ORDER BY 1 DESC;

-- Outer joins with subqueries on distribution column
SELECT
  l1.l_custkey,
  count(*) as cnt
FROM (
  SELECT l_custkey, l_nationkey
  FROM multi_outer_join_left
  WHERE l_comment like '%a%'
) l1
LEFT JOIN (
  SELECT r_custkey, r_name
  FROM multi_outer_join_right
  WHERE r_comment like '%b%'
) l2 ON l1.l_custkey = l2.r_custkey
GROUP BY l1.l_custkey
ORDER BY cnt DESC, l1.l_custkey DESC
LIMIT 20;

-- Add a shard to the left table that overlaps with multiple shards in the right
\set customer_1_data_file :abs_srcdir '/data/customer.1.data'
\set client_side_copy_command '\\copy multi_outer_join_left FROM ' :'customer_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command


-- All outer joins should error out
SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a LEFT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a RIGHT JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT
	min(l_custkey), max(l_custkey)
FROM
	multi_outer_join_left a FULL JOIN multi_outer_join_right b ON (l_custkey = r_custkey);

SELECT
	t_custkey
FROM
	(multi_outer_join_right r1
	LEFT OUTER JOIN multi_outer_join_left l1 ON (l1.l_custkey = r1.r_custkey)) AS
    test(c_custkey, c_nationkey)
    INNER JOIN multi_outer_join_third t1 ON (test.c_custkey = t1.t_custkey)
ORDER BY 1;

-- simple test to ensure anti-joins work with hash-partitioned tables
CREATE TABLE left_values(val int);

SET citus.shard_count to 16;
SET citus.shard_replication_factor to 1;

SELECT create_distributed_table('left_values', 'val');

COPY left_values from stdin;
1
2
3
4
5
\.

CREATE TABLE right_values(val int);

SELECT create_distributed_table('right_values', 'val');

COPY right_values from stdin;
2
3
4
\.

SELECT
    *
FROM
    left_values AS l
    LEFT JOIN right_values AS r ON l.val = r.val
WHERE
    r.val IS NULL
ORDER BY 1 DESC, 2 DESC;
