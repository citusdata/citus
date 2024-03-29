--
-- COMPLEX_COUNT_DISTINCT
--
-- This test file has an alternative output because of the following in PG16:
-- https://github.com/postgres/postgres/commit/1349d2790bf48a4de072931c722f39337e72055e
-- https://github.com/postgres/postgres/commit/f4c7c410ee4a7baa06f51ebb8d5333c169691dd3
-- The alternative output can be deleted when we drop support for PG15
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 16 AS server_version_ge_16;

SET citus.next_shard_id TO 240000;
SET citus.shard_count TO 8;
SET citus.shard_replication_factor TO 1;
SET citus.coordinator_aggregation_strategy TO 'disabled';

CREATE TABLE lineitem_hash (
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

SELECT create_distributed_table('lineitem_hash', 'l_orderkey', 'hash');

\set lineitem_1_data_file :abs_srcdir '/data/lineitem.1.data'
\set lineitem_2_data_file :abs_srcdir '/data/lineitem.2.data'
\set client_side_copy_command '\\copy lineitem_hash FROM ' :'lineitem_1_data_file' ' with delimiter '''|''';'
:client_side_copy_command
\set client_side_copy_command '\\copy lineitem_hash FROM ' :'lineitem_2_data_file' ' with delimiter '''|''';'
:client_side_copy_command

ANALYZE lineitem_hash;


-- count(distinct) is supported on top level query if there
-- is a grouping on the partition key
SELECT
	l_orderkey, count(DISTINCT l_partkey)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;
EXPLAIN (COSTS false, VERBOSE true)
SELECT
	l_orderkey, count(DISTINCT l_partkey)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- it is also supported if there is no grouping or grouping is on non-partition field
SELECT
	count(DISTINCT l_partkey)
	FROM lineitem_hash
	ORDER BY 1 DESC
	LIMIT 10;

EXPLAIN (COSTS false, VERBOSE true)
SELECT
	count(DISTINCT l_partkey)
	FROM lineitem_hash
	ORDER BY 1 DESC
	LIMIT 10;

SELECT
	l_shipmode, count(DISTINCT l_partkey)
	FROM lineitem_hash
	GROUP BY l_shipmode
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

EXPLAIN (COSTS false, VERBOSE true)
SELECT
	l_shipmode, count(DISTINCT l_partkey)
	FROM lineitem_hash
	GROUP BY l_shipmode
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- mixed mode count distinct, grouped by partition column
SELECT
	l_orderkey, count(distinct l_partkey), count(distinct l_shipmode)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY 3 DESC, 2 DESC, 1
	LIMIT 10;

EXPLAIN (COSTS false, VERBOSE true)
SELECT
	l_orderkey, count(distinct l_partkey), count(distinct l_shipmode)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY 3 DESC, 2 DESC, 1
	LIMIT 10;

-- partition/non-partition column count distinct no grouping
SELECT
	count(distinct l_orderkey), count(distinct l_partkey), count(distinct l_shipmode)
	FROM lineitem_hash;

EXPLAIN (COSTS false, VERBOSE true)
SELECT
	count(distinct l_orderkey), count(distinct l_partkey), count(distinct l_shipmode)
	FROM lineitem_hash;

-- distinct/non-distinct on partition and non-partition columns
SELECT
	count(distinct l_orderkey), count(l_orderkey),
	count(distinct l_partkey), count(l_partkey),
	count(distinct l_shipmode), count(l_shipmode)
	FROM lineitem_hash;

-- mixed mode count distinct, grouped by non-partition column
SELECT
	l_shipmode, count(distinct l_partkey), count(distinct l_orderkey)
	FROM lineitem_hash
	GROUP BY l_shipmode
	ORDER BY 1, 2 DESC, 3 DESC;

-- mixed mode count distinct, grouped by non-partition column
-- having on partition column
SELECT
	l_shipmode, count(distinct l_partkey), count(distinct l_orderkey)
	FROM lineitem_hash
	GROUP BY l_shipmode
	HAVING count(distinct l_orderkey) > 1300
	ORDER BY 1, 2 DESC;

-- same but having clause is not on target list
SELECT
	l_shipmode, count(distinct l_partkey)
	FROM lineitem_hash
	GROUP BY l_shipmode
	HAVING count(distinct l_orderkey) > 1300
	ORDER BY 1, 2 DESC;

-- mixed mode count distinct, grouped by non-partition column
-- having on non-partition column
SELECT
	l_shipmode, count(distinct l_partkey), count(distinct l_suppkey)
	FROM lineitem_hash
	GROUP BY l_shipmode
	HAVING count(distinct l_suppkey) > 1550
	ORDER BY 1, 2 DESC;

-- same but having clause is not on target list
SELECT
	l_shipmode, count(distinct l_partkey)
	FROM lineitem_hash
	GROUP BY l_shipmode
	HAVING count(distinct l_suppkey) > 1550
	ORDER BY 1, 2 DESC;

-- count distinct is supported on single table subqueries
SELECT *
	FROM (
		SELECT
			l_orderkey, count(DISTINCT l_partkey)
			FROM lineitem_hash
			GROUP BY l_orderkey) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

SELECT *
	FROM (
		SELECT
			l_partkey, count(DISTINCT l_orderkey)
			FROM lineitem_hash
			GROUP BY l_partkey) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

EXPLAIN (COSTS false, VERBOSE true)
SELECT *
	FROM (
		SELECT
			l_partkey, count(DISTINCT l_orderkey)
			FROM lineitem_hash
			GROUP BY l_partkey) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- count distinct with filters
SELECT
	l_orderkey,
	count(DISTINCT l_suppkey) FILTER (WHERE l_shipmode = 'AIR'),
	count(DISTINCT l_suppkey)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY 2 DESC, 3 DESC, 1
	LIMIT 10;

EXPLAIN (COSTS false, VERBOSE true)
SELECT
	l_orderkey,
	count(DISTINCT l_suppkey) FILTER (WHERE l_shipmode = 'AIR'),
	count(DISTINCT l_suppkey)
	FROM lineitem_hash
	GROUP BY l_orderkey
	ORDER BY 2 DESC, 3 DESC, 1
	LIMIT 10;

-- group by on non-partition column
SELECT
	l_suppkey, count(DISTINCT l_partkey) FILTER (WHERE l_shipmode = 'AIR')
	FROM lineitem_hash
	GROUP BY l_suppkey
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;
-- explaining the same query fails
EXPLAIN (COSTS false, VERBOSE true)
SELECT
	l_suppkey, count(DISTINCT l_partkey) FILTER (WHERE l_shipmode = 'AIR')
	FROM lineitem_hash
	GROUP BY l_suppkey
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- without group by, on partition column
SELECT
	count(DISTINCT l_orderkey) FILTER (WHERE l_shipmode = 'AIR')
	FROM lineitem_hash;

-- without group by, on non-partition column
SELECT
	count(DISTINCT l_partkey) FILTER (WHERE l_shipmode = 'AIR')
	FROM lineitem_hash;

SELECT
	count(DISTINCT l_partkey) FILTER (WHERE l_shipmode = 'AIR'),
	count(DISTINCT l_partkey),
	count(DISTINCT l_shipdate)
	FROM lineitem_hash;

-- filter column already exists in target list
SELECT *
	FROM (
		SELECT
			l_orderkey, count(DISTINCT l_partkey) FILTER (WHERE l_orderkey > 100)
			FROM lineitem_hash
			GROUP BY l_orderkey) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- filter column does not exist in target list
SELECT *
	FROM (
		SELECT
			l_orderkey, count(DISTINCT l_partkey) FILTER (WHERE l_shipmode = 'AIR')
			FROM lineitem_hash
			GROUP BY l_orderkey) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- case expr in count distinct is supported.
-- count orders partkeys if l_shipmode is air
SELECT *
	FROM (
		SELECT
			l_orderkey, count(DISTINCT CASE WHEN l_shipmode = 'AIR' THEN l_partkey ELSE NULL END) as count
			FROM lineitem_hash
			GROUP BY l_orderkey) sub
	WHERE count > 0
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- text like operator is also supported
SELECT *
	FROM (
		SELECT
			l_orderkey, count(DISTINCT CASE WHEN l_shipmode like '%A%' THEN l_partkey ELSE NULL END) as count
			FROM lineitem_hash
			GROUP BY l_orderkey) sub
	WHERE count > 0
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- count distinct is rejected if it does not reference any columns
SELECT *
	FROM (
		SELECT
			l_linenumber, count(DISTINCT 1)
			FROM lineitem_hash
			GROUP BY l_linenumber) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- count distinct is rejected if it does not reference any columns
SELECT *
	FROM (
		SELECT
			l_linenumber, count(DISTINCT (random() * 5)::int)
			FROM lineitem_hash
			GROUP BY l_linenumber) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- even non-const function calls are supported within count distinct
SELECT *
	FROM (
		SELECT
			l_orderkey, count(DISTINCT (random() * 5)::int = l_linenumber)
			FROM lineitem_hash
			GROUP BY l_orderkey) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 0;

-- multiple nested subquery
SELECT
    total,
    avg(avg_count) as total_avg_count
	FROM (
		SELECT
	        number_sum,
	        count(DISTINCT l_suppkey) as total,
	        avg(total_count) avg_count
    		FROM (
    			SELECT
    				l_suppkey,
					sum(l_linenumber) as number_sum,
					count(DISTINCT l_shipmode) as total_count
					FROM
						lineitem_hash
					WHERE
						l_partkey > 100 and
						l_quantity > 2 and
						l_orderkey < 10000
					GROUP BY
						l_suppkey) as distributed_table
			WHERE
				number_sum >= 10
			GROUP BY
				number_sum) as distributed_table_2
	GROUP BY
		total
	ORDER BY
		total_avg_count DESC;

-- multiple cases query
SELECT *
	FROM (
		SELECT
			count(DISTINCT
				CASE
					WHEN l_shipmode = 'TRUCK' THEN l_partkey
					WHEN l_shipmode = 'AIR' THEN l_quantity
					WHEN l_shipmode = 'SHIP' THEN l_discount
					ELSE l_suppkey
				END) as count,
			l_shipdate
		FROM
			lineitem_hash
		GROUP BY
			l_shipdate) sub
	WHERE
		count > 0
	ORDER BY
		1 DESC, 2 DESC
	LIMIT 10;

-- count DISTINCT expression
SELECT *
	FROM (
		SELECT
			l_quantity, count(DISTINCT ((l_orderkey / 1000)  * 1000 ))  as count
			FROM
				lineitem_hash
			GROUP BY
				l_quantity) sub
	WHERE
		count > 0
	ORDER BY
		2 DESC, 1 DESC
	LIMIT 10;

-- count DISTINCT is part of an expression which includes another aggregate
SELECT *
	FROM (
		SELECT
			sum(((l_partkey * l_tax) / 100)) /
				count(DISTINCT
					CASE
						WHEN l_shipmode = 'TRUCK' THEN l_partkey
						ELSE l_suppkey
					END) as avg,
			l_shipmode
			FROM
				lineitem_hash
			GROUP BY
				l_shipmode) sub
	ORDER BY
		1 DESC, 2 DESC
	LIMIT 10;

-- count DISTINCT CASE WHEN expression
SELECT *
	FROM (
		SELECT
			count(DISTINCT
				CASE
					WHEN l_shipmode = 'TRUCK' THEN l_linenumber
					WHEN l_shipmode = 'AIR' THEN l_linenumber + 10
					ELSE 2
				END) as avg
			FROM
				lineitem_hash
			GROUP BY  l_shipdate) sub
	ORDER BY 1 DESC
	LIMIT 10;

-- COUNT DISTINCT (c1, c2)
SELECT *
	FROM
		(SELECT
			l_shipmode,
			count(DISTINCT (l_shipdate, l_tax))
			FROM
				lineitem_hash
			GROUP BY
				l_shipmode) t
	ORDER BY
		2 DESC,1 DESC
	LIMIT 10;

-- distinct on non-var (type cast/field select) columns are also
-- supported if grouped on distribution column
-- random is added to prevent flattening by postgresql
SELECT
	l_orderkey, count(a::int), count(distinct a::int)
	FROM (
		SELECT l_orderkey, l_orderkey * 1.5 a, random() b
			FROM lineitem_hash) sub
	GROUP BY 1
	ORDER BY 1 DESC
	LIMIT 5;

SELECT user_id,
       count(sub.a::int),
       count(DISTINCT sub.a::int),
       count(DISTINCT (sub).a)
FROM
  (SELECT user_id,
          unnest(ARRAY[user_id * 1.5])a,
          random() b
   FROM users_table
   ) sub
GROUP BY 1
ORDER BY 1 DESC
LIMIT 5;

CREATE TYPE test_item AS
(
  id       INTEGER,
  duration INTEGER
);

CREATE TABLE test_count_distinct_array (key int, value int ,  value_arr test_item[]);
SELECT create_distributed_table('test_count_distinct_array', 'key');

INSERT INTO test_count_distinct_array SELECT i, i, ARRAY[(i,i)::test_item] FROM generate_Series(0, 1000) i;

SELECT
	key,
	count(DISTINCT value),
	count(DISTINCT (item)."id"),
	count(DISTINCT (item)."id" * 3)
FROM
	(
		SELECT key, unnest(value_arr) as item, value FROM test_count_distinct_array
	) as sub
GROUP BY 1
ORDER BY 1 DESC
LIMIT 5;

DROP TABLE test_count_distinct_array;

DROP TYPE test_item;

-- other distinct aggregate are not supported
SELECT *
	FROM (
		SELECT
			l_linenumber, sum(DISTINCT l_partkey)
			FROM lineitem_hash
			GROUP BY l_linenumber) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

SELECT *
	FROM (
		SELECT
			l_linenumber, avg(DISTINCT l_partkey)
			FROM lineitem_hash
			GROUP BY l_linenumber) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- whole row references, oid, and ctid are not supported in count distinct
-- test table does not have oid or ctid enabled, so tests for them are skipped
SELECT *
	FROM (
		SELECT
			l_linenumber, count(DISTINCT lineitem_hash)
			FROM lineitem_hash
			GROUP BY l_linenumber) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

SELECT *
	FROM (
		SELECT
			l_linenumber, count(DISTINCT lineitem_hash.*)
			FROM lineitem_hash
			GROUP BY l_linenumber) sub
	ORDER BY 2 DESC, 1 DESC
	LIMIT 10;

-- count distinct pushdown is enabled
SELECT *
   FROM (
       SELECT
           l_shipdate,
           count(DISTINCT
               CASE
                   WHEN l_shipmode = 'TRUCK' THEN l_partkey
                   ELSE NULL
               END) as distinct_part,
           extract(year from l_shipdate) as year
           FROM
               lineitem_hash
           GROUP BY  l_shipdate, year) sub
   WHERE year = 1995
   ORDER BY 2 DESC, 1
   LIMIT 10;


-- count distinct pushdown is enabled
SELECT *
   FROM (
       SELECT
           l_shipdate,
           count(DISTINCT
               CASE
                   WHEN l_shipmode = 'TRUCK' THEN l_partkey
                   ELSE NULL
               END) as distinct_part,
           extract(year from l_shipdate) as year
           FROM
               lineitem_hash
           GROUP BY  l_shipdate, year) sub
   WHERE year = 1995
   ORDER BY 2 DESC, 1
   LIMIT 10;

SELECT *
   FROM (
       SELECT
           l_shipdate,
           count(DISTINCT
               CASE
                   WHEN l_shipmode = 'TRUCK' THEN l_partkey
                   ELSE NULL
               END) as distinct_part,
           extract(year from l_shipdate) as year
           FROM
               lineitem_hash
           GROUP BY  l_shipdate) sub
   WHERE year = 1995
   ORDER BY 2 DESC, 1
   LIMIT 10;

DROP TABLE lineitem_hash;

