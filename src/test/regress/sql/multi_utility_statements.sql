--
-- MULTI_UTILITY_STATEMENTS
--

-- Check that we can run utility statements with embedded SELECT statements on
-- distributed tables. Currently we only support CREATE TABLE AS (SELECT..),
-- DECLARE CURSOR, and COPY ... TO statements.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1000000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1000000;


CREATE TEMP TABLE lineitem_pricing_summary AS 
(
     SELECT
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
    FROM
	lineitem
    WHERE
	l_shipdate <= date '1998-12-01' - interval '90 days'
    GROUP BY
	l_returnflag,
	l_linestatus
    ORDER BY
	l_returnflag,
	l_linestatus
);

SELECT * FROM lineitem_pricing_summary ORDER BY l_returnflag, l_linestatus;

-- Test we can handle joins

SET citus.large_table_shard_count TO 2;

CREATE TABLE shipping_priority AS 
(
    SELECT
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
    FROM
	customer,
	orders,
	lineitem
    WHERE
	c_mktsegment = 'BUILDING'
	AND c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND o_orderdate < date '1995-03-15'
	AND l_shipdate > date '1995-03-15'
    GROUP BY
	l_orderkey,
	o_orderdate,
	o_shippriority
    ORDER BY
	revenue DESC,
	o_orderdate
);

SELECT * FROM shipping_priority;
DROP TABLE shipping_priority;

-- Check COPY against distributed tables works both when specifying a
-- query as the source, and when directly naming a table.

COPY (
    SELECT
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
    FROM
	customer,
	orders,
	lineitem
    WHERE
	c_mktsegment = 'BUILDING'
	AND c_custkey = o_custkey
	AND l_orderkey = o_orderkey
	AND o_orderdate < date '1995-03-15'
	AND l_shipdate > date '1995-03-15'
    GROUP BY
	l_orderkey,
	o_orderdate,
	o_shippriority
    ORDER BY
	revenue DESC,
	o_orderdate
) TO stdout;

-- check copying to file
-- (quiet off to force number of copied records to be displayed)
\set QUIET off
COPY nation TO '/dev/null';
\set QUIET on
-- stdout
COPY nation TO STDOUT;
-- ensure individual cols can be copied out, too
COPY nation(n_name) TO STDOUT;

-- Test that we can create on-commit drop tables, and also test creating with
-- oids, along with changing column names

BEGIN;

CREATE TEMP TABLE customer_few (customer_key) WITH (OIDS) ON COMMIT DROP AS 
    (SELECT * FROM customer WHERE c_nationkey = 1 ORDER BY c_custkey LIMIT 10);

SELECT customer_key, c_name, c_address 
       FROM customer_few ORDER BY customer_key LIMIT 5;

COMMIT;

SELECT customer_key, c_name, c_address 
       FROM customer_few ORDER BY customer_key LIMIT 5;

-- Test DECLARE CURSOR statements

DECLARE holdCursor SCROLL CURSOR WITH HOLD FOR
        SELECT l_orderkey, l_linenumber, l_quantity, l_discount
        FROM lineitem 
        ORDER BY l_orderkey, l_linenumber;

FETCH NEXT FROM holdCursor;
FETCH FORWARD 5 FROM holdCursor;
FETCH LAST FROM holdCursor;
FETCH BACKWARD 5 FROM holdCursor;

-- Test WITHOUT HOLD cursors inside transactions

BEGIN;
DECLARE noHoldCursor SCROLL CURSOR FOR
        SELECT l_orderkey, l_linenumber, l_quantity, l_discount
        FROM lineitem
        ORDER BY l_orderkey, l_linenumber;

FETCH ABSOLUTE 5 FROM noHoldCursor;
COMMIT;
FETCH ABSOLUTE 5 FROM noHoldCursor;
