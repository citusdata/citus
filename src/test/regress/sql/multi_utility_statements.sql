--
-- MULTI_UTILITY_STATEMENTS
--

-- Check that we can run utility statements with embedded SELECT statements on
-- distributed tables. Currently we only support CREATE TABLE AS (SELECT..),
-- DECLARE CURSOR, and COPY ... TO statements.


SET citus.next_shard_id TO 1000000;


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
COPY nation TO :'dev_null';
\set QUIET on
-- stdout
COPY nation TO STDOUT;
-- ensure individual cols can be copied out, too
COPY nation(n_name) TO STDOUT;

-- Test that we can create on-commit drop tables, along with changing column names

BEGIN;

CREATE TEMP TABLE customer_few (customer_key) ON COMMIT DROP AS 
    (SELECT * FROM customer WHERE c_nationkey = 1 ORDER BY c_custkey LIMIT 10);

SELECT customer_key, c_name, c_address 
       FROM customer_few ORDER BY customer_key LIMIT 5;

COMMIT;

SELECT customer_key, c_name, c_address 
       FROM customer_few ORDER BY customer_key LIMIT 5;

-- Test DECLARE CURSOR .. WITH HOLD without parameters that calls ReScan on the top-level CustomScan
CREATE TABLE cursor_me (x int, y int);
SELECT create_distributed_table('cursor_me', 'x');
INSERT INTO cursor_me SELECT s/10, s FROM generate_series(1, 100) s;

DECLARE holdCursor CURSOR WITH HOLD FOR
		SELECT * FROM cursor_me WHERE x = 1 ORDER BY y;

FETCH NEXT FROM holdCursor;
FETCH FORWARD 3 FROM holdCursor;
FETCH LAST FROM holdCursor;
FETCH BACKWARD 3 FROM holdCursor;
FETCH FORWARD 3 FROM holdCursor;

CLOSE holdCursor;

-- Test DECLARE CURSOR .. WITH HOLD with parameter
CREATE OR REPLACE FUNCTION declares_cursor(p int)
RETURNS void AS $$
	DECLARE c CURSOR WITH HOLD FOR SELECT * FROM cursor_me WHERE x = $1;
$$ LANGUAGE SQL;

SELECT declares_cursor(5);

CREATE OR REPLACE FUNCTION cursor_plpgsql(p int)
RETURNS SETOF int AS $$                                                                                                                                                            
DECLARE
  val int;
  my_cursor CURSOR (a INTEGER) FOR SELECT y FROM cursor_me WHERE x = $1 ORDER BY y;
BEGIN
   -- Open the cursor
   OPEN my_cursor(p);

   LOOP
      FETCH my_cursor INTO val;
      EXIT WHEN NOT FOUND;

      RETURN NEXT val;
   END LOOP;

   -- Close the cursor
   CLOSE my_cursor;
END; $$
LANGUAGE plpgsql;

SELECT cursor_plpgsql(4);

DROP FUNCTION declares_cursor(int);
DROP FUNCTION cursor_plpgsql(int);
DROP TABLE cursor_me;

-- Test DECLARE CURSOR statement with SCROLL
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
FETCH BACKWARD noHoldCursor;
COMMIT;
FETCH ABSOLUTE 5 FROM noHoldCursor;

-- Test we don't throw an error for DROP IF EXISTS
DROP DATABASE IF EXISTS not_existing_database;
DROP TABLE IF EXISTS not_existing_table;
DROP SCHEMA IF EXISTS not_existing_schema;
