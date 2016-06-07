--
-- MULTI_AVERAGE_EXPRESSION_ORDER
--
-- This test checks that the group-by columns don't need to be above an average
-- expression, and can be anywhere in the projection order. This is in response
-- to a bug we had due to the average expression introducing new columns.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 450000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 450000;


SELECT
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order,
	l_returnflag,
	l_linestatus
FROM
	lineitem
WHERE
	l_shipdate <= date '1998-12-01' - interval '90 days'
GROUP BY
	l_returnflag,
	l_linestatus
ORDER BY
	l_returnflag,
	l_linestatus;

-- These tests check that distributed averages only consider non-null input
-- values. This is in response to a bug we had due to the distributed average
-- using sum(expression)/count(*) to calculate avg(expression). We now use the
-- correct form sum(expression)/count(expression) for average calculations.

-- Run avg() on an expression that contains some null values

SELECT
    avg(case
            when l_quantity > 20
            then l_quantity
        end)
FROM
    lineitem;

-- Run avg() on an expression that contains only null values

SELECT
    avg(case
            when l_quantity > 5000
            then l_quantity
        end)
FROM
    lineitem;
