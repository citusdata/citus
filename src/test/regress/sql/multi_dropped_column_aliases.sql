
-- Tests that check that our query functionality behaves as expected when the
-- table schema is modified via ALTER statements.


SET citus.next_shard_id TO 620000;


SELECT count(*) FROM customer;
SELECT * FROM customer LIMIT 2;

ALTER TABLE customer ADD COLUMN new_column1 INTEGER;
ALTER TABLE customer ADD COLUMN new_column2 INTEGER;
SELECT count(*) FROM customer;
SELECT * FROM customer LIMIT 2;

ALTER TABLE customer DROP COLUMN new_column1;
ALTER TABLE customer DROP COLUMN new_column2;
SELECT count(*) FROM customer;
SELECT * FROM customer LIMIT 2;

-- Verify joins work with dropped columns.
SELECT count(*) FROM customer, orders WHERE c_custkey = o_custkey;

-- Test joinExpr aliases by performing an outer-join.

SELECT c_custkey
FROM   (customer LEFT OUTER JOIN orders ON (c_custkey = o_custkey)) AS
       test(c_custkey, c_nationkey)
       INNER JOIN lineitem ON (test.c_custkey = l_orderkey)
ORDER BY 1
LIMIT 10;
