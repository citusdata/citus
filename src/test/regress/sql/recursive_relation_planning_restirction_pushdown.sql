----------------------------------------------------
-- recursive_relation_planning_restirction_pushdown
-- In this test file, we mosly test whether Citus
-- can successfully pushdown filters to the subquery
-- that is being recursively planned. This is done
-- for all types of JOINs
----------------------------------------------------

-- all the queries in this file have the
-- same tables/subqueries combination as below
-- because this test aims to hold the query planning
-- steady, but mostly ensure that filters are handled
-- properly. Note that local is the relation that is
-- recursively planned throughout the file

CREATE SCHEMA push_down_filters;
SET search_path TO push_down_filters;

CREATE TABLE local_table (key int, value int, time timestamptz);

CREATE TABLE distributed_table (key int, value int, metadata jsonb);
SELECT create_distributed_table('distributed_table', 'key');

-- Setting the debug level so that filters can be observed
SET client_min_messages TO DEBUG1;

-- for the purposes of these tests, we always want to recursively
-- plan local tables.
SET citus.local_table_join_policy TO "pull-local";


-- there are no filters, hence cannot pushdown any filters
SELECT count(*)
FROM distributed_table u1
JOIN distributed_table u2 USING(key)
JOIN local_table USING (key);

-- scalar array expressions can be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING (key)
WHERE u2.key > ANY(ARRAY[2, 1, 6]);

-- array operators on the table can be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(key)
WHERE  ARRAY[u2.key, u2.value] @> (ARRAY[2, 3]);


-- array operators on different tables cannot be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE  ARRAY[u2.value, u1.value] @> (ARRAY[2, 3]);

-- coerced expressions can be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE (u2.value/2.0 > 2)::int::bool::text::bool;


-- case expression on a single table can be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE (CASE WHEN u2.value > 3 THEN u2.value > 2 ELSE false END);

-- case expression multiple tables cannot be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE (CASE WHEN u1.value > 4000 THEN u2.value / 100 > 1 ELSE false END);

-- coalesce expressions can be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE COALESCE((u2.key/5.0)::int::bool, false);

-- nullif expressions can be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE NULLIF((u2.value/5.0)::int::bool, false);

-- null test can be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE u2.value IS NOT NULL;

-- functions can be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE isfinite(u2.time);

-- functions with multiple tables cannot be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE int4smaller(u2.value, u1.value) = 55;

-- functions with multiple columns from the same tables can be pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE int4smaller(u2.key, u2.value) = u2.key;

-- row expressions can be pushdown
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE row(u2.value, 2, 3) > row(u2.value, 2, 3);



-- multiple expression from the same table can be pushed down together
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
	WHERE
		  (u2.key/1.0)::int::bool::text::bool AND
		  CASE WHEN u2.key > 4000 THEN u2.value / 100 > 1 ELSE false END AND
		  COALESCE((u2.key/50000)::bool, false) AND
		  NULLIF((u2.value/50000)::int::bool, false) AND
		  isfinite(u2.time) AND
		  u2.value IS DISTINCT FROM 50040 AND
		  row(u2.value, 2, 3) > row(2000, 2, 3);


-- subqueries filters are not pushdown
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE u2.value >
    (SELECT avg(key)
     FROM distributed_table);

-- even subqueries with constant values are not pushdowned
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE u2.value > (SELECT 5);

-- filters involving multiple tables aren't pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE  u2.value *  u1.key > 25;


-- filter on other tables can only be pushdown
-- as long as they are equality filters on the
-- joining column
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE  u1.value = 3;


-- but not when the filter is gt, lt or any other thing
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE  u1.value > 3;


-- when the filter is on another column than the
-- join column, that's obviously not pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE  u1.key = 3;


-- or filters on the same table is pushdown
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE u2.value > 4 OR u2.value = 4;

-- and filters on the same table is pushdown
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE u2.value > 2 and u2.time IS NULL;


-- filters on different tables are pushdown
-- only the ones that are not ANDed
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE (u2.value > 2 OR u2.value IS NULL) AND (u2.key > 4 OR u1.key > 3);

-- filters on different tables are pushdown
-- only the ones that are not ANDed
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE (u2.value > 2 OR u2.value IS NULL) OR (u2.key > 4 OR u1.key > 3);


-- filters on different tables are pushdown
-- only the ones that are not ANDed
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE (u2.value > 2 OR u2.value IS NULL) AND (u2.key > 4 OR u1.key > 3);

-- but volatile functions are not pushed down
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE (u2.value > 2 OR u1.value IS NULL) AND (u2.key = 10000 * random() OR u1.key > 3);

-- constant results should be pushed down, but not supported yet
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
WHERE (u2.value > 2 AND false);

-- we can still pushdown WHERE false
-- even if it is a LATERAL join
SELECT count(*)
FROM distributed_table u1
JOIN local_table u2 USING(value)
JOIN LATERAL
  (SELECT value,
          random()
   FROM distributed_table
   WHERE u2.value = 15) AS u3 USING (value)
WHERE (u2.value > 2
       AND FALSE);

\set VERBOSITY terse
RESET client_min_messages;
DROP SCHEMA push_down_filters CASCADE;

