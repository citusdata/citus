----------------------------------------------------
-- recursive_relation_planning_restirction_pushdown
-- In this test file, we mosly test whether Citus
-- can successfully pushdown filters to the subquery
-- that is 
----------------------------------------------------

-- all the queries in this file have the 
-- same tables/subqueries combination as below
-- because this test aims to hold the query planning 
-- steady, but mostly ensure that filters are handled
-- properly. Note that u2 is the relation that is 
-- recursively planned

-- Setting the debug level so that filters can be observed
SET client_min_messages TO DEBUG1;

-- no filters on u2
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1);


-- scalar array expressions can be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE u2.value_1 > ANY(ARRAY[2, 1, 6]);


-- array operators on the table can be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE  ARRAY[u2.value_1, u2.value_2] @> (ARRAY[2, 3]);

-- array operators on different tables cannot be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE  ARRAY[u2.value_1, u1.user_id] @> (ARRAY[2, 3]);


-- coerced expressions can be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE (u2.value_1/2.0 > 2)::int::bool::text::bool;


-- case expression on a single table can be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE (CASE WHEN u2.value_1 > 3 THEN u2.value_1 > 2 ELSE false END);


-- case expression multiple tables cannot be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE (CASE WHEN u1.value_1 > 4000 THEN u2.value_1 / 100 > 1 ELSE false END);


-- coalesce expressions can be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE COALESCE((u2.user_id/5.0)::int::bool, false);


-- nullif expressions can be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE NULLIF((u2.value_2/5.0)::int::bool, false);


-- null test can be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE u2.value_3 IS NOT NULL;


-- functions can be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE isfinite(u2.time);

-- functions with multiple tables cannot be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE int4smaller(u2.value_1, u1.value_1) = 55;

-- functions with multiple columns from the same tables can be pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE int4smaller(u2.value_1, u2.value_2) = u2.value_1;

-- row expressions can be pushdown
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE row(u2.value_1, 2, 3) > row(u2.value_2, 2, 3);

-- multiple expression from the same table can be pushed down together
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
	WHERE
		  (u2.user_id/1.0)::int::bool::text::bool AND
		  CASE WHEN u2.value_1 > 4000 THEN u2.value_2 / 100 > 1 ELSE false END AND
		  COALESCE((u2.user_id/50000)::bool, false) AND
		  NULLIF((u2.value_3/50000)::int::bool, false) AND
		  isfinite(u2.time) AND
		  u2.value_4 IS DISTINCT FROM 50040 AND
		  row(u2.value_4, 2, 3) > row(2000, 2, 3);


-- subqueries are not pushdown
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE u2.value_1 >
    (SELECT avg(user_id)
     FROM events_table);

-- even subqueries with constant values are not pushdowned
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE u2.value_1 >
    (SELECT 5);

-- filters involving multiple tables aren't pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE  u2.value_1 *  u1.user_id > 25;


-- filter on other tables can only be pushdown 
-- as long as they are equality filters on the
-- joining column
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE  u1.value_1 = 3;

-- but not when the filter is gt, lt or any other thing
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE  u1.value_1 > 3;

-- when the filter is on another column than the 
-- join column, that's obviously not pushed down
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE  u1.value_2 = 3;


-- or filters on the same table is pushdown
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE u2.value_1 > 4 OR u2.value_4 = 4;

-- and filters on the same table is pushdown
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE u2.value_1 > 2 and u2.value_4 IS NULL;


-- filters on different tables are pushdown
-- only the ones that are not ANDed
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE (u2.value_1 > 2 OR u2.value_4 IS NULL) AND (u2.user_id > 4 OR u1.user_id > 3);

-- see the comment above
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE (u2.value_1 > 2 OR u2.value_4 IS NULL) OR (u2.user_id > 4 AND u1.user_id > 3);

-- see the comment above
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE (u2.value_1 > 2 OR u1.value_4 IS NULL) AND (u2.user_id > 4 AND u1.user_id > 3);

-- see the comment above
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE (u2.value_1 > 2 OR u1.value_4 IS NULL) AND (u2.user_id > 4 OR u1.user_id > 3);

-- see the comment above
-- but volatile functions are not pushed down 
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE (u2.value_1 > 2 OR u1.value_4 IS NULL) AND (u2.user_id = 10000 * random() OR u1.user_id > 3);

-- TODO: constant results should be pushed down, but not supported yet
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN
  (SELECT value_1,
          random()
   FROM users_table) AS u3 USING (value_1)
WHERE (u2.value_1 > 2 AND false);

-- TODO: what should the behaviour be?
SELECT count(*)
FROM users_table u1
JOIN users_table u2 USING(value_1)
JOIN LATERAL
  (SELECT value_1,
          random()
   FROM users_table
   WHERE u2.value_2 = 15) AS u3 USING (value_1)
WHERE (u2.value_1 > 2
       AND FALSE);




