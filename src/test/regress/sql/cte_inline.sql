--
-- CTE_INLINE
--

CREATE SCHEMA cte_inline;
SET search_path TO cte_inline;
SET citus.next_shard_id TO 1960000;
CREATE TABLE test_table (key int, value text, other_value jsonb);
SELECT create_distributed_table ('test_table', 'key');

INSERT INTO test_table SELECT i % 10, 'test' || i, row_to_json(row(i, i*18, 'test' || i)) FROM generate_series (0, 100) i;

SET client_min_messages TO DEBUG;

-- Citus should not inline this CTE because otherwise it cannot
-- plan the query
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	*
FROM
	test_table LEFT JOIN cte_1 USING (value)
ORDER BY 1 DESC LIMIT 3;

-- Should still not be inlined even if NOT MATERIALIZED is passed
WITH cte_1 AS NOT MATERIALIZED (SELECT * FROM test_table)
SELECT
	*
FROM
	test_table LEFT JOIN cte_1 USING (value)
ORDER BY 2 DESC LIMIT 1;

-- the cte can be inlined because the unsupported
-- part of the query (subquery in WHERE clause)
-- doesn't access the cte
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	cte_1
WHERE
	key IN (
			SELECT
				(SELECT 1)
			FROM
				test_table WHERE key = 1
			);

-- a similar query as the above, and this time the planning
-- fails, but it fails because the subquery in WHERE clause
-- cannot be planned by Citus
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	cte_1
WHERE
	key IN (
			SELECT
				key
			FROM
				test_table
  			 	FOR UPDATE
			);

-- Citus does the inlining, the planning fails
-- and retries without inlining, which works
-- fine later via recursive planning
WITH cte_1 AS
  (SELECT *
   FROM test_table)
SELECT *, (SELECT 1)
FROM
  (SELECT *
   FROM cte_1) AS foo
ORDER BY 2 DESC LIMIT 1;

-- a little more complicated query tree
-- Citus does the inlining, the planning fails
-- and retries without inlining, which works
WITH top_cte AS
  (SELECT *
   FROM test_table)
SELECT count(*)
FROM top_cte,
  (WITH cte_1 AS
     (SELECT *
      FROM test_table) SELECT *, (SELECT 1)
   FROM
     (SELECT *
      FROM cte_1) AS foo) AS bar;

-- CTE is used inside a subquery in WHERE clause
-- the query wouldn't work by inlining, so Citus
-- retries again via recursive planning, which
-- works fine
WITH cte_1 AS
  (SELECT *
   FROM test_table)
SELECT count(*)
FROM test_table
WHERE KEY IN
    (SELECT (SELECT 1)
     FROM
       (SELECT *,
               random()
        FROM
          (SELECT *
           FROM cte_1) AS foo) AS bar);

-- cte_1 is used inside another CTE, but still
-- doesn't work when inlined because it is finally
-- used in an unsupported query
-- but still works fine because recursive planning
-- kicks in
WITH cte_1 AS
  (SELECT *
   FROM test_table)
SELECT (SELECT 1) AS KEY  FROM (
  WITH cte_2 AS (SELECT *, random()
     FROM (SELECT *,random() FROM cte_1) as foo)
SELECT *, random() FROM cte_2) as bar ORDER BY 1 DESC LIMIT 3;

-- in this example, cte_2 can be inlined, because it is not used
-- on any query that Citus cannot plan. However, cte_1 should not be
-- inlined, because it is used with a subquery in target list
WITH cte_1 AS (SELECT * FROM test_table),
     cte_2 AS (select * from test_table)
SELECT
	count(*)
FROM
	(SELECT *, (SELECT 1) FROM cte_1) as foo
		JOIN
	cte_2
		ON (true);

-- unreferenced CTEs are just ignored
-- by Citus/Postgres
WITH a AS (SELECT * FROM test_table)
SELECT
	*, row_number() OVER ()
FROM
	test_table
WHERE
	key = 1
ORDER BY 3 DESC
LIMIT 5;

-- router queries are affected by the distributed
-- cte inlining
WITH a AS (SELECT * FROM test_table WHERE key = 1)
SELECT
	*, (SELECT 1)
FROM
	a
WHERE
	key = 1
ORDER BY 1 DESC
LIMIT 5;

-- non router queries are affected by the distributed
-- cte inlining as well
WITH a AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	a
WHERE
	key = 1;

-- explicitely using NOT MATERIALIZED should result in the same
WITH a AS NOT MATERIALIZED (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	a
WHERE
	key = 1;

-- using MATERIALIZED should cause inlining not to happen
WITH a AS MATERIALIZED (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	a
WHERE
	key = 1;

-- EXPLAIN should show the difference between materialized an not materialized
EXPLAIN (COSTS OFF) WITH a AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	a
WHERE
	key = 1;

EXPLAIN (COSTS OFF) WITH a AS MATERIALIZED (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	a
WHERE
	key = 1;


-- citus should not inline the CTE because it is used multiple times
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	cte_1 as first_entry
		JOIN
	cte_1 as second_entry
		USING (key);

-- NOT MATERIALIZED should cause the query to be inlined twice
WITH cte_1 AS NOT MATERIALIZED (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	cte_1 as first_entry
		JOIN
	cte_1 as second_entry
		USING (key);

-- EXPLAIN should show the differences between MATERIALIZED and NOT MATERIALIZED

\set VERBOSITY terse

SELECT public.coordinator_plan_with_subplans($Q$
EXPLAIN (COSTS OFF) WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	cte_1 as first_entry
		JOIN
	cte_1 as second_entry
		USING (key);
$Q$);

\set VERBOSITY default

EXPLAIN (COSTS OFF) WITH cte_1 AS NOT MATERIALIZED (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	cte_1 as first_entry
		JOIN
	cte_1 as second_entry
		USING (key);

-- ctes with volatile functions are not
-- inlined
WITH cte_1 AS (SELECT *, random() FROM test_table)
SELECT
	key, value
FROM
	cte_1
ORDER BY 2 DESC LIMIT 1;

-- even with NOT MATERIALIZED volatile functions should not be inlined
WITH cte_1 AS NOT MATERIALIZED (SELECT *, random() FROM test_table)
SELECT
	count(*)
FROM
	cte_1;


-- cte_1 should be able to inlined even if
-- it is used one level below
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
(
	WITH ct2 AS (SELECT * FROM cte_1)
	SELECT * FROM ct2
) as foo;

-- a similar query, but there is also
-- one more cte, which relies on the previous
-- CTE
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	count(DISTINCT key)
FROM
(
	WITH cte_2 AS (SELECT * FROM cte_1),
		 cte_3 AS (SELECT * FROM cte_2)
	SELECT * FROM cte_3
) as foo;


-- inlined CTE contains a reference to outer query
-- should be fine (because we pushdown the whole query)
SELECT count(*)
	FROM
	  (SELECT *
	   FROM test_table) AS test_table_cte
	JOIN LATERAL
	  (WITH bar AS  (SELECT *
	      FROM test_table
	      WHERE key = test_table_cte.key)
	  	SELECT *
	   FROM
	      bar
	   LEFT JOIN test_table u2 ON u2.key = bar.key) AS foo ON TRUE;

-- inlined CTE contains a reference to outer query
-- should be fine (even if the recursive planning fails
-- to recursively plan the query)
SELECT count(*)
	FROM
	  (SELECT *
	   FROM test_table) AS test_table_cte
	JOIN LATERAL
	  (WITH bar AS  (SELECT *
	      FROM test_table
	      WHERE key = test_table_cte.key)
	  	SELECT *
	   FROM
	      bar
	   LEFT JOIN test_table u2 ON u2.key = bar.value::int) AS foo ON TRUE;


-- inlined CTE can recursively planned later, that's the decision
-- recursive planning makes
-- LIMIT 5 in cte2 triggers recusrive planning, after cte inlining
WITH cte_1 AS (SELECT * FROM test_table)
SELECT
	*
FROM
(
	WITH ct2 AS (SELECT * FROM cte_1  ORDER BY 1, 2, 3 LIMIT 5)
	SELECT * FROM ct2
) as foo ORDER BY 1 DESC, 2 DESC, 3 DESC LIMIT 5;

-- all nested CTEs can be inlinied
WITH cte_1  AS (
  WITH cte_1 AS (
    WITH cte_1 AS (
      WITH cte_1 AS (
        WITH cte_1 AS (
          WITH cte_1 AS (
            WITH cte_1 AS (SELECT count(*), key FROM  test_table GROUP BY key)
            			   SELECT * FROM cte_1)
          SELECT * FROM cte_1 WHERE key >= 1)
        SELECT * FROM cte_1 WHERE key >= 2)
      SELECT * FROM cte_1 WHERE key >= 3)
    SELECT * FROM cte_1 WHERE key >= 4)
  SELECT * FROM cte_1 WHERE key >= 5)
SELECT * FROM cte_1 WHERE key = 1;



-- ctes can be inlined even if they are used
-- in set operations
WITH cte_1 AS (SELECT * FROM test_table),
	 cte_2 AS (SELECT * FROM test_table)
SELECT count(*) FROM (
(SELECT * FROM cte_1 EXCEPT SELECT * FROM test_table)
UNION
(SELECT * FROM cte_2)) as foo;

-- cte_1 is going to be inlined even inside another set operation
WITH cte_1 AS (SELECT * FROM test_table),
	 cte_2 AS (SELECT * FROM test_table ORDER BY 1 DESC LIMIT 3)
(SELECT *, (SELECT 1) FROM cte_1 EXCEPT SELECT *, 1 FROM test_table)
UNION
(SELECT *, 1 FROM cte_2)
ORDER BY 1,2;


-- cte_1 is safe to inline, even if because after inlining
-- it'd be in a query tree where there is a query that is
-- not supported by Citus unless recursively planned
-- cte_2 is on another queryTree, should be fine
WITH cte_1 AS (SELECT * FROM test_table),
	 cte_2 AS (SELECT * FROM test_table)
(SELECT *, (SELECT key FROM cte_1) FROM test_table)
UNION
(SELECT *, 1 FROM cte_2);

-- after inlining CTEs, the query becomes
-- subquery pushdown with set operations
WITH cte_1 AS (SELECT * FROM test_table),
	 cte_2 AS (SELECT * FROM test_table)
SELECT max(key) FROM
(
	SELECT * FROM cte_1
		UNION
	SELECT * FROM cte_2
) as bar;

-- cte LEFT JOIN subquery should only work
-- when CTE is inlined, as Citus currently
-- doesn't know how to handle intermediate
-- results in the outer parts of outer
-- queries
WITH cte AS (SELECT * FROM test_table)
SELECT
	count(*)
FROM
	cte LEFT JOIN test_table USING (key);

-- the CTEs are very simple, so postgres
-- can pull-up the subqueries after inlining
-- the CTEs, and the query that we send to workers
-- becomes a join between two tables
WITH cte_1 AS (SELECT key FROM test_table),
	 cte_2 AS (SELECT key FROM test_table)
SELECT
	count(*)
FROM
	cte_1 JOIN cte_2 USING (key);


-- the following query is kind of interesting
-- During INSERT .. SELECT via coordinator,
-- Citus moves the CTEs into SELECT part, and plans/execute
-- the SELECT separately. Thus, fist_table_cte can be inlined
-- by Citus -- but not by Postgres
WITH fist_table_cte AS
  (SELECT * FROM test_table)
INSERT INTO test_table
            (key, value)
            SELECT
              key, value
            FROM
              fist_table_cte;

-- the following INSERT..SELECT is even more interesting
-- the CTE becomes pushdownable
INSERT INTO test_table
WITH fist_table_cte AS
  (SELECT * FROM test_table)
    SELECT
      key, value
    FROM
      fist_table_cte;

-- update/delete/modifying ctes
-- we don't support any cte inlining in modifications
-- queries and modifying CTEs
WITH cte_1 AS (SELECT * FROM test_table)
	DELETE FROM test_table WHERE key NOT IN (SELECT key FROM cte_1);

-- NOT MATERIALIZED should not CTEs that are used in a modifying query, because
-- we de still don't support it
WITH cte_1 AS NOT MATERIALIZED (SELECT * FROM test_table)
	DELETE FROM test_table WHERE key NOT IN (SELECT key FROM cte_1);

-- we don't inline CTEs if they are modifying CTEs
WITH cte_1 AS (DELETE FROM test_table WHERE key % 3  = 1 RETURNING key)
SELECT * FROM cte_1 ORDER BY 1 DESC LIMIT 3;

-- NOT MATERIALIZED should not affect modifying CTEs
WITH cte_1 AS NOT MATERIALIZED (DELETE FROM test_table WHERE key % 3 = 0 RETURNING key)
SELECT count(*) FROM cte_1;

-- cte with column aliases
SELECT * FROM test_table,
(WITH cte_1 (x,y) AS (SELECT * FROM test_table),
     cte_2 (z,y) AS (SELECT value, other_value, key FROM test_table),
	 cte_3 (t,m) AS (SELECT z, y, key as cte_2_key FROM cte_2)
		SELECT * FROM cte_2, cte_3) as bar
ORDER BY value, other_value, z, y, t, m, cte_2_key
LIMIT 5;

-- cte used in HAVING subquery just works fine
-- even if it is inlined
WITH cte_1 AS (SELECT max(key) as max FROM test_table)
SELECT
	key, count(*)
FROM
	test_table
GROUP BY
	key
HAVING
	(count(*) > (SELECT max FROM cte_1))
ORDER BY 2 DESC, 1 DESC
LIMIT 5;

-- cte used in ORDER BY just works fine
-- even if it is inlined
WITH cte_1 AS (SELECT max(key) as max FROM test_table)
SELECT
	key
FROM
	test_table JOIN cte_1 ON (key = max)
ORDER BY
	cte_1.max
LIMIT 3;

PREPARE inlined_cte_without_params AS
	WITH cte_1 AS (SELECT count(*) FROM test_table GROUP BY key)
	SELECT * FROM cte_1 ORDER BY 1 DESC LIMIT 3;
PREPARE non_inlined_cte_without_params AS
    WITH cte_1 AS (SELECT * FROM test_table)
    SELECT
        *
    FROM
        test_table LEFT JOIN cte_1 USING (value) ORDER BY 1 DESC, 2 DESC, 3 DESC LIMIT 3;
PREPARE inlined_cte_has_parameter_on_non_dist_key(text) AS
	WITH cte_1 AS (SELECT count(*) FROM test_table WHERE value = $1 GROUP BY key)
	SELECT * FROM cte_1 ORDER BY 1 DESC LIMIT 3;
PREPARE inlined_cte_has_parameter_on_dist_key(int) AS
	WITH cte_1 AS (SELECT count(*) FROM test_table WHERE key > $1 GROUP BY key)
	SELECT * FROM cte_1 ORDER BY 1 DESC LIMIT 3;
PREPARE non_inlined_cte_has_parameter_on_dist_key(int) AS
    WITH cte_1 AS (SELECT * FROM test_table where key > $1)
    SELECT
        *
    FROM
        test_table LEFT JOIN cte_1 USING (value) ORDER BY 1 DESC, 2 DESC, 3 DESC LIMIT 3;
PREPARE retry_planning(int) AS
	 WITH cte_1 AS (SELECT * FROM test_table WHERE key > $1)
	 SELECT json_object_agg(DISTINCT key, value)  FROM cte_1 ORDER BY max(key), min(value) DESC LIMIT 3;


EXECUTE inlined_cte_without_params;
EXECUTE inlined_cte_without_params;
EXECUTE inlined_cte_without_params;
EXECUTE inlined_cte_without_params;
EXECUTE inlined_cte_without_params;
EXECUTE inlined_cte_without_params;

EXECUTE non_inlined_cte_without_params;
EXECUTE non_inlined_cte_without_params;
EXECUTE non_inlined_cte_without_params;
EXECUTE non_inlined_cte_without_params;
EXECUTE non_inlined_cte_without_params;
EXECUTE non_inlined_cte_without_params;

EXECUTE inlined_cte_has_parameter_on_non_dist_key('test1');
EXECUTE inlined_cte_has_parameter_on_non_dist_key('test2');
EXECUTE inlined_cte_has_parameter_on_non_dist_key('test3');
EXECUTE inlined_cte_has_parameter_on_non_dist_key('test4');
EXECUTE inlined_cte_has_parameter_on_non_dist_key('test5');
EXECUTE inlined_cte_has_parameter_on_non_dist_key('test6');

EXECUTE inlined_cte_has_parameter_on_dist_key(1);
EXECUTE inlined_cte_has_parameter_on_dist_key(2);
EXECUTE inlined_cte_has_parameter_on_dist_key(3);
EXECUTE inlined_cte_has_parameter_on_dist_key(4);
EXECUTE inlined_cte_has_parameter_on_dist_key(5);
EXECUTE inlined_cte_has_parameter_on_dist_key(6);

EXECUTE non_inlined_cte_has_parameter_on_dist_key(1);
EXECUTE non_inlined_cte_has_parameter_on_dist_key(2);
EXECUTE non_inlined_cte_has_parameter_on_dist_key(3);
EXECUTE non_inlined_cte_has_parameter_on_dist_key(4);
EXECUTE non_inlined_cte_has_parameter_on_dist_key(5);
EXECUTE non_inlined_cte_has_parameter_on_dist_key(6);

EXECUTE retry_planning(1);
EXECUTE retry_planning(2);
EXECUTE retry_planning(3);
EXECUTE retry_planning(4);
EXECUTE retry_planning(5);
EXECUTE retry_planning(6);

WITH b AS (SELECT * FROM test_table)
SELECT count(*) FROM (SELECT key as x FROM test_table OFFSET 0) as ref LEFT JOIN b ON (ref.x = b.key);

-- this becomes a non-colocated subquery join
-- because after the CTEs are inlined the joins
-- become a non-colocated subquery join
WITH a AS (SELECT * FROM test_table),
b AS (SELECT * FROM test_table)
SELECT count(*) FROM a LEFT JOIN b ON (a.value = b.value);

WITH a AS (SELECT * FROM test_table OFFSET 0),
b AS (SELECT * FROM test_table)
SELECT min(a.key) FROM  a LEFT JOIN b ON (a.value = b.value);

-- after both CTEs are inlined, this becomes non-colocated subquery join
WITH cte_1 AS (SELECT * FROM test_table),
cte_2 AS (SELECT * FROM test_table)
SELECT * FROM cte_1 JOIN cte_2 ON (cte_1.value > cte_2.value) ORDER BY 1,2,3,4,5,6 DESC LIMIT 3;;

-- full join is only supported when both sides are
-- recursively planned
WITH cte_1 AS (SELECT value FROM test_table WHERE key > 1),
     cte_2 AS (SELECT value FROM test_table WHERE key > 3)
SELECT *  FROM cte_1 FULL JOIN cte_2 USING (value) ORDER BY 1 DESC LIMIT 3;;

-- an unsupported agg. for multi-shard queries
-- so CTE has to be recursively planned
WITH cte_1 AS (SELECT * FROM test_table WHERE key > 1)
SELECT json_object_agg(DISTINCT key, value)  FROM cte_1;

-- both cte_1 and cte_2 are going to be inlined.
-- later, cte_2 is recursively planned since it doesn't have
-- GROUP BY but aggragate in a subquery.
-- this is an important example of being able to recursively plan
-- "some" of the CTEs
WITH cte_1 AS (SELECT value FROM test_table WHERE key > 1),
     cte_2 AS (SELECT max(value) as value FROM test_table WHERE key > 3)
SELECT count(*)  FROM cte_1 JOIN cte_2 USING (value);


-- prevent DROP CASCADE to give notices
SET client_min_messages TO ERROR;
DROP SCHEMA cte_inline CASCADE;
