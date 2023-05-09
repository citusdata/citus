CREATE SCHEMA values_subquery;
SET search_path TO values_subquery;

CREATE TABLE test_values (key int, value text, data jsonb);
SELECT create_distributed_table('test_values', 'key');
INSERT INTO test_values SELECT i, i::text,  ('{"value":"' ||  i::text ||  '"}')::jsonb FROM generate_series(0,100)i;

CREATE TABLE test_values_ref (key int);
SELECT create_reference_table('test_values_ref');
INSERT INTO test_values_ref SELECT i FROM generate_series(0,100)i;


-- the aim of this test is to show when Citus can pushdown
-- VALUES and when it cannot. With DEBUG1, we can see the
-- recursive planning, so we can detect the pushdown
SET client_min_messages TO DEBUG1;

-- values in WHERE clause
WITH cte_1 (num,letter) AS (VALUES (1, 'one'), (2, 'two'), (3, 'three'))
SELECT
	count(*)
FROM
	test_values
WHERE key IN (SELECT num FROM cte_1);

-- values in WHERE clause with DISTINCT
WITH cte_1 (num,letter) AS (VALUES (1, 'one'), (2, 'two'), (3, 'three'))
SELECT
	count(*)
FROM
	test_values
WHERE key IN (SELECT DISTINCT num FROM cte_1);


-- we can control the materialization threshold via GUC
-- we set it 2, and the query has 3 tuples, so the planner
-- decides to materialize the VALUES clause
BEGIN;
	SET LOCAL citus.values_materialization_threshold TO 2;
	WITH cte_1 (num,letter) AS (VALUES (1, 'one'), (2, 'two'), (3, 'three'))
	SELECT
		count(*)
	FROM
		test_values
	WHERE key IN (SELECT DISTINCT num FROM cte_1);
COMMIT;

-- we can control the materialization threshold via GUC
-- we set it -1, and the query is never materialized
-- decides to materialize the VALUES clause
BEGIN;
	SET LOCAL citus.values_materialization_threshold TO -1;
	WITH cte_1 (num,letter) AS (VALUES (1, 'one'), (2, 'two'), (3, 'three'))
	SELECT
		count(*)
	FROM
		test_values
	WHERE key IN (SELECT DISTINCT num FROM cte_1);
COMMIT;

-- values with repeat can be pushed down
WITH cte_1 (letter) AS (VALUES (repeat('1',10)))
SELECT
	count(*)
FROM
	test_values
WHERE value IN (SELECT DISTINCT letter FROM cte_1);

-- values in WHERE clause with DISTINCT, and CTE defined in subquery
SELECT
	count(*)
FROM
	test_values
WHERE key
	IN
	(WITH cte_1 (num,letter) AS (VALUES (1, 'one'), (2, 'two'), (3, 'three'))
	 SELECT DISTINCT num FROM cte_1);

-- values in WHERE clause within a subquery
WITH cte_1 (num,letter) AS (VALUES (1, '1'), (2, '2'), (3, '3'))
SELECT
	count(*)
FROM
	test_values
WHERE key
	IN
(SELECT key FROM test_values WHERE value NOT IN (SELECT letter FROM cte_1) GROUP BY key);

-- VALUES nested multiple CTEs
WITH cte_1 (num,letter) AS (VALUES (1, '1'), (2, '2'), (3, '3')),
	 cte_2 (num, letter) AS (SELECT * FROM cte_1)
SELECT count(DISTINCT key) FROM test_values WHERE key >ANY(SELECT num FROM cte_2);

-- values with set operations can be pushed down as long as
-- they are JOINed with a distributed table
SELECT count(*) FROM
(
	(WITH cte_1 (num,letter) AS (VALUES (1, '1'), (2, '2'))
	SELECT key FROM test_values WHERE key >ANY(SELECT num FROM cte_1))
	UNION
	(WITH cte_1 (num,letter) AS (VALUES (2, '2'), (3, '3'))
	SELECT key FROM test_values WHERE key >ANY(SELECT num FROM cte_1))
) as foo;

-- values with set operations can be pushed down as long as
-- they are JOINed with a distributed table
SELECT count(*) FROM
(
	(WITH cte_1 (num,letter) AS (VALUES (1, '1'), (2, '2'))
	SELECT key FROM test_values WHERE key >ANY(SELECT num FROM cte_1))
	UNION ALL
	(WITH cte_1 (num,letter) AS (VALUES (2, '2'), (3, '3'))
	SELECT key FROM test_values WHERE key >ANY(SELECT num FROM cte_1))
) as foo GROUP BY key ORDER BY 1 DESC LIMIT 3;


-- values with set operations cannot be pushed along with
-- distributed tables
SELECT count(*) FROM
(
	(WITH cte_1 (num,letter) AS (VALUES (1, '1'), (2, '2'))
	SELECT num FROM cte_1)
	UNION
	(SELECT key FROM test_values)
) as foo;

-- values with set operations cannot be pushed along with
-- distributed tables
SELECT count(*) FROM
(
	(WITH cte_1 (num,letter) AS (VALUES (1, '1'), (2, '2'))
	SELECT num FROM cte_1)
	UNION ALL
	(SELECT key FROM test_values)
) as foo;

-- values in WHERE clause with a subquery can be pushed down
SELECT
	count(*)
FROM
	test_values
WHERE key IN (SELECT num FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(num, v));

-- values with INNER JOIN
SELECT
	count(*)
FROM
	test_values
		JOIN
	(SELECT a,b FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(a,b) ) as foo (num,letter)
ON (key = num);

-- values with supported OUTER JOIN
SELECT
	count(*)
FROM
	test_values
		LEFT JOIN
	(SELECT a,b FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(a,b) ) as foo (num,letter)
ON (key = num);

-- VALUES with supported OUTER join (since test_values is recursively planned)
SELECT
	count(*)
FROM
	test_values
		RIGHT JOIN
	(SELECT a,b FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(a,b) ) as foo (num,letter)
ON (key = num);

-- values with router queries
SELECT
	count(*)
FROM
	test_values
		LEFT JOIN
	(SELECT a,b FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(a,b) ) as foo (num,letter)
ON (key = num) WHERE key = 1;

-- values with reference tables
SELECT
	count(*)
FROM
	test_values_ref
		LEFT JOIN
	(SELECT a,b FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(a,b) ) as foo (num,letter)
ON (key = num);

-- values with non-coloated subquery join
-- VALUES can still be pushed down, the recursive planning
-- happens for non-colocated join between tables
SELECT
	count(*)
FROM
	test_values WHERE key
		NOT IN
	(WITH cte_1 (num,letter) AS (VALUES (1, 'one'), (2, 'two'), (3, 'three'))
	 SELECT key FROM test_values WHERE value NOT IN (SELECT letter FROM cte_1));


-- values can be recursively planned if merge step is required
WITH cte_1 (num,letter) AS NOT MATERIALIZED (VALUES (1, 'one'), (2, 'two'), (3, 'three')),
cte_2 (num,letter) AS NOT MATERIALIZED  (VALUES (1, 'one'), (2, 'two'), (3, 'three'))
SELECT
	count(*)
FROM
	test_values
		WHERE
			key IN (SELECT count(DISTINCT num) FROM cte_1)
				AND
			key IN (SELECT num FROM cte_2 ORDER BY letter LIMIT 1)
				AND
			key IN (SELECT max(num) FROM cte_1 JOIN cte_2 USING (num));


-- some more complex joins
-- in theory we can pushdown the VALUES here as well
-- but to behave consistently with other recurring tuples
-- we prefer recursive planning
SELECT count(*) as subquery_count
FROM (
  SELECT
    key
  FROM
    test_values
  WHERE
    (value = '5' OR value = '13')
  GROUP BY key HAVING count(distinct value) < 2) as a
  LEFT JOIN (
  SELECT
    (SELECT a FROM (VALUES (1, 'one')) as t(a,b))
  ) AS foo (num)
  ON a.key = foo.num
WHERE foo.num IS NULL
GROUP BY a.key;

-- only immutable functions can be pushed down
WITH cte_1 (num,letter) AS (VALUES (random(), 'one'), (2, 'two'), (3, 'three'))
SELECT
	count(*) > 0
FROM
	test_values
WHERE key IN (SELECT num FROM cte_1);

-- only immutable functions can be pushed down
SELECT
	count(*)
FROM
	test_values
WHERE key IN (SELECT num FROM (VALUES (random(), 'one'), (2, 'two'), (3, 'three')) as t(num, v));

-- only immutable functions can be pushed down
SELECT
	count(*)
FROM
	test_values
		JOIN
	(SELECT a,b FROM (VALUES (1, 'one'), (2, 'two'), (random(), 'three')) as t(a,b) ) as foo (num,letter)
ON (key = num);

-- materialized CTEs are recursively planned always
WITH cte_1 (num,letter) AS MATERIALIZED (VALUES (1, 'one'), (2, 'two'), (3, 'three'))
SELECT
	count(*) > 0
FROM
	test_values
WHERE key IN (SELECT num FROM cte_1);

-- because the FROM clause recurs, the subquery in WHERE
-- clause is recursively planned
SELECT
	num
FROM
	(VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(num, v)
WHERE num > (SELECT max(key) FROM test_values);

-- but, we cannot recursively plan if the subquery that VALUEs is correlated
SELECT
	*
FROM
	test_values as t1
		JOIN LATERAL (
			SELECT
				t1.key
			FROM
				(VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(num, v)
				  WHERE num > (SELECT max(key) FROM test_values)) as foo
	ON (true);



-- VALUES can be the inner relationship in a join
SELECT count(*) FROM
  (SELECT random() FROM test_values JOIN (SELECT a, b FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(a,b)) as values_data(a,b)
  ON test_values.key > values_data.a) subquery_1;

-- VALUES can be the left relationship in a join
SELECT count(*) FROM
  (SELECT random() FROM test_values LEFT JOIN (SELECT a, b FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(a,b)) as values_data(a,b)
  ON test_values.key > values_data.a) subquery_1;

-- VALUES can be the right relationship in a join
SELECT count(*) FROM
  (SELECT random() FROM test_values RIGHT JOIN (SELECT a, b FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(a,b)) as values_data(a,b)
  ON test_values.key > values_data.a) subquery_1;

-- subquery IN WHERE clause need to be recursively planned
-- but it is correlated so cannot be pushed down
SELECT
  count(*)
FROM
 (SELECT a, b FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(a,b)) as values_data(a,b)
WHERE
  NOT EXISTS
      (SELECT
          value
       FROM
          test_values
       WHERE
          test_values.key = values_data.a
      );


-- we can pushdown as long as GROUP BY on dist key
SELECT
  count(*)
FROM
  test_values
WHERE
  key IN
          (
          	SELECT a FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as values_data(a,b)
          )
GROUP BY key
ORDER BY 1 DESC
LIMIT 3;

-- CTEs are not inlined for modification queries
-- so always recursively planned
WITH cte_1 (num,letter) AS (VALUES (1, 'one'), (2, 'two'), (3, 'three'))
UPDATE test_values SET value = '1' WHERE key IN (SELECT num FROM cte_1);

-- we can pushdown modification queries with VALUEs
UPDATE
	test_values
SET
	value = '1'
WHERE
	key IN (SELECT num FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) as t(num, v));

-- we can pushdown modification queries with VALUEs as long as they contain immutable functions
UPDATE
	test_values
SET
	value = '1'
WHERE
	key IN (SELECT num FROM (VALUES (random(), 'one'), (2, 'two'), (3, 'three')) as t(num, v));


-- prepared statements should be fine to pushdown
PREPARE test_values_pushdown(int, int,int) AS
WITH cte_1 (num,letter) AS (VALUES ($1, 'one'), ($2, 'two'), ($3, 'three'))
SELECT
	count(*)
FROM
	test_values
WHERE key IN (SELECT num FROM cte_1);
EXECUTE test_values_pushdown(1,2,3);
EXECUTE test_values_pushdown(1,2,3);
EXECUTE test_values_pushdown(1,2,3);
EXECUTE test_values_pushdown(1,2,3);
EXECUTE test_values_pushdown(1,2,3);
EXECUTE test_values_pushdown(1,2,3);
EXECUTE test_values_pushdown(1,2,3);

-- prepared statements with volatile functions should be still pushed down
-- because the function is evaluated on the coordinator
CREATE OR REPLACE FUNCTION fixed_volatile_value() RETURNS integer VOLATILE AS $$
        BEGIN
                RAISE NOTICE 'evaluated on the coordinator';
                RETURN 1;
        END;
$$ LANGUAGE plpgsql;
EXECUTE test_values_pushdown(fixed_volatile_value(),2,3);

-- threshold should trigger materialization of VALUES in the first
-- statement and pushdown in the second as -1 disables materialization
BEGIN;
	SET LOCAL citus.values_materialization_threshold TO 0;
	EXECUTE test_values_pushdown(1,2,3);
	SET LOCAL citus.values_materialization_threshold TO -1;
	EXECUTE test_values_pushdown(1,2,3);

COMMIT;

SET client_min_messages TO WARNING;
DROP SCHEMA values_subquery CASCADE;
