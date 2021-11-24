--
-- MULTI_FUNCTION_EVALUATION
--
CREATE SCHEMA multi_function_evaluation;
SET search_path TO multi_function_evaluation;

SET citus.next_shard_id TO 1200000;

-- many of the tests in this file is intended for testing non-fast-path
-- router planner, so we're explicitly disabling it in this file.
-- We've bunch of other tests that triggers fast-path-router
SET citus.enable_fast_path_router_planner TO false;

-- nextval() works (no good way to test DEFAULT, or, by extension, SERIAL)

CREATE TABLE example (key INT, value INT);
SET citus.shard_replication_factor TO 2;
SELECT create_distributed_table('example', 'key', shard_count:=1);
RESET citus.shard_replication_factor;
CREATE SEQUENCE example_value_seq;
INSERT INTO example VALUES (1, nextval('example_value_seq'));
SELECT * FROM example;

-- functions called by prepared statements are also evaluated

PREPARE stmt AS INSERT INTO example VALUES (2);
EXECUTE stmt;
EXECUTE stmt;
SELECT * FROM example;

-- non-immutable functions inside CASE/COALESCE aren't allowed

ALTER TABLE example DROP value;
ALTER TABLE example ADD value timestamp;

-- this is allowed because there are no mutable funcs in the CASE
UPDATE example SET value = (CASE WHEN value > timestamp '12-12-1991' THEN timestamp '12-12-1991' ELSE value + interval '1 hour' END) WHERE key = 1;

-- this is allowed because the planner strips away the CASE during constant evaluation
UPDATE example SET value = CASE WHEN true THEN now() ELSE now() + interval '1 hour' END WHERE key = 1;

-- this is not allowed because there're mutable functions in a CaseWhen clause
-- (which we can't easily evaluate on the master)
UPDATE example SET value = (CASE WHEN now() > timestamp '12-12-1991' THEN now() ELSE timestamp '10-24-1190' END) WHERE key = 1;

-- make sure we also check defresult (the ELSE clause)
UPDATE example SET value = (CASE WHEN now() > timestamp '12-12-1991' THEN timestamp '12-12-1191' ELSE now() END) WHERE key = 1;

-- COALESCE is allowed
UPDATE example SET value = COALESCE(null, null, timestamp '10-10-1000') WHERE key = 1;

-- COALESCE is not allowed if there are any mutable functions
UPDATE example SET value = COALESCE(now(), timestamp '10-10-1000') WHERE key = 1;
UPDATE example SET value = COALESCE(timestamp '10-10-1000', now()) WHERE key = 1;

-- RowCompareExpr's are checked for mutability. These are allowed:

ALTER TABLE example DROP value;
ALTER TABLE example ADD value boolean;
ALTER TABLE example ADD time_col timestamptz;

UPDATE example SET value = NULLIF(ROW(1, 2) < ROW(2, 3), true) WHERE key = 1;
UPDATE example SET value = NULLIF(ROW(true, 2) < ROW(value, 3), true) WHERE key = 1;

-- But this RowCompareExpr is not (it passes Var into STABLE)

UPDATE example SET value = NULLIF(
	ROW(date '10-10-1000', 2) < ROW(time_col, 3), true
) WHERE key = 1;

-- DistinctExpr's are also checked for mutability. These are allowed:

UPDATE example SET value = 1 IS DISTINCT FROM 2 WHERE key = 1;
UPDATE example SET value = date '10-10-1000' IS DISTINCT FROM timestamptz '10-10-1000' WHERE key = 1;

-- But this RowCompare references the STABLE = (date, timestamptz) operator

UPDATE example SET value = date '10-10-1000' IS DISTINCT FROM time_col WHERE key = 1;

-- this ScalarArrayOpExpr ("scalar op ANY/ALL (array)") is allowed

UPDATE example SET value = date '10-10-1000' = ANY ('{10-10-1000}'::date[]) WHERE key = 1;

-- this ScalarArrayOpExpr is not, it invokes the STABLE = (timestamptz, date) operator

UPDATE example SET value = time_col = ANY ('{10-10-1000}'::date[]) WHERE key = 1;

-- CoerceViaIO (typoutput -> typinput, a type coercion)

ALTER TABLE example DROP value;
ALTER TABLE example ADD value date;

-- this one is allowed
UPDATE example SET value = (timestamp '10-19-2000 13:29')::date WHERE key = 1;
-- this one is not
UPDATE example SET value = time_col::date WHERE key = 1;

-- ArrayCoerceExpr (applies elemfuncid to each elem)

ALTER TABLE example DROP value;
ALTER TABLE example ADD value date[];

-- this one is allowed
UPDATE example SET value = array[timestamptz '10-20-2013 10:20']::date[] WHERE key = 1;
-- this one is not
UPDATE example SET value = array[time_col]::date[] WHERE key = 1;

-- test that UPDATE and DELETE also have the functions in WHERE evaluated

ALTER TABLE example DROP time_col;
ALTER TABLE example DROP value;
ALTER TABLE example ADD value timestamptz;

INSERT INTO example VALUES (3, now());
UPDATE example SET value = timestamp '10-10-2000 00:00' WHERE key = 3 AND value > now() - interval '1 hour';
SELECT * FROM example WHERE key = 3;

DELETE FROM example WHERE key = 3 AND value < now() - interval '1 hour';
SELECT * FROM example WHERE key = 3;

-- test that function evaluation descends into expressions
CREATE OR REPLACE FUNCTION stable_fn()
RETURNS timestamptz STABLE
LANGUAGE plpgsql
AS $function$
BEGIN
	RAISE NOTICE 'stable_fn called';
	RETURN timestamp '10-10-2000 00:00';
END;
$function$;

INSERT INTO example VALUES (44, (ARRAY[stable_fn(),stable_fn()])[1]);
SELECT * FROM example WHERE key = 44;

-- unnest is a set-returning function, which should not be evaluated
UPDATE example SET value = stable_fn() + interval '2 hours' FROM UNNEST(ARRAY[44, 4]) AS k (key) WHERE example.key = k.key;
SELECT * FROM example WHERE key = 44;

-- create a table with multiple shards to trigger recursive planning
CREATE TABLE table_1 (key int, value timestamptz);
SELECT create_distributed_table('table_1', 'key');

-- the following query will have a read_intermediate_result call, but it should be skipped
DELETE
FROM table_1
WHERE key >= (SELECT min(KEY) FROM table_1)
AND value > now() - interval '1 hour';

CREATE OR REPLACE FUNCTION stable_squared(int)
RETURNS int STABLE
LANGUAGE plpgsql
AS $function$
BEGIN
	RAISE NOTICE 'stable_fn called';
	RETURN $1 * $1;
END;
$function$;
SELECT create_distributed_function('stable_squared(int)');

UPDATE example SET value = timestamp '10-10-2000 00:00'
FROM (SELECT key, stable_squared(count(*)::int) y FROM example GROUP BY key) a WHERE example.key = a.key;

UPDATE example SET value = timestamp '10-10-2000 00:00'
FROM (SELECT key, stable_squared((count(*) OVER ())::int) y FROM example GROUP BY key) a WHERE example.key = a.key;

UPDATE example SET value = timestamp '10-10-2000 00:00'
FROM (SELECT key, stable_squared(grouping(key)) y FROM example GROUP BY key) a WHERE example.key = a.key;

-- https://github.com/citusdata/citus/issues/3939
CREATE TABLE test_table(id int);
SELECT create_distributed_table('test_table', 'id');

CREATE OR REPLACE FUNCTION f(val text DEFAULT 'default')
RETURNS int AS $$ BEGIN RETURN length(val); END; $$ LANGUAGE 'plpgsql' IMMUTABLE;

INSERT INTO test_table VALUES (f('test'));
INSERT INTO test_table VALUES (f());
INSERT INTO test_table VALUES (f(f()::text));

SELECT * FROM test_table ORDER BY 1;

\set VERBOSITY terse
DROP SCHEMA multi_function_evaluation CASCADE;
