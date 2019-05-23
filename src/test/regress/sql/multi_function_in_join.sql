--
-- multi function in join queries aims to test the function calls that are
-- used in joins.
--
-- These functions are supposed to be executed on the worker and to ensure
-- that we wrap those functions inside (SELECT * FROM fnc()) sub queries.
--
-- We do not yet support those functions that:
--  - have lateral joins
--  - have WITH ORDINALITY clause
--  - are user-defined and immutable

CREATE SCHEMA functions_in_joins;
SET search_path TO 'functions_in_joins';
SET citus.next_shard_id TO 2500000;

CREATE TABLE table1 (id int, data int);
SELECT create_distributed_table('table1','id');

INSERT INTO table1
SELECT x, x*x
from generate_series(1, 100) as f (x);

-- Verbose messages for observing the subqueries that wrapped function calls
SET client_min_messages TO DEBUG1;

-- Check joins on a sequence
CREATE SEQUENCE numbers;
SELECT * FROM table1 JOIN nextval('numbers') n ON (id = n) ORDER BY id ASC;

-- Check joins of a function that returns a single integer
CREATE FUNCTION add(integer, integer) RETURNS integer
AS 'SELECT $1 + $2;'
LANGUAGE SQL;
SELECT * FROM table1 JOIN add(3,5) sum ON (id = sum) ORDER BY id ASC;

-- Check join of plpgsql functions
-- a function returning a single integer
CREATE OR REPLACE FUNCTION increment(i integer) RETURNS integer AS $$
BEGIN
  RETURN i + 1;
END;
$$ LANGUAGE plpgsql;
SELECT * FROM table1 JOIN increment(2) val ON (id = val) ORDER BY id ASC;

-- a function that returns a set of integers
CREATE OR REPLACE FUNCTION next_k_integers(IN first_value INTEGER,
                                           IN k INTEGER DEFAULT 3,
                                           OUT result INTEGER)
  RETURNS SETOF INTEGER AS $$
BEGIN
  RETURN QUERY SELECT x FROM generate_series(first_value, first_value+k-1) f(x);
END;
$$ LANGUAGE plpgsql;
SELECT *
FROM table1 JOIN next_k_integers(3,2) next_integers ON (id = next_integers.result)
ORDER BY id ASC;

-- a function returning set of records
CREATE FUNCTION get_set_of_records() RETURNS SETOF RECORD AS $cmd$
SELECT x, x+1 FROM generate_series(0,4) f(x)
$cmd$
LANGUAGE SQL;
SELECT * FROM table1 JOIN get_set_of_records() AS t2(x int, y int) ON (id = x) ORDER BY id ASC;

-- a function returning table
CREATE FUNCTION dup(int) RETURNS TABLE(f1 int, f2 text)
AS $$ SELECT $1, CAST($1 AS text) || ' is text' $$
LANGUAGE SQL;

SELECT f.* FROM table1 t JOIN dup(32) f ON (f1 = id);

-- a stable function
CREATE OR REPLACE FUNCTION the_minimum_id()
  RETURNS INTEGER STABLE AS 'SELECT min(id) FROM table1' LANGUAGE SQL;
SELECT * FROM table1 JOIN the_minimum_id() min_id ON (id = min_id);

-- a built-in immutable function
SELECT * FROM table1 JOIN abs(100) as hundred ON (id = hundred) ORDER BY id ASC;

-- function joins inside a CTE
WITH next_row_to_process AS (
    SELECT * FROM table1 JOIN nextval('numbers') n ON (id = n)
    )
SELECT *
FROM table1, next_row_to_process
WHERE table1.data <= next_row_to_process.data
ORDER BY 1,2 ASC;

-- Multiple functions in an RTE
SELECT * FROM ROWS FROM (next_k_integers(5), next_k_integers(10)) AS f(a, b),
    table1 WHERE id = a ORDER BY id ASC;


-- Custom Type returning function used in a join
CREATE TYPE min_and_max AS (
  minimum INT,
  maximum INT
);

CREATE OR REPLACE FUNCTION max_and_min () RETURNS
  min_and_max AS $$
DECLARE
  result min_and_max%rowtype;
begin
  select into result min(data) as minimum, max(data) as maximum from table1;
  return result;
end;
$$ language plpgsql;

SELECT * FROM table1 JOIN max_and_min() m ON (m.maximum = data OR m.minimum = data) ORDER BY 1,2,3,4;

-- The following tests will fail as we do not support  all joins on
-- all kinds of functions
-- In other words, we cannot recursively plan the functions and hence 
-- the query fails on the workers
SET client_min_messages TO ERROR;
\set VERBOSITY terse

-- function joins in CTE results can create lateral joins that are not supported
WITH one_row AS (
    SELECT * FROM table1 WHERE id=52
    )
SELECT table1.id, table1.data
FROM one_row, table1, next_k_integers(one_row.id, 5) next_five_ids
WHERE table1.id = next_five_ids;

-- a user-defined immutable function
CREATE OR REPLACE FUNCTION the_answer_to_life()
  RETURNS INTEGER IMMUTABLE AS 'SELECT 42' LANGUAGE SQL;

SELECT * FROM table1 JOIN the_answer_to_life() the_answer ON (id = the_answer);

SELECT *
FROM table1
       JOIN next_k_integers(10,5) WITH ORDINALITY next_integers
         ON (id = next_integers.result);

-- WITH ORDINALITY clause
SELECT *
FROM table1
       JOIN next_k_integers(10,5) WITH ORDINALITY next_integers
         ON (id = next_integers.result)
ORDER BY id ASC;

RESET client_min_messages;
DROP SCHEMA functions_in_joins CASCADE;
SET search_path TO DEFAULT;
