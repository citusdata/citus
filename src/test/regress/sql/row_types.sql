-- Tests for row types on the target list
CREATE SCHEMA row_types;
SET search_path TO row_types;

CREATE TABLE test (x int, y int);
SELECT create_distributed_table('test','x');

CREATE OR REPLACE FUNCTION table_returner(INT) RETURNS TABLE(name text, id INT)
AS $$
BEGIN
    RETURN QUERY SELECT $1::text, $1;
END;
$$ language plpgsql;
SELECT create_distributed_function('table_returner(int)');

CREATE OR REPLACE FUNCTION record_returner(INOUT id int, OUT name text)
RETURNS record
AS $$
BEGIN
    id := id + 1;
    name := 'returned';
END;
$$ language plpgsql;
SELECT create_distributed_function('record_returner(int)');

CREATE OR REPLACE FUNCTION identity_returner(x anyelement)
RETURNS anyelement
AS $$
BEGIN
	RETURN x;
END;
$$ language plpgsql;
SELECT create_distributed_function('identity_returner(anyelement)');


INSERT INTO test VALUES (1,2), (1,3), (2,2), (2,3);

-- multi-shard queries support row types
SELECT (x,y) FROM test ORDER BY x, y;
SELECT (x,y) FROM test GROUP BY x, y ORDER BY x, y;
SELECT ARRAY[NULL,(x,(y,x)),NULL,(y,(x,y))] FROM test ORDER BY x, y;
SELECT ARRAY[[(x,(y,x))],[(x,(x,y))]] FROM test ORDER BY x, y;
select distinct (x,y) AS foo, x, y FROM test ORDER BY x, y;
SELECT table_returner(x) FROM test ORDER BY x, y;
SELECT record_returner(x) FROM test ORDER BY x, y;
SELECT NULLIF((x, y), (y, x)) FROM test ORDER BY x, y;
SELECT LEAST((x, y), (y, x)) FROM test ORDER BY x, y;
SELECT GREATEST((x, y), (y, x)) FROM test ORDER BY x, y;
SELECT COALESCE(NULL, (x, y), (y, x)) FROM test ORDER BY x, y;
SELECT CASE x WHEN 2 THEN (x, y) ELSE (y, x) END FROM test ORDER BY x, y;
SELECT CASE x WHEN 2 THEN (x, y) END FROM test ORDER BY x, y;
-- varying shape unsupported
SELECT ARRAY[(x,(y,x),y),(y,(x,y))] FROM test ORDER BY x, y;
SELECT ARRAY[[(x,(y,x))],[((x,x),y)]] FROM test ORDER BY x, y;
SELECT CASE x WHEN 2 THEN (x, y, x) ELSE (y, x) END FROM test ORDER BY x, y;
-- RECORD from polymorphic types unsupported
SELECT identity_returner((x, y)) FROM test ORDER BY x, y;
SELECT array_agg((x, y)) FROM test;

-- router queries support row types
SELECT (x,y) FROM test WHERE x = 1 ORDER BY x, y;
SELECT (x,y) AS foo FROM test WHERE x = 1 ORDER BY x, y;
SELECT ARRAY[NULL,(x,(y,x)),NULL,(y,(x,y))] FROM test WHERE x = 1 ORDER BY x, y;
SELECT ARRAY[[(x,(y,x))],[(x,(x,y))]] FROM test WHERE x = 1 ORDER BY x, y;
select distinct (x,y) AS foo, x, y FROM test WHERE x = 1 ORDER BY x, y;
SELECT table_returner(x) FROM test WHERE x = 1 ORDER BY x, y;
SELECT record_returner(x) FROM test WHERE x = 1 ORDER BY x, y;
SELECT NULLIF((x, y), (y, x)) FROM test WHERE x = 1 ORDER BY x, y;
SELECT LEAST((x, y), (y, x)) FROM test WHERE x = 1 ORDER BY x, y;
SELECT GREATEST((x, y), (y, x)) FROM test WHERE x = 1 ORDER BY x, y;
SELECT COALESCE(NULL, (x, y), (y, x)) FROM test WHERE x = 1 ORDER BY x, y;
SELECT CASE x WHEN 2 THEN (x, y) ELSE (y, x) END FROM test WHERE x = 1 ORDER BY x, y;
SELECT CASE x WHEN 2 THEN (x, y) END FROM test WHERE x = 1 ORDER BY x, y;
-- varying shape unsupported
SELECT ARRAY[(x,(y,x),y),(y,(x,y))] FROM test WHERE x = 1 ORDER BY x, y;
SELECT ARRAY[[(x,(y,x))],[((x,x),y)]] FROM test WHERE x = 1 ORDER BY x, y;
SELECT CASE x WHEN 2 THEN (x, y, x) ELSE (y, x) END FROM test WHERE x = 1 ORDER BY x, y;
-- RECORD from polymorphic types unsupported
SELECT identity_returner((x, y)) FROM test WHERE x = 1 ORDER BY x, y;
SELECT array_agg((x, y)) FROM test WHERE x = 1;

-- nested row expressions
SELECT (x,(x,y)) AS foo FROM test WHERE x = 1 ORDER BY x, y;
SELECT (x,record_returner(x)) FROM test WHERE x = 1 ORDER BY x, y;

-- table functions in row expressions are not supported
SELECT (x,table_returner(x)) FROM test WHERE x = 1 ORDER BY x, y;

-- try prepared statements
PREPARE rec(int) AS SELECT (x,y*$1) FROM test WHERE x = $1  ORDER BY x, y;
EXECUTE rec(1);
EXECUTE rec(1);
EXECUTE rec(1);
EXECUTE rec(1);
EXECUTE rec(1);
EXECUTE rec(1);

SET client_min_messages TO error;
DROP SCHEMA row_types CASCADE;
