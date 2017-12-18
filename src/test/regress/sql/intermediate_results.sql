-- Test functions for copying intermediate results
CREATE SCHEMA intermediate_results;
SET search_path TO 'intermediate_results';

-- in the same transaction we can read a result
BEGIN;
SELECT create_intermediate_result('squares', 'SELECT s, s*s FROM generate_series(1,5) s');
SELECT * FROM read_intermediate_result('squares', 'binary') AS res (x int, x2 int);
COMMIT;

-- in separate transactions, the result is no longer available
SELECT create_intermediate_result('squares', 'SELECT s, s*s FROM generate_series(1,5) s');
SELECT * FROM read_intermediate_result('squares', 'binary') AS res (x int, x2 int);

BEGIN;
CREATE TABLE interesting_squares (user_id text, interested_in text);
SELECT create_distributed_table('interesting_squares', 'user_id');
INSERT INTO interesting_squares VALUES ('jon', '2'), ('jon', '5'), ('jack', '3');

-- put an intermediate result on all workers
SELECT broadcast_intermediate_result('squares', 'SELECT s, s*s FROM generate_series(1,5) s');

-- query the intermediate result in a router query
SELECT x, x2
FROM interesting_squares JOIN (SELECT * FROM read_intermediate_result('squares', 'binary') AS res (x int, x2 int)) squares ON (x::text = interested_in)
WHERE user_id = 'jon'
ORDER BY x;

END;

BEGIN;
-- put an intermediate result on all workers
SELECT broadcast_intermediate_result('squares', 'SELECT s, s*s FROM generate_series(1,5) s');
-- query the intermediate result in a distributed query
SELECT x, x2
FROM interesting_squares
JOIN (SELECT * FROM read_intermediate_result('squares', 'binary') AS res (x int, x2 int)) squares ON (x::text = interested_in)
ORDER BY x;
END;

-- files should now be cleaned up
SELECT x, x2
FROM interesting_squares JOIN (SELECT * FROM read_intermediate_result('squares', 'binary') AS res (x text, x2 int)) squares ON (x = interested_in)
WHERE user_id = 'jon'
ORDER BY x;

-- try to read the file as text, will fail because of binary encoding
BEGIN;
SELECT create_intermediate_result('squares', 'SELECT s, s*s FROM generate_series(1,5) s');
SELECT * FROM read_intermediate_result('squares', 'binary') AS res (x text, x2 int);
END;

-- try to read the file with wrong encoding
BEGIN;
SELECT create_intermediate_result('squares', 'SELECT s, s*s FROM generate_series(1,5) s');
SELECT * FROM read_intermediate_result('squares', 'csv') AS res (x int, x2 int);
END;

-- try a composite type
CREATE TYPE intermediate_results.square_type AS (x text, x2 int);
SELECT run_command_on_workers('CREATE TYPE intermediate_results.square_type AS (x text, x2 int)');

CREATE TABLE stored_squares (user_id text, square intermediate_results.square_type, metadata jsonb);
INSERT INTO stored_squares VALUES ('jon', '(2,4)'::intermediate_results.square_type, '{"value":2}');
INSERT INTO stored_squares VALUES ('jon', '(3,9)'::intermediate_results.square_type, '{"value":3}');
INSERT INTO stored_squares VALUES ('jon', '(4,16)'::intermediate_results.square_type, '{"value":4}');
INSERT INTO stored_squares VALUES ('jon', '(5,25)'::intermediate_results.square_type, '{"value":5}');

-- composite types change the format to text
BEGIN;
SELECT create_intermediate_result('stored_squares', 'SELECT square FROM stored_squares');
SELECT * FROM read_intermediate_result('stored_squares', 'binary') AS res (s intermediate_results.square_type);
COMMIT;

BEGIN;
SELECT create_intermediate_result('stored_squares', 'SELECT square FROM stored_squares');
SELECT * FROM read_intermediate_result('stored_squares', 'text') AS res (s intermediate_results.square_type);
COMMIT;

BEGIN;
-- put an intermediate result in text format on all workers
SELECT broadcast_intermediate_result('stored_squares', 'SELECT square, metadata FROM stored_squares');

-- query the intermediate result in a router query using text format
SELECT * FROM interesting_squares JOIN (
  SELECT * FROM
    read_intermediate_result('stored_squares', 'text') AS res (s intermediate_results.square_type, m jsonb)
) squares
ON ((s).x = interested_in) WHERE user_id = 'jon' ORDER BY 1,2;

-- query the intermediate result in a real-time query using text format
SELECT * FROM interesting_squares JOIN (
  SELECT * FROM
    read_intermediate_result('stored_squares', 'text') AS res (s intermediate_results.square_type, m jsonb)
) squares
ON ((s).x = interested_in) ORDER BY 1,2;

END;

BEGIN;
-- accurate row count estimates for primitive types
SELECT create_intermediate_result('squares', 'SELECT s, s*s FROM generate_series(1,632) s');
EXPLAIN SELECT * FROM read_intermediate_result('squares', 'binary') AS res (x int, x2 int);

-- less accurate results for variable types
SELECT create_intermediate_result('hellos', $$SELECT s, 'hello-'||s FROM generate_series(1,63) s$$);
EXPLAIN SELECT * FROM read_intermediate_result('hellos', 'binary') AS res (x int, y text);

-- not very accurate results for text encoding
SELECT create_intermediate_result('stored_squares', 'SELECT square FROM stored_squares');
EXPLAIN SELECT * FROM read_intermediate_result('stored_squares', 'text') AS res (s intermediate_results.square_type);
END;

-- pipe query output into a result file and create a table to check the result
COPY (SELECT s, s*s FROM generate_series(1,5) s)
TO PROGRAM
  $$psql -h localhost -p 57636 -U postgres -d regression -c "BEGIN; COPY squares FROM STDIN WITH (format result); CREATE TABLE intermediate_results.squares AS SELECT * FROM read_intermediate_result('squares', 'binary') AS res(x int, x2 int); END;"$$
WITH (FORMAT binary);

SELECT * FROM squares ORDER BY x;

DROP SCHEMA intermediate_results CASCADE;
