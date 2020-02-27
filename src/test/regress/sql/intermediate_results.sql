-- Test functions for copying intermediate results
CREATE SCHEMA intermediate_results;
SET search_path TO 'intermediate_results';

-- helper udfs
CREATE OR REPLACE FUNCTION pg_catalog.store_intermediate_result_on_node(nodename text, nodeport int, result_id text, query text)
    RETURNS void
    LANGUAGE C STRICT VOLATILE
    AS 'citus', $$store_intermediate_result_on_node$$;

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


CREATE FUNCTION raise_failed_execution_int_result(query text) RETURNS void AS $$
BEGIN
        EXECUTE query;
        EXCEPTION WHEN OTHERS THEN
        IF SQLERRM LIKE '%does not exist%' THEN
                RAISE 'Task failed to execute';
        ELSIF SQLERRM LIKE '%could not receive query results%' THEN
          RAISE 'Task failed to execute';
        END IF;
END;
$$LANGUAGE plpgsql;

-- don't print the worker port
\set VERBOSITY terse
SET client_min_messages TO ERROR;

-- files should now be cleaned up
SELECT raise_failed_execution_int_result($$
	SELECT x, x2
	FROM interesting_squares JOIN (SELECT * FROM read_intermediate_result('squares', 'binary') AS res (x text, x2 int)) squares ON (x = interested_in)
	WHERE user_id = 'jon'
	ORDER BY x;
$$);

\set VERBOSITY DEFAULT
SET client_min_messages TO DEFAULT;

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
EXPLAIN (COSTS ON) SELECT * FROM read_intermediate_result('squares', 'binary') AS res (x int, x2 int);

-- less accurate results for variable types
SELECT create_intermediate_result('hellos', $$SELECT s, 'hello-'||s FROM generate_series(1,63) s$$);
EXPLAIN (COSTS ON) SELECT * FROM read_intermediate_result('hellos', 'binary') AS res (x int, y text);

-- not very accurate results for text encoding
SELECT create_intermediate_result('stored_squares', 'SELECT square FROM stored_squares');
EXPLAIN (COSTS ON) SELECT * FROM read_intermediate_result('stored_squares', 'text') AS res (s intermediate_results.square_type);
END;

-- pipe query output into a result file and create a table to check the result
COPY (SELECT s, s*s FROM generate_series(1,5) s)
TO PROGRAM
  $$psql -h localhost -p 57636 -U postgres -d regression -c "BEGIN; COPY squares FROM STDIN WITH (format result); CREATE TABLE intermediate_results.squares AS SELECT * FROM read_intermediate_result('squares', 'text') AS res(x int, x2 int); END;"$$
WITH (FORMAT text);

SELECT * FROM squares ORDER BY x;

-- empty shard interval array should raise error
SELECT worker_hash_partition_table(42,1,'SELECT a FROM generate_series(1,100) AS a', 'a', 23, ARRAY[0]);

-- cannot use DDL commands
select broadcast_intermediate_result('a', 'create table foo(int serial)');
select broadcast_intermediate_result('a', 'prepare foo as select 1');
select create_intermediate_result('a', 'create table foo(int serial)');

--
-- read_intermediate_results
--

BEGIN;
SELECT create_intermediate_result('squares_1', 'SELECT s, s*s FROM generate_series(1,3) s'),
       create_intermediate_result('squares_2', 'SELECT s, s*s FROM generate_series(4,6) s'),
       create_intermediate_result('squares_3', 'SELECT s, s*s FROM generate_series(7,10) s');

SELECT count(*) FROM read_intermediate_results(ARRAY[]::text[], 'binary') AS res (x int, x2 int);
SELECT * FROM read_intermediate_results(ARRAY['squares_1']::text[], 'binary') AS res (x int, x2 int);
SELECT * FROM read_intermediate_results(ARRAY['squares_1', 'squares_2', 'squares_3']::text[], 'binary') AS res (x int, x2 int);

COMMIT;

-- in separate transactions, the result is no longer available
SELECT create_intermediate_result('squares_1', 'SELECT s, s*s FROM generate_series(1,5) s');
SELECT * FROM read_intermediate_results(ARRAY['squares_1']::text[], 'binary') AS res (x int, x2 int);

-- error behaviour, and also check that results are deleted on rollback
BEGIN;
SELECT create_intermediate_result('squares_1', 'SELECT s, s*s FROM generate_series(1,3) s');
SAVEPOINT s1;
SELECT * FROM read_intermediate_results(ARRAY['notexistingfile', 'squares_1'], 'binary') AS res (x int, x2 int);
ROLLBACK TO SAVEPOINT s1;
SELECT * FROM read_intermediate_results(ARRAY['squares_1', 'notexistingfile'], 'binary') AS res (x int, x2 int);
ROLLBACK TO SAVEPOINT s1;
SELECT * FROM read_intermediate_results(ARRAY['squares_1', NULL], 'binary') AS res (x int, x2 int);
ROLLBACK TO SAVEPOINT s1;
-- after rollbacks we should be able to run vail read_intermediate_results still.
SELECT count(*) FROM read_intermediate_results(ARRAY['squares_1']::text[], 'binary') AS res (x int, x2 int);
SELECT count(*) FROM read_intermediate_results(ARRAY[]::text[], 'binary') AS res (x int, x2 int);
END;

SELECT * FROM read_intermediate_results(ARRAY['squares_1']::text[], 'binary') AS res (x int, x2 int);

-- Test non-binary format: read_intermediate_results(..., 'text')
BEGIN;
-- ROW(...) types switch the output format to text
SELECT broadcast_intermediate_result('stored_squares_1',
                                     'SELECT s, s*s, ROW(1::text, 2) FROM generate_series(1,3) s'),
       broadcast_intermediate_result('stored_squares_2',
                                     'SELECT s, s*s, ROW(2::text, 3) FROM generate_series(4,6) s');

-- query the intermediate result in a router query using text format
SELECT * FROM interesting_squares JOIN (
  SELECT * FROM
    read_intermediate_results(ARRAY['stored_squares_1', 'stored_squares_2'], 'binary') AS res (x int, x2 int, z intermediate_results.square_type)
) squares
ON (squares.x::text = interested_in) WHERE user_id = 'jon' ORDER BY 1,2;

END;

-- Cost estimation for read_intermediate_results
BEGIN;
-- almost accurate row count estimates for primitive types
SELECT create_intermediate_result('squares_1', 'SELECT s, s*s FROM generate_series(1,632) s'),
       create_intermediate_result('squares_2', 'SELECT s, s*s FROM generate_series(633,1024) s');
EXPLAIN (COSTS ON) SELECT * FROM read_intermediate_results(ARRAY['squares_1', 'squares_2'], 'binary') AS res (x int, x2 int);

-- less accurate results for variable types
SELECT create_intermediate_result('hellos_1', $$SELECT s, 'hello-'||s FROM generate_series(1,63) s$$),
       create_intermediate_result('hellos_2', $$SELECT s, 'hello-'||s FROM generate_series(64,129) s$$);
EXPLAIN (COSTS ON) SELECT * FROM read_intermediate_results(ARRAY['hellos_1', 'hellos_2'], 'binary') AS res (x int, y text);

-- not very accurate results for text encoding
SELECT create_intermediate_result('stored_squares', 'SELECT square FROM stored_squares');
EXPLAIN (COSTS ON) SELECT * FROM read_intermediate_results(ARRAY['stored_squares'], 'text') AS res (s intermediate_results.square_type);
END;

--
-- fetch_intermediate_results
--

-- straightforward, single-result case
BEGIN;
SELECT broadcast_intermediate_result('squares_1', 'SELECT s, s*s FROM generate_series(1, 5) s');
SELECT * FROM fetch_intermediate_results(ARRAY['squares_1']::text[], :'worker_2_host', :worker_2_port);
SELECT * FROM read_intermediate_result('squares_1', 'binary') AS res (x int, x2 int);
SELECT * FROM fetch_intermediate_results(ARRAY['squares_1']::text[], :'worker_1_host', :worker_1_port);
SELECT * FROM read_intermediate_result('squares_1', 'binary') AS res (x int, x2 int);
END;

-- multiple results, and some error cases
BEGIN;
SELECT store_intermediate_result_on_node(:'worker_1_host', :worker_1_port,
                                         'squares_1', 'SELECT s, s*s FROM generate_series(1, 2) s');
SELECT store_intermediate_result_on_node(:'worker_1_host', :worker_1_port,
                                         'squares_2', 'SELECT s, s*s FROM generate_series(3, 4) s');
SAVEPOINT s1;
-- results aren't available on coordinator yet
SELECT * FROM read_intermediate_results(ARRAY['squares_1', 'squares_2']::text[], 'binary') AS res (x int, x2 int);
ROLLBACK TO SAVEPOINT s1;
-- fetch from worker 2 should fail
SELECT * FROM fetch_intermediate_results(ARRAY['squares_1', 'squares_2']::text[], :'worker_2_host', :worker_2_port);
ROLLBACK TO SAVEPOINT s1;
-- still, results aren't available on coordinator yet
SELECT * FROM read_intermediate_results(ARRAY['squares_1', 'squares_2']::text[], 'binary') AS res (x int, x2 int);
ROLLBACK TO SAVEPOINT s1;
-- fetch from worker 1 should succeed
SELECT * FROM fetch_intermediate_results(ARRAY['squares_1', 'squares_2']::text[], :'worker_1_host', :worker_1_port);
SELECT * FROM read_intermediate_results(ARRAY['squares_1', 'squares_2']::text[], 'binary') AS res (x int, x2 int);
-- fetching again should succeed
SELECT * FROM fetch_intermediate_results(ARRAY['squares_1', 'squares_2']::text[], :'worker_1_host', :worker_1_port);
SELECT * FROM read_intermediate_results(ARRAY['squares_1', 'squares_2']::text[], 'binary') AS res (x int, x2 int);
ROLLBACK TO SAVEPOINT s1;
-- empty result id list should succeed
SELECT * FROM fetch_intermediate_results(ARRAY[]::text[], :'worker_1_host', :worker_1_port);
-- null in result id list should error gracefully
SELECT * FROM fetch_intermediate_results(ARRAY[NULL, 'squares_1', 'squares_2']::text[], :'worker_1_host', :worker_1_port);
END;

-- results should have been deleted after transaction commit
SELECT * FROM read_intermediate_results(ARRAY['squares_1', 'squares_2']::text[], 'binary') AS res (x int, x2 int);

DROP SCHEMA intermediate_results CASCADE;
