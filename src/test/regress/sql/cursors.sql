CREATE SCHEMA cursors;
SET search_path TO cursors;

CREATE TABLE distributed_table (key int, value text);
SELECT create_distributed_table('distributed_table', 'key');


-- load some data, but not very small amounts because RETURN QUERY in plpgsql
-- hard codes the cursor fetch to 50 rows on PG 12, though they might increase
-- it sometime in the future, so be mindful
INSERT INTO distributed_table SELECT i  % 10, i::text FROM  generate_series(0, 1000) i;


CREATE OR REPLACE FUNCTION simple_cursor_on_dist_table(cursor_name refcursor) RETURNS refcursor AS '
BEGIN
    OPEN $1 FOR SELECT DISTINCT key FROM distributed_table ORDER BY 1;
    RETURN $1;
END;
' LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cursor_with_intermediate_result_on_dist_table(cursor_name refcursor) RETURNS refcursor AS '
BEGIN
    OPEN $1 FOR
		WITH cte_1 AS (SELECT * FROM distributed_table OFFSET 0)
			SELECT DISTINCT key FROM distributed_table WHERE value in (SELECT value FROM cte_1) ORDER BY 1;
    RETURN $1;
END;
' LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION cursor_with_intermediate_result_on_dist_table_with_param(cursor_name refcursor, filter text) RETURNS refcursor AS '
BEGIN
    OPEN $1 FOR
		WITH cte_1 AS (SELECT * FROM distributed_table WHERE value < $2 OFFSET 0)
			SELECT DISTINCT key FROM distributed_table WHERE value in (SELECT value FROM cte_1) ORDER BY 1;
    RETURN $1;
END;
' LANGUAGE plpgsql;


-- pretty basic query with cursors
-- Citus should plan/execute once and pull
-- the results to coordinator, then serve it
-- from the coordinator
BEGIN;
	SELECT simple_cursor_on_dist_table('cursor_1');
	SET LOCAL citus.log_intermediate_results TO ON;
	SET LOCAL client_min_messages TO DEBUG1;
	FETCH 5 IN cursor_1;
	FETCH 50 IN cursor_1;
	FETCH ALL IN cursor_1;
COMMIT;


BEGIN;
	SELECT cursor_with_intermediate_result_on_dist_table('cursor_1');

	-- multiple FETCH commands should not trigger re-running the subplans
	SET LOCAL citus.log_intermediate_results TO ON;
	SET LOCAL client_min_messages TO DEBUG1;
	FETCH 5 IN cursor_1;
	FETCH 1 IN cursor_1;
	FETCH ALL IN cursor_1;
	FETCH 5 IN cursor_1;
COMMIT;


BEGIN;
	SELECT cursor_with_intermediate_result_on_dist_table_with_param('cursor_1', '600');

	-- multiple FETCH commands should not trigger re-running the subplans
	-- also test with parameters
	SET LOCAL citus.log_intermediate_results TO ON;
	SET LOCAL client_min_messages TO DEBUG1;
	FETCH 1 IN cursor_1;
	FETCH 1 IN cursor_1;
	FETCH 1 IN cursor_1;
	FETCH 1 IN cursor_1;
	FETCH 1 IN cursor_1;
	FETCH 1 IN cursor_1;
	FETCH ALL IN cursor_1;

COMMIT;

 CREATE OR REPLACE FUNCTION value_counter() RETURNS TABLE(counter text) LANGUAGE PLPGSQL AS $function$
 BEGIN
         return query
WITH cte AS
  (SELECT dt.value
   FROM distributed_table dt
   WHERE dt.value in
       (SELECT value
        FROM distributed_table p
        GROUP BY p.value
        HAVING count(*) > 0))

 SELECT * FROM cte;
END;
$function$ ;

SET citus.log_intermediate_results TO ON;
SET client_min_messages TO DEBUG1;
\set VERBOSITY terse
SELECT count(*) from (SELECT value_counter())  as foo;
BEGIN;
	SELECT count(*) from (SELECT value_counter())  as foo;
COMMIT;

-- suppress NOTICEs
SET client_min_messages TO ERROR;
DROP SCHEMA cursors CASCADE;
