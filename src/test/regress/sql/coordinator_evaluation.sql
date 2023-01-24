-- This test relies on metadata being synced
-- that's why is should be executed on MX schedule
CREATE SCHEMA coordinator_evaluation;
SET search_path TO coordinator_evaluation;

-- create a volatile function that returns the local node id
CREATE OR REPLACE FUNCTION get_local_node_id_volatile()
RETURNS INT AS $$
DECLARE localGroupId int;
BEGIN
	SELECT groupid INTO localGroupId FROM pg_dist_local_group;
  RETURN localGroupId;
END; $$ language plpgsql VOLATILE;
SELECT create_distributed_function('get_local_node_id_volatile()');

CREATE OR REPLACE FUNCTION get_local_node_id_volatile_sum_with_param(int)
RETURNS INT AS $$
DECLARE localGroupId int;
BEGIN
	SELECT groupid + $1 INTO localGroupId FROM pg_dist_local_group;
  RETURN localGroupId;
END; $$ language plpgsql VOLATILE;
SELECT create_distributed_function('get_local_node_id_volatile_sum_with_param(int)');


CREATE TABLE coordinator_evaluation_table (key int, value int);
SELECT create_distributed_table('coordinator_evaluation_table', 'key');

-- show that local id is 0, we'll use this information
SELECT get_local_node_id_volatile();

-- load data
INSERT INTO coordinator_evaluation_table SELECT i, i FROM generate_series(0,100)i;

-- we expect that the function is evaluated on the worker node, so we should get a row
SELECT get_local_node_id_volatile() > 0 FROM coordinator_evaluation_table WHERE key = 1;

-- make sure that it is also true for  fast-path router queries with paramaters
PREPARE fast_path_router_with_param(int) AS SELECT get_local_node_id_volatile() > 0 FROM coordinator_evaluation_table WHERE key  = $1;

execute fast_path_router_with_param(1);
execute fast_path_router_with_param(2);
execute fast_path_router_with_param(3);
execute fast_path_router_with_param(4);
execute fast_path_router_with_param(5);
execute fast_path_router_with_param(6);
execute fast_path_router_with_param(7);
execute fast_path_router_with_param(8);

-- same query as fast_path_router_with_param, but with consts
SELECT get_local_node_id_volatile() > 0 FROM coordinator_evaluation_table WHERE key = 1;

PREPARE router_with_param(int) AS SELECT get_local_node_id_volatile() > 0 FROM coordinator_evaluation_table m1 JOIN coordinator_evaluation_table m2 USING(key) WHERE key  = $1;

execute router_with_param(1);
execute router_with_param(2);
execute router_with_param(3);
execute router_with_param(4);
execute router_with_param(5);
execute router_with_param(6);
execute router_with_param(7);
execute router_with_param(8);

-- same query as router_with_param, but with consts
SELECT get_local_node_id_volatile() > 0 FROM coordinator_evaluation_table m1 JOIN coordinator_evaluation_table m2 USING(key) WHERE key  = 1;

-- for multi-shard queries, we  still expect the evaluation to happen on the workers
SELECT count(*), max(get_local_node_id_volatile()) != 0,  min(get_local_node_id_volatile()) != 0 FROM coordinator_evaluation_table;

-- when executed locally, we expect to get the result from the coordinator
SELECT (SELECT count(*) FROM coordinator_evaluation_table), get_local_node_id_volatile() = 0;

-- make sure that we get the results from the workers when the query is sent to workers
SET citus.task_assignment_policy TO "round-robin";
SELECT (SELECT count(*) FROM coordinator_evaluation_table), get_local_node_id_volatile() = 0;

RESET citus.task_assignment_policy;

-- for multi-shard SELECTs, we don't try to evaluate on the coordinator
SELECT min(get_local_node_id_volatile())  > 0 FROM coordinator_evaluation_table;
SELECT count(*) > 0 FROM coordinator_evaluation_table WHERE value >= get_local_node_id_volatile();

-- let's have some tests around expressions

-- for modifications, we expect the evaluation to happen on the coordinator
-- thus the results should be 0
PREPARE insert_with_param_expression(int) AS INSERT INTO coordinator_evaluation_table (key, value) VALUES ($1 + get_local_node_id_volatile(), $1 + get_local_node_id_volatile()) RETURNING key, value;
EXECUTE insert_with_param_expression(0);
EXECUTE insert_with_param_expression(0);
EXECUTE insert_with_param_expression(0);
EXECUTE insert_with_param_expression(0);
EXECUTE insert_with_param_expression(0);
EXECUTE insert_with_param_expression(0);
EXECUTE insert_with_param_expression(0);

-- for modifications, we expect the evaluation to happen on the coordinator
-- thus the results should be 0
PREPARE insert_with_param(int) AS INSERT INTO coordinator_evaluation_table (key, value) VALUES ($1, $1) RETURNING key, value;
EXECUTE insert_with_param(0 + get_local_node_id_volatile());
EXECUTE insert_with_param(0 + get_local_node_id_volatile());
EXECUTE insert_with_param(0 + get_local_node_id_volatile());
EXECUTE insert_with_param(0 + get_local_node_id_volatile());
EXECUTE insert_with_param(0 + get_local_node_id_volatile());
EXECUTE insert_with_param(0 + get_local_node_id_volatile());
EXECUTE insert_with_param(0 + get_local_node_id_volatile());

PREPARE router_select_with_param_expression(int) AS SELECT value > 0 FROM coordinator_evaluation_table WHERE key = $1 + get_local_node_id_volatile();

-- for selects, we expect the evaluation to happen on the workers
-- this means that the query should be hitting multiple workers
SET client_min_messages TO DEBUG2;
EXECUTE router_select_with_param_expression(0);
EXECUTE router_select_with_param_expression(0);
EXECUTE router_select_with_param_expression(0);
EXECUTE router_select_with_param_expression(0);
EXECUTE router_select_with_param_expression(0);
EXECUTE router_select_with_param_expression(0);
EXECUTE router_select_with_param_expression(0);
EXECUTE router_select_with_param_expression(0);

PREPARE router_select_with_param(int) AS SELECT DISTINCT value FROM coordinator_evaluation_table WHERE key = $1;

-- this time the parameter itself is a function, so should be evaluated
-- on the coordinator
EXECUTE router_select_with_param(0 + get_local_node_id_volatile());
EXECUTE router_select_with_param(0 + get_local_node_id_volatile());
EXECUTE router_select_with_param(0 + get_local_node_id_volatile());
EXECUTE router_select_with_param(0 + get_local_node_id_volatile());
EXECUTE router_select_with_param(0 + get_local_node_id_volatile());
EXECUTE router_select_with_param(0 + get_local_node_id_volatile());
EXECUTE router_select_with_param(0 + get_local_node_id_volatile());

-- same calls with functions as the parametres only
EXECUTE router_select_with_param(get_local_node_id_volatile());
EXECUTE router_select_with_param(get_local_node_id_volatile());
EXECUTE router_select_with_param(get_local_node_id_volatile());
EXECUTE router_select_with_param(get_local_node_id_volatile());
EXECUTE router_select_with_param(get_local_node_id_volatile());
EXECUTE router_select_with_param(get_local_node_id_volatile());
EXECUTE router_select_with_param(get_local_node_id_volatile());

-- this time use the parameter inside the function
PREPARE router_select_with_parameter_in_function(int) AS SELECT bool_and(get_local_node_id_volatile_sum_with_param($1) > 1) FROM coordinator_evaluation_table WHERE key = get_local_node_id_volatile_sum_with_param($1);
EXECUTE router_select_with_parameter_in_function(0);
EXECUTE router_select_with_parameter_in_function(0);
EXECUTE router_select_with_parameter_in_function(0);
EXECUTE router_select_with_parameter_in_function(0);
EXECUTE router_select_with_parameter_in_function(0);
EXECUTE router_select_with_parameter_in_function(0);
EXECUTE router_select_with_parameter_in_function(0);

RESET client_min_messages;
RESET citus.log_remote_commands;

-- numeric has different casting affects, so some tests on that
CREATE TABLE coordinator_evaluation_table_2 (key numeric, value numeric);
SELECT create_distributed_table('coordinator_evaluation_table_2', 'key');

 CREATE OR REPLACE FUNCTION TEST_RANDOM (INTEGER, INTEGER) RETURNS INTEGER AS $$
DECLARE
    start_int ALIAS FOR $1;
    end_int ALIAS FOR $2;
BEGIN
    RETURN trunc(random() * (end_int-start_int) + start_int);
END;
$$ LANGUAGE 'plpgsql' STRICT;

SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE PROCEDURE coordinator_evaluation.test_procedure(int)
 LANGUAGE plpgsql
AS $procedure$
DECLARE filterKey INTEGER;
BEGIN
  filterKey := round(coordinator_evaluation.TEST_RANDOM(1,1)) + $1;
  PERFORM DISTINCT value FROM coordinator_evaluation_table_2 WHERE key = filterKey;
END;
$procedure$;
RESET citus.enable_metadata_sync;

-- we couldn't find a meaningful query to write for this
-- however this query fails before https://github.com/citusdata/citus/pull/3454
SET client_min_messages TO DEBUG2;
\set VERBOSITY TERSE
CALL test_procedure(100);
CALL test_procedure(100);
CALL test_procedure(100);
CALL test_procedure(100);
CALL test_procedure(100);
CALL test_procedure(100);
CALL test_procedure(100);

CREATE OR REPLACE PROCEDURE coordinator_evaluation.test_procedure_2(int)
 LANGUAGE plpgsql
AS $procedure$
DECLARE filterKey INTEGER;
BEGIN
  filterKey := round(coordinator_evaluation.TEST_RANDOM(1,1)) + $1;
  INSERT INTO coordinator_evaluation_table_2 VALUES (filterKey, filterKey);
END;
$procedure$;

RESET citus.log_remote_commands ;
RESET client_min_messages;

-- these calls would INSERT key = 101, so test if insert succeeded
CALL test_procedure_2(100);
CALL test_procedure_2(100);
CALL test_procedure_2(100);
CALL test_procedure_2(100);
CALL test_procedure_2(100);
CALL test_procedure_2(100);
CALL test_procedure_2(100);
SELECT count(*) FROM coordinator_evaluation_table_2 WHERE key = 101;

CREATE TYPE comptype_int as (int_a int);
CREATE DOMAIN domain_comptype_int AS comptype_int CHECK ((VALUE).int_a > 0);
CREATE TABLE reference_table(column_a coordinator_evaluation.domain_comptype_int);
SELECT create_reference_table('reference_table');

INSERT INTO reference_table (column_a) VALUES ('(1)');
INSERT INTO reference_table (column_a) VALUES ('(2)'), ('(3)');
INSERT INTO reference_table VALUES ('(4)'), ('(5)');

SELECT * FROM reference_table ORDER BY 1;

-- failing UPDATE on a reference table with a subquery in RETURNING clause that needs to be pushed-down.
-- the error message is not great, but at least we no longer see crashes.
CREATE TABLE ref (a int);
SELECT create_reference_table('ref');
UPDATE ref SET a = 1 RETURNING
	(SELECT pg_catalog.max(latest_end_time) FROM pg_catalog.pg_stat_wal_receiver)
        as c3;

SET client_min_messages TO ERROR;
DROP SCHEMA coordinator_evaluation CASCADE;
