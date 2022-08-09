--
-- GRANT_ON_FUNCTION_PROPAGATION
--
CREATE SCHEMA grant_on_function;
SET search_path TO grant_on_function, public;

-- remove one of the worker nodes to test adding a new node later
SET citus.shard_replication_factor TO 1;
SELECT 1 FROM citus_remove_node('localhost', :worker_2_port);

-- create some simple functions
CREATE OR REPLACE FUNCTION function_notice(text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE '%', $1;
END;
$$;

CREATE OR REPLACE FUNCTION function_notice()
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'Constant Notice';
END;
$$;

CREATE OR REPLACE FUNCTION function_hello()
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'Hello World';
END;
$$;

SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE FUNCTION not_distributed_function()
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'not_distributed_function';
END;
$$;
RESET citus.enable_metadata_sync;

-- create some users and grant them permission on grant_on_function schema
CREATE USER function_user_1;
CREATE USER function_user_2;
CREATE USER function_user_3;
GRANT ALL ON SCHEMA grant_on_function TO function_user_1, function_user_2, function_user_3;

-- do some varying grants
GRANT EXECUTE ON FUNCTION function_notice() TO function_user_1;
GRANT EXECUTE ON FUNCTION function_notice() TO function_user_2 WITH GRANT OPTION;
SET ROLE function_user_2;
GRANT EXECUTE ON FUNCTION function_notice() TO function_user_3;
RESET ROLE;

SELECT create_distributed_function('function_notice()');

-- re-distributing the same function with GRANTs should be fine
SELECT create_distributed_function('function_notice()');


-- check grants propagated correctly after create_distributed_function
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'function_notice' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'function_notice' ORDER BY 1, 2;
\c - - - :master_port

-- do some varying revokes
REVOKE EXECUTE ON FUNCTION grant_on_function.function_notice() FROM function_user_1, function_user_3;
REVOKE GRANT OPTION FOR EXECUTE ON FUNCTION grant_on_function.function_notice() FROM function_user_2 CASCADE;

-- check revokes propagated correctly for the distributed function function_notice()
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'function_notice' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'function_notice' ORDER BY 1, 2;
\c - - - :master_port

REVOKE EXECUTE ON FUNCTION grant_on_function.function_notice() FROM function_user_2;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'function_notice' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'function_notice' ORDER BY 1, 2;
\c - - - :master_port

-- distribute another function
SET search_path TO grant_on_function, public;
SELECT create_distributed_function('function_notice(text)');

-- GRANT .. ON ALL FUNCTIONS IN SCHEMA .. with multiple roles
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA grant_on_function TO function_user_1, function_user_3;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'function_notice' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'function_notice' ORDER BY 1, 2;
\c - - - :master_port

-- REVOKE .. ON ALL FUNCTIONS IN SCHEMA .. with multiple roles
REVOKE EXECUTE ON ALL FUNCTIONS IN SCHEMA grant_on_function FROM function_user_1, function_user_3;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'function_notice' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'function_notice' ORDER BY 1, 2;
\c - - - :master_port

-- distribute another function
SET search_path TO grant_on_function, public;
SELECT create_distributed_function('function_hello()');

-- GRANT with multiple functions and multiple roles
-- function_hello needs no arguments since no other function has that name
GRANT EXECUTE ON FUNCTION grant_on_function.function_hello, grant_on_function.function_notice(), grant_on_function.function_notice(text), grant_on_function.not_distributed_function() TO function_user_2 WITH GRANT OPTION;
SET ROLE function_user_2;
GRANT EXECUTE ON FUNCTION grant_on_function.function_hello, grant_on_function.function_notice(), grant_on_function.function_notice(text), grant_on_function.not_distributed_function() TO function_user_1, function_user_3;
RESET ROLE;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('function_notice', 'function_hello', 'not_distributed_function') ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('function_notice', 'function_hello', 'not_distributed_function') ORDER BY 1, 2;
\c - - - :master_port

-- add the previously removed node
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

-- check if the grants are propagated correctly
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('function_notice', 'function_hello') ORDER BY 1, 2;
\c - - - :worker_2_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('function_notice', 'function_hello') ORDER BY 1, 2;
\c - - - :master_port

-- check that it works correctly with a user that is not distributed
CREATE OR REPLACE FUNCTION not_propagated_function_user_test()
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    RAISE NOTICE 'not_propagated_function_user_test';
END;
$$;
SELECT create_distributed_function('not_propagated_function_user_test()');

SET citus.enable_ddl_propagation TO off;
CREATE USER not_propagated_function_user_4;
SET citus.enable_ddl_propagation TO on;

GRANT EXECUTE ON FUNCTION not_propagated_function_user_test TO function_user_1, not_propagated_function_user_4;

-- check if the grants are propagated correctly
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('not_propagated_function_user_test') ORDER BY 1, 2;
\c - - - :worker_2_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('not_propagated_function_user_test') ORDER BY 1, 2;
\c - - - :master_port

SET search_path TO grant_on_function, public;

-- the following should fail is in plain PG
GRANT EXECUTE ON FUNCTION function_notice(), non_existent_function TO function_user_1;
GRANT EXECUTE ON FUNCTION function_notice() TO function_user_1, non_existent_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA grant_on_function, non_existent_schema TO function_user_1;

-- ROUTINE syntax in GRANT should support functions.
CREATE USER function_user_4;
GRANT EXECUTE ON ROUTINE function_notice() TO function_user_4;

-- check if the grants are propagated correctly
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('function_notice') ORDER BY 1, 2;
\c - - - :worker_2_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('function_notice') ORDER BY 1, 2;
\c - - - :master_port

SET search_path TO grant_on_function, public;

DROP FUNCTION function_notice(), function_notice(text), function_hello, not_distributed_function, not_propagated_function_user_test;

-- add similar tests for procedures

-- remove one of the worker nodes to test adding a new node later
SELECT 1 FROM citus_remove_node('localhost', :worker_2_port);

-- create some simple procedures
CREATE OR REPLACE PROCEDURE procedure_notice(text)
LANGUAGE PLPGSQL AS $proc$
BEGIN
  RAISE NOTICE '%', $1;
END;
$proc$;

CREATE OR REPLACE PROCEDURE procedure_notice()
LANGUAGE PLPGSQL AS $proc$
BEGIN
  RAISE NOTICE 'Constant Notice';
END;
$proc$;

CREATE OR REPLACE PROCEDURE procedure_hello()
LANGUAGE PLPGSQL AS $proc$
BEGIN
  RAISE NOTICE 'Hello World';
END;
$proc$;

SET citus.enable_metadata_sync TO OFF;
CREATE OR REPLACE PROCEDURE not_distributed_procedure()
LANGUAGE PLPGSQL AS $proc$
BEGIN
  RAISE NOTICE 'not_distributed_procedure';
END;
$proc$;
RESET citus.enable_metadata_sync;

-- create some users and grant them permission on grant_on_function schema
CREATE USER procedure_user_1;
CREATE USER procedure_user_2;
CREATE USER procedure_user_3;
GRANT ALL ON SCHEMA grant_on_function TO procedure_user_1, procedure_user_2, procedure_user_3;

-- do some varying grants
GRANT EXECUTE ON PROCEDURE procedure_notice() TO procedure_user_1;
GRANT EXECUTE ON PROCEDURE procedure_notice() TO procedure_user_2 WITH GRANT OPTION;
SET ROLE procedure_user_2;
GRANT EXECUTE ON PROCEDURE procedure_notice() TO procedure_user_3;
RESET ROLE;

SELECT create_distributed_function('procedure_notice()');

-- check grants propagated correctly after create_distributed_function
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'procedure_notice' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'procedure_notice' ORDER BY 1, 2;
\c - - - :master_port

-- do some varying revokes
REVOKE EXECUTE ON PROCEDURE grant_on_function.procedure_notice() FROM procedure_user_1, procedure_user_3;
REVOKE GRANT OPTION FOR EXECUTE ON PROCEDURE grant_on_function.procedure_notice() FROM procedure_user_2 CASCADE;

-- check revokes propagated correctly for the distributed procedure procedure_notice()
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'procedure_notice' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'procedure_notice' ORDER BY 1, 2;
\c - - - :master_port

REVOKE EXECUTE ON PROCEDURE grant_on_function.procedure_notice() FROM procedure_user_2;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'procedure_notice' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'procedure_notice' ORDER BY 1, 2;
\c - - - :master_port

-- distribute another procedure
SET search_path TO grant_on_function, public;
SELECT create_distributed_function('procedure_notice(text)');

-- GRANT .. ON ALL PROCEDURES IN SCHEMA .. with multiple roles
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA grant_on_function TO procedure_user_1, procedure_user_3;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'procedure_notice' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'procedure_notice' ORDER BY 1, 2;
\c - - - :master_port

-- REVOKE .. ON ALL PROCEDURES IN SCHEMA .. with multiple roles
REVOKE EXECUTE ON ALL PROCEDURES IN SCHEMA grant_on_function FROM procedure_user_1, procedure_user_3;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'procedure_notice' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'procedure_notice' ORDER BY 1, 2;
\c - - - :master_port

-- distribute another procedure
SET search_path TO grant_on_function, public;
SELECT create_distributed_function('procedure_hello()');

-- GRANT with multiple procedures and multiple roles
-- procedure_hello needs no arguments since no other procedure has that name
GRANT EXECUTE ON PROCEDURE grant_on_function.procedure_hello, grant_on_function.procedure_notice(), grant_on_function.procedure_notice(text), grant_on_function.not_distributed_procedure() TO procedure_user_2 WITH GRANT OPTION;
SET ROLE procedure_user_2;
GRANT EXECUTE ON PROCEDURE grant_on_function.procedure_hello, grant_on_function.procedure_notice(), grant_on_function.procedure_notice(text), grant_on_function.not_distributed_procedure() TO procedure_user_1, procedure_user_3;
RESET ROLE;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('procedure_notice', 'procedure_hello', 'not_distributed_procedure') ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('procedure_notice', 'procedure_hello', 'not_distributed_procedure') ORDER BY 1, 2;
\c - - - :master_port

-- add the previously removed node
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

-- check if the grants are propagated correctly
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('procedure_notice', 'procedure_hello') ORDER BY 1, 2;
\c - - - :worker_2_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('procedure_notice', 'procedure_hello') ORDER BY 1, 2;
\c - - - :master_port

-- check that it works correctly with a user that is not distributed
CREATE OR REPLACE PROCEDURE not_propagated_procedure_user_test()
LANGUAGE PLPGSQL AS $proc$
BEGIN
  RAISE NOTICE 'not_propagated_procedure_user_test';
END;
$proc$;
SELECT create_distributed_function('not_propagated_procedure_user_test()');

SET citus.enable_ddl_propagation TO off;
CREATE USER not_propagated_procedure_user_4;
SET citus.enable_ddl_propagation TO on;

GRANT EXECUTE ON PROCEDURE not_propagated_procedure_user_test TO procedure_user_1, not_propagated_procedure_user_4;

-- check if the grants are propagated correctly
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('not_propagated_procedure_user_test') ORDER BY 1, 2;
\c - - - :worker_2_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('not_propagated_procedure_user_test') ORDER BY 1, 2;
\c - - - :master_port

SET search_path TO grant_on_function, public;

-- the following should fail is in plain PG
GRANT EXECUTE ON PROCEDURE procedure_notice(), non_existent_procedure TO procedure_user_1;
GRANT EXECUTE ON PROCEDURE procedure_notice() TO procedure_user_1, non_existent_user;
GRANT EXECUTE ON ALL PROCEDURES IN SCHEMA grant_on_function, non_existent_schema TO procedure_user_1;

-- ROUTINE syntax in GRANT should support procedures.
CREATE USER procedure_user_4;
GRANT EXECUTE ON ROUTINE procedure_notice() TO procedure_user_4;

-- check if the grants are propagated correctly
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('procedure_notice') ORDER BY 1, 2;
\c - - - :worker_2_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('procedure_notice') ORDER BY 1, 2;
\c - - - :master_port

SET search_path TO grant_on_function, public;

DROP PROCEDURE procedure_notice(), procedure_notice(text), procedure_hello, not_distributed_procedure, not_propagated_procedure_user_test;

-- add similar tests for aggregates

-- remove one of the worker nodes to test adding a new node later
SELECT 1 FROM citus_remove_node('localhost', :worker_2_port);

-- create some simple aggregates
CREATE AGGREGATE dist_float_avg (float8)
(
    sfunc = float8_accum,
    stype = float8[],
    finalfunc = float8_avg,
    initcond = '{0,0,0}'
);

CREATE AGGREGATE dist_float_avg_2 (float8)
(
    sfunc = float8_accum,
    stype = float8[],
    finalfunc = float8_avg,
    initcond = '{0,0,0}'
);

SET citus.enable_metadata_sync TO OFF;
CREATE AGGREGATE no_dist_float_avg (float8)
(
    sfunc = float8_accum,
    stype = float8[],
    finalfunc = float8_avg,
    initcond = '{0,0,0}'
);

RESET citus.enable_metadata_sync;

-- create some users and grant them permission on grant_on_function schema
CREATE USER aggregate_user_1;
CREATE USER aggregate_user_2;
CREATE USER aggregate_user_3;
GRANT ALL ON SCHEMA grant_on_function TO aggregate_user_1, aggregate_user_2, aggregate_user_3;

-- do some varying grants
GRANT EXECUTE ON ROUTINE dist_float_avg(float8) TO aggregate_user_1;
GRANT EXECUTE ON ROUTINE dist_float_avg(float8) TO aggregate_user_2 WITH GRANT OPTION;
SET ROLE aggregate_user_2;
GRANT EXECUTE ON ROUTINE dist_float_avg(float8) TO aggregate_user_3;
RESET ROLE;

SELECT create_distributed_function('dist_float_avg(float8)');

-- re-distributing the same aggregate with GRANTs should be fine
SELECT create_distributed_function('dist_float_avg(float8)');

-- check grants propagated correctly after create_distributed_function
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'dist_float_avg' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'dist_float_avg' ORDER BY 1, 2;
\c - - - :master_port

-- do some varying revokes
REVOKE EXECUTE ON ROUTINE grant_on_function.dist_float_avg(float8) FROM aggregate_user_1, aggregate_user_3;
REVOKE GRANT OPTION FOR EXECUTE ON ROUTINE grant_on_function.dist_float_avg(float8) FROM aggregate_user_2 CASCADE;

-- check revokes propagated correctly for the distributed aggregate dist_float_avg(float8)
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'dist_float_avg' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'dist_float_avg' ORDER BY 1, 2;
\c - - - :master_port

REVOKE EXECUTE ON ROUTINE grant_on_function.dist_float_avg(float8) FROM aggregate_user_2;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'dist_float_avg' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'dist_float_avg' ORDER BY 1, 2;
\c - - - :master_port

-- GRANT .. ON ALL ROUTINES IN SCHEMA .. with multiple roles
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA grant_on_function TO aggregate_user_1, aggregate_user_3;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'dist_float_avg' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'dist_float_avg' ORDER BY 1, 2;
\c - - - :master_port

-- REVOKE .. ON ALL PROCEDURES IN SCHEMA .. with multiple roles
REVOKE EXECUTE ON ALL ROUTINES IN SCHEMA grant_on_function FROM aggregate_user_1, aggregate_user_3;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'dist_float_avg' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname = 'dist_float_avg' ORDER BY 1, 2;
\c - - - :master_port

-- distribute another aggregate
SET search_path TO grant_on_function, public;
SELECT create_distributed_function('dist_float_avg_2(float8)');

-- GRANT with multiple aggregates and multiple roles
GRANT EXECUTE ON ROUTINE grant_on_function.dist_float_avg(float8), grant_on_function.dist_float_avg_2(float8),
grant_on_function.no_dist_float_avg(float8) TO aggregate_user_2 WITH GRANT OPTION;
SET ROLE aggregate_user_2;
GRANT EXECUTE ON ROUTINE grant_on_function.dist_float_avg(float8), grant_on_function.dist_float_avg_2(float8),
grant_on_function.no_dist_float_avg(float8) TO aggregate_user_1, aggregate_user_3;
RESET ROLE;

SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('dist_float_avg', 'dist_float_avg_2', 'no_dist_float_avg') ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('dist_float_avg', 'dist_float_avg_2', 'no_dist_float_avg') ORDER BY 1, 2;
\c - - - :master_port

-- add the previously removed node
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

-- check if the grants are propagated correctly
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('dist_float_avg', 'dist_float_avg_2') ORDER BY 1, 2;
\c - - - :worker_2_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('dist_float_avg', 'dist_float_avg_2') ORDER BY 1, 2;
\c - - - :master_port

-- check that it works correctly with a user that is not distributed
CREATE AGGREGATE not_propagated_aggregate_user_test (float8)
(
    sfunc = float8_accum,
    stype = float8[],
    finalfunc = float8_avg,
    initcond = '{0,0,0}'
);

SET citus.enable_ddl_propagation TO off;
CREATE USER not_propagated_aggregate_user_4;
SET citus.enable_ddl_propagation TO on;

GRANT EXECUTE ON ROUTINE not_propagated_aggregate_user_test TO aggregate_user_1, not_propagated_aggregate_user_4;

-- check if the grants are propagated correctly
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('not_propagated_aggregate_user_test') ORDER BY 1, 2;
\c - - - :worker_2_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('not_propagated_aggregate_user_test') ORDER BY 1, 2;
\c - - - :master_port

SET search_path TO grant_on_function, public;

-- the following should fail is in plain PG
GRANT EXECUTE ON ROUTINE dist_float_avg(float8), non_existent_aggregate TO aggregate_user_1;
GRANT EXECUTE ON ROUTINE dist_float_avg(float8) TO aggregate_user_1, non_existent_user;
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA grant_on_function, non_existent_schema TO aggregate_user_1;

-- ROUTINE syntax in GRANT should support aggregates.
CREATE USER aggregate_user_4;
GRANT EXECUTE ON ROUTINE dist_float_avg(float8) TO aggregate_user_4;

-- check if the grants are propagated correctly
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('dist_float_avg') ORDER BY 1, 2;
\c - - - :worker_2_port
SELECT proname, pronargs, proacl FROM pg_proc WHERE proname IN ('dist_float_avg') ORDER BY 1, 2;
\c - - - :master_port

SET search_path TO grant_on_function, public;

DROP AGGREGATE dist_float_avg(float8), dist_float_avg_2(float8), no_dist_float_avg(float8), not_propagated_aggregate_user_test(float8);

DROP SCHEMA grant_on_function CASCADE;
DROP USER function_user_1, function_user_2, function_user_3, not_propagated_function_user_4;
DROP USER procedure_user_1, procedure_user_2, procedure_user_3, not_propagated_procedure_user_4;
DROP USER aggregate_user_1, aggregate_user_2, aggregate_user_3, not_propagated_aggregate_user_4;
