--
-- MULTI_UTILITY_WARNINGS
--

-- Tests to check if we inform the user about potential caveats of creating new
-- databases, schemas, and roles.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1010000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1010000;


CREATE DATABASE new_database;

CREATE SCHEMA new_schema;

CREATE ROLE new_role;

CREATE USER new_user;
