--
-- Tests the different DROP commands for cstore_fdw tables.
--
-- DROP FOREIGN TABL
-- DROP SCHEMA
-- DROP EXTENSION
-- DROP DATABASE
--

-- Note that travis does not create
-- cstore_fdw extension in default database (postgres). This has caused
-- different behavior between travis tests and local tests. Thus
-- 'postgres' directory is excluded from comparison to have the same result.

-- store postgres database oid
SELECT oid postgres_oid FROM pg_database WHERE datname = 'postgres' \gset

-- Check that files for the automatically managed table exist in the
-- cstore_fdw/{databaseoid} directory.
SELECT count(*) FROM (
	SELECT pg_ls_dir('cstore_fdw/' || databaseoid ) FROM (
	SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database()
	) AS q1) AS q2;

-- DROP cstore_fdw tables
DROP FOREIGN TABLE contestant;
DROP FOREIGN TABLE contestant_compressed;

-- Create a cstore_fdw table under a schema and drop it.
CREATE SCHEMA test_schema;
CREATE FOREIGN TABLE test_schema.test_table(data int) SERVER cstore_server;
DROP SCHEMA test_schema CASCADE;

-- Check that the files have been deleted and the directory is empty after the
-- DROP table command.
SELECT count(*) FROM (
	SELECT pg_ls_dir('cstore_fdw/' || databaseoid ) FROM (
	SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database()
	) AS q1) AS q2;

SELECT current_database() datname \gset

CREATE DATABASE db_to_drop;
\c db_to_drop
CREATE EXTENSION cstore_fdw;
CREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;
SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database() \gset

CREATE FOREIGN TABLE test_table(data int) SERVER cstore_server;
-- should see 2 files, data and footer file for single table
SELECT count(*) FROM pg_ls_dir('cstore_fdw/' || :databaseoid);

-- should see 2 directories 1 for each database, excluding postgres database
SELECT count(*) FROM pg_ls_dir('cstore_fdw') WHERE pg_ls_dir != :postgres_oid::text;

DROP EXTENSION cstore_fdw CASCADE;

-- should only see 1 directory here
SELECT count(*) FROM pg_ls_dir('cstore_fdw') WHERE pg_ls_dir != :postgres_oid::text;

-- test database drop
CREATE EXTENSION cstore_fdw;
CREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;
SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database() \gset

CREATE FOREIGN TABLE test_table(data int) SERVER cstore_server;

-- should see 2 directories 1 for each database
SELECT count(*) FROM pg_ls_dir('cstore_fdw') WHERE pg_ls_dir != :postgres_oid::text;

\c :datname

DROP DATABASE db_to_drop;

-- should only see 1 directory for the default database
SELECT count(*) FROM pg_ls_dir('cstore_fdw') WHERE pg_ls_dir != :postgres_oid::text;
