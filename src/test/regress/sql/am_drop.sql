--
-- Tests the different DROP commands for cstore_fdw tables.
--
-- DROP TABL
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

SELECT count(*) AS cstore_data_files_before_drop FROM cstore.cstore_data_files \gset

-- DROP cstore_fdw tables
DROP TABLE contestant;
DROP TABLE contestant_compressed;

-- make sure DROP deletes metadata
SELECT :cstore_data_files_before_drop - count(*) FROM cstore.cstore_data_files;

-- Create a cstore_fdw table under a schema and drop it.
CREATE SCHEMA test_schema;
CREATE TABLE test_schema.test_table(data int) USING cstore_tableam;

SELECT count(*) AS cstore_data_files_before_drop FROM cstore.cstore_data_files \gset
DROP SCHEMA test_schema CASCADE;
SELECT :cstore_data_files_before_drop - count(*) FROM cstore.cstore_data_files;

SELECT current_database() datname \gset

CREATE DATABASE db_to_drop;
\c db_to_drop
CREATE EXTENSION citus;
SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database() \gset

CREATE TABLE test_table(data int) USING cstore_tableam;

DROP EXTENSION citus CASCADE;

-- test database drop
CREATE EXTENSION citus;
SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database() \gset

CREATE TABLE test_table(data int) USING cstore_tableam;

\c :datname

DROP DATABASE db_to_drop;
