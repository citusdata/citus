--
-- Tests the different DROP commands for columnar tables.
--
-- DROP TABL
-- DROP SCHEMA
-- DROP EXTENSION
-- DROP DATABASE
--

-- Note that travis does not create
-- citus extension in default database (postgres). This has caused
-- different behavior between travis tests and local tests. Thus
-- 'postgres' directory is excluded from comparison to have the same result.

-- store postgres database oid
SELECT oid postgres_oid FROM pg_database WHERE datname = 'postgres' \gset

SELECT count(distinct storage_id) AS columnar_stripes_before_drop FROM columnar.stripe \gset

-- DROP columnar tables
DROP TABLE contestant;
DROP TABLE contestant_compressed;

-- make sure DROP deletes metadata
SELECT :columnar_stripes_before_drop - count(distinct storage_id) FROM columnar.stripe;

-- Create a columnar table under a schema and drop it.
CREATE SCHEMA test_schema;
CREATE TABLE test_schema.test_table(data int) USING columnar;
INSERT INTO test_schema.test_table VALUES (1);

SELECT count(*) AS columnar_stripes_before_drop FROM columnar.stripe \gset
DROP SCHEMA test_schema CASCADE;
SELECT :columnar_stripes_before_drop - count(distinct storage_id) FROM columnar.stripe;

SELECT current_database() datname \gset

CREATE DATABASE db_to_drop;
\c db_to_drop
CREATE EXTENSION citus_columnar;
SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database() \gset

CREATE TABLE test_table(data int) USING columnar;

DROP EXTENSION citus_columnar CASCADE;

-- test database drop
CREATE EXTENSION citus_columnar;
SELECT oid::text databaseoid FROM pg_database WHERE datname = current_database() \gset

CREATE TABLE test_table(data int) USING columnar;

\c :datname

DROP DATABASE db_to_drop;
