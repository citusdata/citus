--
-- PG15
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 15 AS server_version_ge_15
\gset
\if :server_version_ge_15
\else
\q
\endif

CREATE SCHEMA pg15;
SET search_path TO pg15;
SET citus.next_shard_id TO 960000;
SET citus.shard_count TO 4;

--
-- In PG15, there is an added option to use ICU as global locale provider.
-- pg_collation has three locale-related fields: collcollate and collctype,
-- which are libc-related fields, and a new one colliculocale, which is the
-- ICU-related field. Only the libc-related fields or the ICU-related field
-- is set, never both.
-- Relevant PG commits:
-- f2553d43060edb210b36c63187d52a632448e1d2
-- 54637508f87bd5f07fb9406bac6b08240283be3b
--

-- fail, needs "locale"
CREATE COLLATION german_phonebook_test (provider = icu, lc_collate = 'de-u-co-phonebk');

-- fail, needs "locale"
CREATE COLLATION german_phonebook_test (provider = icu, lc_collate = 'de-u-co-phonebk', lc_ctype = 'de-u-co-phonebk');

-- works
CREATE COLLATION german_phonebook_test (provider = icu, locale = 'de-u-co-phonebk');

-- with icu provider, colliculocale will be set, collcollate and collctype will be null
SELECT result FROM run_command_on_all_nodes('
    SELECT collcollate FROM pg_collation WHERE collname = ''german_phonebook_test'';
');
SELECT result FROM run_command_on_all_nodes('
    SELECT collctype FROM pg_collation WHERE collname = ''german_phonebook_test'';
');
SELECT result FROM run_command_on_all_nodes('
    SELECT colliculocale FROM pg_collation WHERE collname = ''german_phonebook_test'';
');

-- with non-icu provider, colliculocale will be null, collcollate and collctype will be set
CREATE COLLATION default_provider (provider = libc, lc_collate = "POSIX", lc_ctype = "POSIX");

SELECT result FROM run_command_on_all_nodes('
    SELECT collcollate FROM pg_collation WHERE collname = ''default_provider'';
');
SELECT result FROM run_command_on_all_nodes('
    SELECT collctype FROM pg_collation WHERE collname = ''default_provider'';
');
SELECT result FROM run_command_on_all_nodes('
    SELECT colliculocale FROM pg_collation WHERE collname = ''default_provider'';
');

--
-- In PG15, Renaming triggers on partitioned tables had two problems
-- recurses to renaming the triggers on the partitions as well.
-- Here we test that distributed triggers behave the same way.
-- Relevant PG commit:
-- 80ba4bb383538a2ee846fece6a7b8da9518b6866
--

SET citus.enable_unsafe_triggers TO true;

CREATE TABLE sale(
    sale_date date not null,
    state_code text,
    product_sku text,
    units integer)
    PARTITION BY list (state_code);

ALTER TABLE sale ADD CONSTRAINT sale_pk PRIMARY KEY (state_code, sale_date);

CREATE TABLE sale_newyork PARTITION OF sale FOR VALUES IN ('NY');
CREATE TABLE sale_california PARTITION OF sale FOR VALUES IN ('CA');

CREATE TABLE record_sale(
    operation_type text not null,
    product_sku text,
    state_code text,
    units integer,
    PRIMARY KEY(state_code, product_sku, operation_type, units));

SELECT create_distributed_table('sale', 'state_code');
SELECT create_distributed_table('record_sale', 'state_code', colocate_with := 'sale');

CREATE OR REPLACE FUNCTION record_sale()
RETURNS trigger
AS $$
BEGIN
    INSERT INTO pg15.record_sale(operation_type, product_sku, state_code, units)
    VALUES (TG_OP, NEW.product_sku, NEW.state_code, NEW.units);
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER record_sale_trigger
AFTER INSERT OR UPDATE OR DELETE ON sale
FOR EACH ROW EXECUTE FUNCTION pg15.record_sale();

CREATE VIEW sale_triggers AS
    SELECT tgname, tgrelid::regclass, tgenabled
    FROM pg_trigger
    WHERE tgrelid::regclass::text like 'sale%'
    ORDER BY 1, 2;

SELECT * FROM sale_triggers ORDER BY 1, 2;
ALTER TRIGGER "record_sale_trigger" ON "pg15"."sale" RENAME TO "new_record_sale_trigger";
SELECT * FROM sale_triggers ORDER BY 1, 2;

-- test that we can't rename a distributed clone trigger
ALTER TRIGGER "new_record_sale_trigger" ON "pg15"."sale_newyork" RENAME TO "another_trigger_name";

--
-- In PG15, For GENERATED columns, all dependencies of the generation
-- expression are recorded as NORMAL dependencies of the column itself.
-- This requires CASCADE to drop generated cols with the original col.
-- Test this behavior in distributed table, specifically with
-- undistribute_table within a transaction.
-- Relevant PG Commit: cb02fcb4c95bae08adaca1202c2081cfc81a28b5
--

CREATE TABLE generated_stored_ref (
  col_1 int,
  col_2 int,
  col_3 int generated always as (col_1+col_2) stored,
  col_4 int,
  col_5 int generated always as (col_4*2-col_1) stored
);

SELECT create_reference_table ('generated_stored_ref');

-- populate the table
INSERT INTO generated_stored_ref (col_1, col_4) VALUES (1,2), (11,12);
INSERT INTO generated_stored_ref (col_1, col_2, col_4) VALUES (100,101,102), (200,201,202);
SELECT * FROM generated_stored_ref ORDER BY 1,2,3,4,5;

-- fails, CASCADE must be specified
-- will test CASCADE inside the transcation
ALTER TABLE generated_stored_ref DROP COLUMN col_1;

BEGIN;
  -- drops col_1, col_3, col_5
  ALTER TABLE generated_stored_ref DROP COLUMN col_1 CASCADE;
  ALTER TABLE generated_stored_ref DROP COLUMN col_4;

  -- show that undistribute_table works fine
  SELECT undistribute_table('generated_stored_ref');
  INSERT INTO generated_stored_ref VALUES (5);
  SELECT * FROM generated_stored_REF ORDER BY 1;
ROLLBACK;

SELECT undistribute_table('generated_stored_ref');

--
-- In PG15, there is a new command called MERGE
-- It is currently not supported for Citus tables
-- Test the behavior with various commands with Citus table types
-- Relevant PG Commit: 7103ebb7aae8ab8076b7e85f335ceb8fe799097c
--

CREATE TABLE tbl1
(
   x INT
);

CREATE TABLE tbl2
(
    x INT
);

-- on local tables works fine
MERGE INTO tbl1 USING tbl2 ON (true)
WHEN MATCHED THEN DELETE;

-- add coordinator node as a worker
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

-- one table is Citus local table, fails
SELECT citus_add_local_table_to_metadata('tbl1');

MERGE INTO tbl1 USING tbl2 ON (true)
WHEN MATCHED THEN DELETE;

SELECT undistribute_table('tbl1');

-- the other table is Citus local table, fails
SELECT citus_add_local_table_to_metadata('tbl2');

MERGE INTO tbl1 USING tbl2 ON (true)
WHEN MATCHED THEN DELETE;

-- one table is reference, the other local, not supported
SELECT create_reference_table('tbl2');

MERGE INTO tbl1 USING tbl2 ON (true)
WHEN MATCHED THEN DELETE;

-- now, both are reference, still not supported
SELECT create_reference_table('tbl1');

MERGE INTO tbl1 USING tbl2 ON (true)
WHEN MATCHED THEN DELETE;

-- now, both distributed, not works
SELECT undistribute_table('tbl1');
SELECT undistribute_table('tbl2');
SELECT 1 FROM citus_remove_node('localhost', :master_port);

SELECT create_distributed_table('tbl1', 'x');
SELECT create_distributed_table('tbl2', 'x');

MERGE INTO tbl1 USING tbl2 ON (true)
WHEN MATCHED THEN DELETE;

-- also, not inside subqueries & ctes
WITH targq AS (
    SELECT * FROM tbl2
)
MERGE INTO tbl1 USING targq ON (true)
WHEN MATCHED THEN DELETE;

-- crashes on beta3, fixed on 15 stable
--WITH foo AS (
--  MERGE INTO tbl1 USING tbl2 ON (true)
--  WHEN MATCHED THEN DELETE
--) SELECT * FROM foo;

--COPY (
--  MERGE INTO tbl1 USING tbl2 ON (true)
--  WHEN MATCHED THEN DELETE
--) TO stdout;

MERGE INTO tbl1 t
USING tbl2
ON (true)
WHEN MATCHED THEN
    UPDATE SET x = (SELECT count(*) FROM tbl2);

-- test numeric types with negative scale
CREATE TABLE numeric_negative_scale(numeric_column numeric(3,-1), orig_value int);
INSERT into numeric_negative_scale SELECT x,x FROM generate_series(111, 115) x;
-- verify that we can not distribute by a column that has numeric type with negative scale
SELECT create_distributed_table('numeric_negative_scale','numeric_column');
-- However, we can distribute by other columns
SELECT create_distributed_table('numeric_negative_scale','orig_value');

SELECT * FROM numeric_negative_scale ORDER BY 1,2;

-- Clean up
DROP SCHEMA pg15 CASCADE;
