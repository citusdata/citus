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
-- Verify that we can not change the distribution column to the numeric column
SELECT alter_distributed_table('numeric_negative_scale',
                                distribution_column := 'numeric_column');

SELECT * FROM numeric_negative_scale ORDER BY 1,2;

-- verify that numeric types with scale greater than precision are also ok
-- a precision of 2, and scale of 3 means that all the numbers should be less than 10^-1 and of the form 0,0XY
CREATE TABLE numeric_scale_gt_precision(numeric_column numeric(2,3));
SELECT * FROM create_distributed_table('numeric_scale_gt_precision','numeric_column');
INSERT INTO numeric_scale_gt_precision SELECT x FROM generate_series(0.01234, 0.09, 0.005) x;

-- verify that we store only 2 digits, and discard the rest of them.
SELECT * FROM numeric_scale_gt_precision ORDER BY 1;
-- verify we can route queries to the right shards
SELECT * FROM numeric_scale_gt_precision WHERE numeric_column=0.027;

-- test new regex functions
-- print order comments that contain the word `fluffily` at least twice
SELECT o_comment FROM public.orders WHERE regexp_count(o_comment, 'FluFFily', 1, 'i')>=2 ORDER BY 1;
-- print the same items using a different regexp function
SELECT o_comment FROM public.orders WHERE regexp_like(o_comment, 'fluffily.*fluffily') ORDER BY 1;
-- print the position where we find the second fluffily in the comment
SELECT o_comment, regexp_instr(o_comment, 'fluffily.*(fluffily)') FROM public.orders ORDER BY 2 desc LIMIT 5;
-- print the substrings between two `fluffily`
SELECT regexp_substr(o_comment, 'fluffily.*fluffily') FROM public.orders ORDER BY 1 LIMIT 5;
-- replace second `fluffily` with `silkily`
SELECT regexp_replace(o_comment, 'fluffily', 'silkily', 1, 2) FROM public.orders WHERE regexp_like(o_comment, 'fluffily.*fluffily') ORDER BY 1 desc;

-- test new COPY features
-- COPY TO statements with text format and headers
CREATE TABLE copy_test(id int, data int);
SELECT create_distributed_table('copy_test', 'id');
INSERT INTO copy_test SELECT x, x FROM generate_series(1,100) x;
COPY copy_test TO :'temp_dir''copy_test.txt' WITH ( HEADER true, FORMAT text);

-- Create another distributed table with different column names and test COPY FROM with header match
CREATE TABLE copy_test2(id int, data_ int);
SELECT create_distributed_table('copy_test2', 'id');
COPY copy_test2 FROM :'temp_dir''copy_test.txt' WITH ( HEADER match, FORMAT text);

-- verify that the command works if we rename the column
ALTER TABLE copy_test2 RENAME COLUMN data_ TO data;
COPY copy_test2 FROM :'temp_dir''copy_test.txt' WITH ( HEADER match, FORMAT text);
SELECT count(*)=100 FROM copy_test2;

--
-- In PG15, unlogged sequences are supported
-- we support this for distributed sequences as well
--

CREATE SEQUENCE seq1;
CREATE UNLOGGED SEQUENCE "pg15"."seq 2";

-- first, test that sequence persistence is distributed correctly
-- when the sequence is distributed

SELECT relname,
       CASE relpersistence
            WHEN 'u' THEN 'unlogged'
            WHEN 'p' then 'logged'
            ELSE 'unknown'
        END AS logged_info
FROM pg_class
WHERE relname IN ('seq1', 'seq 2') AND relnamespace='pg15'::regnamespace
ORDER BY relname;

CREATE TABLE "seq test"(a int, b int default nextval ('seq1'), c int default nextval ('"pg15"."seq 2"'));

SELECT create_distributed_table('"pg15"."seq test"','a');

\c - - - :worker_1_port
SELECT relname,
       CASE relpersistence
            WHEN 'u' THEN 'unlogged'
            WHEN 'p' then 'logged'
            ELSE 'unknown'
        END AS logged_info
FROM pg_class
WHERE relname IN ('seq1', 'seq 2') AND relnamespace='pg15'::regnamespace
ORDER BY relname;

\c - - - :master_port
SET search_path TO pg15;

-- now, check that we can change sequence persistence using ALTER SEQUENCE

ALTER SEQUENCE seq1 SET UNLOGGED;
-- use IF EXISTS
ALTER SEQUENCE IF EXISTS "seq 2" SET LOGGED;
-- check non-existent sequence as well
ALTER SEQUENCE seq_non_exists SET LOGGED;
ALTER SEQUENCE IF EXISTS seq_non_exists SET LOGGED;

SELECT relname,
       CASE relpersistence
            WHEN 'u' THEN 'unlogged'
            WHEN 'p' then 'logged'
            ELSE 'unknown'
        END AS logged_info
FROM pg_class
WHERE relname IN ('seq1', 'seq 2') AND relnamespace='pg15'::regnamespace
ORDER BY relname;

\c - - - :worker_1_port
SELECT relname,
       CASE relpersistence
            WHEN 'u' THEN 'unlogged'
            WHEN 'p' then 'logged'
            ELSE 'unknown'
        END AS logged_info
FROM pg_class
WHERE relname IN ('seq1', 'seq 2') AND relnamespace='pg15'::regnamespace
ORDER BY relname;

\c - - - :master_port
SET search_path TO pg15;

-- now, check that we can change sequence persistence using ALTER TABLE
ALTER TABLE seq1 SET LOGGED;
ALTER TABLE "seq 2" SET UNLOGGED;

SELECT relname,
       CASE relpersistence
            WHEN 'u' THEN 'unlogged'
            WHEN 'p' then 'logged'
            ELSE 'unknown'
        END AS logged_info
FROM pg_class
WHERE relname IN ('seq1', 'seq 2') AND relnamespace='pg15'::regnamespace
ORDER BY relname;

\c - - - :worker_1_port
SELECT relname,
       CASE relpersistence
            WHEN 'u' THEN 'unlogged'
            WHEN 'p' then 'logged'
            ELSE 'unknown'
        END AS logged_info
FROM pg_class
WHERE relname IN ('seq1', 'seq 2') AND relnamespace='pg15'::regnamespace
ORDER BY relname;

\c - - - :master_port
SET search_path TO pg15;

-- An identity/serial sequence now automatically gets and follows the
-- persistence level (logged/unlogged) of its owning table.
-- Test this behavior as well

CREATE UNLOGGED TABLE test(a bigserial, b bigserial);
SELECT create_distributed_table('test', 'a');

-- show that associated sequence is unlooged
SELECT relname,
       CASE relpersistence
            WHEN 'u' THEN 'unlogged'
            WHEN 'p' then 'logged'
            ELSE 'unknown'
        END AS logged_info
FROM pg_class
WHERE relname IN ('test_a_seq', 'test_b_seq') AND relnamespace='pg15'::regnamespace
ORDER BY relname;

\c - - - :worker_1_port
SELECT relname,
       CASE relpersistence
            WHEN 'u' THEN 'unlogged'
            WHEN 'p' then 'logged'
            ELSE 'unknown'
        END AS logged_info
FROM pg_class
WHERE relname IN ('test_a_seq', 'test_b_seq') AND relnamespace='pg15'::regnamespace
ORDER BY relname;

\c - - - :master_port
SET search_path TO pg15;

-- allow foreign key columns to have SET NULL/DEFAULT on column basis
-- currently only reference tables can support that
CREATE TABLE PKTABLE (tid int, id int, PRIMARY KEY (tid, id));
CREATE TABLE FKTABLE (
  tid int, id int,
  fk_id_del_set_null int,
  fk_id_del_set_default int DEFAULT 0,
  FOREIGN KEY (tid, fk_id_del_set_null) REFERENCES PKTABLE ON DELETE SET NULL (fk_id_del_set_null),
  FOREIGN KEY (tid, fk_id_del_set_default) REFERENCES PKTABLE ON DELETE SET DEFAULT (fk_id_del_set_default)
);

SELECT create_reference_table('PKTABLE');

-- ok, Citus could relax this constraint in the future
SELECT create_distributed_table('FKTABLE', 'tid');

-- with reference tables it should all work fine
SELECT create_reference_table('FKTABLE');

-- show that the definition is expected
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'fktable'::regclass::oid ORDER BY oid;

\c - - - :worker_1_port

SET search_path TO pg15;

-- show that the definition is expected on the worker as well
SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'fktable'::regclass::oid ORDER BY oid;

-- also, make sure that it works as expected
INSERT INTO PKTABLE VALUES (1, 0), (1, 1), (1, 2);
INSERT INTO FKTABLE VALUES
  (1, 1, 1, NULL),
  (1, 2, NULL, 2);
DELETE FROM PKTABLE WHERE id = 1 OR id = 2;
SELECT * FROM FKTABLE ORDER BY id;

\c - - - :master_port
SET search_path TO pg15;

-- test NULL NOT DISTINCT clauses
-- set the next shard id so that the error messages are easier to maintain
SET citus.next_shard_id TO 960050;
CREATE TABLE null_distinct_test(id INT, c1 INT, c2 INT, c3 VARCHAR(10)) ;
SELECT create_distributed_table('null_distinct_test', 'id');

CREATE UNIQUE INDEX idx1_null_distinct_test ON null_distinct_test(id, c1) NULLS DISTINCT ;
CREATE UNIQUE INDEX idx2_null_distinct_test ON null_distinct_test(id, c2) NULLS NOT DISTINCT ;

-- populate with some initial data
INSERT INTO null_distinct_test VALUES (1, 1, 1, 'data1') ;
INSERT INTO null_distinct_test VALUES (1, 2, NULL, 'data2') ;
INSERT INTO null_distinct_test VALUES (1, NULL, 3, 'data3') ;

-- should fail as we already have a null value in c2 column
INSERT INTO null_distinct_test VALUES (1, NULL, NULL, 'data4') ;
INSERT INTO null_distinct_test VALUES (1, NULL, NULL, 'data4') ON CONFLICT DO NOTHING;
INSERT INTO null_distinct_test VALUES (1, NULL, NULL, 'data4') ON CONFLICT (id, c2) DO UPDATE SET c2=100 RETURNING *;

-- should not fail as null values are distinct for c1 column
INSERT INTO null_distinct_test VALUES (1, NULL, 5, 'data5') ;

-- test that unique constraints also work properly
-- since we have multiple (1,NULL) pairs for columns (id,c1) the first will work, second will fail
ALTER TABLE null_distinct_test ADD CONSTRAINT uniq_distinct_c1 UNIQUE NULLS DISTINCT (id,c1);
ALTER TABLE null_distinct_test ADD CONSTRAINT uniq_c1 UNIQUE NULLS NOT DISTINCT (id,c1);

-- show all records in the table for fact checking
SELECT * FROM null_distinct_test ORDER BY c3;

-- test unique nulls not distinct constraints on a reference table
CREATE TABLE reference_uniq_test (
    x int, y int,
    UNIQUE NULLS NOT DISTINCT (x, y)
);
SELECT create_reference_table('reference_uniq_test');
INSERT INTO reference_uniq_test VALUES (1, 1), (1, NULL), (NULL, 1);
-- the following will fail
INSERT INTO reference_uniq_test VALUES (1, NULL);

--
-- PG15 introduces CLUSTER command support for partitioned tables. However, similar to
-- CLUSTER commands with no table name, these queries can not be run inside a transaction
-- block. Therefore, we do not propagate such queries.
--

-- Should print a warning that it will not be propagated to worker nodes.
CLUSTER sale USING sale_pk;

-- verify that we can cluster the partition tables only when replication factor is 1
CLUSTER sale_newyork USING sale_newyork_pkey;

-- create a new partitioned table with shard replicaiton factor 1
SET citus.shard_replication_factor = 1;
CREATE TABLE sale_repl_factor_1 ( LIKE sale )
    PARTITION BY list (state_code);

ALTER TABLE sale_repl_factor_1 ADD CONSTRAINT sale_repl_factor_1_pk PRIMARY KEY (state_code, sale_date);

CREATE TABLE sale_newyork_repl_factor_1 PARTITION OF sale_repl_factor_1 FOR VALUES IN ('NY');
CREATE TABLE sale_california_repl_factor_1 PARTITION OF sale_repl_factor_1 FOR VALUES IN ('CA');

SELECT create_distributed_table('sale_repl_factor_1', 'state_code');

-- Should print a warning that it will not be propagated to worker nodes.
CLUSTER sale_repl_factor_1 USING sale_repl_factor_1_pk;

-- verify that we can still cluster the partition tables now since replication factor is 1
CLUSTER sale_newyork_repl_factor_1 USING sale_newyork_repl_factor_1_pkey;

-- Clean up
RESET citus.shard_replication_factor;
\set VERBOSITY terse
SET client_min_messages TO ERROR;
DROP SCHEMA pg15 CASCADE;
