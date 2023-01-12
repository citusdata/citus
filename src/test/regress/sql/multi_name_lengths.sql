--
-- MULTI_NAME_LENGTHS
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 225000;

SET citus.shard_count TO 2;

-- Verify that a table name > 56 characters gets hashed properly.
CREATE TABLE too_long_12345678901234567890123456789012345678901234567890 (
        col1 integer not null,
        col2 integer not null);
SELECT create_distributed_table('too_long_12345678901234567890123456789012345678901234567890', 'col1');

\c - - :public_worker_1_host :worker_1_port
\dt too_long_*
\c - - :master_host :master_port

SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 2;

-- Verify that the UDF works and rejects bad arguments.
SELECT shard_name(NULL, 666666);
SELECT shard_name(0, 666666);
SELECT shard_name('too_long_12345678901234567890123456789012345678901234567890'::regclass, 666666);
SELECT shard_name('too_long_12345678901234567890123456789012345678901234567890'::regclass, NULL);
SELECT shard_name('too_long_12345678901234567890123456789012345678901234567890'::regclass, -21);

DROP TABLE too_long_12345678901234567890123456789012345678901234567890 CASCADE;


-- Table to use for rename checks.
CREATE TABLE name_lengths (
	col1 integer not null,
	col2 integer not null,
	constraint constraint_a UNIQUE (col1)
	);

SELECT create_distributed_table('name_lengths', 'col1', 'hash');

-- Verify that we CAN add columns with "too-long names", because
-- the columns' names are not extended in the corresponding shard tables.

ALTER TABLE name_lengths ADD COLUMN float_col_12345678901234567890123456789012345678901234567890 FLOAT;
ALTER TABLE name_lengths ADD COLUMN date_col_12345678901234567890123456789012345678901234567890 DATE;
ALTER TABLE name_lengths ADD COLUMN int_col_12345678901234567890123456789012345678901234567890 INTEGER DEFAULT 1;

\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='public.name_lengths_225002'::regclass ORDER BY 1 DESC, 2 DESC;
\c - - :master_host :master_port

-- Placeholders for unsupported add constraints with EXPLICIT names that are too long
ALTER TABLE name_lengths ADD CONSTRAINT nl_unique_12345678901234567890123456789012345678901234567890 UNIQUE (float_col_12345678901234567890123456789012345678901234567890);
ALTER TABLE name_lengths ADD CONSTRAINT nl_exclude_12345678901234567890123456789012345678901234567890 EXCLUDE (int_col_12345678901234567890123456789012345678901234567890 WITH =);
ALTER TABLE name_lengths ADD CONSTRAINT nl_checky_12345678901234567890123456789012345678901234567890 CHECK (date_col_12345678901234567890123456789012345678901234567890 >= '2014-01-01'::date);

\c - - :public_worker_1_host :worker_1_port
SELECT "Constraint", "Definition" FROM table_checks WHERE relid='public.name_lengths_225002'::regclass ORDER BY 1 DESC, 2 DESC;
\c - - :master_host :master_port

-- Rename the table to a too-long name
SET client_min_messages TO DEBUG1;
SET citus.force_max_query_parallelization TO ON;
ALTER TABLE name_lengths RENAME TO name_len_12345678901234567890123456789012345678901234567890;
SELECT * FROM name_len_12345678901234567890123456789012345678901234567890;
ALTER TABLE name_len_12345678901234567890123456789012345678901234567890 RENAME TO name_lengths;
SELECT * FROM name_lengths;

-- Test renames on zero shard distributed tables
CREATE TABLE append_zero_shard_table (a int);
SELECT create_distributed_table('append_zero_shard_table', 'a', 'append');
ALTER TABLE append_zero_shard_table rename TO append_zero_shard_table_12345678901234567890123456789012345678901234567890;


-- Verify that we do not support long renames after parallel queries are executed in transaction block
BEGIN;
ALTER TABLE name_lengths rename col1 to new_column_name;
ALTER TABLE name_lengths RENAME TO name_len_12345678901234567890123456789012345678901234567890;
ROLLBACK;

-- The same operation will work when sequential mode is set
BEGIN;
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
ALTER TABLE name_lengths rename col1 to new_column_name;
ALTER TABLE name_lengths RENAME TO name_len_12345678901234567890123456789012345678901234567890;
ROLLBACK;

RESET client_min_messages;

-- test long partitioned table renames
SET citus.shard_replication_factor TO 1;
CREATE TABLE partition_lengths
(
    tenant_id integer NOT NULL,
    timeperiod timestamp without time zone NOT NULL
) PARTITION BY RANGE (timeperiod);

SELECT create_distributed_table('partition_lengths', 'tenant_id');
CREATE TABLE partition_lengths_p2020_09_28 PARTITION OF partition_lengths FOR VALUES FROM ('2020-09-28 00:00:00') TO ('2020-09-29 00:00:00');

-- verify that we can rename partitioned tables and partitions to too-long names
ALTER TABLE partition_lengths RENAME TO partition_lengths_12345678901234567890123456789012345678901234567890;

-- verify that we can rename partitioned tables and partitions with too-long names
ALTER TABLE partition_lengths_12345678901234567890123456789012345678901234567890 RENAME TO partition_lengths;


-- creating or attaching new partitions with long names
CREATE TABLE partition_lengths_p2020_09_29_12345678901234567890123456789012345678901234567890 (LIKE partition_lengths_p2020_09_28);
ALTER TABLE partition_lengths
    ATTACH PARTITION partition_lengths_p2020_09_29_12345678901234567890123456789012345678901234567890
    FOR VALUES FROM ('2020-09-29 00:00:00') TO ('2020-09-30 00:00:00');
CREATE TABLE partition_lengths_p2020_09_30_12345678901234567890123456789012345678901234567890
    PARTITION OF partition_lengths
    FOR VALUES FROM ('2020-09-30 00:00:00') TO ('2020-10-01 00:00:00');
CREATE TABLE partition_lengths_p2020_10_01_12345678901234567890123456789012345678901234567890
    PARTITION OF partition_lengths
    FOR VALUES FROM ('2020-10-01 00:00:00') TO ('2020-10-02 00:00:00');
DROP TABLE partition_lengths_p2020_09_29_12345678901234567890123456789012345678901234567890;

-- Placeholders for unsupported operations
\set VERBOSITY TERSE

-- renaming distributed table partitions are not supported
ALTER TABLE partition_lengths_p2020_09_28 RENAME TO partition_lengths_p2020_09_28_12345678901234567890123456789012345678901234567890;

-- renaming distributed table constraints are not supported
ALTER TABLE name_lengths RENAME CONSTRAINT unique_12345678901234567890123456789012345678901234567890 TO unique2_12345678901234567890123456789012345678901234567890;

DROP TABLE partition_lengths CASCADE;
\set VERBOSITY DEFAULT

-- Verify that we can create indexes with very long names on zero shard tables.
CREATE INDEX append_zero_shard_table_idx_12345678901234567890123456789012345678901234567890 ON append_zero_shard_table_12345678901234567890123456789012345678901234567890(a);

-- Verify that CREATE INDEX on already distributed table has proper shard names.

CREATE INDEX tmp_idx_12345678901234567890123456789012345678901234567890 ON name_lengths(col2);

\c - - :public_worker_1_host :worker_1_port
SET citus.override_table_visibility TO FALSE;
SELECT "relname", "Column", "Type", "Definition" FROM index_attrs WHERE
    relname SIMILAR TO 'tmp_idx_%\_\d{6}' ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC;
\c - - :master_host :master_port

-- Verify that a new index name > 63 characters is auto-truncated
-- by the parser/rewriter before further processing, just as in Postgres.
CREATE INDEX tmp_idx_123456789012345678901234567890123456789012345678901234567890 ON name_lengths(col2);

-- Verify we can rename indexes with long names
ALTER INDEX tmp_idx_123456789012345678901234567890123456789012345678901234567890 RENAME TO tmp_idx_newname_123456789012345678901234567890123456789012345678901234567890;

\c - - :public_worker_1_host :worker_1_port
SET citus.override_table_visibility TO FALSE;
SELECT "relname", "Column", "Type", "Definition" FROM index_attrs WHERE
    relname SIMILAR TO 'tmp_idx_%\_\d{6}' ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC;
\c - - :master_host :master_port

SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 2;

-- Verify that distributed tables with too-long names
-- for CHECK constraints are no trouble.
CREATE TABLE sneaky_name_lengths (
	col1 integer not null,
        col2 integer not null,
        int_col_12345678901234567890123456789012345678901234567890 integer not null,
        CHECK (int_col_12345678901234567890123456789012345678901234567890 > 100)
        );
SELECT create_distributed_table('sneaky_name_lengths', 'col1', 'hash');
DROP TABLE sneaky_name_lengths CASCADE;

CREATE TABLE sneaky_name_lengths (
        int_col_123456789012345678901234567890123456789012345678901234 integer UNIQUE not null,
        col2 integer not null,
        CONSTRAINT checky_12345678901234567890123456789012345678901234567890 CHECK (int_col_123456789012345678901234567890123456789012345678901234 > 100)
        );

\di public.sneaky_name_lengths*
SELECT "Constraint", "Definition" FROM table_checks WHERE relid='public.sneaky_name_lengths'::regclass ORDER BY 1 DESC, 2 DESC;

SELECT create_distributed_table('sneaky_name_lengths', 'int_col_123456789012345678901234567890123456789012345678901234', 'hash');

\c - - :public_worker_1_host :worker_1_port
SELECT c1.relname AS sneaky_index_name,
       c2.oid AS sneaky_shard_oid
FROM pg_class c1
    JOIN pg_index i ON i.indexrelid = c1.oid
    JOIN pg_class c2 ON i.indrelid = c2.oid
WHERE c1.relname LIKE 'sneaky_name_lengths_int_col_%'
    AND c2.relname LIKE 'sneaky_name_lengths_%'
    AND c1.relkind = 'i'
ORDER BY 1 ASC, 2 ASC
LIMIT 1 \gset

\di :sneaky_index_name
SELECT "Constraint", "Definition" FROM table_checks WHERE relid= :sneaky_shard_oid ORDER BY 1 DESC, 2 DESC;
\c - - :master_host :master_port

SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 2;

DROP TABLE sneaky_name_lengths CASCADE;

-- verify that named constraint with too-long name gets hashed properly
CREATE TABLE sneaky_name_lengths (
	col1 integer not null,
        col2 integer not null,
        int_col_12345678901234567890123456789012345678901234567890 integer not null,
        constraint unique_12345678901234567890123456789012345678901234567890 UNIQUE (col1)
        );
SELECT create_distributed_table('sneaky_name_lengths', 'col1', 'hash');

\c - - :public_worker_1_host :worker_1_port
SET citus.override_table_visibility TO FALSE;

SELECT c1.relname AS unique_index_name
FROM pg_class c1
    JOIN pg_index i ON i.indexrelid = c1.oid
    JOIN pg_class c2 ON i.indrelid = c2.oid
WHERE c1.relname LIKE 'unique_123456789%'
    AND c2.relname LIKE 'sneaky_name_lengths_%'
    AND c1.relkind = 'i'
ORDER BY 1 ASC
LIMIT 1 \gset

\di :unique_index_name
\c - - :master_host :master_port

SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 2;

DROP TABLE sneaky_name_lengths CASCADE;

-- Verify that much larger shardIds are handled properly
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 2250000000000;
CREATE TABLE too_long_12345678901234567890123456789012345678901234567890 (
        col1 integer not null,
        col2 integer not null);
SELECT create_distributed_table('too_long_12345678901234567890123456789012345678901234567890', 'col1', 'hash');

\c - - :public_worker_1_host :worker_1_port
SET citus.override_table_visibility TO FALSE;
\dt *225000000000*
\c - - :master_host :master_port

SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 2;

DROP TABLE too_long_12345678901234567890123456789012345678901234567890 CASCADE;

-- Verify that multi-byte boundaries are respected for databases with UTF8 encoding.
CREATE TABLE U&"elephant_!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D" UESCAPE '!' (
        col1 integer not null PRIMARY KEY,
        col2 integer not null);
SELECT create_distributed_table(U&'elephant_!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D' UESCAPE '!', 'col1', 'hash');

-- Verify that quoting is used in shard_name
SELECT shard_name(U&'elephant_!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D' UESCAPE '!'::regclass, min(shardid))
FROM pg_dist_shard
WHERE logicalrelid = U&'elephant_!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D' UESCAPE '!'::regclass;

\c - - :public_worker_1_host :worker_1_port
SET citus.override_table_visibility TO FALSE;
\dt public.elephant_*[0-9]+
\di public.elephant_*[0-9]+
\c - - :master_host :master_port

SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 2;

-- Verify that shard_name UDF supports schemas
CREATE SCHEMA multi_name_lengths;
CREATE TABLE multi_name_lengths.too_long_12345678901234567890123456789012345678901234567890 (
        col1 integer not null,
        col2 integer not null);
SELECT create_distributed_table('multi_name_lengths.too_long_12345678901234567890123456789012345678901234567890', 'col1', 'hash');

SELECT shard_name('multi_name_lengths.too_long_12345678901234567890123456789012345678901234567890'::regclass, min(shardid))
FROM pg_dist_shard
WHERE logicalrelid = 'multi_name_lengths.too_long_12345678901234567890123456789012345678901234567890'::regclass;


DROP TABLE multi_name_lengths.too_long_12345678901234567890123456789012345678901234567890;

-- Clean up.
DROP TABLE name_lengths CASCADE;
DROP TABLE U&"elephant_!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D" UESCAPE '!' CASCADE;
RESET citus.force_max_query_parallelization;
DROP SCHEMA multi_name_lengths CASCADE;
DROP TABLE append_zero_shard_table_12345678901234567890123456789012345678901234567890;
