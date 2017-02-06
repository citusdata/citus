--
-- MULTI_NAME_LENGTHS
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 225000;

SET citus.multi_shard_commit_protocol = '2pc';

-- Verify that a table name > 56 characters gets hashed properly.
CREATE TABLE too_long_12345678901234567890123456789012345678901234567890 (
        col1 integer not null,
        col2 integer not null);
SELECT master_create_distributed_table('too_long_12345678901234567890123456789012345678901234567890', 'col1', 'hash');
SELECT master_create_worker_shards('too_long_12345678901234567890123456789012345678901234567890', '2', '2');

\c - - - :worker_1_port
\d too_long_*
\c - - - :master_port

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

SELECT master_create_distributed_table('name_lengths', 'col1', 'hash');
SELECT master_create_worker_shards('name_lengths', '2', '2');

-- Verify that we CAN add columns with "too-long names", because
-- the columns' names are not extended in the corresponding shard tables.

ALTER TABLE name_lengths ADD COLUMN float_col_12345678901234567890123456789012345678901234567890 FLOAT;
ALTER TABLE name_lengths ADD COLUMN date_col_12345678901234567890123456789012345678901234567890 DATE;
ALTER TABLE name_lengths ADD COLUMN int_col_12345678901234567890123456789012345678901234567890 INTEGER DEFAULT 1;

-- Placeholders for unsupported ALTER TABLE to add constraints with implicit names that are likely too long
ALTER TABLE name_lengths ADD UNIQUE (float_col_12345678901234567890123456789012345678901234567890);
ALTER TABLE name_lengths ADD EXCLUDE (int_col_12345678901234567890123456789012345678901234567890 WITH =);
ALTER TABLE name_lengths ADD CHECK (date_col_12345678901234567890123456789012345678901234567890 > '2014-01-01'::date);

\c - - - :worker_1_port
\d name_lengths_*
\c - - - :master_port

-- Placeholders for unsupported add constraints with EXPLICIT names that are too long
ALTER TABLE name_lengths ADD CONSTRAINT nl_unique_12345678901234567890123456789012345678901234567890 UNIQUE (float_col_12345678901234567890123456789012345678901234567890);
ALTER TABLE name_lengths ADD CONSTRAINT nl_exclude_12345678901234567890123456789012345678901234567890 EXCLUDE (int_col_12345678901234567890123456789012345678901234567890 WITH =);
ALTER TABLE name_lengths ADD CONSTRAINT nl_checky_12345678901234567890123456789012345678901234567890 CHECK (date_col_12345678901234567890123456789012345678901234567890 >= '2014-01-01'::date);

\c - - - :worker_1_port
\d nl_*
\c - - - :master_port

-- Placeholders for RENAME operations
ALTER TABLE name_lengths RENAME TO name_len_12345678901234567890123456789012345678901234567890;
ALTER TABLE name_lengths RENAME CONSTRAINT unique_12345678901234567890123456789012345678901234567890 TO unique2_12345678901234567890123456789012345678901234567890;

-- Verify that CREATE INDEX on already distributed table has proper shard names.

CREATE INDEX tmp_idx_12345678901234567890123456789012345678901234567890 ON name_lengths(col2);

\c - - - :worker_1_port
\d tmp_idx_*
\c - - - :master_port

-- Verify that a new index name > 63 characters is auto-truncated
-- by the parser/rewriter before further processing, just as in Postgres.
CREATE INDEX tmp_idx_123456789012345678901234567890123456789012345678901234567890 ON name_lengths(col2);

\c - - - :worker_1_port
\d tmp_idx_*
\c - - - :master_port

-- Verify that distributed tables with too-long names 
-- for CHECK constraints are no trouble.
CREATE TABLE sneaky_name_lengths (
	col1 integer not null,
        col2 integer not null,
        int_col_12345678901234567890123456789012345678901234567890 integer not null,
        CHECK (int_col_12345678901234567890123456789012345678901234567890 > 100)
        );
SELECT master_create_distributed_table('sneaky_name_lengths', 'col1', 'hash');
SELECT master_create_worker_shards('sneaky_name_lengths', '2', '2');
DROP TABLE sneaky_name_lengths CASCADE;

CREATE TABLE sneaky_name_lengths (
        int_col_123456789012345678901234567890123456789012345678901234 integer UNIQUE not null,
        col2 integer not null,
        CONSTRAINT checky_12345678901234567890123456789012345678901234567890 CHECK (int_col_123456789012345678901234567890123456789012345678901234 > 100)
        );
\d sneaky_name_lengths*

SELECT master_create_distributed_table('sneaky_name_lengths', 'int_col_123456789012345678901234567890123456789012345678901234', 'hash');
SELECT master_create_worker_shards('sneaky_name_lengths', '2', '2');

\c - - - :worker_1_port
\d sneaky_name_lengths*
\c - - - :master_port

DROP TABLE sneaky_name_lengths CASCADE;

-- verify that named constraint with too-long name gets hashed properly
CREATE TABLE sneaky_name_lengths (
	col1 integer not null,
        col2 integer not null,
        int_col_12345678901234567890123456789012345678901234567890 integer not null,
        constraint unique_12345678901234567890123456789012345678901234567890 UNIQUE (col1)
        );
SELECT master_create_distributed_table('sneaky_name_lengths', 'col1', 'hash');
SELECT master_create_worker_shards('sneaky_name_lengths', '2', '2');

\c - - - :worker_1_port
\d sneaky_name_lengths*
\c - - - :master_port

DROP TABLE sneaky_name_lengths CASCADE;

-- Verify that much larger shardIds are handled properly
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 2250000000000;
CREATE TABLE too_long_12345678901234567890123456789012345678901234567890 (
        col1 integer not null,
        col2 integer not null);
SELECT master_create_distributed_table('too_long_12345678901234567890123456789012345678901234567890', 'col1', 'hash');
SELECT master_create_worker_shards('too_long_12345678901234567890123456789012345678901234567890', '2', '2');

\c - - - :worker_1_port
\d too_long_*
\c - - - :master_port

DROP TABLE too_long_12345678901234567890123456789012345678901234567890 CASCADE;

-- Verify that multi-byte boundaries are respected for databases with UTF8 encoding.
CREATE TABLE U&"elephant_!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D" UESCAPE '!' (
        col1 integer not null PRIMARY KEY,
        col2 integer not null);
SELECT master_create_distributed_table(U&'elephant_!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D' UESCAPE '!', 'col1', 'hash');
SELECT master_create_worker_shards(U&'elephant_!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D' UESCAPE '!', '2', '2');

\c - - - :worker_1_port
\d elephant_*
\c - - - :master_port

-- Clean up.
DROP TABLE name_lengths CASCADE;
DROP TABLE U&"elephant_!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D!0441!043B!043E!043D" UESCAPE '!' CASCADE;
