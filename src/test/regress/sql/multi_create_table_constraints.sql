--
-- MULTI_CREATE_TABLE_CONSTRAINTS
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 365000;

--  test that Citus forbids unique and EXCLUDE constraints on append-partitioned tables.

CREATE TABLE uniq_cns_append_tables
(
	partition_col integer UNIQUE,
	other_col integer
);
SELECT master_create_distributed_table('uniq_cns_append_tables', 'partition_col', 'append');

CREATE TABLE excl_cns_append_tables
(
	partition_col integer,
	other_col integer,
        EXCLUDE (partition_col WITH =)
);
SELECT master_create_distributed_table('excl_cns_append_tables', 'partition_col', 'append');

-- test that Citus cannot distribute unique constraints that do not include
-- the partition column on hash-partitioned tables.

CREATE TABLE pk_on_non_part_col
(
	partition_col integer,
	other_col integer PRIMARY KEY
);
SELECT master_create_distributed_table('pk_on_non_part_col', 'partition_col', 'hash');

CREATE TABLE uq_on_non_part_col
(
	partition_col integer,
	other_col integer UNIQUE
);
SELECT master_create_distributed_table('uq_on_non_part_col', 'partition_col', 'hash');

CREATE TABLE ex_on_non_part_col
(
	partition_col integer,
	other_col integer,
	EXCLUDE (other_col WITH =)
);
SELECT master_create_distributed_table('ex_on_non_part_col', 'partition_col', 'hash');

-- now show that Citus can distribute unique and EXCLUDE constraints that 
-- include the partition column for hash-partitioned tables.
-- However, EXCLUDE constraints must include the partition column with
-- an equality operator.
-- These tests are for UNNAMED constraints.

CREATE TABLE pk_on_part_col
(
	partition_col integer PRIMARY KEY,
	other_col integer
);
SELECT master_create_distributed_table('pk_on_part_col', 'partition_col', 'hash');

CREATE TABLE uq_part_col
(
	partition_col integer UNIQUE,
	other_col integer
);
SELECT master_create_distributed_table('uq_part_col', 'partition_col', 'hash');

CREATE TABLE uq_two_columns
(
	partition_col integer,
	other_col integer,
	UNIQUE (partition_col, other_col)
);
SELECT master_create_distributed_table('uq_two_columns', 'partition_col', 'hash');
SELECT master_create_worker_shards('uq_two_columns', '4', '2');
INSERT INTO uq_two_columns (partition_col, other_col) VALUES (1,1);
INSERT INTO uq_two_columns (partition_col, other_col) VALUES (1,1);

CREATE TABLE ex_on_part_col
(
	partition_col integer,
	other_col integer,
	EXCLUDE (partition_col WITH =)
);
SELECT master_create_distributed_table('ex_on_part_col', 'partition_col', 'hash');
SELECT master_create_worker_shards('ex_on_part_col', '4', '2');
INSERT INTO ex_on_part_col (partition_col, other_col) VALUES (1,1);
INSERT INTO ex_on_part_col (partition_col, other_col) VALUES (1,2);

CREATE TABLE ex_on_two_columns
(
	partition_col integer,
	other_col integer,
	EXCLUDE (partition_col WITH =, other_col WITH =)
);
SELECT master_create_distributed_table('ex_on_two_columns', 'partition_col', 'hash');
SELECT master_create_worker_shards('ex_on_two_columns', '4', '2');
INSERT INTO ex_on_two_columns (partition_col, other_col) VALUES (1,1);
INSERT INTO ex_on_two_columns (partition_col, other_col) VALUES (1,1);

CREATE TABLE ex_on_two_columns_prt
(
	partition_col integer,
	other_col integer,
	EXCLUDE (partition_col WITH =, other_col WITH =) WHERE (other_col > 100)
);
SELECT master_create_distributed_table('ex_on_two_columns_prt', 'partition_col', 'hash');
SELECT master_create_worker_shards('ex_on_two_columns_prt', '4', '2');
INSERT INTO ex_on_two_columns_prt (partition_col, other_col) VALUES (1,1);
INSERT INTO ex_on_two_columns_prt (partition_col, other_col) VALUES (1,1);
INSERT INTO ex_on_two_columns_prt (partition_col, other_col) VALUES (1,101);
INSERT INTO ex_on_two_columns_prt (partition_col, other_col) VALUES (1,101);

CREATE TABLE ex_wrong_operator
(
	partition_col tsrange,
	other_col tsrange,
	EXCLUDE USING gist (other_col WITH =, partition_col WITH &&)
);
SELECT master_create_distributed_table('ex_wrong_operator', 'partition_col', 'hash');

CREATE TABLE ex_overlaps
(
	partition_col tsrange,
	other_col tsrange,
	EXCLUDE USING gist (other_col WITH &&, partition_col WITH =)
);
SELECT master_create_distributed_table('ex_overlaps', 'partition_col', 'hash');
SELECT master_create_worker_shards('ex_overlaps', '4', '2');
INSERT INTO ex_overlaps (partition_col, other_col) VALUES ('[2016-01-01 00:00:00, 2016-02-01 00:00:00]', '[2016-01-01 00:00:00, 2016-02-01 00:00:00]');
INSERT INTO ex_overlaps (partition_col, other_col) VALUES ('[2016-01-01 00:00:00, 2016-02-01 00:00:00]', '[2016-01-15 00:00:00, 2016-02-01 00:00:00]');

-- now show that Citus can distribute unique and EXCLUDE constraints that 
-- include the partition column, for hash-partitioned tables. 
-- However, EXCLUDE constraints must include the partition column with
-- an equality operator.
-- These tests are for NAMED constraints.

CREATE TABLE pk_on_part_col_named
(
	partition_col integer CONSTRAINT pk_on_part_col_named_pk PRIMARY KEY,
	other_col integer
);
SELECT master_create_distributed_table('pk_on_part_col_named', 'partition_col', 'hash');

CREATE TABLE uq_part_col_named
(
	partition_col integer CONSTRAINT uq_part_col_named_uniq UNIQUE,
	other_col integer
);
SELECT master_create_distributed_table('uq_part_col_named', 'partition_col', 'hash');

CREATE TABLE uq_two_columns_named
(
	partition_col integer,
	other_col integer,
	CONSTRAINT uq_two_columns_named_uniq UNIQUE (partition_col, other_col)
);
SELECT master_create_distributed_table('uq_two_columns_named', 'partition_col', 'hash');
SELECT master_create_worker_shards('uq_two_columns_named', '4', '2');
INSERT INTO uq_two_columns_named (partition_col, other_col) VALUES (1,1);
INSERT INTO uq_two_columns_named (partition_col, other_col) VALUES (1,1);

CREATE TABLE ex_on_part_col_named
(
	partition_col integer,
	other_col integer,
	CONSTRAINT ex_on_part_col_named_exclude EXCLUDE (partition_col WITH =)
);
SELECT master_create_distributed_table('ex_on_part_col_named', 'partition_col', 'hash');
SELECT master_create_worker_shards('ex_on_part_col_named', '4', '2');
INSERT INTO ex_on_part_col_named (partition_col, other_col) VALUES (1,1);
INSERT INTO ex_on_part_col_named (partition_col, other_col) VALUES (1,2);

CREATE TABLE ex_on_two_columns_named
(
	partition_col integer,
	other_col integer,
	CONSTRAINT ex_on_two_columns_named_exclude EXCLUDE (partition_col WITH =, other_col WITH =)
);
SELECT master_create_distributed_table('ex_on_two_columns_named', 'partition_col', 'hash');
SELECT master_create_worker_shards('ex_on_two_columns_named', '4', '2');
INSERT INTO ex_on_two_columns_named (partition_col, other_col) VALUES (1,1);
INSERT INTO ex_on_two_columns_named (partition_col, other_col) VALUES (1,1);

CREATE TABLE ex_multiple_excludes
(
	partition_col integer,
	other_col integer,
	other_other_col integer,
	CONSTRAINT ex_multiple_excludes_excl1 EXCLUDE (partition_col WITH =, other_col WITH =),
	CONSTRAINT ex_multiple_excludes_excl2 EXCLUDE (partition_col WITH =, other_other_col WITH =)
);
SELECT master_create_distributed_table('ex_multiple_excludes', 'partition_col', 'hash');
SELECT master_create_worker_shards('ex_multiple_excludes', '4', '2');
INSERT INTO ex_multiple_excludes (partition_col, other_col, other_other_col) VALUES (1,1,1);
INSERT INTO ex_multiple_excludes (partition_col, other_col, other_other_col) VALUES (1,1,2);
INSERT INTO ex_multiple_excludes (partition_col, other_col, other_other_col) VALUES (1,2,1);

CREATE TABLE ex_wrong_operator_named
(
	partition_col tsrange,
	other_col tsrange,
	CONSTRAINT ex_wrong_operator_named_exclude EXCLUDE USING gist (other_col WITH =, partition_col WITH &&)
);
SELECT master_create_distributed_table('ex_wrong_operator_named', 'partition_col', 'hash');

CREATE TABLE ex_overlaps_named
(
	partition_col tsrange,
	other_col tsrange,
	CONSTRAINT ex_overlaps_operator_named_exclude EXCLUDE USING gist (other_col WITH &&, partition_col WITH =)
);
SELECT master_create_distributed_table('ex_overlaps_named', 'partition_col', 'hash');
SELECT master_create_worker_shards('ex_overlaps_named', '4', '2');
INSERT INTO ex_overlaps_named (partition_col, other_col) VALUES ('[2016-01-01 00:00:00, 2016-02-01 00:00:00]', '[2016-01-01 00:00:00, 2016-02-01 00:00:00]');
INSERT INTO ex_overlaps_named (partition_col, other_col) VALUES ('[2016-01-01 00:00:00, 2016-02-01 00:00:00]', '[2016-01-15 00:00:00, 2016-02-01 00:00:00]');

-- now show that Citus allows unique constraints on range-partitioned tables.

CREATE TABLE uq_range_tables
(
	partition_col integer UNIQUE,
	other_col integer
);
SELECT master_create_distributed_table('uq_range_tables', 'partition_col', 'range');

-- show that CHECK constraints are distributed.
CREATE TABLE check_example
(
	partition_col integer UNIQUE,
	other_col integer CHECK (other_col >= 100),
	other_other_col integer CHECK (abs(other_other_col) >= 100)
);
SELECT master_create_distributed_table('check_example', 'partition_col', 'hash');
SELECT master_create_worker_shards('check_example', '2', '2');

\c - - - :worker_1_port
\d check_example*
\c - - - :worker_2_port
\d check_example*
\c - - - :master_port

-- drop unnecessary tables
DROP TABLE pk_on_non_part_col, uq_on_non_part_col CASCADE;
DROP TABLE pk_on_part_col, uq_part_col, uq_two_columns CASCADE;
DROP TABLE ex_on_part_col, ex_on_two_columns, ex_on_two_columns_prt, ex_multiple_excludes, ex_overlaps CASCADE;
DROP TABLE ex_on_part_col_named, ex_on_two_columns_named, ex_overlaps_named CASCADE;
DROP TABLE uq_range_tables, check_example CASCADE;

-- test dropping table with foreign keys
SET citus.shard_count = 4;
SET citus.shard_replication_factor = 1;

CREATE TABLE raw_table_1 (user_id int, UNIQUE(user_id));
SELECT create_distributed_table('raw_table_1', 'user_id');

CREATE TABLE raw_table_2 (user_id int REFERENCES raw_table_1(user_id), UNIQUE(user_id));
SELECT create_distributed_table('raw_table_2', 'user_id');

-- see that the constraint exists
\d raw_table_2

-- should be prevented by the foreign key
DROP TABLE raw_table_1;

-- should cleanly drop the remote shards
DROP TABLE raw_table_1 CASCADE;

-- see that the constraint also dropped
\d raw_table_2

-- drop the table as well
DROP TABLE raw_table_2;
