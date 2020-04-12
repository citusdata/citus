--
-- MULTI_CREATE_TABLE_CONSTRAINTS
--

SET citus.next_shard_id TO 365000;

--  test that Citus forbids unique and EXCLUDE constraints on append-partitioned tables.

CREATE TABLE uniq_cns_append_tables
(
	partition_col integer UNIQUE,
	other_col integer
);
SELECT create_distributed_table('uniq_cns_append_tables', 'partition_col', 'append');

CREATE TABLE excl_cns_append_tables
(
	partition_col integer,
	other_col integer,
        EXCLUDE (partition_col WITH =)
);
SELECT create_distributed_table('excl_cns_append_tables', 'partition_col', 'append');

-- test that Citus cannot distribute unique constraints that do not include
-- the partition column on hash-partitioned tables.

CREATE TABLE pk_on_non_part_col
(
	partition_col integer,
	other_col integer PRIMARY KEY
);
SELECT create_distributed_table('pk_on_non_part_col', 'partition_col', 'hash');

CREATE TABLE uq_on_non_part_col
(
	partition_col integer,
	other_col integer UNIQUE
);
SELECT create_distributed_table('uq_on_non_part_col', 'partition_col', 'hash');

CREATE TABLE ex_on_non_part_col
(
	partition_col integer,
	other_col integer,
	EXCLUDE (other_col WITH =)
);
SELECT create_distributed_table('ex_on_non_part_col', 'partition_col', 'hash');

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
SELECT create_distributed_table('pk_on_part_col', 'partition_col', 'hash');

CREATE TABLE uq_part_col
(
	partition_col integer UNIQUE,
	other_col integer
);
SELECT create_distributed_table('uq_part_col', 'partition_col', 'hash');

CREATE TABLE uq_two_columns
(
	partition_col integer,
	other_col integer,
	UNIQUE (partition_col, other_col)
);
SELECT create_distributed_table('uq_two_columns', 'partition_col', 'hash');
INSERT INTO uq_two_columns (partition_col, other_col) VALUES (1,1);
INSERT INTO uq_two_columns (partition_col, other_col) VALUES (1,1);

CREATE TABLE ex_on_part_col
(
	partition_col integer,
	other_col integer,
	EXCLUDE (partition_col WITH =)
);
SELECT create_distributed_table('ex_on_part_col', 'partition_col', 'hash');
INSERT INTO ex_on_part_col (partition_col, other_col) VALUES (1,1);
INSERT INTO ex_on_part_col (partition_col, other_col) VALUES (1,2);

CREATE TABLE ex_on_two_columns
(
	partition_col integer,
	other_col integer,
	EXCLUDE (partition_col WITH =, other_col WITH =)
);
SELECT create_distributed_table('ex_on_two_columns', 'partition_col', 'hash');
INSERT INTO ex_on_two_columns (partition_col, other_col) VALUES (1,1);
INSERT INTO ex_on_two_columns (partition_col, other_col) VALUES (1,1);

CREATE TABLE ex_on_two_columns_prt
(
	partition_col integer,
	other_col integer,
	EXCLUDE (partition_col WITH =, other_col WITH =) WHERE (other_col > 100)
);
SELECT create_distributed_table('ex_on_two_columns_prt', 'partition_col', 'hash');
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
SELECT create_distributed_table('ex_wrong_operator', 'partition_col', 'hash');

CREATE TABLE ex_overlaps
(
	partition_col tsrange,
	other_col tsrange,
	EXCLUDE USING gist (other_col WITH &&, partition_col WITH =)
);
SELECT create_distributed_table('ex_overlaps', 'partition_col', 'hash');
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
SELECT create_distributed_table('pk_on_part_col_named', 'partition_col', 'hash');

CREATE TABLE uq_part_col_named
(
	partition_col integer CONSTRAINT uq_part_col_named_uniq UNIQUE,
	other_col integer
);
SELECT create_distributed_table('uq_part_col_named', 'partition_col', 'hash');

CREATE TABLE uq_two_columns_named
(
	partition_col integer,
	other_col integer,
	CONSTRAINT uq_two_columns_named_uniq UNIQUE (partition_col, other_col)
);
SELECT create_distributed_table('uq_two_columns_named', 'partition_col', 'hash');
INSERT INTO uq_two_columns_named (partition_col, other_col) VALUES (1,1);
INSERT INTO uq_two_columns_named (partition_col, other_col) VALUES (1,1);

CREATE TABLE ex_on_part_col_named
(
	partition_col integer,
	other_col integer,
	CONSTRAINT ex_on_part_col_named_exclude EXCLUDE (partition_col WITH =)
);
SELECT create_distributed_table('ex_on_part_col_named', 'partition_col', 'hash');
INSERT INTO ex_on_part_col_named (partition_col, other_col) VALUES (1,1);
INSERT INTO ex_on_part_col_named (partition_col, other_col) VALUES (1,2);

CREATE TABLE ex_on_two_columns_named
(
	partition_col integer,
	other_col integer,
	CONSTRAINT ex_on_two_columns_named_exclude EXCLUDE (partition_col WITH =, other_col WITH =)
);
SELECT create_distributed_table('ex_on_two_columns_named', 'partition_col', 'hash');
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
SELECT create_distributed_table('ex_multiple_excludes', 'partition_col', 'hash');
INSERT INTO ex_multiple_excludes (partition_col, other_col, other_other_col) VALUES (1,1,1);
INSERT INTO ex_multiple_excludes (partition_col, other_col, other_other_col) VALUES (1,1,2);
INSERT INTO ex_multiple_excludes (partition_col, other_col, other_other_col) VALUES (1,2,1);

CREATE TABLE ex_wrong_operator_named
(
	partition_col tsrange,
	other_col tsrange,
	CONSTRAINT ex_wrong_operator_named_exclude EXCLUDE USING gist (other_col WITH =, partition_col WITH &&)
);
SELECT create_distributed_table('ex_wrong_operator_named', 'partition_col', 'hash');

CREATE TABLE ex_overlaps_named
(
	partition_col tsrange,
	other_col tsrange,
	CONSTRAINT ex_overlaps_operator_named_exclude EXCLUDE USING gist (other_col WITH &&, partition_col WITH =)
);
SELECT create_distributed_table('ex_overlaps_named', 'partition_col', 'hash');
INSERT INTO ex_overlaps_named (partition_col, other_col) VALUES ('[2016-01-01 00:00:00, 2016-02-01 00:00:00]', '[2016-01-01 00:00:00, 2016-02-01 00:00:00]');
INSERT INTO ex_overlaps_named (partition_col, other_col) VALUES ('[2016-01-01 00:00:00, 2016-02-01 00:00:00]', '[2016-01-15 00:00:00, 2016-02-01 00:00:00]');

-- now show that Citus allows unique constraints on range-partitioned tables.

CREATE TABLE uq_range_tables
(
	partition_col integer UNIQUE,
	other_col integer
);
SELECT create_distributed_table('uq_range_tables', 'partition_col', 'range');

-- show that CHECK constraints are distributed.
CREATE TABLE check_example
(
	partition_col integer UNIQUE,
	other_col integer CHECK (other_col >= 100),
	other_other_col integer CHECK (abs(other_other_col) >= 100)
);
SELECT create_distributed_table('check_example', 'partition_col', 'hash');
\c - - :public_worker_1_host :worker_1_port
SELECT "Column", "Type", "Definition" FROM index_attrs WHERE
    relid = 'check_example_partition_col_key_365056'::regclass;
SELECT "Constraint", "Definition" FROM table_checks WHERE relid='public.check_example_365056'::regclass;
\c - - :master_host :master_port

-- Index-based constraints are created with shard-extended names, but others
-- (e.g. expression-based table CHECK constraints) do _not_ have shardids in
-- their object names, _at least originally as designed_. At some point, we
-- mistakenly started extending _all_ constraint names, but _only_ for ALTER
-- TABLE ... ADD CONSTRAINT commands (yes, even non-index constraints). So now
-- the _same_ constraint definition could result in a non-extended name if made
-- using CREATE TABLE and another name if made using AT ... ADD CONSTRAINT. So
-- DROP CONSTRAINT started erroring because _it_ was also changed to always do
-- shard-id extension. We've fixed that by looking for the non-extended name
-- first and using it for DROP or VALIDATE commands that could be targeting it.

-- As for the actual test: drop a constraint created by CREATE TABLE ... CHECK,
-- which per the above description would have been created with a non-extended
-- object name, but previously would have failed DROP as DROP does extension.
ALTER TABLE check_example DROP CONSTRAINT check_example_other_col_check;

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
SELECT "Constraint", "Definition" FROM table_fkeys WHERE relid='raw_table_2'::regclass;

-- should be prevented by the foreign key
DROP TABLE raw_table_1;

-- should cleanly drop the remote shards
DROP TABLE raw_table_1 CASCADE;

-- see that the constraint also dropped
SELECT "Constraint", "Definition" FROM table_fkeys WHERE relid='raw_table_2'::regclass;

-- drop the table as well
DROP TABLE raw_table_2;
