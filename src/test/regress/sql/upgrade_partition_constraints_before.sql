-- run this test only when old citus version is earlier than 10.0
\set upgrade_test_old_citus_version `echo "$CITUS_OLD_VERSION"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int < 10
AS upgrade_test_old_citus_version_lt_10_0;
\gset
\if :upgrade_test_old_citus_version_lt_10_0
\else
\q
\endif

-- test cases for #3970
CREATE SCHEMA test_3970;
SET search_path = test_3970;

--1. create a partitioned table
CREATE TABLE part_table (
    work_ymdt timestamp without time zone NOT NULL,
    seq bigint NOT NULL,
    my_seq bigint NOT NULL,
    work_memo character varying(150),
    CONSTRAINT work_memo_check CHECK ((octet_length((work_memo)::text) <= 150))
)
PARTITION BY RANGE (work_ymdt);

--2. perform create_distributed_table
SELECT create_distributed_table('part_table', 'seq');

--3. add a partition
CREATE TABLE part_table_p202008 PARTITION OF part_table FOR VALUES FROM ('2020-08-01 00:00:00') TO ('2020-09-01 00:00:00');

--4. add a check constraint
ALTER TABLE part_table ADD CONSTRAINT my_seq CHECK (my_seq > 0);

--5. add a partition
--   This command will fail as the child table has a wrong constraint name
CREATE TABLE part_table_p202009 PARTITION OF part_table FOR VALUES FROM ('2020-09-01 00:00:00') TO ('2020-10-01 00:00:00');

-- Add another constraint with a long name that will get truncated with a hash
ALTER TABLE part_table ADD CONSTRAINT ck_012345678901234567890123456789012345678901234567890123456789 CHECK (my_seq > 0);
