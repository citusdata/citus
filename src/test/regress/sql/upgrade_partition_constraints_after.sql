-- run this test only when old citus version is 9.0
\set upgrade_test_old_citus_version `echo "$upgrade_test_old_citus_version"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int = 9 AND
       substring(:'upgrade_test_old_citus_version', 'v\d+\.(\d+)\.\d+')::int = 0
AS upgrade_test_old_citus_version_e_9_0;
\gset
\if :upgrade_test_old_citus_version_e_9_0
\else
\q
\endif

-- test cases for #3970
SET search_path = test_3970;

--5. add a partition
--   This command will fail as the child table has a wrong constraint name
CREATE TABLE part_table_p202009 PARTITION OF part_table FOR VALUES FROM ('2020-09-01 00:00:00') TO ('2020-10-01 00:00:00');

-- fix constraint names on partitioned table shards
SELECT fix_pre_citus10_partitioned_table_constraint_names('part_table'::regclass);

--5. add a partition
CREATE TABLE part_table_p202009 PARTITION OF part_table FOR VALUES FROM ('2020-09-01 00:00:00') TO ('2020-10-01 00:00:00');

RESET search_path;
DROP SCHEMA test_3970 CASCADE;
