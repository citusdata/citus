-- run this test only when old citus version is earlier than 14.0
\set upgrade_test_old_citus_version `echo "$CITUS_OLD_VERSION"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int < 14
AS upgrade_test_old_citus_version_lt_14_0;
\gset
\if :upgrade_test_old_citus_version_lt_14_0
\else
\q
\endif

-- Make sure we still have different colocation groups.
select result as colocationids_different from run_command_on_all_nodes($$
    select count(distinct(colocationid)) = 3
    from pg_dist_partition
    join pg_class on (logicalrelid = pg_class.oid)
    join pg_namespace on (relnamespace = pg_namespace.oid)
    join pg_dist_colocation using (colocationid)
    where pg_namespace.nspname = 'Post_14_!''upgrade'
$$);

-- However, since our test suite executed citus_finish_citus_upgrade()
-- after the upgrade, the collations for each colocation group should
-- be adjusted correctly now.
select result as colocation_group_collations_different from run_command_on_all_nodes($$
    select count(distinct(distributioncolumncollation)) = 3
    from pg_dist_partition
    join pg_class on (logicalrelid = pg_class.oid)
    join pg_namespace on (relnamespace = pg_namespace.oid)
    join pg_dist_colocation using (colocationid)
    where pg_namespace.nspname = 'Post_14_!''upgrade'
$$);
