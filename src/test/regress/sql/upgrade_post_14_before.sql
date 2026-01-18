-- run this test only when old citus version is earlier than 14.0
\set upgrade_test_old_citus_version `echo "$CITUS_OLD_VERSION"`
SELECT substring(:'upgrade_test_old_citus_version', 'v(\d+)\.\d+\.\d+')::int < 14
AS upgrade_test_old_citus_version_lt_14_0;
\gset
\if :upgrade_test_old_citus_version_lt_14_0
\else
\q
\endif

SET citus.shard_replication_factor TO 1;

SET client_min_messages TO WARNING;
DROP SCHEMA IF EXISTS "Post_14_!'upgrade" CASCADE;

CREATE SCHEMA "Post_14_!'upgrade";
SET search_path = "Post_14_!'upgrade";

create table test_c1_tbl_1(text_col text);
create table test_c1_tbl_2(text_col text);
create table test_c1_tbl_3(text_col text);

create table test_c2_tbl_1(text_col text collate "en-x-icu");
create table test_c2_tbl_2(text_col text collate "en-x-icu");
create table test_c2_tbl_3(text_col text collate "en-x-icu");

-- also some tables with weird names
create table "test_c3_Tbl_!'@#_1" (text_col text collate "C");
create table "test_c3_Tbl_!'@#_2" (text_col text collate "C");
create table "test_c3_Tbl_!'@#_3" (text_col text collate "C");

select create_distributed_table('test_c1_tbl_1', 'text_col', colocate_with => 'none');
select create_distributed_table('test_c1_tbl_2', 'text_col', colocate_with => 'test_c1_tbl_1');
select create_distributed_table('test_c1_tbl_3', 'text_col', colocate_with => 'test_c1_tbl_1');

select create_distributed_table('test_c2_tbl_1', 'text_col', colocate_with => 'none');
select create_distributed_table('test_c2_tbl_2', 'text_col', colocate_with => 'test_c2_tbl_1');
select create_distributed_table('test_c2_tbl_3', 'text_col', colocate_with => 'test_c2_tbl_1');

SELECT create_distributed_table('"test_c3_Tbl_!''@#_1"', 'text_col', colocate_with => 'none');
SELECT create_distributed_table('"test_c3_Tbl_!''@#_2"', 'text_col', colocate_with => '"test_c3_Tbl_!''@#_1"');
SELECT create_distributed_table('"test_c3_Tbl_!''@#_3"', 'text_col', colocate_with => '"test_c3_Tbl_!''@#_1"');

-- Make sure we have different colocation groups as we forced them
-- to be so by providing colocate_with=>'none' for the first table in each
-- group.
select result as colocationids_different from run_command_on_all_nodes($$
    select count(distinct(colocationid)) = 3
    from pg_dist_partition
    join pg_class on (logicalrelid = pg_class.oid)
    join pg_namespace on (relnamespace = pg_namespace.oid)
    join pg_dist_colocation using (colocationid)
    where pg_namespace.nspname = 'Post_14_!''upgrade'
$$);

-- However, given that right now we're on an older version Citus,
-- i.e., a version that doesn't have https://github.com/citusdata/citus/pull/8257,
-- the collation for each group was assigned incorrectly, i.e., all
-- assumed the default collation for the type, as if we created the tables
-- in all groups as in what we did for test_c1_tbl_* above.
select result as colocation_group_collations_same from run_command_on_all_nodes($$
    select count(distinct(distributioncolumncollation)) = 1
    from pg_dist_partition
    join pg_class on (logicalrelid = pg_class.oid)
    join pg_namespace on (relnamespace = pg_namespace.oid)
    join pg_dist_colocation using (colocationid)
    where pg_namespace.nspname = 'Post_14_!''upgrade'
$$);
