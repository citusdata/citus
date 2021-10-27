CREATE TABLE ref_not_autoconverted(a int unique);
CREATE TABLE citus_local_autoconverted(a int unique references ref_not_autoconverted(a));
CREATE TABLE citus_local_not_autoconverted(a int unique);
select create_reference_table('ref_not_autoconverted');
select citus_add_local_table_to_metadata('citus_local_not_autoconverted');
select logicalrelid, autoconverted from pg_dist_partition
    where logicalrelid IN ('citus_local_autoconverted'::regclass,
                           'citus_local_not_autoconverted'::regclass);
