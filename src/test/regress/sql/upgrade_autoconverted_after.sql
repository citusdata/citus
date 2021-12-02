select logicalrelid, autoconverted from pg_dist_partition
    where logicalrelid IN ('citus_schema.citus_local_autoconverted'::regclass,
                           'citus_schema.citus_local_not_autoconverted'::regclass);
