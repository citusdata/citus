select logicalrelid, autoconverted from pg_dist_partition
    where logicalrelid IN ('citus_local_autoconverted'::regclass,
                           'citus_local_not_autoconverted'::regclass);
