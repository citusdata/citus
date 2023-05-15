-- citus--12.0-1--11.3-1

-- Throw an error if user has any distributed tables without a shard key.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_dist_partition
        WHERE repmodel != 't' AND partmethod = 'n' AND colocationid != 0)
    THEN
        RAISE EXCEPTION 'cannot downgrade Citus because there are '
                        'distributed tables without a shard key.'
        USING HINT = 'You can find the distributed tables without a shard '
                     'key in the cluster by using the following query: '
                     '"SELECT * FROM citus_tables WHERE distribution_column '
                     '= ''<none>'' AND colocation_id > 0".',
        DETAIL = 'To downgrade Citus to an older version, you should '
                 'first convert those tables to Postgres tables by '
                 'executing SELECT undistribute_table("%s").';
    END IF;
END;
$$ LANGUAGE plpgsql;
