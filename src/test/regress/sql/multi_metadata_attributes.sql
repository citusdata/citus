
-- if the output of following query changes, we might need to change
-- some heap_getattr() calls to heap_deform_tuple(). This errors out in
-- postgres versions before 11. If this test fails check out
-- https://github.com/citusdata/citus/pull/2464 for an explanation of what to
-- do. Once you used the new code for the table you can add it to the NOT IN
-- part of the query so new changes to it won't affect this test.
SELECT attrelid::regclass, attname, atthasmissing, attmissingval
FROM pg_attribute
WHERE atthasmissing AND attrelid NOT IN ('pg_dist_node'::regclass,
                                         'pg_dist_rebalance_strategy'::regclass,
                                         'pg_dist_partition'::regclass,
                                         'pg_dist_object'::regclass,
                                         'pg_dist_local_group'::regclass)
ORDER BY attrelid, attname;
