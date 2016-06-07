--
-- MULTI_VERIFY_NO_JOIN_WITH_ALIAS
--

-- This test checks that we simply emit an error message instead of trying to
-- fetch and join a shard which has an alias set.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1020000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1020000;


-- Show that the join works without an alias

SELECT COUNT(*) FROM lineitem, part WHERE l_partkey = p_partkey;

-- Assign an alias to the parts shard

UPDATE pg_dist_shard SET shardalias = 'my_alias' WHERE shardid = 290000;

-- Attempt a join which uses this shard

SELECT COUNT(*) FROM lineitem, part WHERE l_partkey = p_partkey;

-- Remove the alias from the parts shard

UPDATE pg_dist_shard SET shardalias = NULL WHERE shardid = 290000;
