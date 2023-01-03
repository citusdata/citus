// isolation_logical_replication_nonsu_nonbypassrls
// test moving a single shard that has rls
// owned by a user that is neither superuser nor bypassrls
// PG15 added extra permission checks within logical replication
// this test makes sure that target table owners should still
// be able to replicate despite RLS policies.
// Relevant PG commit: a2ab9c06ea15fbcb2bfde570986a06b37f52bcca

setup
{
    -- setup involves a lot of DDL inside a single tx block, so use sequential mode
    SET LOCAL citus.multi_shard_modify_mode TO 'sequential';

    SET citus.max_cached_conns_per_worker to 0;
    SET citus.next_shard_id TO 1234000;
	SET citus.shard_count TO 4;
	SET citus.shard_replication_factor TO 1;

    CREATE TABLE dist(column1 int PRIMARY KEY, column2 int);
    SELECT create_distributed_table('dist', 'column1');

    CREATE USER new_user;
    GRANT ALL ON SCHEMA public TO new_user;

	SELECT get_shard_id_for_distribution_column('dist', 23) INTO selected_shard;
    GRANT ALL ON TABLE selected_shard TO new_user;
}

teardown
{
    CREATE OR REPLACE PROCEDURE isolation_cleanup_orphaned_resources()
        LANGUAGE C
        AS 'citus', $$isolation_cleanup_orphaned_resources$$;
    COMMENT ON PROCEDURE isolation_cleanup_orphaned_resources()
        IS 'cleanup orphaned resources';
    CALL isolation_cleanup_orphaned_resources();
    DROP TABLE selected_shard;
    DROP TABLE dist;
    REVOKE ALL ON SCHEMA public FROM new_user;
    DROP USER new_user;
}

session "s1"

step "s1-no-connection-cache"
{
	SET citus.max_cached_conns_per_worker to 0;
}

step "s1-table-owner-new_user"
{
    ALTER TABLE dist OWNER TO new_user;
}

step "s1-table-enable-rls"
{
    ALTER TABLE dist ENABLE ROW LEVEL SECURITY;
}

step "s1-table-force-rls"
{
    ALTER TABLE dist FORCE ROW LEVEL SECURITY;
}

step "s1-user-spec"
{
    SELECT rolname, rolsuper, rolbypassrls FROM pg_authid WHERE rolname = 'new_user';
}

step "s1-begin"
{
	BEGIN;
}

step "s1-set-role"
{
    SET ROLE new_user;
}

step "s1-move-placement"
{
    SELECT citus_move_shard_placement((SELECT * FROM selected_shard), 'localhost', 57638, 'localhost', 57637);
}

step "s1-reset-role"
{
	RESET ROLE;
}

step "s1-end"
{
	COMMIT;
}

step "s1-select"
{
    SELECT * FROM dist ORDER BY column1;
}

step "s1-get-shard-distribution"
{
    SELECT shardid, nodeport FROM pg_dist_placement INNER JOIN pg_dist_node ON (pg_dist_placement.groupid = pg_dist_node.groupid) WHERE shardstate != 4 AND shardid IN (SELECT * FROM selected_shard) ORDER BY nodeport;
}

session "s2"

step "s2-no-connection-cache"
{
	SET citus.max_cached_conns_per_worker to 0;
}

step "s2-insert"
{
    INSERT INTO dist VALUES (23, 23);
}

session "s3"

step "s3-no-connection-cache"
{
	SET citus.max_cached_conns_per_worker to 0;
}

step "s3-acquire-advisory-lock"
{
    SELECT pg_advisory_lock(44000, 55152);
}

step "s3-release-advisory-lock"
{
    SELECT pg_advisory_unlock(44000, 55152);
}

// first permutation enables row level security
// second permutation forces row level security
// either way we should be able to complete the shard move
// Check out https://github.com/citusdata/citus/pull/6369#discussion_r979823178 for details

permutation "s1-table-owner-new_user" "s1-table-enable-rls" "s1-get-shard-distribution" "s1-user-spec" "s3-acquire-advisory-lock" "s1-begin" "s1-set-role" "s1-move-placement" "s2-insert" "s3-release-advisory-lock" "s1-reset-role" "s1-end" "s1-select" "s1-get-shard-distribution"
// running no connection cache commands on 2nd permutation because of #3785
// otherwise citus_move_shard_placement fails with permission error of new_user
permutation "s1-no-connection-cache" "s2-no-connection-cache" "s3-no-connection-cache" "s1-table-owner-new_user" "s1-table-force-rls" "s1-get-shard-distribution" "s1-user-spec" "s3-acquire-advisory-lock" "s1-begin" "s1-set-role" "s1-move-placement" "s2-insert" "s3-release-advisory-lock" "s1-reset-role" "s1-end" "s1-select" "s1-get-shard-distribution"
