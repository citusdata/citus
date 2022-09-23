// test moving a single shard that has rls
// owned by a user that is neither superuser nor bypassrls
// PG15 added extra permission checks within logical replication
// this test makes sure that target table owners should still
// be able to replicate despite RLS policies.
setup
{
	SET citus.shard_count TO 4;
	SET citus.shard_replication_factor TO 1;

    CREATE USER new_user;
    GRANT ALL ON SCHEMA public TO new_user;

    CREATE TABLE dist(column1 int PRIMARY KEY, column2 int);
    SELECT create_distributed_table('dist', 'column1');

	SELECT get_shard_id_for_distribution_column('dist', 23) INTO selected_shard;
    GRANT ALL ON TABLE selected_shard TO new_user;
}

teardown
{
    DROP TABLE selected_shard;
    DROP TABLE dist;
    REVOKE ALL ON SCHEMA public FROM new_user;
    DROP USER new_user;
}

session "s1"

step "s1-table-owner-new_user"
{
    ALTER TABLE dist OWNER TO new_user;
}

step "s1-table-enable-rls"
{
    ALTER TABLE dist ENABLE ROW LEVEL SECURITY;
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
    SELECT nodeport FROM pg_dist_placement INNER JOIN pg_dist_node ON (pg_dist_placement.groupid = pg_dist_node.groupid) WHERE shardstate != 4 AND shardid IN (SELECT * FROM selected_shard) ORDER BY nodeport;
}

session "s2"

step "s2-insert"
{
    INSERT INTO dist VALUES (23, 23);
}

session "s3"

step "s3-acquire-advisory-lock"
{
    SELECT pg_advisory_lock(44000, 55152);
}

step "s3-release-advisory-lock"
{
    SELECT pg_advisory_unlock(44000, 55152);
}

permutation "s1-table-owner-new_user" "s1-table-enable-rls" "s1-get-shard-distribution" "s1-user-spec" "s3-acquire-advisory-lock" "s1-begin" "s1-set-role" "s1-move-placement" "s2-insert" "s3-release-advisory-lock" "s1-end" "s1-select" "s1-get-shard-distribution"
