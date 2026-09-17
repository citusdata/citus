// Verify that UPDATE and DELETE remain available while a shard without a
// replica identity is moved using force_logical_auto_identity.
setup
{
	SET citus.shard_count TO 8;
	SET citus.shard_replication_factor TO 1;

	CREATE TABLE logical_replicate_no_identity (x int, y int);
	SELECT create_distributed_table('logical_replicate_no_identity', 'x');
	INSERT INTO logical_replicate_no_identity VALUES (15, 15);

	SELECT get_shard_id_for_distribution_column(
		'logical_replicate_no_identity', 15) INTO selected_shard;
}

teardown
{
	DROP TABLE selected_shard;
	DROP TABLE logical_replicate_no_identity;
}

session "s1"

step "s1-move-placement"
{
	SELECT master_move_shard_placement(
		(SELECT * FROM selected_shard),
		'localhost', 57637,
		'localhost', 57638,
		shard_transfer_mode => 'force_logical_auto_identity');
}

step "s1-select"
{
	SELECT * FROM logical_replicate_no_identity ORDER BY x;
}

step "s1-get-shard-distribution"
{
	SELECT nodeport
	FROM pg_dist_placement
	INNER JOIN pg_dist_node USING (groupid)
	WHERE shardstate != 4
	  AND shardid IN (SELECT * FROM selected_shard)
	ORDER BY nodeport;
}

step "s1-check-replica-identity"
{
	SELECT DISTINCT result
	FROM run_command_on_placements(
		'logical_replicate_no_identity',
		'SELECT relreplident FROM pg_class WHERE oid = ''%s''::regclass');
}

session "s2"

step "s2-update"
{
	UPDATE logical_replicate_no_identity SET y = y + 1 WHERE x = 15;
}

step "s2-delete"
{
	DELETE FROM logical_replicate_no_identity WHERE x = 15;
}

session "s3"

// The shard move takes this advisory lock immediately before the initial copy.
// Holding it lets s2 modify the source while its publication is active.
step "s3-acquire-advisory-lock"
{
	SELECT pg_advisory_lock(44000, 55152);
}

step "s3-release-advisory-lock"
{
	SELECT pg_advisory_unlock(44000, 55152);
}

permutation "s3-acquire-advisory-lock" "s1-move-placement" "s2-update" "s3-release-advisory-lock" "s1-select" "s1-get-shard-distribution" "s1-check-replica-identity"
permutation "s3-acquire-advisory-lock" "s1-move-placement" "s2-delete" "s3-release-advisory-lock" "s1-select" "s1-get-shard-distribution" "s1-check-replica-identity"
