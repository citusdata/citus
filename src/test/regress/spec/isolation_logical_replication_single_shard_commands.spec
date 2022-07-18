// we use 15 as the partition key value through out the test
// so setting the corresponding shard here is useful
setup
{
	SET citus.shard_count TO 8;
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE logical_replicate_placement (x int PRIMARY KEY, y int);
	SELECT create_distributed_table('logical_replicate_placement', 'x');

	SELECT get_shard_id_for_distribution_column('logical_replicate_placement', 15) INTO selected_shard;
}

teardown
{
  DROP TABLE selected_shard;
	DROP TABLE logical_replicate_placement;
}


session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-move-placement"
{
    	SELECT master_move_shard_placement((SELECT * FROM selected_shard), 'localhost', 57637, 'localhost', 57638);
}

step "s1-end"
{
	COMMIT;
}

step "s1-select"
{
  SELECT * FROM logical_replicate_placement order by y;
}

step "s1-insert"
{
    INSERT INTO logical_replicate_placement VALUES (15, 15);
}

step "s1-get-shard-distribution"
{
  select nodeport from pg_dist_placement inner join pg_dist_node on(pg_dist_placement.groupid = pg_dist_node.groupid) where shardstate != 4 AND shardid in (SELECT * FROM selected_shard) order by nodeport;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-move-placement"
{
	SELECT master_move_shard_placement(
		get_shard_id_for_distribution_column('logical_replicate_placement', 4),
		'localhost', 57637, 'localhost', 57638);
}

step "s2-select"
{
    SELECT * FROM logical_replicate_placement ORDER BY y;
}

step "s2-insert"
{
    INSERT INTO logical_replicate_placement VALUES (15, 15);
}

step "s2-select-for-update"
{
    SELECT * FROM logical_replicate_placement WHERE x=15 FOR UPDATE;
}

step "s2-delete"
{
    DELETE FROM logical_replicate_placement WHERE x = 15;
}

step "s2-update"
{
    UPDATE logical_replicate_placement SET y = y + 1 WHERE x = 15;
}

step "s2-upsert"
{
    INSERT INTO logical_replicate_placement VALUES (15, 15);

    INSERT INTO logical_replicate_placement VALUES (15, 15) ON CONFLICT (x) DO UPDATE SET y = logical_replicate_placement.y + 1;
}

step "s2-end"
{
	  COMMIT;
}

session "s3"

// this advisory lock with (almost) random values are only used
// for testing purposes. For details, check Citus' logical replication
// source code
step "s3-acquire-advisory-lock"
{
    SELECT pg_advisory_lock(44000, 55152);
}

step "s3-release-advisory-lock"
{
    SELECT pg_advisory_unlock(44000, 55152);
}

##// nonblocking tests lie below ###

// move placement first
// the following tests show the non-blocking modifications while shard is being moved
// in fact, the shard move blocks the writes for a very short duration of time
// by using an advisory and allowing the other commands continue to run, we prevent
// the modifications to block on that blocking duration
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement" "s2-insert" "s3-release-advisory-lock" "s1-end" "s1-select" "s1-get-shard-distribution"
permutation "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement" "s2-upsert" "s3-release-advisory-lock" "s1-end" "s1-select"  "s1-get-shard-distribution"
permutation "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement" "s2-update" "s3-release-advisory-lock" "s1-end" "s1-select" "s1-get-shard-distribution"
permutation "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement" "s2-delete" "s3-release-advisory-lock" "s1-end" "s1-select" "s1-get-shard-distribution"
permutation "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement" "s2-select" "s3-release-advisory-lock" "s1-end" "s1-get-shard-distribution"
permutation "s1-insert" "s3-acquire-advisory-lock" "s1-begin" "s1-move-placement" "s2-select-for-update" "s3-release-advisory-lock" "s1-end" "s1-get-shard-distribution"

// move placement second
// force shard-move to be a blocking call intentionally
permutation "s1-begin" "s2-begin" "s2-insert" "s1-move-placement" "s2-end" "s1-end" "s1-select" "s1-get-shard-distribution"
permutation "s1-begin" "s2-begin" "s2-upsert" "s1-move-placement" "s2-end" "s1-end" "s1-select"  "s1-get-shard-distribution"
permutation "s1-insert" "s1-begin" "s2-begin" "s2-update" "s1-move-placement" "s2-end" "s1-end" "s1-select" "s1-get-shard-distribution"
permutation "s1-insert" "s1-begin" "s2-begin" "s2-delete" "s1-move-placement" "s2-end" "s1-end" "s1-select" "s1-get-shard-distribution"
permutation "s1-insert" "s1-begin" "s2-begin" "s2-select" "s1-move-placement" "s2-end" "s1-end" "s1-get-shard-distribution"
permutation "s1-insert" "s1-begin" "s2-begin" "s2-select-for-update" "s1-move-placement" "s2-end" "s1-end" "s1-get-shard-distribution"


// This test actually blocks because we don't want two non blocking shard moves at the same time
permutation "s1-begin" "s2-begin" "s1-move-placement" "s2-move-placement" "s1-end" "s2-end"
