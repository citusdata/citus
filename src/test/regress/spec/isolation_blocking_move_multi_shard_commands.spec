// we use 15 as partition key values through out the test
// so setting the corresponding shard here is useful

setup
{
  SELECT citus_internal.replace_isolation_tester_func();
  SELECT citus_internal.refresh_isolation_tester_prepared_statement();

  SET citus.shard_count TO 8;
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE logical_replicate_placement (x int PRIMARY KEY, y int);
	SELECT create_distributed_table('logical_replicate_placement', 'x');

	SELECT get_shard_id_for_distribution_column('logical_replicate_placement', 15) INTO selected_shard;

}

teardown
{
  SELECT citus_internal.restore_isolation_tester_func();

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
    	SELECT master_move_shard_placement(get_shard_id_for_distribution_column, 'localhost', 57637, 'localhost', 57638, shard_transfer_mode:='block_writes') FROM selected_shard;
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
    INSERT INTO logical_replicate_placement VALUES (15, 15), (172, 172);
}

step "s1-get-shard-distribution"
{
    select nodeport from pg_dist_placement inner join pg_dist_node on(pg_dist_placement.groupid = pg_dist_node.groupid) where shardid in (SELECT * FROM selected_shard) order by nodeport;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-select"
{
    SELECT * FROM logical_replicate_placement ORDER BY y;
}

step "s2-insert"
{
    INSERT INTO logical_replicate_placement VALUES (15, 15), (172, 172);
}

step "s2-delete"
{
    DELETE FROM logical_replicate_placement;
}

step "s2-update"
{
    UPDATE logical_replicate_placement SET y = y + 1;
}

step "s2-upsert"
{
    INSERT INTO logical_replicate_placement VALUES (15, 15), (172, 172);

    INSERT INTO logical_replicate_placement VALUES (15, 15), (172, 172) ON CONFLICT (x) DO UPDATE SET y = logical_replicate_placement.y + 1;
}

step "s2-copy"
{
	COPY logical_replicate_placement FROM PROGRAM 'echo "1,1\n2,2\n3,3\n4,4\n5,5\n15,30"' WITH CSV;
}

step "s2-truncate"
{
	TRUNCATE logical_replicate_placement;
}

step "s2-alter-table"
{
	ALTER TABLE logical_replicate_placement ADD COLUMN z INT;
}

step "s2-end"
{
	COMMIT;
}

permutation "s1-begin" "s2-begin" "s2-insert" "s1-move-placement"  "s2-end"  "s1-end" "s1-select" "s1-get-shard-distribution"
permutation "s1-begin" "s2-begin" "s2-upsert" "s1-move-placement" "s2-end" "s1-end" "s1-select"  "s1-get-shard-distribution"
permutation "s1-insert" "s1-begin" "s2-begin" "s2-update" "s1-move-placement" "s2-end" "s1-end" "s1-select" "s1-get-shard-distribution"
permutation "s1-insert" "s1-begin" "s2-begin" "s2-delete" "s1-move-placement" "s2-end" "s1-end" "s1-select" "s1-get-shard-distribution"
permutation "s1-insert" "s1-begin" "s2-begin" "s2-select" "s1-move-placement" "s2-end" "s1-end" "s1-get-shard-distribution"
permutation "s1-begin" "s2-begin" "s2-copy" "s1-move-placement" "s2-end" "s1-end" "s1-select" "s1-get-shard-distribution"
permutation "s1-insert" "s1-begin" "s2-begin" "s2-truncate" "s1-move-placement" "s2-end" "s1-end" "s1-select" "s1-get-shard-distribution"
permutation "s1-begin" "s2-begin" "s2-alter-table" "s1-move-placement" "s2-end" "s1-end" "s1-select" "s1-get-shard-distribution"

