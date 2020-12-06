setup
{
	SELECT 1 FROM master_add_node('localhost', 57637);
	SELECT 1 FROM master_add_node('localhost', 57638);
	CREATE TABLE colocated1 (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('colocated1', 'test_id', 'hash');
	CREATE TABLE colocated2 (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('colocated2', 'test_id', 'hash');
	CREATE TABLE non_colocated (test_id integer NOT NULL, data text);
	SELECT create_distributed_table('non_colocated', 'test_id', 'hash', 'none');
}

teardown
{
	DROP TABLE non_colocated;
	DROP TABLE colocated2;
	DROP TABLE colocated1;
	SELECT master_set_node_property('localhost', 57638, 'shouldhaveshards', true);
}

session "s1"

step "s1-rebalance-c1"
{
	BEGIN;
	select rebalance_table_shards('colocated1');
}

step "s1-replicate-c1"
{
	BEGIN;
	select replicate_table_shards('colocated1');
}

step "s1-rebalance-nc"
{
	BEGIN;
	select rebalance_table_shards('non_colocated');
}

step "s1-replicate-nc"
{
	BEGIN;
	select replicate_table_shards('non_colocated');
}

step "s1-commit"
{
	COMMIT;
}

session "s2"


step "s2-rebalance-c2"
{
	select rebalance_table_shards('colocated2');
}

step "s2-replicate-c2"
{
	select replicate_table_shards('colocated2');
}

step "s2-rebalance-nc"
{
	select rebalance_table_shards('non_colocated');
}

step "s2-replicate-nc"
{
	select replicate_table_shards('non_colocated');
}

step "s2-rebalance-all"
{
	select rebalance_table_shards();
}

step "s2-drain"
{
	select master_drain_node('localhost', 57638);
}


// disallowed because it's the same table
permutation "s1-rebalance-nc" "s2-rebalance-nc" "s1-commit"
permutation "s1-rebalance-nc" "s2-replicate-nc" "s1-commit"
permutation "s1-replicate-nc" "s2-rebalance-nc" "s1-commit"
permutation "s1-replicate-nc" "s2-replicate-nc" "s1-commit"

// disallowed because it's the same colocation group
permutation "s1-rebalance-c1" "s2-rebalance-c2" "s1-commit"
permutation "s1-rebalance-c1" "s2-replicate-c2" "s1-commit"
permutation "s1-replicate-c1" "s2-rebalance-c2" "s1-commit"
permutation "s1-replicate-c1" "s2-replicate-c2" "s1-commit"

// allowed because it's a different colocation group
permutation "s1-rebalance-c1" "s2-rebalance-nc" "s1-commit"
permutation "s1-rebalance-c1" "s2-replicate-nc" "s1-commit"
permutation "s1-replicate-c1" "s2-rebalance-nc" "s1-commit"
permutation "s1-replicate-c1" "s2-replicate-nc" "s1-commit"

// disallowed because we because colocated1 is part of all
permutation "s1-rebalance-c1" "s2-rebalance-all" "s1-commit"
permutation "s1-replicate-c1" "s2-rebalance-all" "s1-commit"
permutation "s1-rebalance-nc" "s2-rebalance-all" "s1-commit"
permutation "s1-replicate-nc" "s2-rebalance-all" "s1-commit"

// disallowed because we because draining is rebalancing
permutation "s1-rebalance-c1" "s2-drain" "s1-commit"
permutation "s1-replicate-c1" "s2-drain" "s1-commit"
permutation "s1-rebalance-nc" "s2-drain" "s1-commit"
permutation "s1-replicate-nc" "s2-drain" "s1-commit"
