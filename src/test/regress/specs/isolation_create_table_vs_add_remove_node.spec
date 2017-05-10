setup
{
	SELECT 1 FROM master_add_node('localhost', 57637);
	SELECT * FROM master_get_active_worker_nodes() ORDER BY node_name, node_port;
}

teardown
{
	DROP TABLE IF EXISTS dist_table;

	SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-add-node-2"
{
	SELECT 1 FROM master_add_node('localhost', 57638);
}

step "s1-remove-node-2"
{
	SELECT * FROM master_remove_node('localhost', 57638);
}

step "s1-abort"
{
	ABORT;
}

step "s1-commit"
{
	COMMIT;
}

step "s1-query-table"
{
	SELECT * FROM dist_table;
}

step "s1-show-nodes"
{
	SELECT nodename, nodeport, isactive FROM pg_dist_node ORDER BY nodename, nodeport;
}

step "s1-show-placements"
{
	SELECT
		nodename, nodeport
	FROM
		pg_dist_shard_placement JOIN pg_dist_shard USING (shardid)
	WHERE
		logicalrelid = 'dist_table'::regclass
	ORDER BY
		nodename, nodeport;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-create-table-1"
{
	SET citus.shard_count TO 4;
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE dist_table (x int, y int);
	SELECT create_distributed_table('dist_table', 'x');
}

step "s2-create-table-2"
{
	SET citus.shard_count TO 4;
	SET citus.shard_replication_factor TO 2;
	CREATE TABLE dist_table (x int, y int);
	SELECT create_distributed_table('dist_table', 'x');
}

step "s2-select"
{
	SELECT * FROM dist_table;
}

step "s2-commit"
{
	COMMIT;
}

# session 1 adds a node, session 2 creates a distributed table
permutation "s1-begin" "s1-add-node-2" "s2-create-table-1" "s1-commit" "s1-show-placements" "s2-select"
permutation "s1-begin" "s1-add-node-2" "s2-create-table-1" "s1-abort" "s1-show-placements" "s2-select"
permutation "s2-begin" "s2-create-table-1" "s1-add-node-2" "s2-commit" "s1-show-placements" "s2-select"

# session 1 removes a node, session 2 creates a distributed table
permutation "s1-add-node-2" "s1-begin" "s1-remove-node-2" "s2-create-table-1" "s1-commit" "s1-show-placements" "s2-select"
permutation "s1-add-node-2" "s1-begin" "s1-remove-node-2" "s2-create-table-1" "s1-abort" "s1-show-placements" "s2-select"
permutation "s1-add-node-2" "s2-begin" "s2-create-table-1" "s1-remove-node-2" "s2-commit" "s1-show-placements" "s2-select"

# session 1 removes a node, session 2 creates a distributed table with replication factor 2, should throw a sane error
permutation "s1-add-node-2" "s1-begin" "s1-remove-node-2" "s2-create-table-2" "s1-commit" "s2-select"
permutation "s1-add-node-2" "s1-begin" "s2-create-table-2" "s1-remove-node-2" "s1-commit" "s2-select"
