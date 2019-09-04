# the test expects to have zero nodes in pg_dist_node at the beginning
# add single one of the nodes for the purpose of the test
setup
{
	SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
	SELECT 1 FROM master_add_node('localhost', 57637);
}

teardown
{
    DROP TABLE IF EXISTS t1 CASCADE;
	SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-nodata"
{
	SELECT * from master_make_nodata_node('localhost', 57637);
}

step "s1-commit"
{
	COMMIT;
}


session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-create-distributed-table"
{
	CREATE TABLE t1 (a int);
	-- session needs to have replication factor set to 1, can't do in setup
	SET citus.shard_replication_factor TO 1;
	SELECT create_distributed_table('t1', 'a');
}

step "s2-update-node"
{
	select * from master_update_node((select nodeid from pg_dist_node where nodeport = 57637), 'localhost', 57638)
}


step "s2-commit"
{
	COMMIT;
}

permutation "s1-begin" "s2-begin" "s2-create-distributed-table" "s1-nodata" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s1-nodata" "s2-create-distributed-table" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-nodata" "s2-update-node" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s2-update-node" "s1-nodata" "s2-commit" "s1-commit"
