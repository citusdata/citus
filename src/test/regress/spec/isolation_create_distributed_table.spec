setup
{
  	CREATE TABLE table_to_distribute(id int);
    CREATE TABLE table_to_colocate(id int);
}

teardown
{
	DROP TABLE table_to_distribute CASCADE;
    DROP TABLE table_to_colocate CASCADE;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-create_distributed_table"
{
	SELECT create_distributed_table('table_to_distribute', 'id');
}

step "s1_set-shard-property"
{
	SELECT citus_shard_property_set(shardid, anti_affinity=>'true')
    FROM pg_dist_shard WHERE logicalrelid = 'table_to_distribute'::regclass
    ORDER BY shardid LIMIT 1;
}

step "s1-copy_to_local_table"
{
	COPY table_to_distribute FROM PROGRAM 'echo 0 && echo 1 && echo 2 && echo 3 && echo 4 && echo 5 && echo 6 && echo 7 && echo 8';
}

step "s1-commit"
{
    COMMIT;
}

step "s1-rollback"
{
    ROLLBACK;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-create_distributed_table"
{
	SELECT create_distributed_table('table_to_distribute', 'id');
}

step "s2-create_distributed_table_colocated"
{
	SELECT create_distributed_table('table_to_colocate', 'id', colocate_with=>'table_to_distribute');
}

step "s2-copy_to_local_table"
{
	COPY table_to_distribute FROM PROGRAM 'echo 0 && echo 1 && echo 2 && echo 3 && echo 4 && echo 5 && echo 6 && echo 7 && echo 8';
}

step "s2-commit"
{
	COMMIT;
}

//concurrent create_distributed_table on empty table
permutation "s1-begin" "s2-begin" "s1-create_distributed_table" "s2-create_distributed_table" "s1-commit" "s2-commit"

//concurrent create_distributed_table vs. copy to table
permutation "s1-begin" "s2-begin" "s1-create_distributed_table" "s2-copy_to_local_table" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s2-copy_to_local_table" "s1-create_distributed_table" "s2-commit" "s1-commit"

//concurrent create_distributed_table on non-empty table
permutation "s1-copy_to_local_table" "s1-begin" "s2-begin" "s1-create_distributed_table" "s2-create_distributed_table" "s1-commit" "s2-commit"

//concurrent create_distributed_table vs citus_shard_property_set
permutation "s1-create_distributed_table" "s1-begin" "s2-begin" "s1_set-shard-property" "s2-create_distributed_table_colocated" "s1-rollback" "s2-commit"
permutation "s1-create_distributed_table" "s1-begin" "s2-begin" "s2-create_distributed_table_colocated" "s1_set-shard-property" "s1-rollback" "s2-commit"
