setup
{	
  	CREATE TABLE table_to_distribute(id int);
}

teardown
{
	DROP TABLE table_to_distribute CASCADE;
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

step "s1-copy_to_local_table"
{
	INSERT INTO table_to_distribute SELECT s FROM generate_series(0, 8) s;
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

step "s2-create_distributed_table"
{
	SELECT create_distributed_table('table_to_distribute', 'id');
}

step "s2-copy_to_local_table"
{
	INSERT INTO table_to_distribute SELECT s FROM generate_series(0, 8) s;
}

step "s2-commit"
{
	COMMIT;
}

#concurrent create_distributed_table on empty table
permutation "s1-begin" "s2-begin" "s1-create_distributed_table" "s2-create_distributed_table" "s1-commit" "s2-commit"

#concurrent create_distributed_table vs. copy to table
permutation "s1-begin" "s2-begin" "s1-create_distributed_table" "s2-copy_to_local_table" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s2-copy_to_local_table" "s1-create_distributed_table" "s2-commit" "s1-commit"

#concurrent create_distributed_table on non-empty table
permutation "s1-copy_to_local_table" "s1-begin" "s2-begin" "s1-create_distributed_table" "s2-create_distributed_table" "s1-commit" "s2-commit"
