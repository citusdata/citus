setup
{	
  	CREATE TABLE table_to_delete_from(id int);

  	SELECT create_distributed_table('table_to_delete_from', 'id', 'append');

  	COPY table_to_delete_from FROM PROGRAM 'echo "0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10"';
}

teardown
{
	DROP TABLE table_to_delete_from CASCADE;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-master_apply_delete_command_all_shard"
{
   	SELECT master_apply_delete_command($$DELETE FROM table_to_delete_from WHERE id >= 0$$);
}

step "s1-master_apply_delete_command_row"
{
   	SELECT master_apply_delete_command($$DELETE FROM table_to_delete_from WHERE id >= 0 and id < 3$$);
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

step "s2-master_apply_delete_command_all_shard"
{
   	SELECT master_apply_delete_command($$DELETE FROM table_to_delete_from WHERE id >= 0$$);
}

step "s2-master_apply_delete_command_row"
{
   	SELECT master_apply_delete_command($$DELETE FROM table_to_delete_from WHERE id >= 0 and id < 3$$);
}

step "s2-commit"
{
	COMMIT;
}

#concurrent master_apply_delete_command vs master_apply_delete_command
permutation "s1-begin" "s2-begin" "s1-master_apply_delete_command_all_shard" "s2-master_apply_delete_command_all_shard" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-master_apply_delete_command_all_shard" "s2-master_apply_delete_command_row" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-master_apply_delete_command_row" "s2-master_apply_delete_command_all_shard" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-master_apply_delete_command_row" "s2-master_apply_delete_command_row" "s1-commit" "s2-commit"
