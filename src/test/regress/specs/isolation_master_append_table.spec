setup
{	
  	CREATE TABLE table_to_append(id int);
  	CREATE TABLE table_to_be_appended(id int);
  	CREATE TABLE table_to_distribute(id int);

  	SELECT create_distributed_table('table_to_append', 'id', 'append');
  	INSERT INTO table_to_be_appended VALUES(1),(2),(3),(4),(5),(6);
  	INSERT INTO table_to_distribute SELECT generate_series(1,100);

  	COPY table_to_append FROM PROGRAM 'echo "0\n7\n8\n9\n10"';
}

teardown
{
	DROP TABLE table_to_append CASCADE;
	DROP TABLE table_to_be_appended CASCADE;
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

step "s1-master_append_table_to_shard"
{
   	SELECT	
   		master_append_table_to_shard(shardid, 'table_to_be_appended', 'localhost', 57636)
	FROM
		pg_dist_shard
	WHERE
		'table_to_append'::regclass::oid = logicalrelid;
}

step "s1-master_apply_delete_command"
{
   	SELECT master_apply_delete_command($$DELETE FROM table_to_append WHERE id >= 0$$);
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

step "s2-master_append_table_to_shard"
{

   	SELECT	
   		master_append_table_to_shard(shardid, 'table_to_be_appended', 'localhost', 57636)
	FROM
		pg_dist_shard
	WHERE
		'table_to_append'::regclass::oid = logicalrelid;
}

step "s2-master_apply_delete_command"
{
   	SELECT master_apply_delete_command($$DELETE FROM table_to_append WHERE id >= 0$$);
}

step "s2-commit"
{
	COMMIT;
}

# concurrent master_append_table_to_shard tests
permutation "s1-begin" "s2-begin" "s1-master_append_table_to_shard" "s2-master_append_table_to_shard" "s1-commit" "s2-commit"

#concurrent create_distributed_table
permutation "s1-begin" "s2-begin" "s1-create_distributed_table" "s2-create_distributed_table" "s1-commit" "s2-commit"

# concurrent master_append_table_to_shard vs master_apply_delete_command tests
permutation "s1-begin" "s2-begin" "s1-master_append_table_to_shard" "s2-master_apply_delete_command" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-master_apply_delete_command" "s2-master_append_table_to_shard" "s1-commit" "s2-commit"

#concurrent master_apply_delete_command vs master_apply_delete_command
permutation "s1-begin" "s2-begin" "s1-master_apply_delete_command" "s2-master_apply_delete_command" "s1-commit" "s2-commit"