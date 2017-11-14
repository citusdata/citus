setup
{	
	ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 230000;

  	CREATE TABLE table_to_append(id int);
  	CREATE TABLE table_to_be_appended(id int);

  	SELECT create_distributed_table('table_to_append', 'id', 'append');
  	INSERT INTO table_to_be_appended VALUES(0),(1),(2),(3),(4),(5);

  	COPY table_to_append FROM PROGRAM 'echo "1\n2\n3\n4\n5"';
}

teardown
{
	DROP TABLE table_to_append CASCADE;
	DROP TABLE table_to_be_appended CASCADE;
}

session "s1"

step "s1-begin"
{
    BEGIN;
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

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-begin"
{
	BEGIN;
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
step "s2-commit"
{
	COMMIT;
}

permutation "s1-begin" "s2-begin" "s1-master_append_table_to_shard" "s2-master_append_table_to_shard" "s1-commit" "s2-commit"