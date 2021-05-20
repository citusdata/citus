setup
{
   CREATE OR REPLACE FUNCTION wake_up_connection_pool_waiters()
   RETURNS void
   LANGUAGE C STABLE STRICT
   AS 'citus', $$wake_up_connection_pool_waiters$$;

   CREATE OR REPLACE FUNCTION set_max_shared_pool_size(int)
   RETURNS void
   LANGUAGE C STABLE STRICT
   AS 'citus', $$set_max_shared_pool_size$$;

   CREATE TABLE test (a int, b  int);
   SET citus.shard_count TO 32;
   SELECT create_distributed_table('test', 'a');
   INSERT INTO test SELECT i, i FROM generate_series(0,100)i;
}

teardown
{

	 SELECT set_max_shared_pool_size(100);
	DROP FUNCTION wake_up_connection_pool_waiters();
	DROP FUNCTION set_max_shared_pool_size(int);
	DROP TABLE test;
}

session "s1"


step "s1-begin"
{
	BEGIN;
}

step "s1-count-slow"
{
	SELECT pg_sleep(0.1), count(*) FROM test;
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-select"
{
       SELECT count(*) FROM test;
}

session "s3"

step "s3-lower-pool-size"
{
	SELECT set_max_shared_pool_size(5);
}

step "s3-increase-pool-size"
{
	SELECT set_max_shared_pool_size(100);
}

permutation "s3-lower-pool-size" "s1-begin" "s1-count-slow" "s3-increase-pool-size" "s2-select" "s1-commit"

