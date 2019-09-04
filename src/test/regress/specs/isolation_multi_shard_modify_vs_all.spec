setup
{	
  	SELECT citus_internal.replace_isolation_tester_func();
  	SELECT citus_internal.refresh_isolation_tester_prepared_statement();

	SET citus.shard_replication_factor to 1;
	SET citus.shard_count to 32;
	SET citus.multi_shard_modify_mode to 'parallel';

	CREATE TABLE users_test_table(user_id int, value_1 int, value_2 int, value_3 int);
	SELECT create_distributed_table('users_test_table', 'user_id');
	INSERT INTO users_test_table VALUES
	(1, 5, 6, 7),
	(2, 12, 7, 18),
	(3, 23, 8, 25),
	(4, 42, 9, 23),
	(5, 35, 10, 17),
	(6, 21, 11, 25),
	(7, 27, 12, 18);

	CREATE TABLE events_test_table (user_id int, value_1 int, value_2 int, value_3 int);
	SELECT create_distributed_table('events_test_table', 'user_id');
	INSERT INTO events_test_table VALUES
	(1, 5, 7, 7),
	(3, 11, 78, 18),
	(5, 22, 9, 25),
	(7, 41, 10, 23),
	(1, 20, 12, 25),
	(3, 26, 13, 18),
	(5, 17, 14, 4);
}

teardown
{
	DROP TABLE users_test_table;
	DROP TABLE events_test_table;
	SELECT citus_internal.restore_isolation_tester_func();
	SET citus.shard_count to 4;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-change_connection_mode_to_sequential"
{
    set citus.multi_shard_modify_mode to 'sequential';
}

step "s1-update_all_value_1"
{
	UPDATE users_test_table SET value_1 = 3;
}

step "s1-update_even_concurrently"
{
	SET citus.enable_deadlock_prevention TO off;
	UPDATE users_test_table SET value_1 = 3 WHERE user_id % 2 = 0;
	SET citus.enable_deadlock_prevention TO on;
}

step "s1-update_value_1_of_1_or_3_to_5"
{
	UPDATE users_test_table SET value_1 = 5 WHERE user_id = 1 or user_id = 3;
}

step "s1-update_value_1_of_1_or_3_to_7"
{
	UPDATE users_test_table SET value_1 = 7 WHERE user_id = 1 or user_id = 3;
}

step "s1-update_value_1_of_2_or_4_to_5"
{
	UPDATE users_test_table SET value_1 = 5 WHERE user_id = 2 or user_id = 4;
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

step "s2-change_connection_mode_to_sequential"
{
    set citus.multi_shard_modify_mode to 'sequential';
}

step "s2-select" 
{
	SELECT * FROM users_test_table ORDER BY value_2, value_3;
}

step "s2-insert-to-table"
{
	INSERT INTO users_test_table VALUES (1,2,3,4);
}

step "s2-insert-into-select"
{
	INSERT INTO users_test_table SELECT * FROM events_test_table;
}

step "s2-update_all_value_1"
{
	UPDATE users_test_table SET value_1 = 6;
}

step "s2-update_odd_concurrently"
{
	SET citus.enable_deadlock_prevention = off;
	UPDATE users_test_table SET value_1 = 3 WHERE user_id % 2 = 1;
	SET citus.enable_deadlock_prevention TO on;
}

step "s2-update_value_1_of_1_or_3_to_8"
{
	UPDATE users_test_table SET value_1 = 8 WHERE user_id = 1 or user_id = 3;
}

step "s2-update_value_1_of_4_or_6_to_4"
{
	UPDATE users_test_table SET value_1 = 4 WHERE user_id = 4 or user_id = 6;
}

step "s2-commit"
{
	COMMIT;
}

# test with parallel connections
permutation "s1-begin" "s1-update_all_value_1" "s2-begin" "s2-select" "s1-commit" "s2-select" "s2-commit"
permutation "s1-begin" "s1-update_all_value_1" "s2-begin" "s2-update_all_value_1" "s1-commit" "s2-commit"

# test without deadlock prevention (first does not conflict, second does)
permutation "s1-begin" "s1-update_even_concurrently" "s2-begin" "s2-update_odd_concurrently" "s1-commit" "s2-commit"
permutation "s1-begin" "s1-update_even_concurrently" "s2-begin" "s2-update_value_1_of_4_or_6_to_4" "s1-commit" "s2-commit"

# test with shard pruning (should not conflict)
permutation "s1-begin" "s1-update_value_1_of_1_or_3_to_5" "s2-begin" "s2-update_value_1_of_4_or_6_to_4" "s1-commit" "s2-commit" "s2-select" 
permutation "s1-begin" "s1-update_value_1_of_1_or_3_to_5" "s2-begin" "s2-update_value_1_of_1_or_3_to_8" "s1-commit" "s2-commit" "s2-select" 

# test with inserts
permutation "s1-begin" "s1-update_all_value_1" "s2-begin" "s2-insert-to-table" "s1-commit" "s2-commit" "s2-select" 
permutation "s1-begin" "s1-update_all_value_1" "s2-begin" "s2-insert-into-select" "s1-commit" "s2-commit" "s2-select"

# multi-shard update affecting the same rows
permutation "s1-begin" "s2-begin" "s1-update_value_1_of_1_or_3_to_5" "s2-update_value_1_of_1_or_3_to_8" "s1-commit" "s2-commit"
# multi-shard update affecting the different rows
permutation "s1-begin" "s2-begin" "s2-update_value_1_of_1_or_3_to_8" "s1-update_value_1_of_2_or_4_to_5" "s2-commit" "s1-commit"

# test with sequential connections, sequential tests should not block each other
# if they are targeting different shards. If multiple connections updating the same
# row, second one must wait for the first one.
permutation "s1-begin" "s1-change_connection_mode_to_sequential" "s1-update_all_value_1" "s2-begin" "s2-change_connection_mode_to_sequential" "s2-update_all_value_1" "s1-commit" "s2-commit" "s2-select"
permutation "s1-begin" "s1-change_connection_mode_to_sequential" "s1-update_value_1_of_1_or_3_to_5" "s2-begin" "s2-change_connection_mode_to_sequential" "s2-update_value_1_of_1_or_3_to_8" "s1-commit" "s2-commit" "s2-select"
permutation "s1-begin" "s1-change_connection_mode_to_sequential" "s1-update_value_1_of_1_or_3_to_5" "s2-begin" "s2-change_connection_mode_to_sequential" "s2-update_value_1_of_4_or_6_to_4" "s1-commit" "s2-commit" "s2-select"
# multi-shard update affecting the same rows
permutation "s1-begin" "s2-begin" "s1-change_connection_mode_to_sequential" "s2-change_connection_mode_to_sequential" "s1-update_value_1_of_1_or_3_to_5" "s2-update_value_1_of_1_or_3_to_8" "s1-commit" "s2-commit"
# multi-shard update affecting the different rows
permutation "s1-begin" "s2-begin" "s1-change_connection_mode_to_sequential" "s2-change_connection_mode_to_sequential" "s2-update_value_1_of_1_or_3_to_8" "s1-update_value_1_of_2_or_4_to_5" "s1-commit" "s2-commit"
