setup
{
	SET citus.shard_replication_factor to 2;
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
	SET citus.shard_replication_factor to 1;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-insert_to_events_test_table"
{
    INSERT INTO events_test_table VALUES(4,6,8,10);
}

step "s1-update_events_test_table"
{
	UPDATE users_test_table SET value_1 = 3;
}

step "s1-delete_events_test_table"
{
	DELETE FROM events_test_table WHERE user_id = 1 or user_id = 3;
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

step "s2-modify_with_subquery_v1"
{
    UPDATE users_test_table SET value_2 = 5 FROM events_test_table WHERE users_test_table.user_id = events_test_table.user_id;
}

step "s2-commit"
{
	COMMIT;
}

// tests to check locks on subqueries are taken
permutation "s1-begin" "s2-begin" "s2-modify_with_subquery_v1" "s1-insert_to_events_test_table" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-modify_with_subquery_v1" "s1-update_events_test_table" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s2-modify_with_subquery_v1" "s1-delete_events_test_table" "s2-commit" "s1-commit"
permutation "s1-begin" "s2-begin" "s1-insert_to_events_test_table" "s2-modify_with_subquery_v1" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-update_events_test_table" "s2-modify_with_subquery_v1" "s1-commit" "s2-commit"
permutation "s1-begin" "s2-begin" "s1-delete_events_test_table" "s2-modify_with_subquery_v1" "s1-commit" "s2-commit"

