setup
{
	SET citus.shard_replication_factor TO 1;

	CREATE TABLE test_table(column1 int, column2 int);
	SELECT create_distributed_table('test_table', 'column1');

	CREATE USER test_user_1;
	SELECT run_command_on_workers('CREATE USER test_user_1');

	CREATE USER test_user_2;
	SELECT run_command_on_workers('CREATE USER test_user_2');

	CREATE USER test_readonly;
	SELECT run_command_on_workers('CREATE USER test_readonly');

	CREATE USER test_monitor;
	SELECT run_command_on_workers('CREATE USER test_monitor');

	GRANT pg_monitor TO test_monitor;
	SELECT run_command_on_workers('GRANT pg_monitor TO test_monitor');
}

teardown
{
	DROP TABLE test_table;
	DROP USER test_user_1, test_user_2, test_readonly, test_monitor;
	SELECT run_command_on_workers('DROP USER test_user_1, test_user_2, test_readonly, test_monitor');
}

session "s1"

// run_command_on_placements is done in a separate step because the setup is executed as a single transaction
step "s1-grant"
{
	GRANT ALL ON test_table TO test_user_1;
	SELECT bool_and(success) FROM run_command_on_placements('test_table', 'GRANT ALL ON TABLE %s TO test_user_1');

	GRANT ALL ON test_table TO test_user_2;
	SELECT bool_and(success) FROM run_command_on_placements('test_table', 'GRANT ALL ON TABLE %s TO test_user_2');
}

step "s1-begin-insert"
{
	BEGIN;
	SET ROLE test_user_1;
	INSERT INTO test_table VALUES (100, 100);
}

step "s1-commit"
{
	COMMIT;
}

session "s2"

step "s2-begin-insert"
{
	BEGIN;
	SET ROLE test_user_2;
	INSERT INTO test_table VALUES (200, 200);
}

step "s2-commit"
{
	COMMIT;
}

session "s3"

step "s3-as-admin"
{
	-- Admin should be able to see all transactions
	SELECT count(*) FROM get_all_active_transactions() WHERE datid != 0;
	SELECT count(*) FROM get_global_active_transactions() WHERE datid != 0;
}

step "s3-as-user-1"
{
	-- User should only be able to see its own transactions
	SET ROLE test_user_1;
	SELECT count(*) FROM get_all_active_transactions() WHERE datid != 0;
	SELECT count(*) FROM get_global_active_transactions() WHERE datid != 0;
}

step "s3-as-readonly"
{
	-- Other user should not see transactions
	SET ROLE test_readonly;
	SELECT count(*) FROM get_all_active_transactions() WHERE datid != 0;
	SELECT count(*) FROM get_global_active_transactions() WHERE datid != 0;
}

step "s3-as-monitor"
{
	-- Monitor should see all transactions
	SET ROLE test_monitor;
	SELECT count(*) FROM get_all_active_transactions() WHERE datid != 0;
	SELECT count(*) FROM get_global_active_transactions() WHERE datid != 0;
}

permutation "s1-grant" "s1-begin-insert" "s2-begin-insert" "s3-as-admin" "s3-as-user-1" "s3-as-readonly" "s3-as-monitor" "s1-commit" "s2-commit"
