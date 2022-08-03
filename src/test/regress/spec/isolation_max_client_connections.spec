setup
{
	SET citus.shard_replication_factor TO 1;

	CREATE USER my_user;
	SELECT run_command_on_workers('CREATE USER my_user');

    CREATE TABLE my_table (test_id integer NOT NULL, data text);
    SELECT create_distributed_table('my_table', 'test_id');

	GRANT USAGE ON SCHEMA public TO my_user;
	GRANT SELECT ON TABLE my_table TO my_user;

	SET citus.enable_ddl_propagation TO OFF;
	CREATE FUNCTION make_external_connection_to_node(text,int,text,text)
	RETURNS void
	AS 'citus'
	LANGUAGE C STRICT;
	RESET citus.enable_ddl_propagation;

	SELECT run_command_on_workers('ALTER SYSTEM SET citus.max_client_connections TO 1');
	SELECT run_command_on_workers('SELECT pg_reload_conf()');
}

teardown
{
	SELECT run_command_on_workers('ALTER SYSTEM RESET citus.max_client_connections');
	SELECT run_command_on_workers('SELECT pg_reload_conf()');
	DROP TABLE my_table;
}

session "s1"

// Setup runs as a transaction, so run_command_on_placements must be separate
step "s1-grant"
{
	SELECT result FROM run_command_on_placements('my_table', 'GRANT SELECT ON TABLE %s TO my_user');
}

// Open one external connection as non-superuser, is allowed
step "s1-connect"
{
	SELECT make_external_connection_to_node('localhost', 57637, 'my_user', current_database());
}

session "s2"

// Open another external connection as non-superuser, not allowed
step "s2-connect"
{
	SELECT make_external_connection_to_node('localhost', 57637, 'my_user', current_database());
}

// Open another external connection as superuser, allowed
step "s2-connect-superuser"
{
	SELECT make_external_connection_to_node('localhost', 57637, 'postgres', current_database());
}

session "s3"

// Open internal connections as non-superuser, allowed
step "s3-select"
{
	SET ROLE my_user;
	SELECT count(*) FROM my_table;
}

permutation "s1-grant" "s1-connect" "s2-connect" "s2-connect-superuser" "s3-select"
