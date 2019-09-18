# Create and use UDF to send commands from the same connection. Also make the cluster
# ready for testing MX functionalities.
setup
{
        CREATE OR REPLACE FUNCTION start_session_level_connection_to_node(text, integer)
            RETURNS void
            LANGUAGE C STRICT VOLATILE
            AS 'citus', $$start_session_level_connection_to_node$$;

        CREATE OR REPLACE FUNCTION run_commands_on_session_level_connection_to_node(text)
            RETURNS void
            LANGUAGE C STRICT VOLATILE
            AS 'citus', $$run_commands_on_session_level_connection_to_node$$;

        CREATE OR REPLACE FUNCTION stop_session_level_connection_to_node()
            RETURNS void
            LANGUAGE C STRICT VOLATILE
            AS 'citus', $$stop_session_level_connection_to_node$$;

        SELECT citus_internal.replace_isolation_tester_func();
        SELECT citus_internal.refresh_isolation_tester_prepared_statement();

        -- start_metadata_sync_to_node can not be run inside a transaction block
        -- following is a workaround to overcome that
        -- port numbers are hard coded at the moment
        SELECT master_run_on_worker(
                ARRAY['localhost']::text[],
                ARRAY[57636]::int[],
                ARRAY[format('SELECT start_metadata_sync_to_node(''%s'', %s)', nodename, nodeport)]::text[],
                false)
        FROM pg_dist_node;

        SET citus.replication_model to streaming;
	SET citus.shard_replication_factor TO 1;

	CREATE TABLE dist_table(id integer, value integer);
	SELECT create_distributed_table('dist_table', 'id');
}

# Create and use UDF to close the connection opened in the setup step. Also return the cluster
# back to the initial state.
teardown
{
        DROP TABLE IF EXISTS dist_table CASCADE;
        SELECT citus_internal.restore_isolation_tester_func();
}

session "s1"

# We do not need to begin a transaction on coordinator, since it will be open on workers.

step "s1-start-session-level-connection"
{
        SELECT start_session_level_connection_to_node('localhost', 57637);
}

step "s1-begin-on-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s1-update"
{
	SELECT run_commands_on_session_level_connection_to_node('UPDATE dist_table SET value=15 WHERE id=5');
}

step "s1-delete"
{
	SELECT run_commands_on_session_level_connection_to_node('DELETE FROM dist_table WHERE id=5');
}

step "s1-commit-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "s1-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}


session "s2"

# We do not need to begin a transaction on coordinator, since it will be open on workers.

step "s2-start-session-level-connection"
{
        SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "s2-begin-on-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s2-delete"
{
	SELECT run_commands_on_session_level_connection_to_node('DELETE FROM dist_table WHERE id=5');
}

step "s2-copy"
{
	SELECT run_commands_on_session_level_connection_to_node('COPY dist_table FROM PROGRAM ''echo 5, 50 && echo 9, 90 && echo 10, 100''WITH CSV');
}

step "s2-alter-table"
{
	ALTER TABLE dist_table DROP value;
}

step "s2-select-for-update"
{
	SELECT run_commands_on_session_level_connection_to_node('SELECT * FROM dist_table WHERE id=5 FOR UPDATE');
}

step "s2-coordinator-create-index-concurrently"
{
	CREATE INDEX CONCURRENTLY dist_table_index ON dist_table(id);
}

step "s2-commit-worker"
{
        SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

step "s2-stop-connection"
{
        SELECT stop_session_level_connection_to_node();
}


session "s3"

step "s3-select-count"
{
	SELECT COUNT(*) FROM dist_table;
}


permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-delete" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-copy" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-alter-table" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection" "s3-select-count"
permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-update" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-select-for-update" "s1-commit-worker" "s2-commit-worker" "s1-stop-connection" "s2-stop-connection"
#Not able to test the next permutation, until issue with CREATE INDEX CONCURRENTLY's locks is resolved. Issue #2966 
#permutation "s1-start-session-level-connection" "s1-begin-on-worker" "s1-delete" "s2-coordinator-create-index-concurrently" "s1-commit-worker" "s3-select-count" "s1-stop-connection"
