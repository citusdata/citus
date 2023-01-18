// Create and use UDF to send commands from the same connection. Also make the cluster
// ready for testing MX functionalities.
setup
{
    SET citus.enable_metadata_sync TO off;
    CREATE OR REPLACE FUNCTION start_session_level_connection_to_node(text, integer)
        RETURNS void
        LANGUAGE C STRICT VOLATILE
        AS 'citus', $$start_session_level_connection_to_node$$;

    CREATE OR REPLACE FUNCTION override_backend_data_gpid(bigint)
        RETURNS void
        LANGUAGE C STRICT IMMUTABLE
        AS 'citus', $$override_backend_data_gpid$$;

    SELECT run_command_on_workers($$SET citus.enable_metadata_sync TO off;CREATE OR REPLACE FUNCTION override_backend_data_gpid(bigint)
        RETURNS void
        LANGUAGE C STRICT IMMUTABLE
        AS 'citus'$$);

    CREATE OR REPLACE FUNCTION run_commands_on_session_level_connection_to_node(text)
        RETURNS void
        LANGUAGE C STRICT VOLATILE
        AS 'citus', $$run_commands_on_session_level_connection_to_node$$;

    CREATE OR REPLACE FUNCTION stop_session_level_connection_to_node()
        RETURNS void
        LANGUAGE C STRICT VOLATILE
        AS 'citus', $$stop_session_level_connection_to_node$$;
    RESET citus.enable_metadata_sync;

    -- start_metadata_sync_to_node can not be run inside a transaction block
    -- following is a workaround to overcome that
    -- port numbers are hard coded at the moment
    SELECT master_run_on_worker(
            ARRAY['localhost']::text[],
            ARRAY[57636]::int[],
            ARRAY[format('SELECT start_metadata_sync_to_node(''%s'', %s)', nodename, nodeport)]::text[],
            false)
    FROM pg_dist_node;

	SET citus.shard_replication_factor TO 1;
}
