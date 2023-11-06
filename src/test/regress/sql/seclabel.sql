SELECT citus_remove_node('localhost', :worker_2_port);

CREATE FUNCTION citus_test_register_label_provider()
  RETURNS void
  LANGUAGE C
  AS 'citus', $$citus_test_register_label_provider$$;

SELECT citus_test_register_label_provider();
CREATE ROLE user1;

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

ALTER SYSTEM SET citus.max_cached_conns_per_worker TO 2;

SELECT start_session_level_connection_to_node('localhost', :worker_1_port);

SELECT run_commands_on_session_level_connection_to_node('SELECT citus_test_register_label_provider()');

SET citus.log_remote_commands TO on;

SECURITY LABEL for citus_tests_label_provider ON ROLE user1 IS 'citus_classified';

RESET citus.log_remote_commands;

SELECT stop_session_level_connection_to_node();

ALTER SYSTEM RESET citus.max_cached_conns_per_worker;

DROP FUNCTION stop_session_level_connection_to_node, run_commands_on_session_level_connection_to_node,
              override_backend_data_gpid, start_session_level_connection_to_node;

DROP FUNCTION citus_test_register_label_provider;

DROP ROLE user1;

SELECT 1 FROM citus_add_node('localhost', :worker_2_port);
