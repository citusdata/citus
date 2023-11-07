--
-- SECLABEL
--
-- Test suite for SECURITY LABEL ON ROLE statements
--

-- first we remove one of the worker nodes to be able to test
-- citus_add_node later
SELECT citus_remove_node('localhost', :worker_2_port);

-- now we register a label provider
CREATE FUNCTION citus_test_register_label_provider()
  RETURNS void
  LANGUAGE C
  AS 'citus', $$citus_test_register_label_provider$$;
SELECT citus_test_register_label_provider();

CREATE ROLE user1;

-- check an invalid label for our current dummy hook citus_test_object_relabel
SECURITY LABEL ON ROLE user1 IS 'invalid_label';

-- if we disable metadata_sync, the command will not be propagated
SET citus.enable_metadata_sync TO off;
SECURITY LABEL ON ROLE user1 IS 'citus_unclassified';
SELECT objtype, objname, provider, label FROM pg_seclabels;

\c - - - :worker_1_port
SELECT objtype, objname, provider, label FROM pg_seclabels;

\c - - - :master_port
SELECT citus_test_register_label_provider();

-- check that we only support propagating for roles
SET citus.shard_replication_factor to 1;
CREATE TABLE a (a int);
SELECT create_distributed_table('a', 'a');
SECURITY LABEL ON TABLE a IS 'citus_classified';
DROP TABLE a;

-- the registered label provider is per session only
-- this means that we need to maintain the same connection to the worker node
-- in order for the label provider to be visible there
-- hence here we create the necessary session_level_connection_to_node functions
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

-- now we establish a connection to the worker node
SELECT start_session_level_connection_to_node('localhost', :worker_1_port);

-- with that same connection, we register the label provider in the worker node
SELECT run_commands_on_session_level_connection_to_node('SELECT citus_test_register_label_provider()');

SET citus.log_remote_commands TO on;
SET citus.grep_remote_commands = '%SECURITY LABEL%';

-- then we run a security label statement which will use the same connection to the worker node
-- it should finish successfully
SECURITY LABEL for citus_tests_label_provider ON ROLE user1 IS 'citus_classified';
SECURITY LABEL for citus_tests_label_provider ON ROLE user1 IS NULL;
SECURITY LABEL for citus_tests_label_provider ON ROLE user1 IS 'citus_unclassified';

RESET citus.log_remote_commands;
SELECT stop_session_level_connection_to_node();

\c - - - :worker_1_port
SELECT objtype, objname, provider, label FROM pg_seclabels;

\c - - - :master_port

-- adding a new node will fail because the label provider is not there
-- however, this is enough for testing as we can see that the SECURITY LABEL commands
-- will be propagated when adding a new node
SET citus.log_remote_commands TO on;
SET citus.grep_remote_commands = '%SECURITY LABEL%';
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

-- cleanup
RESET citus.log_remote_commands;
DROP FUNCTION stop_session_level_connection_to_node, run_commands_on_session_level_connection_to_node,
              override_backend_data_gpid, start_session_level_connection_to_node;
SELECT run_command_on_workers($$ DROP FUNCTION override_backend_data_gpid $$);
DROP FUNCTION citus_test_register_label_provider;
DROP ROLE user1;
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);
