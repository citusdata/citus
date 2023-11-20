--
-- SECLABEL
--
-- Test suite for SECURITY LABEL ON ROLE statements
--

-- first we remove one of the worker nodes to be able to test
-- citus_add_node later
SELECT citus_remove_node('localhost', :worker_2_port);

-- create two roles, one with characters that need escaping
CREATE ROLE user1;
CREATE ROLE "user 2";

-- check an invalid label for our current dummy hook citus_test_object_relabel
SECURITY LABEL FOR "citus '!tests_label_provider" ON ROLE user1 IS 'invalid_label';

-- if we disable metadata_sync, the command will not be propagated
SET citus.enable_metadata_sync TO off;
SECURITY LABEL FOR "citus '!tests_label_provider" ON ROLE user1 IS 'citus_unclassified';
SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;

RESET citus.enable_metadata_sync;

-- check that we only support propagating for roles
SET citus.shard_replication_factor to 1;
-- distributed table
CREATE TABLE a (a int);
SELECT create_distributed_table('a', 'a');
-- distributed view
CREATE VIEW v_dist AS SELECT * FROM a;
-- distributed function
CREATE FUNCTION notice(text) RETURNS void LANGUAGE plpgsql AS $$
    BEGIN RAISE NOTICE '%', $1; END; $$;

SECURITY LABEL ON TABLE a IS 'citus_classified';
SECURITY LABEL ON FUNCTION notice IS 'citus_unclassified';
SECURITY LABEL ON VIEW v_dist IS 'citus_classified';

SELECT node_type, result FROM get_citus_tests_label_provider_labels('a') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('notice(text)') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('v_dist') ORDER BY node_type;

\c - - - :worker_1_port
SECURITY LABEL ON TABLE a IS 'citus_classified';
SECURITY LABEL ON FUNCTION notice IS 'citus_unclassified';
SECURITY LABEL ON VIEW v_dist IS 'citus_classified';

\c - - - :master_port
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('notice(text)') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('v_dist') ORDER BY node_type;

DROP TABLE a CASCADE;
DROP FUNCTION notice;

-- test that SECURITY LABEL statement is actually propagated for ROLES
SET citus.log_remote_commands TO on;
SET citus.grep_remote_commands = '%SECURITY LABEL%';

-- we have exactly one provider loaded, so we may not include the provider in the command
SECURITY LABEL for "citus '!tests_label_provider" ON ROLE user1 IS 'citus_classified';
SECURITY LABEL ON ROLE user1 IS NULL;
SECURITY LABEL ON ROLE user1 IS 'citus_unclassified';
SECURITY LABEL for "citus '!tests_label_provider" ON ROLE "user 2" IS 'citus ''!unclassified';

\c - - - :worker_1_port
-- command not allowed from worker node
SECURITY LABEL for "citus '!tests_label_provider" ON ROLE user1 IS 'citus ''!unclassified';

\c - - - :master_port
RESET citus.log_remote_commands;

SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"user 2"') ORDER BY node_type;

-- add a new node and check that it also propagates the SECURITY LABEL statement to the new node
SET citus.log_remote_commands TO on;
SET citus.grep_remote_commands = '%SECURITY LABEL%';
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"user 2"') ORDER BY node_type;

-- cleanup
RESET citus.log_remote_commands;
DROP ROLE user1, "user 2";
