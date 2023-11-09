--
-- SECLABEL
--
-- Test suite for SECURITY LABEL ON ROLE statements
--

-- first we remove one of the worker nodes to be able to test
-- citus_add_node later
SELECT citus_remove_node('localhost', :worker_2_port);

CREATE ROLE user1;

-- check an invalid label for our current dummy hook citus_test_object_relabel
SECURITY LABEL ON ROLE user1 IS 'invalid_label';

-- if we disable metadata_sync, the command will not be propagated
SET citus.enable_metadata_sync TO off;
SECURITY LABEL ON ROLE user1 IS 'citus_unclassified';
SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1');

RESET citus.enable_metadata_sync;

-- check that we only support propagating for roles
SET citus.shard_replication_factor to 1;
CREATE TABLE a (a int);
SELECT create_distributed_table('a', 'a');
SECURITY LABEL ON TABLE a IS 'citus_classified';
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a');
DROP TABLE a;

SET citus.log_remote_commands TO on;
SET citus.grep_remote_commands = '%SECURITY LABEL%';

-- then we run a security label statement which will use the same connection to the worker node
-- it should finish successfully
SECURITY LABEL for citus_tests_label_provider ON ROLE user1 IS 'citus_classified';
SECURITY LABEL for citus_tests_label_provider ON ROLE user1 IS NULL;
SECURITY LABEL for citus_tests_label_provider ON ROLE user1 IS 'citus_unclassified';

RESET citus.log_remote_commands;

SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1');

-- adding a new node will fail because the label provider is not there
-- however, this is enough for testing as we can see that the SECURITY LABEL commands
-- will be propagated when adding a new node
SET citus.log_remote_commands TO on;
SET citus.grep_remote_commands = '%SECURITY LABEL%';
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1');

-- cleanup
RESET citus.log_remote_commands;
DROP ROLE user1;
