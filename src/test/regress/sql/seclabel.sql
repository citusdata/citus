--
-- SECLABEL
--
-- Test suite for SECURITY LABEL statements:
-- SECURITY LABEL ON <object> IS <definition>
--
-- Citus can propagate ROLE, TABLE and COLUMN objects

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

-- check that we only support propagating for roles, tables and columns;
-- support for VIEW and FUNCTION is not there (yet)
SET citus.shard_replication_factor to 1;
-- distributed table
CREATE TABLE a (a int);
SELECT create_distributed_table('a', 'a');
-- distributed view
CREATE VIEW v_dist AS SELECT * FROM a;
-- distributed function
CREATE FUNCTION notice(text) RETURNS void LANGUAGE plpgsql AS $$
    BEGIN RAISE NOTICE '%', $1; END; $$;

SECURITY LABEL ON FUNCTION notice IS 'citus_unclassified';
SECURITY LABEL ON VIEW v_dist IS 'citus_classified';

SELECT node_type, result FROM get_citus_tests_label_provider_labels('notice(text)') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('v_dist') ORDER BY node_type;

\c - - - :worker_1_port
SECURITY LABEL ON FUNCTION notice IS 'citus_unclassified';
SECURITY LABEL ON VIEW v_dist IS 'citus_classified';

\c - - - :master_port
SELECT node_type, result FROM get_citus_tests_label_provider_labels('notice(text)') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('v_dist') ORDER BY node_type;

DROP FUNCTION notice;

-- test that SECURITY LABEL statement is actually propagated for ROLES, TABLES and COLUMNS
SET citus.log_remote_commands TO on;
SET citus.grep_remote_commands = '%SECURITY LABEL%';

-- we have exactly one provider loaded, so we may not include the provider in the command
SECURITY LABEL for "citus '!tests_label_provider" ON ROLE user1 IS 'citus_classified';
SECURITY LABEL ON ROLE user1 IS NULL;
SECURITY LABEL ON ROLE user1 IS 'citus_unclassified';
SECURITY LABEL for "citus '!tests_label_provider" ON ROLE "user 2" IS 'citus_classified';

SECURITY LABEL ON TABLE a IS 'citus_classified';
SECURITY LABEL for "citus '!tests_label_provider" ON COLUMN a.a IS 'citus_classified';

-- ROLE, TABLE and COLUMN should be propagated to the worker
SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"user 2"') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a.a') ORDER BY node_type;

\c - - - :worker_1_port
SET citus.log_remote_commands TO on;
SET citus.grep_remote_commands = '%SECURITY LABEL%';
-- command from the worker node should be propagated to the coordinator
SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;
SECURITY LABEL for "citus '!tests_label_provider" ON ROLE user1 IS 'citus_classified';
SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;

SELECT node_type, result FROM get_citus_tests_label_provider_labels('a.a') ORDER BY node_type;
SECURITY LABEL for "citus '!tests_label_provider" ON COLUMN a.a IS 'citus ''!unclassified';
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a.a') ORDER BY node_type;

RESET citus.log_remote_commands;
SECURITY LABEL for "citus '!tests_label_provider" ON ROLE "user 2" IS 'citus ''!unclassified';
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"user 2"') ORDER BY node_type;
\c - - - :master_port

SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a.a') ORDER BY node_type;

SET citus.shard_replication_factor to 1;

-- Distributed table with delimited identifiers
CREATE TABLE "Dist T" ("col.1" int); 
SELECT create_distributed_table('"Dist T"', 'col.1');

SECURITY LABEL ON TABLE "Dist T" IS 'citus_classified';
SECURITY LABEL ON COLUMN "Dist T"."col.1" IS 'citus_classified';

SELECT node_type, result FROM get_citus_tests_label_provider_labels('"Dist T"') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"Dist T".col.1') ORDER BY node_type;

-- Add and Drop column 
CREATE TABLE tddl (a1 int, b1 int, c1 int); 
SELECT create_distributed_table('tddl', 'c1'); 

ALTER TABLE tddl ADD COLUMN d1 varchar(128); 

-- Security label on tddl.d1 is propagated to all nodes 
SECURITY LABEL ON COLUMN tddl.d1 IS 'citus_classified'; 
SELECT node_type, result FROM get_citus_tests_label_provider_labels('tddl.d1') ORDER BY node_type;

-- Drop column d1, security label should be removed from all nodes
ALTER TABLE tddl DROP COLUMN d1;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('tddl.d1') ORDER BY node_type;

-- Define security labels before distributed table creation
CREATE TABLE tb (a1 int, b1 int, c1 int);
SECURITY LABEL ON TABLE tb IS 'citus_classified';
SECURITY LABEL ON COLUMN tb.a1 IS 'citus_classified';

SELECT create_distributed_table('tb', 'a1');

SELECT node_type, result FROM get_citus_tests_label_provider_labels('tb') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('tb.a1') ORDER BY node_type;

-- Similar test with reference table; security labels should be propagated to the worker.
CREATE TABLE tref (a1 int, b1 int, c1 int);
SECURITY LABEL ON TABLE tref IS 'citus_classified';
SECURITY LABEL ON COLUMN tref.b1 IS 'citus_classified';

SELECT create_reference_table('tref');

SELECT node_type, result FROM get_citus_tests_label_provider_labels('tref') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('tref.b1') ORDER BY node_type;

-- Distributed table with delimited identifiers - 2
CREATE TABLE "Dist T2" ("col one" int);
SELECT create_distributed_table('"Dist T2"', 'col one');

SECURITY LABEL ON TABLE "Dist T2" IS 'citus_classified';
SECURITY LABEL ON COLUMN "Dist T2"."col one" IS 'citus_classified';

SELECT node_type, result FROM get_citus_tests_label_provider_labels('"Dist T2"') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"Dist T2".col one') ORDER BY node_type;

-- TODO: repeat the table and column tests using an explicit schema
-- eg CREATE SCHEMA label_test; SET search_path TO label_test; -- repeat tests

-- add a new node and check that it also propagates the SECURITY LABEL statement to the new node
SET citus.log_remote_commands TO on;
SET citus.grep_remote_commands = '%SECURITY LABEL%';
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"user 2"') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a.a') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"Dist T"') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"Dist T".col.1') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"Dist T2"') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"Dist T2"."col one"') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('tb') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('tb.a1') ORDER BY node_type;

-- disable the GUC and check that the command is not propagated
SET citus.enable_alter_role_propagation TO off;
SECURITY LABEL ON ROLE user1 IS 'citus_unclassified';
SECURITY LABEL ON TABLE a IS 'citus_unclassified';
SECURITY LABEL ON COLUMN a.a IS 'citus_classified';

SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a.a') ORDER BY node_type;

\c - - - :worker_2_port
SET citus.log_remote_commands TO on;
SET citus.grep_remote_commands = '%SECURITY LABEL%';
SET citus.enable_alter_role_propagation TO off;
SECURITY LABEL ON ROLE user1 IS 'citus ''!unclassified';
SECURITY LABEL ON TABLE a IS 'citus ''!unclassified';
SECURITY LABEL ON COLUMN a.a IS 'citus_unclassified';

SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('a.a') ORDER BY node_type;

RESET citus.enable_alter_role_propagation;

\c - - - :master_port
-- cleanup
DROP TABLE a CASCADE;
DROP TABLE "Dist T" CASCADE;
DROP TABLE "Dist T2" CASCADE;
DROP TABLE tb CASCADE;
DROP TABLE tref CASCADE;
DROP TABLE tddl CASCADE;
RESET citus.log_remote_commands;
DROP ROLE user1, "user 2";
