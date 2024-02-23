-- SECLABEL
--
-- Test suite for running SECURITY LABEL ON ROLE statements from non-main databases

SET citus.enable_create_database_propagation to ON;

CREATE DATABASE database1;
CREATE DATABASE database2;

\c - - - :worker_1_port
SET citus.enable_create_database_propagation to ON;
CREATE DATABASE database_w1;


\c - - - :master_port
CREATE ROLE user1;
\c database1
SHOW citus.main_db;
SHOW citus.superuser;

CREATE ROLE "user 2";

-- Set a SECURITY LABEL on a role from a non-main database
SECURITY LABEL FOR "citus '!tests_label_provider" ON ROLE user1 IS 'citus_classified';
SECURITY LABEL FOR "citus '!tests_label_provider" ON ROLE "user 2" IS 'citus_unclassified';

-- Check the result
\c regression
SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('"user 2"') ORDER BY node_type;

\c database1
-- Set a SECURITY LABEL on database, it should not be propagated
SECURITY LABEL FOR "citus '!tests_label_provider" ON DATABASE database1 IS 'citus_classified';

-- Set a SECURITY LABEL on a table, it should not be propagated
CREATE TABLE a (i int);
SECURITY LABEL ON TABLE a IS 'citus_classified';

\c regression
SELECT node_type, result FROM get_citus_tests_label_provider_labels('database1') ORDER BY node_type;

-- Check that only the SECURITY LABEL for ROLES is propagated to the non-main databases on other nodes
\c database_w1 - - :worker_1_port
SELECT provider, objtype, label, objname FROM pg_seclabels ORDER BY objname;


-- Check the result after a transaction
BEGIN;
SECURITY LABEL FOR "citus '!tests_label_provider" ON ROLE user1 IS 'citus_unclassified';
SECURITY LABEL FOR "citus '!tests_label_provider" ON DATABASE database_w1 IS 'citus_classified';
COMMIT;

\c regression
SELECT node_type, result FROM get_citus_tests_label_provider_labels('database_w1') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels('user1') ORDER BY node_type;

BEGIN;
SECURITY LABEL FOR "citus '!tests_label_provider" ON ROLE "user 2" IS 'citus_classified';
ROLLBACK;

SELECT node_type, result FROM get_citus_tests_label_provider_labels('"user 2"') ORDER BY node_type;

-- clean up
SET citus.enable_create_database_propagation to ON;
DROP DATABASE database1;
DROP DATABASE database2;
DROP DATABASE database_w1;
DROP ROLE user1;
DROP ROLE "user 2";
RESET citus.enable_create_database_propagation;
