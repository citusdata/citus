CREATE SCHEMA other_databases;
SET search_path TO other_databases;

SET citus.next_shard_id TO 10231023;

CREATE DATABASE other_db1;

\c other_db1
SHOW citus.main_db;

-- check that empty citus.superuser gives error
SET citus.superuser TO '';
CREATE USER empty_superuser;
SET citus.superuser TO 'postgres';

CREATE USER other_db_user1;
CREATE USER other_db_user2;

BEGIN;
CREATE USER other_db_user3;
CREATE USER other_db_user4;
COMMIT;

BEGIN;
CREATE USER other_db_user5;
CREATE USER other_db_user6;
ROLLBACK;

BEGIN;
CREATE USER other_db_user7;
SELECT 1/0;
COMMIT;

CREATE USER other_db_user8;

\c regression
SELECT usename FROM pg_user WHERE usename LIKE 'other\_db\_user%' ORDER BY 1;

\c - - - :worker_1_port
SELECT usename FROM pg_user WHERE usename LIKE 'other\_db\_user%' ORDER BY 1;

\c - - - :master_port
-- some user creation commands will fail but let's make sure we try to drop them just in case
DROP USER IF EXISTS other_db_user1, other_db_user2, other_db_user3, other_db_user4, other_db_user5, other_db_user6, other_db_user7, other_db_user8;

-- Make sure non-superuser roles cannot use internal GUCs
-- but they can still create a role
CREATE USER nonsuperuser CREATEROLE;
GRANT ALL ON SCHEMA citus_internal TO nonsuperuser;
SET ROLE nonsuperuser;
SELECT citus_internal.execute_command_on_remote_nodes_as_user($$SELECT 'dangerous query'$$, 'postgres');

\c other_db1
SET citus.local_hostname TO '127.0.0.1';
SET ROLE nonsuperuser;

-- Make sure that we don't try to access pg_dist_node.
-- Otherwise, we would get the following error:
--   ERROR:  cache lookup failed for pg_dist_node, called too early?
CREATE USER other_db_user9;

RESET ROLE;
RESET citus.local_hostname;
RESET ROLE;
\c regression
SELECT usename FROM pg_user WHERE usename LIKE 'other\_db\_user%' ORDER BY 1;

\c - - - :worker_1_port
SELECT usename FROM pg_user WHERE usename LIKE 'other\_db\_user%' ORDER BY 1;

\c - - - :master_port
REVOKE ALL ON SCHEMA citus_internal FROM nonsuperuser;
DROP USER other_db_user9, nonsuperuser;

-- test from a worker
\c - - - :worker_1_port

CREATE DATABASE other_db2;

\c other_db2

CREATE USER worker_user1;

BEGIN;
CREATE USER worker_user2;
COMMIT;

BEGIN;
CREATE USER worker_user3;
ROLLBACK;

\c regression
SELECT usename FROM pg_user WHERE usename LIKE 'worker\_user%' ORDER BY 1;

\c - - - :master_port
SELECT usename FROM pg_user WHERE usename LIKE 'worker\_user%' ORDER BY 1;

-- some user creation commands will fail but let's make sure we try to drop them just in case
DROP USER IF EXISTS worker_user1, worker_user2, worker_user3;

\c - - - :worker_1_port
DROP DATABASE other_db2;
\c - - - :master_port

DROP SCHEMA other_databases;
DROP DATABASE other_db1;
