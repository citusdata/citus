ALTER SYSTEM SET citus.recover_2pc_interval TO -1;
SELECT pg_reload_conf();

SELECT $definition$
CREATE OR REPLACE FUNCTION test.maintenance_worker()
    RETURNS pg_stat_activity
    LANGUAGE plpgsql
AS $$
DECLARE
   activity record;
BEGIN
    DO 'BEGIN END'; -- Force maintenance daemon to start
    -- we don't want to wait forever; loop will exit after 20 seconds
    FOR i IN 1 .. 200 LOOP
        PERFORM pg_stat_clear_snapshot();
        SELECT * INTO activity FROM pg_stat_activity
        WHERE application_name = 'Citus Maintenance Daemon' AND datname = current_database();
        IF activity.pid IS NOT NULL THEN
            RETURN activity;
        ELSE
            PERFORM pg_sleep(0.1);
        END IF ;
    END LOOP;
    -- fail if we reach the end of this loop
    raise 'Waited too long for maintenance daemon to start';
END;
$$;
$definition$ create_function_test_maintenance_worker
\gset

CREATE DATABASE db1;

SELECT oid AS db1_oid
FROM pg_database
WHERE datname = 'db1'
\gset

\c - - - :worker_1_port

CREATE DATABASE db1;

\c - - - :worker_2_port

CREATE DATABASE db1;

\c db1 - - :worker_1_port

CREATE EXTENSION citus;

\c db1 - - :worker_2_port

CREATE EXTENSION citus;

\c db1 - - :master_port

CREATE EXTENSION citus;


SELECT citus_add_node('localhost', :worker_1_port);
SELECT citus_add_node('localhost', :worker_2_port);

SELECT current_database();

CREATE SCHEMA test;
:create_function_test_maintenance_worker

-- check maintenance daemon is started
SELECT datname, current_database(),
       usename, (SELECT extowner::regrole::text FROM pg_extension WHERE extname = 'citus')
FROM test.maintenance_worker();

SELECT *
FROM pg_dist_node;

CREATE DATABASE db2;

SELECT oid AS db2_oid
FROM pg_database
WHERE datname = 'db2'
\gset

\c - - - :worker_1_port
CREATE DATABASE db2;
\c - - - :worker_2_port
CREATE DATABASE db2;


\c db2 - - :worker_1_port
CREATE EXTENSION citus;
\c db2 - - :worker_2_port
CREATE EXTENSION citus;
\c db2 - - :master_port
CREATE EXTENSION citus;

SELECT citus_add_node('localhost', :worker_1_port);
SELECT citus_add_node('localhost', :worker_2_port);

SELECT current_database();

CREATE SCHEMA test;
:create_function_test_maintenance_worker

-- check maintenance daemon is started
SELECT datname, current_database(),
       usename, (SELECT extowner::regrole::text FROM pg_extension WHERE extname = 'citus')
FROM test.maintenance_worker();

SELECT *
FROM pg_dist_node;

SELECT groupid AS worker_1_group_id
FROM pg_dist_node
WHERE nodeport = :worker_1_port;
\gset

SELECT groupid AS worker_2_group_id
FROM pg_dist_node
WHERE nodeport = :worker_2_port;
\gset

-- Prepare transactions on first database
\c db1 - - :worker_1_port

BEGIN;
CREATE TABLE should_abort
(
    value int
);
SELECT 'citus_0_1234_0_0_' || :'db1_oid' AS transaction_1_worker_1_db_1_name
\gset
PREPARE TRANSACTION :'transaction_1_worker_1_db_1_name';

BEGIN;
CREATE TABLE should_commit
(
    value int
);
SELECT 'citus_0_1234_1_0_' || :'db1_oid' AS transaction_2_worker_1_db_1_name
\gset
PREPARE TRANSACTION :'transaction_2_worker_1_db_1_name';

\c db1 - - :worker_2_port

BEGIN;
CREATE TABLE should_abort
(
    value int
);
SELECT 'citus_0_1234_0_0_' || :'db1_oid' AS transaction_1_worker_2_db_1_name
\gset
PREPARE TRANSACTION :'transaction_1_worker_2_db_1_name';

BEGIN;
CREATE TABLE should_commit
(
    value int
);
SELECT 'citus_0_1234_1_0_' || :'db1_oid' AS transaction_2_worker_2_db_1_name
\gset
PREPARE TRANSACTION :'transaction_2_worker_2_db_1_name';

-- Prepare transactions on second database
\c db2 - - :worker_1_port

BEGIN;
CREATE TABLE should_abort
(
    value int
);
SELECT 'citus_0_1234_3_0_' || :'db2_oid' AS transaction_1_worker_1_db_2_name
\gset
PREPARE TRANSACTION :'transaction_1_worker_1_db_2_name';

BEGIN;
CREATE TABLE should_commit
(
    value int
);
SELECT 'citus_0_1234_4_0_' || :'db2_oid' AS transaction_2_worker_1_db_2_name
\gset
PREPARE TRANSACTION :'transaction_2_worker_1_db_2_name';

\c db2 - - :worker_2_port

BEGIN;
CREATE TABLE should_abort
(
    value int
);
SELECT 'citus_0_1234_3_0_' || :'db2_oid' AS transaction_1_worker_2_db_2_name
\gset
PREPARE TRANSACTION :'transaction_1_worker_2_db_2_name';

BEGIN;
CREATE TABLE should_commit
(
    value int
);
SELECT 'citus_0_1234_4_0_' || :'db2_oid' AS transaction_2_worker_2_db_2_name
\gset
PREPARE TRANSACTION :'transaction_2_worker_2_db_2_name';

\c db1 - - :master_port

INSERT INTO pg_dist_transaction
VALUES (:worker_1_group_id, :'transaction_2_worker_1_db_1_name'),
       (:worker_2_group_id, :'transaction_2_worker_2_db_1_name');
INSERT INTO pg_dist_transaction
VALUES (:worker_1_group_id, 'citus_0_should_be_forgotten_' || :'db1_oid'),
       (:worker_2_group_id, 'citus_0_should_be_forgotten_' || :'db1_oid');

\c db2 - - :master_port

INSERT INTO pg_dist_transaction
VALUES (:worker_1_group_id, :'transaction_2_worker_1_db_2_name'),
       (:worker_2_group_id, :'transaction_2_worker_2_db_2_name');
INSERT INTO pg_dist_transaction
VALUES (:worker_1_group_id, 'citus_0_should_be_forgotten_' || :'db2_oid'),
       (:worker_2_group_id, 'citus_0_should_be_forgotten_' || :'db2_oid');

\c db1 - - :master_port

SELECT count(*) != 0
FROM pg_dist_transaction;

SELECT recover_prepared_transactions() > 0;

SELECT count(*) = 0
FROM pg_dist_transaction;

\c db2 - - :master_port

SELECT count(*) != 0
FROM pg_dist_transaction;

SELECT recover_prepared_transactions() > 0;

SELECT count(*) = 0
FROM pg_dist_transaction;

\c regression - - :master_port

SELECT count(pg_terminate_backend(pid)) > 0
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db1' ;

DROP DATABASE db1;

SELECT count(pg_terminate_backend(pid)) > 0
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db2' ;
DROP DATABASE db2;

\c - - - :worker_1_port

SELECT count(pg_terminate_backend(pid)) > 0
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db1' ;

DROP DATABASE db1;

SELECT count(pg_terminate_backend(pid)) > 0
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db2' ;
DROP DATABASE db2;

\c - - - :worker_2_port

-- Count of terminated sessions is not important for the test,
-- it is just to make output predictable
SELECT count(pg_terminate_backend(pid)) >= 0
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db1' ;

DROP DATABASE db1;

SELECT count(pg_terminate_backend(pid)) >= 0
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db2' ;
DROP DATABASE db2;
