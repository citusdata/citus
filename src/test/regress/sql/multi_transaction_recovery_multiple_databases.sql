ALTER SYSTEM SET citus.recover_2pc_interval TO -1;
SELECT pg_reload_conf();

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

SELECT pg_sleep_for('5 SECONDS');

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

SELECT pg_sleep_for('5 SECONDS');

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

BEGIN;
CREATE TABLE should_be_sorted_into_middle
(
    value int
);
SELECT 'citus_0_1234_2_0_' || :'db1_oid' AS transaction_3_worker_1_db_1_name
\gset
PREPARE TRANSACTION :'transaction_3_worker_1_db_1_name';

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

BEGIN;
CREATE TABLE should_be_sorted_into_middle
(
    value int
);
SELECT 'citus_0_1234_2_0_' || :'db1_oid' AS transaction_3_worker_2_db_1_name
\gset
PREPARE TRANSACTION :'transaction_3_worker_2_db_1_name';

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

BEGIN;
CREATE TABLE should_be_sorted_into_middle
(
    value int
);
SELECT 'citus_0_1234_5_0_' || :'db2_oid' AS transaction_3_worker_1_db_2_name
\gset
PREPARE TRANSACTION :'transaction_3_worker_1_db_2_name';

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

BEGIN;
CREATE TABLE should_be_sorted_into_middle
(
    value int
);
SELECT 'citus_0_1234_5_0_' || :'db2_oid' AS transaction_3_worker_2_db_2_name
\gset
PREPARE TRANSACTION :'transaction_3_worker_2_db_2_name';

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

SELECT count(*)
FROM pg_dist_transaction;

SELECT recover_prepared_transactions();

SELECT count(*)
FROM pg_dist_transaction;

\c db2 - - :master_port

SELECT count(*)
FROM pg_dist_transaction;

SELECT recover_prepared_transactions();

SELECT count(*)
FROM pg_dist_transaction;

\c regression - - :master_port

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db1' ;

DROP DATABASE db1;

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db2' ;
DROP DATABASE db2;

\c - - - :worker_1_port

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db1' ;

DROP DATABASE db1;

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db2' ;
DROP DATABASE db2;

\c - - - :worker_2_port

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db1' ;

DROP DATABASE db1;

SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND datname = 'db2' ;
DROP DATABASE db2;
