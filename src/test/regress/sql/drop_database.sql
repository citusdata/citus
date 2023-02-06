-- coordinator
CREATE SCHEMA drop_database;
SET search_path TO drop_database;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 35137400;

CREATE DATABASE citus_created;

\c citus_created
CREATE EXTENSION citus;

CREATE DATABASE citus_not_created;

\c citus_not_created
DROP DATABASE citus_created;

\c regression
DROP DATABASE citus_not_created;

-- worker1
\c - - - :worker_1_port

SET search_path TO drop_database;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 35137400;

CREATE DATABASE citus_created;

\c citus_created
CREATE EXTENSION citus;

CREATE DATABASE citus_not_created;

\c citus_not_created
DROP DATABASE citus_created;

\c regression
DROP DATABASE citus_not_created;

\c - - - :master_port

SET client_min_messages TO WARNING;
DROP SCHEMA drop_database CASCADE;
