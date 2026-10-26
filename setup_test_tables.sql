-- Setup test tables for PostgreSQL 18 compatibility testing
\c citus

SET citus.shard_replication_factor = 1;
SET citus.shard_count = 4;

-- Create tables if they don't exist
CREATE TABLE IF NOT EXISTS users_table (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint);
CREATE TABLE IF NOT EXISTS events_table (user_id int, time timestamp, event_type int, value_2 int, value_3 float, value_4 bigint);

-- Create distributed tables
SELECT create_distributed_table('users_table', 'user_id');
SELECT create_distributed_table('events_table', 'user_id');

-- Insert some test data
INSERT INTO users_table VALUES 
(1, now(), 10, 20, 1.5, 100),
(2, now(), 30, 40, 2.5, 200),
(3, now(), 50, 60, 3.5, 300);

INSERT INTO events_table VALUES 
(1, now(), 1, 1, 1.1, 10),
(2, now(), 2, 2, 2.2, 20),
(3, now(), 3, 3, 3.3, 30),
(1, now(), 4, 4, 4.4, 40);
