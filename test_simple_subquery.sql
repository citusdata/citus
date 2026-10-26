-- Simple test to isolate the PostgreSQL 18 issue
-- This should show where the ?column? error occurs

-- Drop and recreate to start fresh
DROP SCHEMA IF EXISTS simple_test CASCADE;
CREATE SCHEMA simple_test;
SET search_path TO simple_test, public;

CREATE TABLE events_table (
    user_id int,
    event_type int,
    value_2 int
);

SELECT create_distributed_table('events_table', 'user_id');

INSERT INTO events_table VALUES 
(1, 1, 1),
(2, 2, 2),
(3, 3, 3);

SET client_min_messages TO DEBUG1;

-- This is the simplest subquery that should trigger the issue
SELECT user_id, event_type FROM events_table WHERE value_2 < 3 ORDER BY 1, 2 OFFSET 3;

-- Clean up
SET search_path TO public;
DROP SCHEMA simple_test CASCADE;
