
-- Create a simple test to reproduce the issue
CREATE SCHEMA debug_test;
SET search_path TO debug_test, public;
SET client_min_messages TO DEBUG1;

-- Simple query that should generate intermediate results
SELECT DISTINCT user_id FROM (
    SELECT users_table.user_id 
    FROM users_table 
    WHERE user_id < 3
    ORDER BY 1 
    LIMIT 2
) as sub1 ORDER BY 1 DESC;

