-- Test aggregate with GROUP BY to isolate the problem
\c citus

-- Tables should already exist
CREATE TABLE IF NOT EXISTS users_table (user_id int, value_1 int, value_2 int);
CREATE TABLE IF NOT EXISTS events_table (user_id int, event_type int, value_2 int);

-- Populate with minimal data
INSERT INTO users_table (user_id, value_1, value_2) VALUES (1, 10, 100), (2, 20, 200) ON CONFLICT DO NOTHING;
INSERT INTO events_table (user_id, event_type, value_2) VALUES (1, 1, 1), (2, 2, 2) ON CONFLICT DO NOTHING;

-- Test simple aggregate without GROUP BY (should work)
SELECT avg(event_type) FROM events_table;

-- Test simple aggregate with GROUP BY (might fail)
SELECT avg(event_type) FROM events_table GROUP BY user_id;

-- Test nested aggregate with GROUP BY (likely to fail)
SELECT avg(event_type) 
FROM (
    SELECT event_type, user_id
    FROM events_table 
    WHERE value_2 < 3
) sub
GROUP BY sub.user_id;
