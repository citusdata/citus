-- Focus on the ?column? issue specifically
\c citus

-- Tables should already exist, just in case
CREATE TABLE IF NOT EXISTS users_table (user_id int, value_1 int, value_2 int);
CREATE TABLE IF NOT EXISTS events_table (user_id int, event_type int, value_2 int);

-- Populate with minimal data
INSERT INTO users_table (user_id, value_1, value_2) VALUES (1, 10, 100), (2, 20, 200) ON CONFLICT DO NOTHING;
INSERT INTO events_table (user_id, event_type, value_2) VALUES (1, 1, 1), (2, 2, 2) ON CONFLICT DO NOTHING;

-- Test the simplified ?column? issue
SELECT DISTINCT user_id
FROM (
    SELECT users_table.user_id 
    FROM users_table,
    (
        SELECT avg(event_type) as avg_val
        FROM (
            SELECT event_type, a_users.user_id
            FROM users_table as a_users, 
            (
                SELECT user_id, event_type 
                FROM events_table 
                WHERE value_2 < 3 
                ORDER BY 1, 2 
                LIMIT 1
            ) as foo
            WHERE foo.user_id = a_users.user_id
        ) bar, users_table as b_users
        WHERE bar.user_id = b_users.user_id
        GROUP BY b_users.value_1
    ) as baz
    WHERE baz.avg_val < users_table.user_id
    ORDER BY 1
    LIMIT 1
) as sub1;
