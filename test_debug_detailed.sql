-- Debug test with detailed logging
\c citus

SET client_min_messages TO DEBUG2;
SET citus.log_remote_commands TO ON;

-- Test the failing query with detailed logging
SELECT DISTINCT user_id
FROM (
    SELECT users_table.user_id 
    FROM users_table,
    (
        SELECT avg(event_type) as avg_val
        FROM (
            SELECT event_type, users_table.user_id
            FROM users_table, 
            (
                SELECT user_id, event_type 
                FROM events_table 
                WHERE value_2 < 3 
                ORDER BY 1, 2 
                LIMIT 1
            ) as foo
            WHERE foo.user_id = users_table.user_id
        ) bar, users_table
        WHERE bar.user_id = users_table.user_id
        GROUP BY users_table.value_1
    ) as baz
    WHERE baz.avg_val < users_table.user_id
    ORDER BY 1
    LIMIT 1
) as sub1;
