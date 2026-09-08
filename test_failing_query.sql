-- Test only the failing complex nested query
\c citus

SET client_min_messages TO DEBUG5;

-- The exact failing pattern (this should fail without our fix)
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
GROUP BY b_users.value_1;
