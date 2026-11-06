-- Test deeply nested queries with JOINs and GROUP BY
\c citus

-- Test the exact failing pattern step by step
-- Step 1: Simple subquery with JOIN (should work)
SELECT avg(event_type) as avg_val
FROM (
    SELECT event_type, users_table.user_id
    FROM users_table, events_table
    WHERE events_table.user_id = users_table.user_id
) sub
GROUP BY sub.user_id;

-- Step 2: Add one more level of nesting (might fail)
SELECT avg_val
FROM (
    SELECT avg(event_type) as avg_val
    FROM (
        SELECT event_type, users_table.user_id
        FROM users_table, events_table
        WHERE events_table.user_id = users_table.user_id
    ) sub
    GROUP BY sub.user_id
) outer_sub;

-- Step 3: The exact failing pattern (this should fail)
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
