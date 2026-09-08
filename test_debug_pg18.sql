-- Test to debug PostgreSQL 1-- Test the actual failing pattern before
        SELECT users_table.value_1 as value1
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
        GROUP BY value1;
-- First let's test with our existing setup
\c citus

-- Simple subquery that should work
SELECT user_id FROM (SELECT user_id FROM users_table WHERE user_id = 1) sub;

-- Test aggregate subquery (this might have ?column? issue)
SELECT * FROM (SELECT avg(user_id) FROM users_table) sub;

-- Test complex subquery similar to the failing one
SELECT * FROM (
    SELECT avg(event_type) as avg_val
    FROM (
        SELECT event_type 
        FROM events_table 
        WHERE value_2 < 3 
        LIMIT 1
    ) foo
) bar;

--Test simple alias
SELECT value_1 as value1
from ( 
    select * from (
        select * from users_table
    ) as tab2 
) as tab;

-- Test the actual failing pattern before
        SELECT users_table.value_1 as value1
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
   ;

-- Test the actual failing pattern
-- Test the simplified ?column? issue
SELECT DISTINCT user_id
FROM (
    SELECT users_table.user_id 
    FROM users_table,
    (
        SELECT event_type as avg_val
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
    ) as baz
    WHERE baz.avg_val < users_table.user_id
    ORDER BY 1
    LIMIT 1
) as sub1;


-- Test the actual failing pattern
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