CREATE TABLE local_users (user_id int, event_type int);
INSERT INTO local_users VALUES (0, 0), (1, 4), (1, 7), (2, 1), (3, 3), (5, 4), (6, 2), (10, 7);


-- Nested CTEs
WITH users_events AS (
  WITH users_events_2 AS (
    SELECT
        users_table.user_id as user_id,
        events_table.event_type as event_type
    FROM
        users_table,
        events_table
    WHERE
        users_table.user_id = events_table.user_id
    GROUP BY
        users_table.user_id,
        events_table.event_type
  )
  SELECT
      u_events.user_id, events_table.event_type
  FROM
    users_events_2 as u_events,
    events_table
  WHERE
    u_events.user_id = events_table.user_id
  )
SELECT 
  * 
FROM 
  users_events
ORDER BY
  1, 2
LIMIT 20;

-- Nested CTEs
WITH users_events AS (
  WITH users_events AS (
    WITH users_events AS (
      WITH users_events AS (
        WITH users_events AS (
          WITH users_events AS (
            WITH users_events AS (
              SELECT
                  users_table.user_id as user_id,
                  events_table.event_type as event_type
              FROM
                  users_table,
                  events_table
              WHERE
                  users_table.user_id = events_table.user_id
              GROUP BY
                  users_table.user_id,
                  events_table.event_type
            
            )
            SELECT 
                uid, event_type, value_2, value_3
            FROM
               (
                (
                  SELECT 
                    user_id as uid
                  FROM 
                    users_events 
                ) users
                join
                  events_table
                on
                  users.uid = events_table.event_type
                ) a
            )
          SELECT 
            * 
          FROM 
            users_events
          ORDER BY
            1, 3, 2, 4
          LIMIT 100
        )
        SELECT 
          * 
        FROM 
          users_events
        LIMIT 90
      )
      SELECT 
        * 
      FROM 
        users_events
      LIMIT 50
    )
    SELECT 
      uid, event_type, value_2, sum(value_3) as sum_3
    FROM 
      users_events
    GROUP BY
      1, 2, 3
    LIMIT 40
  )
  SELECT 
    uid, event_type, sum(value_2) as sum_2, sum(sum_3) as sum_3
  FROM 
    users_events
  GROUP BY
    1, 2
  LIMIT 30   
)
SELECT 
  uid, avg(event_type), sum(sum_2), sum(sum_3)
FROM 
  users_events
GROUP BY
  1;


-- Nested CTEs
WITH users_events AS (
  -- router select query
  WITH users_events_1 AS (
    SELECT
      *
    FROM
      users_table
    WHERE
      user_id = 1
  ),
  -- real-time select query
  users_events_2_3 AS (
    SELECT
      *
    FROM
      users_table
    WHERE
      user_id = 2
      OR
      user_id = 3
  ),
  -- router select query
  -- sub CTE is a real-time executor query but the top level is router select
  users_events_4 AS (
    WITH users_events_4_5 AS (
    SELECT
      *
    FROM
      users_table
    WHERE
      user_id = 4
      OR
      user_id = 5
    )
    SELECT
        *
    FROM
        users_events_4_5
    WHERE
        user_id = 4
  ),
  -- merge all the results from CTEs
  merged_users AS (
      SELECT
        *
      FROM
        users_events_1
    UNION
      SELECT
        *
      FROM
        users_events_2_3
    UNION 
      SELECT
        *
      FROM
        users_events_4
  )
  SELECT
    *
  FROM
    merged_users
)
SELECT 
  * 
FROM
  users_events
ORDER BY
  1, 2, 3, 4, 5, 6
LIMIT
  20;


-- Nested CTEs - joined with local table. Not supported yet.
WITH users_events AS (
  -- router select query
  WITH users_events_1 AS (
    SELECT
      *
    FROM
      users_table
    WHERE
      user_id = 1
  ),
  -- real-time select query
  users_events_2_3 AS (
    SELECT
      *
    FROM
      users_table
    WHERE
      user_id = 2
      OR
      user_id = 3
  ),
  -- router select query
  -- sub CTE is a real-time executor query but the top level is router select
  users_events_4 AS (
    WITH users_events_4_5 AS (
    SELECT
      *
    FROM
      users_table
    WHERE
      user_id = 4
      OR
      user_id = 5
    )
    SELECT
        *
    FROM
        users_events_4_5
    WHERE
        user_id = 4
  ),
  -- merge all the results from CTEs
  merged_users AS (
      SELECT
        *
      FROM
        users_events_1
    UNION
      SELECT
        *
      FROM
        users_events_2_3
    UNION 
      SELECT
        *
      FROM
        users_events_4
  ),
  -- join with a local table 
  local_users AS (
    SELECT
      *
    FROM
      (SELECT user_id as uid
      FROM local_users) l_users
      join
      merged_users
      on (l_users.uid = merged_users.user_id)
  )
  SELECT
    *
  FROM
    local_users
)
SELECT 
  * 
FROM
  users_events
ORDER BY
  1, 2, 3, 4, 5, 6
LIMIT
  20;


-- nested CTEs should not be able to reference outer CTEs even if the query doesn't call CTE
WITH users_events AS (
  WITH users_events_2 AS (
    SELECT
        *
    FROM
        users_events
  )
  SELECT
      *
  FROM
    users_table
)
SELECT 
  * 
FROM 
  users_events
ORDER BY
  1, 2
LIMIT 20;

WITH users_events AS (
  WITH users_events_2 AS (
    SELECT
        *
    FROM
        users_events
  )
  SELECT
      *
  FROM
    users_table
)
SELECT 
  * 
FROM 
  users_table
ORDER BY
  1, 2
LIMIT 20;


-- Even if it is not called, nested CTEs should not be able to reference outer CTE
WITH nested1 AS (
  WITH nested2_1 AS (
    SELECT * FROM users_table
  ),
  nested2_2 AS (
    SELECT * FROM nested1
  )
  SELECT * FROM nested2_1
)
SELECT * FROM nested1;


-- Outer CTE cannot reference the inner CTE in another CTE.
WITH nested1 AS (
  WITH nested1_1 AS (
    SELECT * FROM users_table
  )
  SELECT * FROM nested1_1
),
nested2 AS (
  SELECT * FROM nested1_1
)
SELECT * FROM nested2;

-- inner CTE cannot reference another inner CTE if they are in two different outer CTEs
WITH nested1 AS (
  WITH nested1_1 AS (
    SELECT * FROM users_table
  )
  SELECT * FROM nested1_1
),
nested2 AS (
  WITH nested2_1 AS (
    SELECT * FROM nested1_1
  )
  SELECT * FROM nested2_1
)
SELECT * FROM nested2;


-- inner CTE cannot reference the outer CTE
WITH nested1 AS (
  WITH nested1_1 AS (
    SELECT * FROM nested2
  )
  SELECT * FROM nested1_1
),
nested2 AS (
  WITH nested2_1 AS (
    SELECT * FROM nested1_1
  )
  SELECT * FROM nested2_1
)
SELECT * FROM nested2;

