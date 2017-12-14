-- Complex nested CTEs
CREATE SCHEMA with_nested;
SET search_path tO with_nested, public;

CREATE TABLE with_nested.local_users (user_id int, event_type int);
INSERT INTO local_users VALUES (0, 0), (1, 4), (1, 7), (2, 1), (3, 3), (5, 4), (6, 2), (10, 7);

-- Can refer to outer CTE because it is recursively planned first
WITH cte_1 AS (
  SELECT DISTINCT user_id FROM users_table
),
cte_2 AS (
  WITH cte_1_1 AS (
     SELECT * FROM cte_1 WHERE user_id > 1
  )
  SELECT * FROM cte_1_1 WHERE user_id < 3
)
SELECT user_id FROM cte_2 LIMIT 1;

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
  )
  -- merge all the results from CTEs
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
  users_events
ORDER BY
  1, 2, 3, 4, 5, 6
LIMIT
  20;

-- access to uncle, use window function, apply aggregates, use group by, LIMIT/OFFSET
WITH cte1 AS (
  WITH cte1_1 AS (
    WITH cte1_1_1 AS (
      SELECT user_id, time, value_2 FROM users_table
    ),
    cte1_1_2 AS (
      SELECT
        user_id, count
      FROM (
              SELECT
                      user_id,
                      count(value_2) OVER (PARTITION BY user_id)
              FROM
                      users_table
              GROUP BY 1, users_table.value_2
      )aa
      GROUP BY
        1,2
      ORDER BY
        1,2
      LIMIT
        4
      OFFSET
        2
    )
    SELECT cte1_1_1.user_id, cte1_1_1.time, cte1_1_2.count FROM cte1_1_1 join cte1_1_2 on cte1_1_1.user_id=cte1_1_2.user_id
  ),
  cte1_2 AS (
    WITH cte1_2_1 AS (
      SELECT
        user_id, time, avg(value_1)::real as value_1, min(value_2) as value_2
      FROM
        users_table
      GROUP BY
        1, 2
    ),
    cte1_2_2 AS (
      SELECT cte1_2_1.user_id, cte1_1.time, cte1_2_1.value_1, cte1_1.count FROM cte1_2_1 join cte1_1 on cte1_2_1.time=cte1_1.time and cte1_2_1.user_id=cte1_1.user_id
    )
    SELECT * FROM cte1_2_2
  )
  SELECT * FROM cte1_2
),
cte2 AS (
  WITH cte2_1 AS (
    WITH cte2_1_1 AS (
      SELECT * FROM cte1
    )
    SELECT user_id, time, value_1, min(count) FROM cte2_1_1 GROUP BY 1, 2, 3 ORDER BY 1,2,3
  )
  SELECT * FROM cte2_1 LIMIT 3 OFFSET 2
)
SELECT * FROM cte2;

DROP SCHEMA with_nested CASCADE;
