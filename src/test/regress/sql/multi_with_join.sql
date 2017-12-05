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
    *
FROM
    users_events
ORDER BY
  1, 2
LIMIT
  20;


-- Two colocated CTE under a non-colocated join 
WITH colocated_1 AS (
  SELECT 
    users_table.user_id, events_table.value_2
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id AND event_type IN (1, 2, 3)
),
colocated_2 AS (
  SELECT 
    users_table.user_id, events_table.value_2
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.user_id AND event_type IN (4, 5, 6)
)
SELECT colocated_1.user_id, count(*) 
FROM
  colocated_1, colocated_2
WHERE
  colocated_1.value_2 = colocated_2.value_2
GROUP BY
  1
ORDER BY
  2 DESC, 1;


SET citus.task_executor_type TO 'task-tracker';
-- Two non-colocated CTE under a co-located join 
WITH non_colocated_1 AS (
  SELECT 
    users_table.user_id
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.value_2 AND event_type IN (1, 2, 3)
),
non_colocated_2 AS (
  SELECT 
    users_table.user_id
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.value_2 AND event_type IN (4, 5, 6)
)

SELECT non_colocated_1.user_id, count(*) 
FROM
  non_colocated_1, non_colocated_2
WHERE
  non_colocated_1.user_id = non_colocated_2.user_id
GROUP BY
  1
ORDER BY
  2 DESC, 1;
  

WITH non_colocated AS (
  WITH colocated AS (
    SELECT 
      users_table.user_id as uid, events_table.value_2
    FROM 
      users_table, events_table
    WHERE
      users_table.user_id = events_table.user_id AND event_type IN (1, 2)
  ),
  colocated_2 AS (
    SELECT 
      users_table.user_id as uid, events_table.value_2
    FROM 
      users_table, events_table
    WHERE
      users_table.user_id = events_table.user_id AND event_type IN (3, 4)
  )
  SELECT
    colocated.uid, colocated.value_2
  FROM
    colocated, colocated_2
  WHERE
    colocated.value_2 = colocated_2.value_2
),
non_colocated_2 AS (
  SELECT 
    users_table.user_id as uid, events_table.value_2
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.event_type AND event_type IN (5, 6)
)
SELECT
  sum(non_colocated.uid), sum(non_colocated.value_2), sum(non_colocated_2.value_2)
FROM
  non_colocated, non_colocated_2
WHERE 
  non_colocated.uid = non_colocated_2.uid
;


-- this query doesn't make much sense though but tests colocated join and non-colocated join together
WITH users_events AS (
  WITH colocated_join AS (
    SELECT
      users_table.user_id as uid, events_table.event_type
    FROM
        users_table
      join
        events_table
      on (users_table.user_id = events_table.user_id)
    WHERE
      users_table.value_2 IN (1, 2)
  ),

  non_colocated_join AS (
    SELECT
      users_table.user_id as n_uid, events_table.event_type
    FROM
        users_table
      join
        events_table
      on (users_table.value_2 = events_table.value_2)
  )
  SELECT
    uid, colocated_join.event_type
  FROM
      colocated_join
    INNER join
      non_colocated_join
    on (colocated_join.event_type = non_colocated_join.event_type)
)
SELECT
  sum(uid), sum(event_type)
FROM
  users_events;
SET citus.task_executor_type = 'real-time';


-- Subqueries in WHERE and FROM are mixed
-- In this query, only subquery in WHERE is not a colocated join
WITH users_events AS (
  WITH colocated_join AS (
    SELECT
      users_table.user_id as uid, event_type
    FROM
        users_table
      join
        events_table
      on (users_table.user_id = events_table.user_id)
    WHERE
      events_table.event_type IN (1, 2, 3)
  ),
  colocated_join_2 AS (
    SELECT
      users_table.user_id, event_type
    FROM
        users_table
      join
        events_table
      on (users_table.user_id = events_table.user_id)
    WHERE
      events_table.event_type IN (4, 5, 6)
  )
  SELECT
    uid, colocated_join.event_type
  FROM
    colocated_join,
    colocated_join_2
  WHERE
    colocated_join.uid = colocated_join_2.user_id AND
    colocated_join.event_type IN (
    WITH some_events AS (
      SELECT
        event_type
      FROM
        events_table
      WHERE 
        user_id < 100 
      GROUP BY 
        1 
      ORDER BY 
        1 
      LIMIT 10
    )
    SELECT
      *
    FROM
      some_events
  )
)
SELECT
  *
FROM
  users_events;
