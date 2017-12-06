CREATE TABLE reference_table(user_id int);
SELECT create_reference_table('reference_table');
INSERT INTO reference_table VALUES (6), (7);

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
  users_events
ORDER BY 
  1, 2
LIMIT 
  20;


-- cte LEFT JOIN distributed_table should error out
WITH cte AS (
  SELECT * FROM users_table WHERE user_id = 1
)
SELECT cte.user_id, cte.time, events_table.event_type FROM cte LEFT JOIN events_table on cte.user_id=events_table.user_id
ORDER BY 
  1,2,3
LIMIT 
  10;


SELECT a.user_id, a.time, events_table.event_type FROM 
  (
    WITH cte AS (
      SELECT * FROM users_table WHERE user_id = 1
    )
    SELECT * FROM cte
  ) a
  LEFT JOIN 
    events_table 
  on 
    a.user_id=events_table.user_id
ORDER BY 
  1,2,3
LIMIT 
  10;

-- cte LEFT JOIN cte should work
WITH cte AS (
  SELECT * FROM users_table WHERE user_id IN (1, 2)
),
cte_2 AS (
  SELECT * FROM users_table WHERE user_id IN (2, 3)
)
SELECT cte.user_id, cte.time, cte_2.value_2 FROM cte LEFT JOIN cte_2 on cte.user_id=cte_2.user_id
ORDER BY 
  1,2,3
LIMIT 
  10;


-- cte as a subquery in left join errors out
SELECT a.user_id, a.time, b.value_2 FROM 
  (
    WITH cte AS (
      SELECT * FROM users_table WHERE user_id IN (1, 2)
    )
    SELECT * FROM cte
  ) a
  LEFT JOIN 
  (
    WITH cte_2 AS (
      SELECT * FROM users_table WHERE user_id IN (2, 3)
    )
    SELECT * FROM cte_2
  ) b
  on 
    a.user_id=b.user_id
ORDER BY 
  1,2,3
LIMIT 
  10;


-- cte JOIN reference_table should be router plannable
EXPLAIN (COSTS false, VERBOSE true)
WITH cte AS (
  SELECT * FROM users_table
)
SELECT * FROM cte join reference_table ON cte.user_id + 1 = reference_table.user_id;


-- the most outer query is router plannable
EXPLAIN (COSTS false, VERBOSE true)
WITH cte_1 AS (
  WITH cte_1_1 AS (
    SELECT * FROM users_table WHERE value_2 IN (2, 3, 4)
  ),
  cte_1_2 AS (
    SELECT cte_1_1.user_id, event_type FROM cte_1_1, events_table where cte_1_1.user_id = events_table.user_id
  )
  SELECT * FROM cte_1_2 JOIN reference_table on cte_1_2.user_id = reference_table.user_id
)
SELECT * FROM cte_1;


-- Inner CTE should be router plannable
EXPLAIN (COSTS false, VERBOSE true)
WITH cte_1 AS (
  WITH cte_1_1 AS (
    SELECT * FROM users_table WHERE value_2 IN (2, 3, 4)
  ),
  cte_1_2 AS (
    SELECT cte_1_1.user_id, event_type FROM cte_1_1, events_table where cte_1_1.user_id = events_table.user_id
  )
  SELECT * FROM cte_1_2 JOIN reference_table on cte_1_2.user_id = reference_table.user_id
)
SELECT * FROM cte_1 JOIN events_table on cte_1.event_type=events_table.event_type;
