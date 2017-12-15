-- More complex CTEs in WHERE

SET citus.enable_repartition_joins TO on;

-- CTE in WHERE basic
WITH events AS (
  SELECT 
    event_type 
  FROM 
    events_table 
  WHERE
    user_id < 5 
  GROUP BY
    event_type
  ORDER BY 
    event_type
  LIMIT 10
)
SELECT
  count(*)
FROM
  events_table
WHERE
  event_type
IN
  (SELECT
    event_type
  FROM
    events);

WITH users AS (
    SELECT
      events_table.user_id
    FROM
      events_table, users_table
    WHERE
      events_table.user_id = users_table.user_id
    GROUP BY 
      1
    ORDER BY
      1
    LIMIT 10
) 
SELECT
  count(*)
FROM
  events_table
WHERE
  user_id IN
    (
      SELECT
        *
      FROM
        users
    );


WITH users AS (
    SELECT
      events_table.user_id
    FROM
      events_table, users_table
    WHERE
      events_table.user_id = users_table.user_id
    GROUP BY 
      1
    ORDER BY
      1
    LIMIT 10
) 
SELECT
  count(*)
FROM
  events_table
WHERE
  user_id IN
    (
      SELECT
        *
      FROM
        users
    );


-- CTE with non-colocated join in WHERE
WITH users AS (
    SELECT
      events_table.user_id
    FROM
      events_table, users_table
    WHERE
      events_table.value_2 = users_table.value_2
    GROUP BY 
      1
    ORDER BY
      1
    LIMIT 10
) 
SELECT
  count(*)
FROM
  events_table
WHERE
  user_id IN
    (
      SELECT
        *
      FROM
        users
    );

-- CTE in WHERE basic
-- this is a tricky query that hits an aggresive
-- check in subquery puwhdown after the recursive planning
-- where LIMIT should be allowed
-- if the query contains only intermediate results
SELECT
  count(*)
FROM
  events_table
WHERE
  event_type
IN
  (WITH events AS (
    SELECT 
      event_type 
    FROM 
      events_table 
    WHERE user_id < 5 
    GROUP BY 
      1 
    ORDER BY 
      1)
    SELECT * FROM events LIMIT 10
  );

-- CTE with non-colocated join in WHERE
-- this is a tricky query that hits an aggresive
-- check in subquery puwhdown after the recursive planning
-- where LIMIT should be allowed
-- if the query contains only intermediate results
SELECT
  count(*)
FROM
  events_table
WHERE
  user_id IN
    (WITH users AS
      (SELECT
          events_table.user_id
        FROM
          events_table, users_table
        WHERE
          events_table.value_2 = users_table.value_2
        GROUP BY 
          1
        ORDER BY
          1
      )
      SELECT * FROM users LIMIT 10
    );
