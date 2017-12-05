
CREATE TABLE local_users (user_id int, event_type int);
INSERT INTO local_users VALUES (0, 0), (1, 4), (1, 7), (2, 1), (3, 3), (5, 4), (6, 2), (10, 7);

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


SET citus.task_executor_type TO 'task-tracker';
-- Co-location tests

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


SET citus.task_executor_type TO 'real-time';
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



-- CTE in WHERE basic
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

SET citus.task_executor_type = 'task-tracker';
-- CTE with non-colocated join in WHERE
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


-- prepared statements
PREPARE prepared_test_1 AS
WITH basic AS(
  SELECT * FROM users_table
)
SELECT
  * 
FROM
  basic
WHERE
  basic.value_2 IN (1, 2, 3)
ORDER BY
  1, 2, 3, 4, 5, 6
LIMIT 10;


PREPARE prepared_test_2 AS
WITH users_events AS(
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
),
event_attendee_count AS(
  SELECT
    event_type, count(distinct user_id)
  FROM
    users_events
  GROUP BY
    1
),
user_coolness AS(
  SELECT
    user_id,
    sum(count)
  FROM
    users_events
    join
    event_attendee_count
    on (users_events.event_type = event_attendee_count.event_type)
  GROUP BY
    user_id
)
SELECT
  * 
FROM
  user_coolness
ORDER BY
  2, 1;


PREPARE prepared_test_3 AS
WITH users_events AS(
  -- events 1 and 2 only
  WITH spec_events AS(
    SELECT 
      *
    FROM
      events_table
    WHERE
      event_type IN (1, 2)
  )
  -- users who have done 1 or 2
  SELECT
      users_table.user_id,
      spec_events.event_type
  FROM
    users_table
    join
    spec_events
    on (users_table.user_id = spec_events.user_id)
  ORDER BY
    1,
    event_type
),
event_attendee_count AS(
  -- distinct attendee count of each event in users_event
  WITH event_attendee_count AS(
    SELECT
      event_type, count(distinct user_id)
    FROM
      users_events
    GROUP BY
      1
  )
  -- distinct attendee count of first 3 events
  SELECT
    *
  FROM
    event_attendee_count
  ORDER BY
    event_type
  LIMIT 3
),
-- measure how cool an attendee is by checking the number of events he attended
user_coolness AS(
  SELECT
    user_id,
    sum(count)
  FROM
    users_events
    join
    event_attendee_count
    on (users_events.event_type = event_attendee_count.event_type)
  GROUP BY
    user_id
)
SELECT
  * 
FROM
  user_coolness
ORDER BY
  2, 1;



EXECUTE prepared_test_1;
EXECUTE prepared_test_1;
EXECUTE prepared_test_1;
EXECUTE prepared_test_1;
EXECUTE prepared_test_1;
EXECUTE prepared_test_1;

EXECUTE prepared_test_2;
EXECUTE prepared_test_2;
EXECUTE prepared_test_2;
EXECUTE prepared_test_2;
EXECUTE prepared_test_2;
EXECUTE prepared_test_2;

EXECUTE prepared_test_3;
EXECUTE prepared_test_3;
EXECUTE prepared_test_3;
EXECUTE prepared_test_3;
EXECUTE prepared_test_3;
EXECUTE prepared_test_3;

DEALLOCATE ALL;
