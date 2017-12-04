
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
