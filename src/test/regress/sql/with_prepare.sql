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
    event_type, count(user_id)
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
  2, 1
LIMIT
  10;


PREPARE prepared_test_3(integer) AS
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
      event_type, count(user_id)
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
    on (users_events.event_type = $1)
  GROUP BY
    user_id
)
SELECT
  * 
FROM
  user_coolness
ORDER BY
  2, 1
LIMIT
  10;


PREPARE prepared_test_4(integer, integer, integer) AS
WITH basic AS(
  SELECT * FROM users_table WHERE value_2 IN ($1, $2, $3)
)
SELECT
  * 
FROM
  basic
ORDER BY
  1, 2, 3, 4, 5, 6
LIMIT 10;


-- prepared statement which inserts in a CTE should fail
PREPARE prepared_partition_column_insert(integer) AS
WITH prepared_insert AS (
  INSERT INTO users_table VALUES ($1) RETURNING *
)
SELECT * FROM prepared_insert;


PREPARE prepared_test_5(integer, integer, integer) AS
-- router select query
WITH users_events_1 AS (
  SELECT
    *
  FROM
    users_table
  WHERE
    user_id = $1
),
-- real-time select query
users_events_2_3 AS (
  SELECT
    *
  FROM
    users_table
  WHERE
    user_id = $2
    OR
    user_id = $3
),
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
)
SELECT
  *
FROM
  merged_users
ORDER BY
  1, 2, 3, 4, 5, 6
LIMIT 10;

-- Prepare a statement with a sublink in WHERE clause and recurring tuple in FORM
PREPARE prepared_test_6 AS
WITH event_id AS (
	SELECT user_id as events_user_id, time as events_time, event_type
	FROM events_table
)
SELECT
	count(*) 
FROM
	event_id
WHERE
	events_user_id IN (SELECT user_id FROM users_table);

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

EXECUTE prepared_test_3(1);
EXECUTE prepared_test_3(2);
EXECUTE prepared_test_3(3);
EXECUTE prepared_test_3(4);
EXECUTE prepared_test_3(5);
EXECUTE prepared_test_3(6);

EXECUTE prepared_test_4(1, 2, 3);
EXECUTE prepared_test_4(2, 3, 4);
EXECUTE prepared_test_4(3, 4, 5);
EXECUTE prepared_test_4(4, 5, 6);
EXECUTE prepared_test_4(5, 6, 7);
EXECUTE prepared_test_4(6, 7, 8);

EXECUTE prepared_test_5(1, 2, 3);
EXECUTE prepared_test_5(2, 3, 4);
EXECUTE prepared_test_5(3, 4, 5);
EXECUTE prepared_test_5(4, 5, 6);
EXECUTE prepared_test_5(5, 6, 7);
EXECUTE prepared_test_5(6, 7, 8);

EXECUTE prepared_test_6;
EXECUTE prepared_test_6;
EXECUTE prepared_test_6;
EXECUTE prepared_test_6;
EXECUTE prepared_test_6;
EXECUTE prepared_test_6;

EXECUTE prepared_partition_column_insert(1);
EXECUTE prepared_partition_column_insert(2);
EXECUTE prepared_partition_column_insert(3);
EXECUTE prepared_partition_column_insert(4);
EXECUTE prepared_partition_column_insert(5);
EXECUTE prepared_partition_column_insert(6);

DEALLOCATE ALL;
