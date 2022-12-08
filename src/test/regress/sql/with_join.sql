CREATE SCHEMA with_join;
SET search_path TO with_join, public;
SET citus.next_shard_id TO 1501000;

CREATE TABLE with_join.reference_table(user_id int);
SELECT create_reference_table('with_join.reference_table');
INSERT INTO reference_table VALUES (6), (7);

SET citus.enable_repartition_joins TO on;

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

-- Two non-colocated CTE under a co-located join
WITH non_colocated_1 AS (
  SELECT
    users_table.user_id
  FROM
    users_table, events_table
  WHERE
    users_table.user_id = events_table.value_2 AND event_type IN (1, 2, 3)
),
non_colocated_2 AS MATERIALIZED (
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


-- Subqueries in WHERE and FROM are mixed
-- In this query, only subquery in WHERE is not a colocated join
-- but we're able to recursively plan that as well
WITH users_events AS MATERIALIZED (
  WITH colocated_join AS MATERIALIZED (
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
  colocated_join_2 AS MATERIALIZED (
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
    WITH some_events AS MATERIALIZED (
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
  DISTINCT uid
FROM
  users_events
ORDER BY
  1 DESC
LIMIT
  5;

-- cte LEFT JOIN distributed_table should work
-- as long as the CTE is recursively planned
WITH cte AS MATERIALIZED (
  SELECT * FROM users_table WHERE user_id = 1 ORDER BY value_1
)
SELECT
  cte.user_id, cte.time, events_table.event_type
FROM
  cte
LEFT JOIN
  events_table ON cte.user_id = events_table.user_id
ORDER BY
  1,2,3
LIMIT
  5;


-- cte RIGHT JOIN distributed_table should work
WITH cte AS MATERIALIZED (
  SELECT * FROM users_table WHERE user_id = 1 ORDER BY value_1
)
SELECT
  cte.user_id, cte.time, events_table.event_type
FROM
  cte
RIGHT JOIN
  events_table ON cte.user_id = events_table.user_id
ORDER BY
  1,2,3
LIMIT
  5;

-- distributed_table LEFT JOIN cte should work
WITH cte AS MATERIALIZED (
  SELECT * FROM users_table WHERE value_1 = 1 ORDER BY value_1
)
SELECT
  cte.user_id, cte.time, events_table.event_type
FROM
  events_table
LEFT JOIN
  cte ON cte.user_id = events_table.user_id
ORDER BY
  1,2,3
LIMIT
  5;

-- distributed_table RIGHT JOIN cte should work
WITH cte AS MATERIALIZED (
  SELECT * FROM users_table WHERE value_1 = 1 ORDER BY value_1
)
SELECT
  cte.user_id, cte.time, events_table.event_type
FROM
  events_table
RIGHT JOIN
  cte ON cte.user_id = events_table.user_id
ORDER BY
  1,2,3
LIMIT
  5;

-- cte FULL JOIN distributed_table should work
WITH cte AS MATERIALIZED (
  SELECT * FROM users_table WHERE user_id = 1 ORDER BY value_1
)
SELECT
  cte.user_id, cte.time, events_table.event_type
FROM
  events_table
FULL JOIN
  cte ON cte.user_id = events_table.user_id
ORDER BY
  1,2,3
LIMIT
  5;


-- Joins with reference tables are planned as router queries
WITH cte AS MATERIALIZED (
  SELECT value_2, max(user_id) AS user_id FROM users_table WHERE value_2 = 1 GROUP BY value_2 HAVING count(*) > 1
)
SELECT
  row_number() OVER(), cte.user_id
FROM
  cte
FULL JOIN
  reference_table ON cte.user_id + 1 = reference_table.user_id
ORDER BY
  user_id
LIMIT
  5;


-- full outer join should work between a reference table and a cte
WITH cte AS (
  SELECT max(user_id) AS max_user_id FROM users_table
)
SELECT user_id, time FROM
  (SELECT r1.user_id AS user_id FROM cte FULL JOIN reference_table r1 ON (cte.max_user_id = r1.user_id)) AS foo
  INNER JOIN users_table USING (user_id)
ORDER BY 1,2;


-- some more tests for more complex outer-joins
-- with reference tables
CREATE TABLE distributed_1 (col1 int, col2 int, distrib_col int);
CREATE TABLE distributed_2 (col1 int, col2 int, distrib_col int);
CREATE TABLE reference_1 (col1 int, col2 int);
CREATE TABLE reference_2(col1 int, col2 int);

SELECT create_distributed_table('distributed_1','distrib_col');
SELECT create_distributed_table('distributed_2','distrib_col');
SELECT create_reference_table('reference_1');
SELECT create_reference_table('reference_2');

INSERT INTO distributed_1 SELECT i, i, i FROM generate_series(0,100) i;
INSERT INTO distributed_2 SELECT i%2, i%2, i%2 FROM generate_series(0,100) i;
INSERT INTO reference_1 SELECT i%3, i%3 FROM generate_series(0,100) i;
INSERT INTO reference_2 SELECT i%4, i%4 FROM generate_series(0,100) i;

select count(*) from distributed_1 AS d1
LEFT JOIN reference_1 AS r1 ON d1.col2=r1.col2
LEFT JOIN reference_2 AS r2 ON r2.col1 = r1.col1
join (select distrib_col,count(*) from distributed_2 group by distrib_col) d2 ON d2.distrib_col=d1.distrib_col;

with d2 AS MATERIALIZED (select distrib_col,count(*) from distributed_2 group by distrib_col)
select count(*) from distributed_1 AS d1
LEFT JOIN reference_1 AS r1 ON d1.col2=r1.col2
LEFT JOIN reference_2 AS r2 ON r2.col1 = r1.col1
join d2 ON d2.distrib_col=d1.distrib_col;

with d2 AS MATERIALIZED (select distrib_col,col1 from distributed_2)
select count(*) from distributed_1 AS d1
LEFT JOIN reference_1 AS r1 ON d1.col2=r1.col2
LEFT JOIN reference_2 AS r2 ON r2.col1 = r1.col1
join d2 ON d2.distrib_col=d1.distrib_col;

with cte_1 AS MATERIALIZED (select col1 from reference_1)
select count(*) from distributed_1 AS d1
LEFT JOIN reference_1 AS r1 ON d1.col2=r1.col2
LEFT JOIN reference_2 AS r2 ON r2.col1 = r1.col1
join cte_1 ON cte_1.col1=d1.distrib_col;

RESET client_min_messages;
DROP SCHEMA with_join CASCADE;
