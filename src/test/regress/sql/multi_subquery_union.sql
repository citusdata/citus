--
-- multi subquery toplevel union queries aims to expand existing subquery pushdown
-- regression tests to cover more cases
-- the tables that are used depends to multi_insert_select_behavioral_analytics_create_table.sql

-- We don't need shard id sequence here, so commented out to prevent conflicts with concurrent tests
-- SET citus.next_shard_id TO 1400000;

SET citus.enable_router_execution TO false;
-- a very simple union query
SELECT user_id, counter
FROM (
    SELECT user_id, value_2 % 10 AS counter FROM events_table WHERE event_type IN (1, 2) 
      UNION 
    SELECT user_id, value_2 % 10 AS counter FROM events_table WHERE event_type IN (5, 6) 
) user_id 
ORDER BY 2 DESC,1
LIMIT 5;

-- can use different filters on partition columns
SELECT *
FROM (
    SELECT user_id, max(value_2) FROM users_table WHERE user_id = 1 GROUP BY user_id
      UNION ALL
    SELECT user_id, max(value_2) FROM users_table WHERE user_id = 5 GROUP BY user_id
) user_id
ORDER BY 2 DESC,1
LIMIT 5;

-- a very simple union query with reference table
SELECT user_id, counter
FROM (
    SELECT user_id, value_2 % 10 AS counter FROM events_table WHERE event_type IN (1, 2) 
      UNION 
    SELECT user_id, value_2 % 10 AS counter FROM events_reference_table WHERE event_type IN (5, 6) 
) user_id 
ORDER BY 2 DESC,1
LIMIT 5;

-- the same query with union all
SELECT user_id, counter
FROM (
    SELECT user_id, value_2 % 10 AS counter FROM events_table WHERE event_type IN (1, 2) 
      UNION ALL
    SELECT user_id, value_2 % 10 AS counter FROM events_table WHERE event_type IN (5, 6) 
) user_id 
ORDER BY 2 DESC,1
LIMIT 5;

-- the same query with union all and reference table
SELECT user_id, counter
FROM (
    SELECT user_id, value_2 % 10 AS counter FROM events_table WHERE event_type IN (1, 2) 
      UNION ALL
    SELECT user_id, value_2 % 10 AS counter FROM events_reference_table WHERE event_type IN (5, 6) 
) user_id 
ORDER BY 2 DESC,1
LIMIT 5;

-- the same query with group by
SELECT user_id, sum(counter) 
FROM (
    SELECT user_id, value_2 % 10 AS counter FROM events_table WHERE event_type IN (1, 2) 
      UNION 
    SELECT user_id, value_2 % 10 AS counter FROM events_table WHERE event_type IN (5, 6) 
) user_id 
GROUP BY 1
ORDER BY 2 DESC,1
LIMIT 5;

-- the same query with UNION ALL clause
SELECT user_id, sum(counter) 
FROM (
    SELECT user_id, value_2 % 10 AS counter FROM events_table WHERE event_type IN (1, 2) 
      UNION ALL
    SELECT user_id, value_2 % 10 AS counter FROM events_table WHERE event_type IN (5, 6) 
) user_id 
GROUP BY 1
ORDER BY 2 DESC,1
LIMIT 5;

-- the same query target list entries shuffled
SELECT user_id, sum(counter) 
FROM (
    SELECT value_2 % 10 AS counter, user_id FROM events_table WHERE event_type IN (1, 2) 
      UNION 
    SELECT value_2 % 10 AS counter, user_id FROM events_table WHERE event_type IN (5, 6) 
) user_id 
GROUP BY 1
ORDER BY 2 DESC,1
LIMIT 5;

-- same query with GROUP BY
SELECT user_id, sum(counter) 
FROM (
    SELECT user_id, value_2 AS counter FROM events_table WHERE event_type IN (1, 2) 
      UNION 
    SELECT user_id, value_2 AS counter FROM events_table WHERE event_type IN (5, 6) 
) user_id 
GROUP BY 
  user_id 
--HAVING sum(counter) > 900 
ORDER BY 1,2 DESC LIMIT 5;


-- the same query target list entries shuffled but this time the subqueries target list
-- is shuffled
SELECT user_id, sum(counter) 
FROM (
    SELECT value_2 AS counter, user_id FROM events_table WHERE event_type IN (1, 2) 
      UNION 
    SELECT value_2 AS counter, user_id FROM events_table WHERE event_type IN (5, 6) 
) user_id 
GROUP BY 
  user_id 
--HAVING sum(counter) > 900 
ORDER BY 1,2 DESC LIMIT 5;


-- similar query this time more subqueries and target list contains a resjunk entry
SELECT sum(counter) 
FROM (
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 1 GROUP BY user_id HAVING sum(value_2) > 5
      UNION 
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 2 and value_1 < 3 GROUP BY user_id HAVING sum(value_2) > 25
      UNION
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 3 and value_1 < 4 GROUP BY user_id HAVING sum(value_2) > 25
        UNION
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 4 and value_1 < 5 GROUP BY user_id HAVING sum(value_2) > 25
        UNION
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 5 and value_1 < 6 GROUP BY user_id HAVING sum(value_2) > 25
) user_id 
GROUP BY user_id ORDER BY 1 DESC LIMIT 5;

-- similar query this time more subqueries with reference table and target list contains a resjunk entry
SELECT sum(counter) 
FROM (
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 1 GROUP BY user_id HAVING sum(value_2) > 25
      UNION 
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 2 and value_1 < 3 GROUP BY user_id HAVING sum(value_2) > 25
      UNION
    SELECT user_id, sum(value_2) AS counter FROM users_reference_table where value_1 < 3 and value_1 < 4 GROUP BY user_id HAVING sum(value_2) > 25
        UNION
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 4 and value_1 < 5 GROUP BY user_id HAVING sum(value_2) > 25
        UNION
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 5 and value_1 < 6 GROUP BY user_id HAVING sum(value_2) > 25
) user_id 
GROUP BY user_id ORDER BY 1 DESC LIMIT 5;

-- similar query as above, with UNION ALL
SELECT sum(counter) 
FROM (
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 1 GROUP BY user_id HAVING sum(value_2) > 250
      UNION ALL
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 2 and value_1 < 3 GROUP BY user_id HAVING sum(value_2) > 25
      UNION ALL
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 3 and value_1 < 4 GROUP BY user_id HAVING sum(value_2) > 25
        UNION ALL
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 4 and value_1 < 5 GROUP BY user_id HAVING sum(value_2) > 25
        UNION ALL
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 5 and value_1 < 6 GROUP BY user_id HAVING sum(value_2) > 25
) user_id 
GROUP BY user_id ORDER BY 1 DESC LIMIT 5;

-- unions within unions
SELECT *
FROM (
        ( SELECT user_id,
                 sum(counter)
         FROM
           (SELECT 
              user_id, sum(value_2) AS counter
            FROM 
              users_table
            GROUP BY 
              user_id
          UNION 
            SELECT 
              user_id, sum(value_2) AS counter
            FROM 
              events_table
            GROUP BY 
              user_id) user_id_1
         GROUP BY 
          user_id)
      UNION
        (SELECT 
            user_id, sum(counter)
         FROM
           (SELECT 
              user_id, sum(value_2) AS counter
            FROM 
              users_table
            GROUP BY 
              user_id
          UNION 
            SELECT 
              user_id, sum(value_2) AS counter          
            FROM 
              events_table
            GROUP BY 
              user_id) user_id_2
         GROUP BY 
            user_id)) AS ftop 
ORDER BY 2 DESC, 1 DESC 
LIMIT 5;

-- unions within unions with reference table
SELECT *
FROM (
        ( SELECT user_id,
                 sum(counter)
         FROM
           (SELECT 
              user_id, sum(value_2) AS counter
            FROM 
              users_table
            GROUP BY 
              user_id
          UNION 
            SELECT 
              user_id, sum(value_2) AS counter
            FROM 
              events_reference_table
            GROUP BY 
              user_id) user_id_1
         GROUP BY 
          user_id)
      UNION
        (SELECT 
            user_id, sum(counter)
         FROM
           (SELECT 
              user_id, sum(value_2) AS counter
            FROM 
              users_table
            GROUP BY 
              user_id
          UNION 
            SELECT 
              user_id, sum(value_2) AS counter          
            FROM 
              events_table
            GROUP BY 
              user_id) user_id_2
         GROUP BY 
            user_id)) AS ftop 
ORDER BY 2 DESC, 1 DESC 
LIMIT 5;

-- top level unions are wrapped into top level aggregations
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 0 AS event
                     FROM 
                        events_table as  "events"
                     WHERE 
                        event_type IN (1, 2)) events_subquery_1) 
               UNION 
                 (SELECT *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 1 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (2, 3) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 2 AS event
                     FROM 
                        events_table as  "events"
                     WHERE 
                        event_type IN (4, 5) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 3 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (6, 1)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
) as final_query
GROUP BY types
ORDER BY types;

-- exactly the same query
-- but wrapper unions are removed from the inner part of the query
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  (SELECT *, random()
   FROM
     (SELECT 
        "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        (SELECT 
            "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM(
                  (SELECT 
                    "events"."user_id", "events"."time", 0 AS event
                   FROM 
                    events_table as  "events"
                   WHERE 
                    event_type IN (1, 2))
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 1 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (2, 3) )
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 2 AS event
                     FROM 
                        events_table as  "events"
                     WHERE 
                        event_type IN (4, 5) )
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 3 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (6, 1))) t1
         GROUP BY "t1"."user_id") AS t) "q" 
) as final_query
GROUP BY types
ORDER BY types;

-- again excatly the same query with top level wrapper removed
SELECT ("q"."event_types") as types, count(*) AS sumOfEventType
FROM
     ( SELECT "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                  (SELECT 
                    "events"."user_id", "events"."time", 0 AS event
                   FROM 
                    events_table as  "events"
                   WHERE 
                    event_type IN (1, 2))
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 1 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (2, 3) )
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 2 AS event
                     FROM 
                        events_table as  "events"
                     WHERE 
                        event_type IN (4, 5) )
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 3 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (6, 1))) t1
         GROUP BY "t1"."user_id") AS t) "q" 
GROUP BY types
ORDER BY types;

-- again same query but with only two top level empty queries (i.e., no group bys)
SELECT *
FROM
     ( SELECT * 
      FROM
        ( SELECT "t1"."user_id"
         FROM (
                  (SELECT 
                    "events"."user_id", "events"."time", 0 AS event
                   FROM 
                    events_table as  "events"
                   WHERE 
                    event_type IN (1, 2))
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 1 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (2, 3) )
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 2 AS event
                     FROM 
                        events_table as  "events"
                     WHERE 
                        event_type IN (4, 5) )
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 3 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (6, 1))) t1
        ) AS t) "q" 
ORDER BY 1 
LIMIT 5;

-- a very similar query UNION ALL
SELECT ("q"."event_types") as types, count(*) AS sumOfEventType
FROM
     ( SELECT "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                  (SELECT 
                    "events"."user_id", "events"."time", 0 AS event
                   FROM 
                    events_table as  "events"
                   WHERE 
                    event_type IN (1, 2))
               UNION ALL
                    (SELECT 
                        "events"."user_id", "events"."time", 1 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (2, 3) )
               UNION ALL
                    (SELECT 
                        "events"."user_id", "events"."time", 2 AS event
                     FROM 
                        events_table as  "events"
                     WHERE 
                        event_type IN (4, 5) )
               UNION ALL
                    (SELECT 
                        "events"."user_id", "events"."time", 3 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (6, 1))) t1
         GROUP BY "t1"."user_id") AS t) "q" 
GROUP BY types
ORDER BY types;

-- some UNION ALL queries that are going to be pulled up
SELECT 
  count(*)
FROM 
(
  (SELECT user_id FROM users_table)
    UNION ALL
  (SELECT user_id FROM events_table)
) b;

-- some UNION ALL queries that are going to be pulled up with reference table
SELECT 
  count(*)
FROM 
(
  (SELECT user_id FROM users_table)
    UNION ALL
  (SELECT user_id FROM events_reference_table)
) b;

-- similar query without top level agg
SELECT 
  user_id
FROM 
(
  (SELECT user_id FROM users_table)
    UNION ALL
  (SELECT user_id FROM events_table)
) b
ORDER BY 1 DESC
LIMIT 5;

-- similar query with multiple target list entries
SELECT 
  user_id, value_3
FROM 
(
  (SELECT value_3, user_id FROM users_table)
    UNION ALL
  (SELECT value_3, user_id FROM events_table)
) b
ORDER BY 1 DESC, 2 DESC
LIMIT 5;

-- similar query group by inside the subqueries
SELECT 
  user_id, value_3_sum
FROM 
(
  (SELECT sum(value_3) as value_3_sum, user_id FROM users_table GROUP BY user_id)
    UNION ALL
  (SELECT sum(value_3) as value_3_sum, user_id FROM users_table GROUP BY user_id)
) b
ORDER BY 2 DESC, 1 DESC
LIMIT 5;

-- similar query top level group by
SELECT 
  user_id, sum(value_3)
FROM 
(
  (SELECT value_3, user_id FROM users_table)
    UNION ALL
  (SELECT value_3, user_id FROM events_table)
) b
GROUP BY 1
ORDER BY 2 DESC, 1 DESC
LIMIT 5;

-- a long set operation list
SELECT 
  user_id, value_3
FROM 
(
  (SELECT value_3, user_id FROM events_table where event_type IN (1, 2))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (2, 3))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (3, 4))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (4, 5))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (5, 6))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (1, 6))
) b
ORDER BY 1 DESC, 2 DESC
LIMIT 5;

-- no partition key on the top
SELECT 
  max(value_3)
FROM 
(
  (SELECT value_3, user_id FROM events_table where event_type IN (1, 2))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (2, 3))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (3, 4))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (4, 5))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (5, 6))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (1, 6))
) b
GROUP BY user_id
ORDER BY 1 DESC
LIMIT 5;


-- now lets also have some unsupported queries

-- group by is not on the partition key
-- but we can still recursively plan it, though that is not suffient for pushdown
-- of the whole query
SELECT user_id, sum(counter) 
FROM (
    SELECT user_id, sum(value_2) AS counter FROM events_table GROUP BY user_id
      UNION
    SELECT value_1 as user_id, sum(value_2) AS counter FROM users_table GROUP BY value_1
) user_id 
GROUP BY user_id;

-- partition key is not selected
SELECT sum(counter) 
FROM (
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 1 GROUP BY user_id HAVING sum(value_2) > 25
      UNION 
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 2 and value_1 < 3 GROUP BY user_id HAVING sum(value_2) > 25
      UNION
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 3 and value_1 < 4 GROUP BY user_id HAVING sum(value_2) > 25
      UNION
    SELECT user_id, sum(value_2) AS counter FROM users_table where value_1 < 4 and value_1 < 5 GROUP BY user_id HAVING sum(value_2) > 25
      UNION
    SELECT 2 * user_id, sum(value_2) AS counter FROM users_table where value_1 < 5 and value_1 < 6 GROUP BY user_id HAVING sum(value_2) > 25
) user_id 
GROUP BY user_id ORDER BY 1 DESC LIMIT 5;

-- excepts within unions are not supported
SELECT * FROM
(
(
  SELECT user_id, sum(counter) 
    FROM (
     SELECT user_id, sum(value_2) AS counter FROM users_table GROUP BY user_id
        UNION 
     SELECT user_id, sum(value_2) AS counter FROM events_table GROUP BY user_id
  ) user_id_1
  GROUP BY user_id
) 
UNION
(
  SELECT user_id, sum(counter) 
    FROM (
      SELECT user_id, sum(value_2) AS counter FROM users_table GROUP BY user_id
        EXCEPT 
      SELECT user_id, sum(value_2) AS counter FROM events_table GROUP BY user_id
) user_id_2
  GROUP BY user_id)
) as ftop;

-- non-equi join are not supported since there is no equivalence between the partition column
SELECT user_id, sum(counter) 
FROM (
    SELECT user_id, sum(value_2) AS counter FROM users_table GROUP BY user_id
      UNION 
    SELECT events_table.user_id, sum(events_table.value_2) AS counter FROM events_table, users_table WHERE users_table.user_id > events_table.user_id GROUP BY 1
) user_id 
GROUP BY user_id;

-- non-equi join also not supported for UNION ALL
SELECT user_id, sum(counter)
FROM (
    SELECT user_id, sum(value_2) AS counter FROM users_table GROUP BY user_id
      UNION ALL
    SELECT events_table.user_id, sum(events_table.value_2) AS counter FROM events_table, users_table WHERE users_table.user_id > events_table.user_id GROUP BY 1
) user_id 
GROUP BY user_id;

-- joins inside unions are supported -- slightly more comlex than the above
SELECT * FROM
(
(
  SELECT user_id, sum(counter) 
    FROM (
     SELECT user_id, sum(value_2) AS counter FROM users_table GROUP BY user_id
       UNION 
     SELECT user_id, sum(value_2) AS counter FROM events_table GROUP BY user_id
  ) user_id_1
  GROUP BY user_id
) 
UNION
(
  SELECT user_id, sum(counter) 
    FROM (
      SELECT user_id, sum(value_2) AS counter FROM users_table GROUP BY user_id
        UNION 
      SELECT events_table.user_id, sum(events_table.value_2) AS counter FROM events_table, users_table WHERE (events_table.user_id = users_table.user_id) GROUP BY events_table.user_id
) user_id_2
  GROUP BY user_id)
) as ftop
ORDER BY 2, 1
LIMIT 10;

-- mix up the joins a bit
SELECT * FROM
(
(
  SELECT sum(users_table.value_2), events_table.user_id
  FROM users_table, events_table
  WHERE users_table.user_id = events_Table.user_id
  GROUP BY events_table.user_id
)
UNION
(
  SELECT sum(users_table.value_2), user_id
  FROM users_table LEFT JOIN events_table USING (user_id)
  GROUP BY user_id
)
) ftop
ORDER BY 2, 1
LIMIT 10;

SELECT * FROM
(
(
  SELECT value_2, user_id
  FROM users_table
)
UNION
(
  SELECT sum(users_table.value_2), user_id
  FROM users_table RIGHT JOIN events_table USING (user_id)
  GROUP BY user_id
)
) ftop
ORDER BY 2, 1
LIMIT 10;

-- UNION ALL with joins is supported
SELECT * FROM
(
(
  SELECT sum(users_table.value_2), events_table.user_id
  FROM users_table, events_table
  WHERE users_table.user_id = events_Table.user_id
  GROUP BY events_table.user_id
)
UNION ALL
(
  SELECT sum(users_table.value_2), user_id
  FROM users_table JOIN events_table USING (user_id)
  GROUP BY user_id
)
) ftop
ORDER BY 2, 1
LIMIT 10;

-- offset inside the union
SELECT user_id, sum(counter) 
FROM (
    SELECT user_id, sum(value_2) AS counter FROM events_table GROUP BY user_id
      UNION
    SELECT user_id, sum(value_2) AS counter FROM users_table GROUP BY user_id OFFSET 4
) user_id 
GROUP BY user_id;

-- lower level union does not return partition key with the other relations
SELECT *
FROM (
        ( SELECT user_id,
                 sum(counter)
         FROM
           (SELECT 
              user_id, sum(value_2) AS counter
            FROM 
              users_table
            GROUP BY 
              user_id
          UNION 
            SELECT 
              user_id, sum(value_2) AS counter
            FROM 
              events_table
            GROUP BY 
              user_id) user_id_1
         GROUP BY 
          user_id)
      UNION
        (SELECT 
            user_id, sum(counter)
         FROM
           (SELECT 
              sum(value_2) AS counter, user_id
            FROM 
              users_table
            GROUP BY 
              user_id
          UNION 
            SELECT 
              user_id, sum(value_2) AS counter          
            FROM 
              events_table
            GROUP BY 
              user_id) user_id_2
         GROUP BY 
            user_id)) AS ftop;


-- some UNION all queries that are going to be pulled up
SELECT 
  count(*)
FROM 
(
  (SELECT user_id FROM users_table)
    UNION ALL
  (SELECT 2 * user_id FROM events_table)
) b;

-- last query does not have partition key
SELECT 
  user_id, value_3
FROM 
(
  (SELECT value_3, user_id FROM events_table where event_type IN (1, 2))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (2, 3))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (3, 4))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (4, 5))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (5, 6))
    UNION ALL
  (SELECT value_3, value_2 FROM events_table where event_type IN (1, 6))
) b
ORDER BY 1 DESC, 2 DESC
LIMIT 5;

-- we don't allow joins within unions
SELECT 
  count(*)
FROM 
(
  (SELECT user_id FROM users_table)
    UNION ALL
  (SELECT users_table.user_id FROM events_table, users_table WHERE events_table.user_id = users_table.user_id)
) b;

-- we don't support pushing down subqueries without relations
-- recursive planning can replace that query, though the whole
-- query is not safe to pushdown
SELECT 
  count(*)
FROM 
(
  (SELECT user_id FROM users_table)
    UNION ALL
  (SELECT 1)
) b;

-- we don't support pushing down subqueries without relations
-- recursive planning can replace that query, though the whole
-- query is not safe to pushdown
SELECT 
  *
FROM 
(
  (SELECT user_id FROM users_table)
    UNION ALL
  (SELECT (random() * 100)::int)
) b;

-- we don't support subqueries without relations
SELECT 
  user_id, value_3
FROM 
(
  (SELECT value_3, user_id FROM events_table where event_type IN (1, 2))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (2, 3))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (3, 4))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (4, 5))
    UNION ALL
  (SELECT value_3, user_id FROM events_table where event_type IN (5, 6))
    UNION ALL
  (SELECT 1, 2)
) b
ORDER BY 1 DESC, 2 DESC
LIMIT 5;

-- we don't support pushing down subqueries without relations
-- recursive planning can replace that query, though the whole
-- query is not safe to pushdown
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 0 AS event
                     FROM 
                        events_table as  "events"
                     WHERE 
                        event_type IN (1, 2)) events_subquery_1) 
               UNION 
                 (SELECT *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 1 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (2, 3) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 2 AS event
                     FROM 
                        events_table as  "events"
                     WHERE 
                        event_type IN (4, 5) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT 1, now(), 3 AS event) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
) as final_query
GROUP BY types
ORDER BY types;

SET citus.enable_router_execution TO true;

DROP TABLE events_reference_table;
DROP TABLE users_reference_table;
