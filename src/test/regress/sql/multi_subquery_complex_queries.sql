--
-- multi subquery complex queries aims to expand existing subquery pushdown
-- regression tests to cover more caeses
-- the tables that are used depends to multi_insert_select_behavioral_analytics_create_table.sql
--

-- We don't need shard id sequence here, so commented out to prevent conflicts with concurrent tests
-- SET citus.next_shard_id TO 1400000;

 --
 -- UNIONs and JOINs mixed
 --
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT
                    *
                  FROM
                   (SELECT
                      "events"."user_id", "events"."time", 0 AS event
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                 UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_2)
               UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                      event_type IN (5, 6) ) events_subquery_3)
                UNION
                  (SELECT
                      *
                   FROM
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (1, 6)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
    ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

-- same query with target entries shuffled inside UNIONs
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
                          "events"."time", 0 AS event, "events"."user_id"
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                 UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 1 AS event, "events"."user_id"
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_2)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 2 AS event, "events"."user_id"
                     FROM
                      events_table as  "events"
                     WHERE
                      event_type IN (5, 6) ) events_subquery_3)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 3 AS event, "events"."user_id"
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (1, 6)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
  ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

-- supported through recursive planning since events_subquery_2 doesn't have partition key on the target list
-- within the shuffled target list
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
                          "events"."time", 0 AS event, "events"."user_id"
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                 UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 1 AS event, "events"."user_id" * 2
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_2)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 2 AS event, "events"."user_id"
                     FROM
                      events_table as  "events"
                     WHERE
                      event_type IN (5, 6) ) events_subquery_3)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 3 AS event, "events"."user_id"
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (1, 6)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
  ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

-- supported through recursive planning since events_subquery_2 doesn't have partition key on the target list
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
                          "events"."time", 0 AS event, "events"."user_id"
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                 UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 1 AS event, "events"."value_2" as user_id
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_2)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 2 AS event, "events"."user_id"
                     FROM
                      events_table as  "events"
                     WHERE
                      event_type IN (5, 6) ) events_subquery_3)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 3 AS event, "events"."user_id"
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (1, 6)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
  ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

-- we can support arbitrary subqueries within UNIONs
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT
      *, random()
    FROM
     (SELECT
        "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT
            "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
          FROM (
                (SELECT
                    *
                 FROM
                   (SELECT
                          "events"."time", 0 AS event, "events"."user_id"
                    FROM
                      events_table as "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                UNION
                 (SELECT *
                  FROM
                    (
                          SELECT * FROM
                          (
                              SELECT
                                max("events"."time"),
                                0 AS event,
                                "events"."user_id"
                              FROM
                                events_table as  "events", users_table as "users"
                              WHERE
                                events.user_id = users.user_id AND
                                event_type IN (1, 2)
                                GROUP BY   "events"."user_id"
                          ) as events_subquery_5
                     ) events_subquery_2)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 2 AS event, "events"."user_id"
                     FROM
                        events_table as  "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_3)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                       "events"."time", 3 AS event, "events"."user_id"
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (5, 6)) events_subquery_4)
                 ) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
     ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

SET citus.enable_repartition_joins to ON;
SET client_min_messages TO DEBUG1;

-- recursively planned since events_subquery_5 is not joined on partition key
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT
      *, random()
    FROM
     (SELECT
        "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT
            "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
          FROM (
                (SELECT
                    *
                 FROM
                   (SELECT
                          "events"."time", 0 AS event, "events"."user_id"
                    FROM
                      events_table as "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                UNION
                 (SELECT *
                  FROM
                    (
                          SELECT * FROM
                          (
                              SELECT
                                max("events"."time"),
                                0 AS event,
                                "events"."user_id"
                              FROM
                                events_table as  "events", users_table as "users"
                              WHERE
                                events.user_id = users.value_2 AND
                                event_type IN (1, 2)
                                GROUP BY   "events"."user_id"
                          ) as events_subquery_5
                     ) events_subquery_2)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 2 AS event, "events"."user_id"
                     FROM
                        events_table as  "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_3)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                       "events"."time", 3 AS event, "events"."user_id"
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (5, 6)) events_subquery_4)
                 ) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
     ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

RESET client_min_messages;
SET citus.enable_repartition_joins to OFF;

-- recursively planned since the join is not equi join
SET client_min_messages TO DEBUG1;
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT
                    *
                  FROM
                   (SELECT
                      "events"."user_id", "events"."time", 0 AS event
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                 UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_2)
               UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                      event_type IN (5, 6) ) events_subquery_3)
                UNION
                  (SELECT
                      *
                   FROM
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (4, 5)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
    ON (t.user_id != q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;
RESET client_min_messages;

-- not supported since subquery 3 includes a JOIN with non-equi join
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT
                    *
                  FROM
                   (SELECT
                      "events"."user_id", "events"."time", 0 AS event
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                 UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_2)
               UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        events_table as  "events",  users_table as "users"
                     WHERE
                      event_type IN (5, 6)  AND users.user_id != events.user_id ) events_subquery_3)
                UNION
                  (SELECT
                      *
                   FROM
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (4, 5)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
    ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

-- similar query with more union statements (to enable UNION tree become larger)
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
                      event_type IN (1, 2) ) events_subquery_1)
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
                        event_type IN (3, 4) ) events_subquery_3)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (4, 5)) events_subquery_4)

                  UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 4 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (5, 6)) events_subquery_5)

                  UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 5 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (6, 1)) events_subquery_6)

                  UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 6 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (2, 5)) events_subquery_6)
                 ) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 0 and value_1 < 4) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;


--
-- UNION ALL Queries
--
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
                      event_type IN (1, 2) ) events_subquery_1)
                UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (3, 4) ) events_subquery_2)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                        event_type IN (5, 6) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (6, 1)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 0 and value_1 < 4) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- same query target list entries shuffled
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
                          "events"."time", 0 AS event, "events"."user_id"
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 1 AS event, "events"."user_id"
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (3, 4) ) events_subquery_2)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 2 AS event, "events"."user_id"
                     FROM
                        events_table as  "events"
                     WHERE
                        event_type IN (5, 6) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 3 AS event, "events"."user_id"
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (1, 6)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- supported through recursive planning since subquery 3 does not have partition key
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
                      event_type IN (1, 2) ) events_subquery_1)
                UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (3, 4) ) events_subquery_2)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."value_2", "events"."time", 2 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                        event_type IN (5, 6) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (1, 6)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 0 and value_1 < 4) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- supported through recursive planning since events_subquery_4 does not have partition key on the
-- target list
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
                          "events"."time", 0 AS event, "events"."user_id"
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 1 AS event, "events"."user_id"
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (3, 4) ) events_subquery_2)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 2 AS event, "events"."user_id"
                     FROM
                        events_table as  "events"
                     WHERE
                        event_type IN (5, 6) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."time", 3 AS event, "events"."user_id" * 2
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (1, 6)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- union all with inner and left joins
SELECT user_id, count(*) as cnt
FROM
  (SELECT first_query.user_id, random()
   FROM
     ( SELECT
        "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
       FROM
        ( SELECT
            "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 0 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                        event_type IN (1, 2) ) events_subquery_1)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (3, 4) ) events_subquery_2)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                        event_type IN (5, 6) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (1, 6)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "first_query"
INNER JOIN
     (SELECT "t"."user_id"
      FROM
        (SELECT
            "users"."user_id"
         FROM
           users_table as "users"
        WHERE
          value_1 > 0 and value_1 < 4) AS t
        LEFT OUTER JOIN
        (
          SELECT
            DISTINCT "events"."user_id" as user_id
          FROM
            events_table as "events"
          WHERE
            event_type IN (0, 6)
          GROUP BY
            user_id
         ) as t2
    ON (t2.user_id = t.user_id) WHERE t2.user_id is NULL) as second_query
  ON ("first_query".user_id = "second_query".user_id)) as final_query
GROUP BY
  user_id ORDER BY cnt DESC, user_id DESC
LIMIT 10;

-- recursively planned since the join between t and t2 is not equi join
-- union all with inner and left joins
SET client_min_messages TO DEBUG1;
SELECT user_id, count(*) as cnt
FROM
  (SELECT first_query.user_id, random()
   FROM
     ( SELECT
        "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
       FROM
        ( SELECT
            "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 0 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                        event_type IN (1, 2) ) events_subquery_1)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (3, 4) ) events_subquery_2)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                        event_type IN (5, 6) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (1, 6)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "first_query"
INNER JOIN
     (SELECT "t"."user_id"
      FROM
        (SELECT
            "users"."user_id"
         FROM
           users_table as "users"
        WHERE
          value_1 > 0 and value_1 < 4) AS t
        LEFT OUTER JOIN
        (
          SELECT
            DISTINCT "events"."user_id" as user_id
          FROM
            events_table as "events"
          WHERE
            event_type IN (0, 6)
          GROUP BY
            user_id
         ) as t2
    ON (t2.user_id > t.user_id) WHERE t2.user_id is NULL) as second_query
  ON ("first_query".user_id = "second_query".user_id)) as final_query
GROUP BY
  user_id ORDER BY cnt DESC, user_id DESC
LIMIT 10;

RESET client_min_messages;

 --
 -- Union, inner join and left join
 --
SELECT user_id, count(*) as cnt
FROM
  (SELECT first_query.user_id, random()
   FROM
     ( SELECT
        "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
       FROM
        ( SELECT
            "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 0 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                        event_type IN (1, 2) ) events_subquery_1)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (3, 4) ) events_subquery_2)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                        event_type IN (5, 6) ) events_subquery_3)
               UNION
                 (SELECT *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                        events_table as "events"
                     WHERE
                        event_type IN (1, 6)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "first_query"
INNER JOIN
     (SELECT "t"."user_id"
      FROM
        (SELECT
            "users"."user_id"
         FROM
           users_table as "users"
        WHERE
          value_1 > 0 and value_1 < 4) AS t
        LEFT OUTER JOIN
        (
          SELECT
            DISTINCT "events"."user_id" as user_id
          FROM
            events_table as "events"
          WHERE
            event_type IN (0, 6)
          GROUP BY
            user_id
         ) as t2
    ON (t2.user_id = t.user_id) WHERE t2.user_id is NULL) as second_query
  ON ("first_query".user_id = "second_query".user_id)) as final_query
GROUP BY
  user_id ORDER BY cnt DESC, user_id DESC
LIMIT 10;

-- Simple LATERAL JOINs with GROUP BYs in each side
-- need to set subquery_pushdown due to limit for next 2 queries
SET citus.subquery_pushdown to ON;
SELECT *
FROM
  (SELECT "some_users_data".user_id, lastseen
   FROM
     (SELECT user_id, max(time) AS lastseen
      FROM
        (SELECT user_id, time
         FROM
           (SELECT
              user_id, time
            FROM
              events_table as "events"
            WHERE
              user_id > 1 and user_id < 4) "events_1"
         ORDER BY
          time DESC
         LIMIT 1000) "recent_events_1"
      GROUP BY
        user_id
      ORDER BY
        max(time) DESC) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table as "users"
      WHERE
        "users"."user_id" = "some_recent_users"."user_id" AND
        users.value_2 > 1 and users.value_2 < 3
      LIMIT 1) "some_users_data"
    ON TRUE
   ORDER BY
    lastseen DESC
   LIMIT 50) "some_users"
order BY
  user_id
LIMIT 50;

-- same query with subuqery joins in topmost select
SELECT "some_users_data".user_id, lastseen
FROM
     (SELECT user_id, max(time) AS lastseen
      FROM
        (SELECT user_id, time
         FROM
           (SELECT
              user_id, time
            FROM
              events_table as "events"
            WHERE
              user_id > 1 and user_id < 4) "events_1"
         ORDER BY
           time DESC
         LIMIT 1000) "recent_events_1"
      GROUP BY
        user_id
      ORDER BY
        max(TIME) DESC) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table as "users"
      WHERE
        "users"."user_id" = "some_recent_users"."user_id" AND
        users.value_2 > 1 and users.value_2 < 3
      ORDER BY 1 LIMIT 1) "some_users_data"
     ON TRUE
ORDER BY
  user_id
limit 50;

-- reset subquery_pushdown
SET citus.subquery_pushdown to OFF;

-- mixture of recursively planned subqueries and correlated subqueries
SELECT "some_users_data".user_id, lastseen
FROM
     (SELECT user_id, max(time) AS lastseen
      FROM
        (SELECT user_id, time
         FROM
           (SELECT
              user_id, time
            FROM
              events_table as "events"
            WHERE
              user_id > 1 and user_id < 4) "events_1"
         ORDER BY
           time DESC
         LIMIT 1000) "recent_events_1"
      GROUP BY
        user_id
      ORDER BY
        max(TIME) DESC) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table as "users"
      WHERE
        "users"."value_1" = "some_recent_users"."user_id" AND
        users.value_2 > 1 and users.value_2 < 3
      ORDER BY 1 LIMIT 1) "some_users_data"
     ON TRUE
ORDER BY
  user_id
limit 50;

SELECT "some_users_data".user_id, lastseen
FROM
     (SELECT 2 * user_id as user_id, max(time) AS lastseen
      FROM
        (SELECT user_id, time
         FROM
           (SELECT
              user_id, time
            FROM
              events_table as "events"
            WHERE
              user_id > 1 and user_id < 4) "events_1"
         ORDER BY
           time DESC
         LIMIT 1000) "recent_events_1"
      GROUP BY
        user_id
      ORDER BY
        max(TIME) DESC) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table as "users"
      WHERE
        "users"."user_id" = "some_recent_users"."user_id" AND
        users.value_2 > 1 and users.value_2 < 3
      ORDER BY 1 LIMIT 1) "some_users_data"
     ON TRUE
ORDER BY
  user_id
limit 50;

-- LATERAL JOINs used with INNER JOINs
SET citus.subquery_pushdown to ON;
SELECT user_id, lastseen
FROM
  (SELECT
      "some_users_data".user_id, lastseen
   FROM
     (SELECT
        filter_users_1.user_id, time AS lastseen
      FROM
        (SELECT
            user_where_1_1.user_id
         FROM
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 3 and value_1 > 2) user_where_1_1
         INNER JOIN
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 3 and value_2 > 3) user_where_1_join_1
           ON ("user_where_1_1".user_id = "user_where_1_join_1".user_id))
        filter_users_1
      JOIN LATERAL
        (SELECT
            user_id, time
         FROM
          events_table as "events"
         WHERE
           user_id > 1 and user_id < 3 AND
           user_id = filter_users_1.user_id
         ORDER BY
          time DESC
         LIMIT 1) "last_events_1"
        ON TRUE
      ORDER BY
        time DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table  as "users"
      WHERE
        "users"."user_id" = "some_recent_users"."user_id" AND
        "users"."value_2" > 4
      ORDER BY 1 LIMIT 1) "some_users_data"
    ON TRUE
   ORDER BY
      lastseen DESC
   LIMIT 10) "some_users"
ORDER BY
  user_id DESC, lastseen DESC
LIMIT 10;

--
-- A similar query with topmost select is dropped
-- and replaced by aggregation. Notice the heavy use of limit
--
SELECT "some_users_data".user_id, MAX(lastseen), count(*)
   FROM
     (SELECT
        filter_users_1.user_id, time AS lastseen
      FROM
        (SELECT
            user_where_1_1.user_id
         FROM
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 3 and value_1 > 2) user_where_1_1
         INNER JOIN
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 3 and value_2 > 3) user_where_1_join_1
           ON ("user_where_1_1".user_id = "user_where_1_join_1".user_id)) filter_users_1
      JOIN LATERAL
        (SELECT
            user_id, time
         FROM
          events_table as "events"
         WHERE
          user_id > 1 and user_id < 3 and user_id = filter_users_1.user_id
         ORDER BY
          time DESC
         LIMIT 1) "last_events_1" ON true
      ORDER BY time DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table  as "users"
      WHERE
        "users"."user_id" = "some_recent_users"."user_id" AND
        "users"."value_2" > 4
      ORDER BY 1 LIMIT 1) "some_users_data" ON true
GROUP BY 1
ORDER BY 2, 1 DESC
LIMIT 10;

SET citus.subquery_pushdown to OFF;

-- not supported since the inner JOIN is not equi join and LATERAL JOIN prevents recursive planning
SET client_min_messages TO DEBUG2;
SELECT user_id, lastseen
FROM
  (SELECT
      "some_users_data".user_id, lastseen
   FROM
     (SELECT
        filter_users_1.user_id, time AS lastseen
      FROM
        (SELECT
            user_where_1_1.user_id
         FROM
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 4 and value_1 > 2) user_where_1_1
         INNER JOIN
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 4  and value_2 > 3) user_where_1_join_1
           ON ("user_where_1_1".user_id != "user_where_1_join_1".user_id)) filter_users_1
      JOIN LATERAL
        (SELECT
            user_id, time
         FROM
          events_table as "events"
         WHERE
          user_id > 1 and user_id < 4 and user_id = filter_users_1.user_id
         ORDER BY
          time DESC
         LIMIT 1) "last_events_1" ON true
      ORDER BY time DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table  as "users"
      WHERE
        "users"."user_id" = "some_recent_users"."user_id" AND
        "users"."value_2" > 4
      ORDER BY 1 LIMIT 1) "some_users_data" ON true
   ORDER BY
    lastseen DESC
   LIMIT 10) "some_users"
ORDER BY
  user_id DESC, lastseen DESC
LIMIT 10;


SET citus.enable_repartition_joins to ON;
SET client_min_messages TO DEBUG1;

-- recursively planner since the inner JOIN is not on the partition key
SELECT user_id, lastseen
FROM
  (SELECT
      "some_users_data".user_id, lastseen
   FROM
     (SELECT
        filter_users_1.user_id, time AS lastseen
      FROM
        (SELECT
            user_where_1_1.user_id
         FROM
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 4 and value_1 > 2) user_where_1_1
         INNER JOIN
           (SELECT
              "users"."user_id", "users"."value_1"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 4 and value_2 > 3) user_where_1_join_1
           ON ("user_where_1_1".user_id = "user_where_1_join_1".value_1)) filter_users_1
      JOIN LATERAL
        (SELECT
            user_id, time
         FROM
          events_table as "events"
         WHERE
          user_id > 1 and user_id < 4 and user_id = filter_users_1.user_id
         ORDER BY
          time DESC
         LIMIT 1) "last_events_1" ON true
      ORDER BY time DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table  as "users"
      WHERE
        "users"."user_id" = "some_recent_users"."user_id" AND
        "users"."value_2" > 4
      ORDER BY 1 LIMIT 1) "some_users_data" ON true
   ORDER BY
    lastseen DESC
   LIMIT 10) "some_users"
ORDER BY
  user_id DESC, lastseen DESC
LIMIT 10;


SET citus.enable_repartition_joins to OFF;
RESET client_min_messages;

-- not supported since upper LATERAL JOIN is not equi join
SELECT user_id, lastseen
FROM
  (SELECT
      "some_users_data".user_id, lastseen
   FROM
     (SELECT
        filter_users_1.user_id, time AS lastseen
      FROM
        (SELECT
            user_where_1_1.user_id
         FROM
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 3 and value_1 > 2) user_where_1_1
         INNER JOIN
           (SELECT
              "users"."user_id", "users"."value_1"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 3 and value_2 > 3) user_where_1_join_1
           ON ("user_where_1_1".user_id = "user_where_1_join_1".user_id)) filter_users_1
      JOIN LATERAL
        (SELECT
            user_id, time
         FROM
          events_table as "events"
         WHERE
          user_id > 1 and user_id < 3 and user_id != filter_users_1.user_id
         ORDER BY
          time DESC
         LIMIT 1) "last_events_1" ON true
      ORDER BY time DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table  as "users"
      WHERE
        "users"."user_id" = "some_recent_users"."user_id" AND
        "users"."value_2" > 4
      LIMIT 1) "some_users_data" ON true
   ORDER BY
    lastseen DESC
   LIMIT 10) "some_users"
ORDER BY
  user_id DESC, lastseen DESC
LIMIT 10;

-- complex lateral join between inner join and correlated subquery
SELECT user_id, lastseen
FROM
  (SELECT
      "some_users_data".user_id, lastseen
   FROM
     (SELECT
        filter_users_1.user_id, time AS lastseen
      FROM
        (SELECT
            user_where_1_1.user_id
         FROM
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 3 and value_1 > 2) user_where_1_1
         INNER JOIN
           (SELECT
              "users"."user_id", "users"."value_1"
            FROM
              users_table as "users"
            WHERE
              user_id > 1 and user_id < 3 and value_2 > 3) user_where_1_join_1
           ON ("user_where_1_1".user_id = "user_where_1_join_1".user_id)) filter_users_1
      JOIN LATERAL
        (SELECT
            user_id, time
         FROM
          events_table as "events"
         WHERE
          user_id > 1 and user_id < 3 and user_id = filter_users_1.user_id
         ORDER BY
          time DESC
         LIMIT 1) "last_events_1" ON true
      ORDER BY time DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table  as "users"
      WHERE
        "users"."value_1" = "some_recent_users"."user_id" AND
        "users"."value_2" > 4
      ORDER BY 1 LIMIT 1) "some_users_data" ON true
   ORDER BY
    lastseen DESC
   LIMIT 10) "some_users"
ORDER BY
  user_id DESC, lastseen DESC
LIMIT 10;

-- NESTED INNER JOINs
SELECT
 count(*) AS value, "generated_group_field"
 FROM
  (SELECT
      DISTINCT "pushedDownQuery"."real_user_id", "generated_group_field"
    FROM
     (SELECT
        "eventQuery"."real_user_id", "eventQuery"."time", random(), ("eventQuery"."value_2") AS "generated_group_field"
      FROM
        (SELECT
            *
         FROM
           (SELECT
              "events"."time", "events"."user_id", "events"."value_2"
            FROM
              events_table as "events"
            WHERE
              user_id > 1 and user_id < 4 AND event_type IN (4, 5) ) "temp_data_queries"
            INNER  JOIN
           (SELECT
              user_where_1_1.real_user_id
            FROM
              (SELECT
                  "users"."user_id" as real_user_id
               FROM
                users_table as "users"
               WHERE
                 user_id > 1 and user_id < 4 and value_2 > 3 ) user_where_1_1
            INNER JOIN
              (SELECT
                  "users"."user_id"
               FROM
                users_table as "users"
               WHERE
                user_id > 1 and user_id < 4 and value_3 > 3 ) user_where_1_join_1
           ON ("user_where_1_1".real_user_id = "user_where_1_join_1".user_id)) "user_filters_1"
      ON ("temp_data_queries".user_id = "user_filters_1".real_user_id)) "eventQuery") "pushedDownQuery") "pushedDownQuery"
GROUP BY
  "generated_group_field"
ORDER BY
  generated_group_field DESC, value DESC;


SET citus.enable_repartition_joins to ON;
SET client_min_messages TO DEBUG1;

-- recursively planned since the first inner join is not on the partition key
SELECT
 count(*) AS value, "generated_group_field"
 FROM
  (SELECT
      DISTINCT "pushedDownQuery"."real_user_id", "generated_group_field"
    FROM
     (SELECT
        "eventQuery"."real_user_id", "eventQuery"."time", random(), ("eventQuery"."value_2") AS "generated_group_field"
      FROM
        (SELECT
            *
         FROM
           (SELECT
              "events"."time", "events"."user_id", "events"."value_2"
            FROM
              events_table as "events"
            WHERE
              user_id > 1 and user_id < 4 AND event_type IN (4, 5) ) "temp_data_queries"
            INNER  JOIN
           (SELECT
              user_where_1_1.real_user_id
            FROM
              (SELECT
                  "users"."user_id" as real_user_id
               FROM
                users_table as "users"
               WHERE
                 user_id > 1 and user_id < 4 and value_2 > 3 ) user_where_1_1
            INNER JOIN
              (SELECT
                  "users"."user_id", "users"."value_2"
               FROM
                users_table as "users"
               WHERE
                user_id > 1 and user_id < 4 and value_3 > 3 ) user_where_1_join_1
           ON ("user_where_1_1".real_user_id = "user_where_1_join_1".value_2)) "user_filters_1"
      ON ("temp_data_queries".user_id = "user_filters_1".real_user_id)) "eventQuery") "pushedDownQuery") "pushedDownQuery"
GROUP BY
  "generated_group_field"
ORDER BY
  generated_group_field DESC, value DESC;

-- recursive planning kicked-in since the non-equi join is among subqueries
SELECT
 count(*) AS value, "generated_group_field"
 FROM
  (SELECT
      DISTINCT "pushedDownQuery"."real_user_id", "generated_group_field"
    FROM
     (SELECT
        "eventQuery"."real_user_id", "eventQuery"."time", random(), ("eventQuery"."value_2") AS "generated_group_field"
      FROM
        (SELECT
            *
         FROM
           (SELECT
              "events"."time", "events"."user_id", "events"."value_2"
            FROM
              events_table as "events"
            WHERE
              user_id > 1 and user_id < 4 AND event_type IN (4, 5) ) "temp_data_queries"
            INNER  JOIN
           (SELECT
              user_where_1_1.real_user_id
            FROM
              (SELECT
                  "users"."user_id" as real_user_id
               FROM
                users_table as "users"
               WHERE
                 user_id > 1 and user_id < 4 and value_2 > 3 ) user_where_1_1
            INNER JOIN
              (SELECT
                  "users"."user_id", "users"."value_2"
               FROM
                users_table as "users"
               WHERE
                user_id > 1 and user_id < 4 and value_3 > 3 ) user_where_1_join_1
           ON ("user_where_1_1".real_user_id >= "user_where_1_join_1".user_id)) "user_filters_1"
      ON ("temp_data_queries".user_id = "user_filters_1".real_user_id)) "eventQuery") "pushedDownQuery") "pushedDownQuery"
GROUP BY
  "generated_group_field"
ORDER BY
  generated_group_field DESC, value DESC;


SET citus.enable_repartition_joins to OFF;
RESET client_min_messages;

-- single level inner joins
SELECT
  "value_3", count(*) AS cnt
FROM
	(SELECT
      "value_3", "user_id",  random()
  	 FROM
    	(SELECT
          users_in_segment_1.user_id, value_3
         FROM
      		(SELECT
              user_id, value_3 * 2 as value_3
        	 FROM
         		(SELECT
                user_id, value_3
          		 FROM
           			(SELECT
                    "users"."user_id", value_3
                 FROM
                  users_table as "users"
                 WHERE
                  user_id > 1 and user_id < 4 and value_2 > 2
                ) simple_user_where_1
            ) all_buckets_1
        ) users_in_segment_1
        JOIN
        (SELECT
            "users"."user_id"
         FROM
          users_table as "users"
         WHERE
          user_id > 1 and user_id < 4 and value_2 > 3
        ) some_users_data
        ON ("users_in_segment_1".user_id = "some_users_data".user_id)
    ) segmentalias_1) "tempQuery"
GROUP BY "value_3"
ORDER BY cnt, value_3 DESC LIMIT 10;



SET citus.enable_repartition_joins to ON;
SET client_min_messages TO DEBUG1;

-- although there is no column equality at all
-- still recursive planning plans "some_users_data"
-- and the query becomes OK
SELECT
  "value_3", count(*) AS cnt
FROM
  (SELECT
      "value_3", "user_id",  random()
     FROM
      (SELECT
          users_in_segment_1.user_id, value_3
         FROM
          (SELECT
              user_id, value_3 * 2 as value_3
           FROM
            (SELECT
                user_id, value_3
               FROM
                (SELECT
                    "users"."user_id", value_3
                 FROM
                  users_table as "users"
                 WHERE
                  user_id > 1 and user_id < 4 and value_2 > 2
                ) simple_user_where_1
            ) all_buckets_1
        ) users_in_segment_1
        JOIN
        (SELECT
            "users"."user_id"
         FROM
          users_table as "users"
         WHERE
          user_id > 1 and user_id < 4 and value_2 > 3
        ) some_users_data
        ON (true)
    ) segmentalias_1) "tempQuery"
GROUP BY "value_3"
ORDER BY cnt, value_3 DESC LIMIT 10;

SET citus.enable_repartition_joins to OFF;
RESET client_min_messages;

-- nested LATERAL JOINs
SET citus.subquery_pushdown to ON;
SELECT *
FROM
  (SELECT "some_users_data".user_id, "some_recent_users".value_3
   FROM
     (SELECT
        filter_users_1.user_id, value_3
      FROM
        (SELECT
            "users"."user_id"
         FROM
          users_table as "users"
         WHERE
          user_id > 1 and user_id < 4 and users.value_2 = 2) filter_users_1
      JOIN LATERAL
        (SELECT
            user_id, value_3
         FROM
          events_table as "events"
         WHERE
           user_id > 1 and user_id < 4 AND
           ("events".user_id = "filter_users_1".user_id)
         ORDER BY
           value_3 DESC
         LIMIT 1) "last_events_1" ON true
      ORDER BY value_3 DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table as "users"
      WHERE
        "users"."user_id" = "some_recent_users"."user_id" AND
        users.value_2 > 2
      LIMIT 1) "some_users_data" ON true
   ORDER BY
    value_3 DESC
LIMIT 10) "some_users"
ORDER BY
  value_3 DESC, user_id ASC
LIMIT 10;

-- nested lateral join at top most level
SELECT "some_users_data".user_id, "some_recent_users".value_3
FROM
	(SELECT
      filter_users_1.user_id, value_3
     FROM
     	(SELECT
          "users"."user_id"
       FROM
        users_table as "users"
       WHERE
        user_id > 1 and user_id < 4 and users.value_2 = 2
        ) filter_users_1
        JOIN LATERAL
        (SELECT
            user_id, value_3
         FROM
          events_table as "events"
         WHERE
          user_id > 1 and user_id < 4 AND
          ("events".user_id = "filter_users_1".user_id)
         ORDER BY
          value_3 DESC
         LIMIT 1
        ) "last_events_1" ON true
     ORDER BY value_3 DESC
     LIMIT 10
   ) "some_recent_users"
   JOIN LATERAL
   (SELECT
      "users".user_id
    FROM
      users_table as "users"
    WHERE
      "users"."user_id" = "some_recent_users"."user_id" AND
      users.value_2 > 2
    LIMIT 1
   ) "some_users_data" ON true
ORDER BY
  value_3 DESC, user_id ASC
LIMIT 10;

-- longer nested lateral joins
SELECT *
FROM
  (SELECT "some_users_data".user_id, "some_recent_users".value_3
   FROM
     (SELECT filter_users_1.user_id, value_3
      FROM
        (SELECT
            "users"."user_id"
         FROM
          users_table as "users"
         WHERE
          user_id > 1 and user_id < 4 and users.value_2 = 2) filter_users_1
      JOIN LATERAL
        (SELECT
            user_id, value_3
         FROM
          events_table as "events"
         WHERE
          user_id > 1 and user_id < 4 AND
          ("events".user_id = "filter_users_1".user_id)
         ORDER BY
          value_3 DESC
         LIMIT 1) "last_events_1" ON true
      ORDER BY
        value_3 DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table as "users"
      WHERE
        "users"."user_id" = "some_recent_users"."user_id" AND
        users.value_2 > 2
      LIMIT 1) "some_users_data" ON true
   ORDER BY
    value_3 DESC
   LIMIT 10) "some_users"
ORDER BY
  value_3 DESC, user_id DESC
LIMIT 10;

-- longer nested lateral join wth top level join
SELECT "some_users_data".user_id, "some_recent_users".value_3
FROM
	(SELECT filter_users_1.user_id, value_3
     FROM
     	(SELECT
          "users"."user_id"
       FROM
        users_table as "users"
       WHERE
        user_id > 1 and user_id < 4 and users.value_2 = 2
        ) filter_users_1
        JOIN LATERAL
        (SELECT
            user_id, value_3
         FROM
          events_table as "events"
         WHERE
          user_id > 1 and user_id < 4
         AND
          ("events".user_id = "filter_users_1".user_id)
         ORDER BY
          value_3 DESC
         LIMIT 1
        ) "last_events_1" ON TRUE
     ORDER BY value_3 DESC
     LIMIT 10
    ) "some_recent_users"
    JOIN LATERAL
    (SELECT
        "users".user_id
     FROM
      users_table as "users"
     WHERE
      "users"."user_id" = "some_recent_users"."user_id" AND
      users.value_2 > 2
     LIMIT 1
    ) "some_users_data" ON TRUE
ORDER BY value_3 DESC, user_id DESC
LIMIT 10;

SET citus.subquery_pushdown to OFF;

-- LEFT JOINs used with INNER JOINs
SELECT
count(*) AS cnt, "generated_group_field"
 FROM
  (SELECT
      "eventQuery"."user_id", random(), generated_group_field
   FROM
     (SELECT
        "multi_group_wrapper_1".*, generated_group_field, random()
      FROM
        (SELECT *
         FROM
           (SELECT
              "events"."time", "events"."user_id" as event_user_id
            FROM
              events_table as "events"
            WHERE
              user_id > 4) "temp_data_queries"
           INNER JOIN
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 4 and value_2 = 5) "user_filters_1"
           ON ("temp_data_queries".event_user_id = "user_filters_1".user_id)) AS "multi_group_wrapper_1"
        LEFT JOIN
        (SELECT
            "users"."user_id" AS "user_id", value_2 AS "generated_group_field"
         FROM
          users_table as "users") "left_group_by_1"
        ON ("left_group_by_1".user_id = "multi_group_wrapper_1".event_user_id)) "eventQuery") "pushedDownQuery"
  group BY
    "generated_group_field"
  ORDER BY
    cnt DESC, generated_group_field ASC
  LIMIT 10;

-- single table subquery, no JOINS involved
SELECT
count(*) AS cnt, user_id
FROM
  (SELECT
      "eventQuery"."user_id", random()
   FROM
     (SELECT
        "events"."user_id"
      FROM
        events_table "events"
      WHERE
        event_type IN (1, 2)) "eventQuery") "pushedDownQuery"
GROUP BY
  "user_id"
ORDER BY
  cnt DESC, user_id DESC
LIMIT 10;

-- lateral joins in the nested manner
SET citus.subquery_pushdown to ON;
SELECT *
FROM
  (SELECT
      "some_users_data".user_id, value_2
   FROM
     (SELECT user_id, max(value_2) AS value_2
      FROM
        (SELECT user_id, value_2
         FROM
           (SELECT
              user_id, value_2
            FROM
              events_table as "events"
            WHERE
              user_id > 1 and user_id < 3) "events_1"
         ORDER BY
          value_2 DESC
         LIMIT 10000) "recent_events_1"
      GROUP BY
        user_id
      ORDER BY
        max(value_2) DESC) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table as "users"
      WHERE
        "users"."user_id" = "some_recent_users"."user_id" AND
        value_2 > 4
      LIMIT 1) "some_users_data" ON true
   ORDER BY
    value_2 DESC
   LIMIT 10) "some_users"
ORDER BY
    value_2 DESC, user_id DESC
LIMIT 10;
SET citus.subquery_pushdown to OFF;

-- on side of the lateral join can be recursively plannen, then pushed down
SELECT *
FROM
  (SELECT
      "some_users_data".user_id, value_2
   FROM
     (SELECT user_id, max(value_2) AS value_2
      FROM
        (SELECT user_id, value_2
         FROM
           (SELECT
              user_id, value_2
            FROM
              events_table as "events"
            WHERE
              user_id > 1 and user_id < 3) "events_1"
         ORDER BY
          value_2 DESC
         LIMIT 10000) "recent_events_1"
      GROUP BY
        user_id
      ORDER BY
        max(value_2) DESC) "some_recent_users"
   JOIN LATERAL
     (SELECT
        "users".user_id
      FROM
        users_table as "users"
      WHERE
        "users"."value_2" = "some_recent_users"."user_id" AND
        value_2 > 4
      ORDER BY 1 LIMIT 1) "some_users_data" ON true
   ORDER BY
    value_2 DESC
   LIMIT 10) "some_users"
ORDER BY
    value_2 DESC, user_id DESC
LIMIT 10;

-- lets test some unsupported set operations

-- not supported since we use INTERSECT
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT
                    *
                  FROM
                   (SELECT
                      "events"."user_id", "events"."time", 0 AS event
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                 UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_2)
               UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                      event_type IN (5, 6) ) events_subquery_3)
                INTERSECT
                  (SELECT
                      *
                   FROM
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (4, 5)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
    ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

-- supported through recursive planning
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *
   FROM
     ( SELECT "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT
                    *
                  FROM
                   (SELECT
                      "events"."user_id", "events"."time", 0 AS event
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                 UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_2)
               UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                      event_type IN (5, 6) ) events_subquery_3)
                UNION
                  (SELECT
                      *
                   FROM
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                      events_table as "events"
                     WHERE
                      event_type IN (4, 5)) events_subquery_4) ORDER BY 1, 2 OFFSET 3) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
    ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

-- not supported due to non relation rte
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT
                    *
                  FROM
                   (SELECT
                      "events"."user_id", "events"."time", 0 AS event
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                 UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_2)
               UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                      event_type IN (5, 6) ) events_subquery_3)
                UNION
                  (SELECT
                      *
                   FROM
                    (SELECT
                        1 as user_id, now(), 3 AS event
                    ) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT
        "users"."user_id"
      FROM
        users_table as "users"
      WHERE
        value_1 > 0 and value_1 < 4) AS t
    ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

-- similar to the above, but constant rte is on the right side of the query
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT
                    *
                  FROM
                   (SELECT
                      "events"."user_id", "events"."time", 0 AS event
                    FROM
                      events_table as  "events"
                    WHERE
                      event_type IN (1, 2) ) events_subquery_1)
                 UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_table as "events"
                     WHERE
                      event_type IN (3, 4) ) events_subquery_2)
               UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        events_table as  "events"
                     WHERE
                      event_type IN (5, 6) ) events_subquery_3)
                UNION
                  (SELECT
                      *
                   FROM
                    (SELECT
                        1 as user_id, now(), 3 AS event
                    ) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q"
INNER JOIN
     (SELECT 1 as user_id) AS t
    ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

-- we've fixed a bug related to joins w/wout alias
-- while implementing top window functions
-- thus adding some tests related to that (i.e., next 3 tests)
WITH users_events AS
(
  SELECT
    user_id
  FROM
    users_table
)
SELECT
  uid,
  event_type,
  value_2,
  value_3
FROM (
  (SELECT
    user_id as uid
  FROM
    users_events
  ) users
  JOIN
    events_table
  ON
    users.uid = events_table.event_type
  ) a
ORDER BY
  1,2,3,4
LIMIT 5;

-- the following queries are almost the same,
-- the only difference is the final GROUP BY
SELECT a.user_id, avg(b.value_2) as subquery_avg
FROM
  (SELECT
      user_id
   FROM
      users_table
   WHERE
      (value_1 > 2)
   GROUP BY
      user_id
   HAVING
      count(distinct value_1) > 2
  ) as a
  LEFT JOIN
  (SELECT
      DISTINCT ON (value_2) value_2 , user_id, value_3
   FROM
      users_table
   WHERE
      (value_1 > 3)
   ORDER BY
      1,2,3
  ) AS b
  USING (user_id)
GROUP BY user_id
ORDER BY 1, 2;

-- see the comment for the above query
SELECT a.user_id, avg(b.value_2) as subquery_avg
FROM
  (SELECT
      user_id
   FROM
      users_table
   WHERE
      (value_1 > 2)
   GROUP BY
      user_id
   HAVING
      count(distinct value_1) > 2
  ) as a
  LEFT JOIN
  (SELECT
      DISTINCT ON (value_2) value_2 , user_id, value_3
   FROM
      users_table
   WHERE
      (value_1 > 3)
   ORDER BY
      1,2,3
  ) AS b
  USING (user_id)
GROUP BY a.user_id
ORDER BY 1, 2;

-- queries where column aliases are used
-- the query is not very complex. join is given an alias with aliases
-- for each output column
SELECT k1
FROM (
	SELECT k1, random()
	FROM (users_table JOIN events_table USING (user_id)) k (k1, k2, k3)) l
ORDER BY k1
LIMIT 5;

SELECT DISTINCT k1
FROM (
	SELECT k1, random()
	FROM (users_table JOIN events_table USING (user_id)) k (k1, k2, k3)) l
ORDER BY k1
LIMIT 5;

SELECT x1, x3, value_2
FROM (users_table u FULL JOIN events_table e ON (u.user_id = e.user_id)) k(x1, x2, x3, x4, x5)
ORDER BY 1, 2, 3
LIMIT 5;

SELECT x1, x3, value_2
FROM (users_table u FULL JOIN events_table e USING (user_id)) k(x1, x2, x3, x4, x5)
ORDER BY 1, 2, 3
LIMIT 5;

SELECT c_custkey
FROM  (users_table LEFT OUTER JOIN events_table ON (users_table.user_id = events_table.user_id)) AS test(c_custkey, c_nationkey)
	INNER JOIN users_table as u2 ON (test.c_custkey = u2.user_id)
ORDER BY 1 DESC
LIMIT 10;

SELECT c_custkey, date_trunc('minute', max(c_nationkey))
FROM  (users_table LEFT OUTER JOIN events_table ON (users_table.user_id = events_table.user_id)) AS test(c_custkey, c_nationkey)
	INNER JOIN users_table as u2 ON (test.c_custkey = u2.user_id)
GROUP BY 1
ORDER BY 2, 1
LIMIT 10;

SELECT c_custkey, date_trunc('minute', max(c_nationkey))
FROM  (users_table LEFT OUTER JOIN events_table ON (users_table.user_id = events_table.user_id)) AS test(c_custkey, c_nationkey)
	INNER JOIN users_table as u2 ON (test.c_custkey = u2.user_id)
GROUP BY 1
HAVING extract(minute from max(c_nationkey))  >= 45
ORDER BY 2, 1
LIMIT 10;

SELECT user_id
FROM (users_table JOIN events_table USING (user_id)) AS test(user_id, c_nationkey)
	FULL JOIN users_table AS u2 USING (user_id)
ORDER BY 1 DESC
LIMIT 10;

-- nested joins
SELECT bar, value_3_table.value_3
FROM ((users_table
      JOIN (events_table INNER JOIN users_reference_table foo ON (events_table.user_id = foo.value_2)) AS deeper_join(user_id_deep)
     	ON (users_table.user_id = deeper_join.user_id_deep)) AS test(c_custkey, c_nationkey)
		LEFT JOIN users_table AS u2 ON (test.c_custkey = u2.user_id)) outer_test(bar,foo)

	JOIN LATERAL (SELECT value_3 FROM events_table WHERE user_id = bar) as value_3_table ON true
GROUP BY 1,2
ORDER BY 2 DESC, 1 DESC
LIMIT 10;

-- lateral joins
SELECT bar,
       value_3_table.value_3
FROM ((users_table
       JOIN (events_table
             INNER JOIN users_reference_table foo ON (events_table.user_id = foo.value_2)) AS deeper_join(user_id_deep) ON (users_table.user_id = deeper_join.user_id_deep)) AS test(c_custkey, c_nationkey)
      LEFT JOIN users_table AS u2 ON (test.c_custkey = u2.user_id)) outer_test(bar, foo)
JOIN LATERAL
  (SELECT value_3
   FROM events_table
   WHERE user_id = bar) AS value_3_table ON TRUE
GROUP BY 1, 2
ORDER BY 2 DESC, 1 DESC
LIMIT 10;

--Joins inside subqueries are the sources of the values in the target list:
SELECT bar, foo.value_3, c_custkey, test_2.time_2 FROM
(
	SELECT bar, value_3_table.value_3, random()
	FROM ((users_table
	      JOIN (events_table INNER JOIN users_reference_table foo ON (events_table.user_id = foo.value_2)) AS deeper_join(user_id_deep)
	     	ON (users_table.user_id = deeper_join.user_id_deep)) AS test(c_custkey, c_nationkey)
			LEFT JOIN users_table AS u2 ON (test.c_custkey = u2.user_id)) outer_test(bar,foo)

		JOIN LATERAL (SELECT value_3 FROM events_table WHERE user_id = bar) as value_3_table ON true
	GROUP BY 1,2
) as foo, (users_table
	      JOIN (events_table INNER JOIN users_reference_table foo ON (events_table.user_id = foo.value_2)) AS deeper_join_2(user_id_deep)
	     	ON (users_table.user_id = deeper_join_2.user_id_deep)) AS test_2(c_custkey, time_2) WHERE foo.bar = test_2.c_custkey
ORDER BY 2 DESC, 1 DESC, 3 DESC, 4 DESC
LIMIT 10;
