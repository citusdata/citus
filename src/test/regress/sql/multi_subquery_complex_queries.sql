--
-- multi subquery complex queries aims to expand existing subquery pushdown
-- regression tests to cover more caeses
-- the tables that are used depends to multi_insert_select_behavioral_analytics_create_table.sql
--

-- We don't need shard id sequence here, so commented out to prevent conflicts with concurrent tests
-- ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1400000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1400000;
 
SET citus.subquery_pushdown TO TRUE;
SET citus.enable_router_execution TO FALSE;

 -- 
 -- UNIONs and JOINs mixed
 --
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT "events"."user_id",
                          "events"."time",
                         0 AS event
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- same query with target entries shuffled inside UNIONs
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT 
                          "events"."time",
                         0 AS event,
                         "events"."user_id"
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 1 AS event, "events"."user_id"
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 2 AS event, "events"."user_id"
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 3 AS event, "events"."user_id"
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- not supported since events_subquery_2 doesn't have partition key on the target list
-- within the shuffled target list
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT 
                          "events"."time",
                         0 AS event,
                         "events"."user_id"
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 1 AS event, "events"."user_id" * 2
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 2 AS event, "events"."user_id"
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 3 AS event, "events"."user_id"
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- not supported since events_subquery_2 doesn't have partition key on the target list
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT 
                          "events"."time",
                         0 AS event,
                         "events"."user_id"
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 1 AS event, "events"."value_2" as user_id
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 2 AS event, "events"."user_id"
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 3 AS event, "events"."user_id"
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- not supported since events_subquery_2 doesn't have partition key on the target list
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT "events"."user_id",
                          "events"."time",
                         0 AS event
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- we can support arbitrary subqueries within UNIONs
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT 
                          "events"."time",
                         0 AS event,
                         "events"."user_id"
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
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
                                event_type IN (10, 11, 12, 13, 14, 15)
                                GROUP BY   "events"."user_id"
                          ) as events_subquery_5

                     ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 2 AS event, "events"."user_id"
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 3 AS event, "events"."user_id"
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- not supported since events_subquery_5 is not joined on partition key
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT 
                          "events"."time",
                         0 AS event,
                         "events"."user_id"
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
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
                                event_type IN (10, 11, 12, 13, 14, 15)
                                GROUP BY   "events"."user_id"
                          ) as events_subquery_5

                     ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 2 AS event, "events"."user_id"
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 3 AS event, "events"."user_id"
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- not supported since the join is not equi join
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT "events"."user_id",
                          "events"."time",
                         0 AS event
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id != q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- not supported since subquery 3 includes a JOIN with non-equi join
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT 
                          "events"."time",
                         0 AS event,
                         "events"."user_id"
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 1 AS event, "events"."value_2" as user_id
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 2 AS event, "events"."user_id"
                     FROM events_table as  "events", users_table as "users"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) AND users.user_id != events.user_id) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 3 AS event, "events"."user_id"
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- similar query with more union statements (to enable UNION tree become larger)
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT "events"."user_id",
                          "events"."time",
                         0 AS event
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)

                  UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 4 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (31, 32, 33, 34, 35, 36)) events_subquery_5)

                  UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 5 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (37, 38, 39, 40, 41, 42)) events_subquery_6)

                  UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 6 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (50, 51, 52, 53, 54, 55)) events_subquery_6)
                 ) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;


-- 
-- UNION ALL Queries
--
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT "events"."user_id",
                          "events"."time",
                         0 AS event
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION  ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- same query target list entries shuffled
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT 
                          "events"."time",
                         0 AS event,
                         "events"."user_id"
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION ALL
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 1 AS event, "events"."user_id"
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 2 AS event, "events"."user_id"
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 3 AS event, "events"."user_id"
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- not supported since subquery 3 does not have partition key
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT "events"."user_id",
                          "events"."time",
                         0 AS event
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION  ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."value_2", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- not supported since events_subquery_4 does not have partition key on the 
-- target list
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT 
                          "events"."time",
                         0 AS event,
                         "events"."user_id"
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION ALL
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 1 AS event, "events"."user_id"
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 2 AS event, "events"."user_id"
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT  "events"."time", 3 AS event, 2 * "events"."user_id"
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- union all with inner and left joins
SELECT user_id, count(*) as cnt
FROM
  (SELECT first_query.user_id,
          Random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                          ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id",
                            "events"."time",
                            0 AS event
                     FROM events_table as  "events"
                     WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "first_query" 
INNER JOIN
     (SELECT "t"."user_id"
      FROM
        (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t
        LEFT  OUTER JOIN
        (
          SELECT DISTINCT "events"."user_id" as user_id
         FROM events_table as "events"
          WHERE event_type IN (35, 36, 37, 38)
          GROUP BY user_id
         ) as t2 
        on (t2.user_id = t.user_id) WHERE t2.user_id is NULL) as second_query
        ON ("first_query".user_id = "second_query".user_id)) as final_query 
GROUP BY user_id ORDER BY cnt DESC, user_id DESC LIMIT 10;

-- not supported since the join between t and t2 is not equi join
-- union all with inner and left joins
SELECT user_id, count(*) as cnt
FROM
  (SELECT first_query.user_id,
          Random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                          ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id",
                            "events"."time",
                            0 AS event
                     FROM events_table as  "events"
                     WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "first_query" 
INNER JOIN
     (SELECT "t"."user_id"
      FROM
        (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t
        LEFT  OUTER JOIN
        (
          SELECT DISTINCT "events"."user_id" as user_id
         FROM events_table as "events"
          WHERE event_type IN (35, 36, 37, 38)
          GROUP BY user_id
         ) as t2 
        on (t2.user_id > t.user_id) WHERE t2.user_id is NULL) as second_query
        ON ("first_query".user_id = "second_query".user_id)) as final_query 
GROUP BY user_id ORDER BY cnt DESC, user_id DESC LIMIT 10;

 -- 
 -- Union, inner join and left join
 --
SELECT user_id, count(*) as cnt
FROM
  (SELECT first_query.user_id,
          Random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                          ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id",
                            "events"."time",
                            0 AS event
                     FROM events_table as  "events"
                     WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "first_query" 
INNER JOIN
     (SELECT "t"."user_id"
      FROM
        (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t
        LEFT  OUTER JOIN
        (
          SELECT DISTINCT "events"."user_id" as user_id
         FROM events_table as "events"
          WHERE event_type IN (35, 36, 37, 38)
          GROUP BY user_id
         ) as t2 
        on (t2.user_id = t.user_id) WHERE t2.user_id is NULL) as second_query
        ON ("first_query".user_id = "second_query".user_id)) as final_query 
GROUP BY user_id ORDER BY cnt DESC, user_id DESC LIMIT 10;

 -- 
 -- Unions, left / inner joins
 --
SELECT user_id, count(*) as cnt
FROM
  (SELECT first_query.user_id,
          Random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id",
                            "events"."time",
                            0 AS event
                     FROM events_table as  "events"
                     WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
              UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "first_query" 
INNER JOIN
     (SELECT "t"."user_id"
      FROM
        (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t
        left  OUTER JOIN
        (SELECT DISTINCT("events"."user_id")
         FROM events_table as "events"
          WHERE event_type IN (26, 27, 28, 29, 30, 13)
          GROUP BY user_id
        ) as t2 on t2.user_id = t.user_id where t2.user_id is NULL) as second_query
        ON ("first_query".user_id = "second_query".user_id)) as final_query 
GROUP BY user_id ORDER BY cnt DESC, user_id DESC LIMIT 10;


-- Simple LATERAL JOINs with GROUP BYs in each side
SELECT *
FROM
  (SELECT "some_users_data".user_id, lastseen
   FROM
     (SELECT user_id,
             Max(time) AS lastseen
      FROM
        (SELECT user_id,
                time
         FROM
           (SELECT user_id,
                   time
            FROM events_table as "events"
            WHERE user_id > 10 and user_id < 40) "events_1"
         ORDER BY time DESC
         LIMIT 1000) "recent_events_1"
      GROUP BY user_id
      ORDER BY max(time) DESC) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table as "users"
      WHERE "users"."user_id" = "some_recent_users"."user_id"
        AND users.value_2 > 50 and users.value_2 < 55
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY lastseen DESC
   LIMIT 50) "some_users"
     order BY user_id
     limit 50;

-- same query with subuqery joins in topmost select
SELECT "some_users_data".user_id, lastseen
FROM
     (SELECT user_id,
             Max(TIME) AS lastseen
      FROM
        (SELECT user_id,
                TIME
         FROM
           (SELECT user_id,
                   TIME
            FROM events_table as "events"
            WHERE user_id > 10 and user_id < 40) "events_1"
         ORDER BY TIME DESC
         LIMIT 1000) "recent_events_1"
      GROUP BY user_id
      ORDER BY max(TIME) DESC) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table as "users"
      WHERE "users"."user_id" = "some_recent_users"."user_id"
        AND users.value_2 > 50 and users.value_2 < 55
      LIMIT 1) "some_users_data" ON TRUE
ORDER BY user_id
limit 50;

-- not supported since JOIN is not on the partition key
SELECT *
FROM
  (SELECT "some_users_data".user_id, lastseen
   FROM
     (SELECT user_id,
             Max(time) AS lastseen
      FROM
        (SELECT user_id,
                time
         FROM
           (SELECT user_id,
                   time
            FROM events_table as "events"
            WHERE user_id > 10 and user_id < 40) "events_1"
         ORDER BY time DESC
         LIMIT 1000) "recent_events_1"
      GROUP BY user_id
      ORDER BY max(time) DESC) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table as "users"
      WHERE "users"."value_1" = "some_recent_users"."user_id"
        AND users.value_2 > 50 and users.value_2 < 55
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY lastseen DESC
   LIMIT 50) "some_users"
     order BY user_id
     limit 50;

-- not supported since JOIN is not on the partition key
-- see (2 * user_id as user_id) target list element
SELECT *
FROM
  (SELECT "some_users_data".user_id, lastseen
   FROM
     (SELECT 2 * user_id as user_id,
             (time) AS lastseen
      FROM
        (SELECT user_id,
                time
         FROM
           (SELECT user_id,
                   time
            FROM events_table as "events"
            WHERE user_id > 10 and user_id < 40) "events_1"
         ORDER BY time DESC
         LIMIT 1000) "recent_events_1"
      ) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table as "users"
      WHERE "users"."user_id" = "some_recent_users"."user_id"
        AND users.value_2 > 50 and users.value_2 < 55
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY lastseen DESC
   LIMIT 50) "some_users"
     order BY user_id
     limit 50;

-- LATERAL JOINs used with INNER JOINs
SELECT user_id, lastseen
FROM
  (SELECT "some_users_data".user_id, lastseen
   FROM
     (SELECT filter_users_1.user_id,
             time AS lastseen
      FROM
        (SELECT user_where_1_1.user_id
         FROM
           (SELECT "users"."user_id"
            FROM users_table as "users"
            WHERE user_id > 12 and user_id < 16 and value_1 > 20) user_where_1_1
         INNER JOIN
           (SELECT "users"."user_id"
            FROM users_table as "users"
            WHERE   user_id > 12 and user_id < 16 and value_2 > 60) user_where_1_join_1 
           ON ("user_where_1_1".user_id = "user_where_1_join_1".user_id)) filter_users_1 
      JOIN LATERAL
        (SELECT user_id,
                time
         FROM events_table as "events"
         WHERE  user_id > 12 and user_id < 16 and user_id = filter_users_1.user_id
         ORDER BY time DESC
         LIMIT 1) "last_events_1" ON TRUE
      ORDER BY time DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table  as "users"
      WHERE "users"."user_id" = "some_recent_users"."user_id"
             AND "users"."value_2" > 70
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY lastseen DESC
   LIMIT 10) "some_users"
order BY user_id DESC
limit 10;

--
-- A similar query with topmost select is dropped
-- and replaced by aggregation. Notice the heavy use of limit
--
SELECT "some_users_data".user_id, MAX(lastseen), count(*)
   FROM
     (SELECT filter_users_1.user_id,
             TIME AS lastseen
      FROM
        (SELECT user_where_1_1.user_id
         FROM
           (SELECT "users"."user_id"
            FROM users_table as "users"
            WHERE user_id > 12 and user_id < 16 and value_1 > 20) user_where_1_1
         INNER JOIN
           (SELECT "users"."user_id"
            FROM users_table as "users"
            WHERE   user_id > 12 and user_id < 16 and value_2 > 60) user_where_1_join_1 
           ON ("user_where_1_1".user_id = "user_where_1_join_1".user_id)) filter_users_1 
      JOIN LATERAL
        (SELECT user_id,
                TIME
         FROM events_table as "events"
         WHERE  user_id > 12 and user_id < 16 and user_id = filter_users_1.user_id
         ORDER BY TIME DESC
         LIMIT 1) "last_events_1" ON TRUE
      ORDER BY TIME DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table  as "users"
      WHERE "users"."user_id" = "some_recent_users"."user_id"
             AND "users"."value_2" > 70
      LIMIT 1) "some_users_data" ON TRUE
GROUP BY 1
ORDER BY 2, 1 DESC
limit 10;

-- not supported since the inner JOIN is not equi join
SELECT user_id, lastseen
FROM
  (SELECT "some_users_data".user_id, lastseen
   FROM
     (SELECT filter_users_1.user_id,
             time AS lastseen
      FROM
        (SELECT user_where_1_1.user_id
         FROM
           (SELECT "users"."user_id"
            FROM users_table as "users"
            WHERE user_id > 12 and user_id < 16 and value_1 > 20) user_where_1_1
         INNER JOIN
           (SELECT "users"."user_id"
            FROM users_table as "users"
            WHERE   user_id > 12 and user_id < 16 and value_2 > 60) user_where_1_join_1 
           ON ("user_where_1_1".user_id != "user_where_1_join_1".user_id)) filter_users_1 
      JOIN LATERAL
        (SELECT user_id,
                time
         FROM events_table as "events"
         WHERE  user_id > 12 and user_id < 16 and user_id = filter_users_1.user_id
         ORDER BY time DESC
         LIMIT 1) "last_events_1" ON TRUE
      ORDER BY time DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table  as "users"
      WHERE "users"."user_id" = "some_recent_users"."user_id"
             AND "users"."value_2" > 70
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY lastseen DESC
   LIMIT 10) "some_users"
order BY user_id DESC
limit 10;

-- not supported since the inner JOIN is not on the partition key
SELECT user_id, lastseen
FROM
  (SELECT "some_users_data".user_id, lastseen
   FROM
     (SELECT filter_users_1.user_id,
             time AS lastseen
      FROM
        (SELECT user_where_1_1.user_id
         FROM
           (SELECT "users"."user_id"
            FROM users_table as "users"
            WHERE user_id > 12 and user_id < 16 and value_1 > 20) user_where_1_1
         INNER JOIN
           (SELECT "users"."user_id", "users"."value_1"
            FROM users_table as "users"
            WHERE   user_id > 12 and user_id < 16 and value_2 > 60) user_where_1_join_1 
           ON ("user_where_1_1".user_id = "user_where_1_join_1".value_1)) filter_users_1 
      JOIN LATERAL
        (SELECT user_id,
                time
         FROM events_table as "events"
         WHERE  user_id > 12 and user_id < 16 and user_id = filter_users_1.user_id
         ORDER BY time DESC
         LIMIT 1) "last_events_1" ON TRUE
      ORDER BY time DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table  as "users"
      WHERE "users"."user_id" = "some_recent_users"."user_id"
             AND "users"."value_2" > 70
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY lastseen DESC
   LIMIT 10) "some_users"
order BY user_id DESC
limit 10;


-- not supported since upper LATERAL JOIN is not equi join
SELECT user_id, lastseen
FROM
  (SELECT "some_users_data".user_id, lastseen
   FROM
     (SELECT filter_users_1.user_id,
             time AS lastseen
      FROM
        (SELECT user_where_1_1.user_id
         FROM
           (SELECT "users"."user_id"
            FROM users_table as "users"
            WHERE user_id > 12 and user_id < 16 and value_1 > 20) user_where_1_1
         INNER JOIN
           (SELECT "users"."user_id", "users"."value_1"
            FROM users_table as "users"
            WHERE   user_id > 12 and user_id < 16 and value_2 > 60) user_where_1_join_1 
           ON ("user_where_1_1".user_id = "user_where_1_join_1".user_id)) filter_users_1 
      JOIN LATERAL
        (SELECT user_id,
                time
         FROM events_table as "events"
         WHERE  user_id > 12 and user_id < 16 and user_id != filter_users_1.user_id
         ORDER BY time DESC
         LIMIT 1) "last_events_1" ON TRUE
      ORDER BY time DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table  as "users"
      WHERE "users"."user_id" = "some_recent_users"."user_id"
             AND "users"."value_2" > 70
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY lastseen DESC
   LIMIT 10) "some_users"
order BY user_id DESC
limit 10;

-- not supported since lower LATERAL JOIN is not on the partition key
SELECT user_id, lastseen
FROM
  (SELECT "some_users_data".user_id, lastseen
   FROM
     (SELECT filter_users_1.user_id,
             time AS lastseen
      FROM
        (SELECT user_where_1_1.user_id
         FROM
           (SELECT "users"."user_id"
            FROM users_table as "users"
            WHERE user_id > 12 and user_id < 16 and value_1 > 20) user_where_1_1
         INNER JOIN
           (SELECT "users"."user_id", "users"."value_1"
            FROM users_table as "users"
            WHERE   user_id > 12 and user_id < 16 and value_2 > 60) user_where_1_join_1 
           ON ("user_where_1_1".user_id = "user_where_1_join_1".user_id)) filter_users_1 
      JOIN LATERAL
        (SELECT user_id,
                time
         FROM events_table as "events"
         WHERE  user_id > 12 and user_id < 16 and user_id = filter_users_1.user_id
         ORDER BY time DESC
         LIMIT 1) "last_events_1" ON TRUE
      ORDER BY time DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table  as "users"
      WHERE "users"."value_1" = "some_recent_users"."user_id"
             AND "users"."value_2" > 70
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY lastseen DESC
   LIMIT 10) "some_users"
order BY user_id DESC
limit 10;

-- NESTED INNER JOINs
SELECT 
 count(*) AS value, "generated_group_field" 
 FROM
  (SELECT DISTINCT "pushedDownQuery"."real_user_id", "generated_group_field"
    FROM
     (SELECT "eventQuery"."real_user_id", "eventQuery"."time", random(), ("eventQuery"."value_2") AS "generated_group_field"
      FROM
        (SELECT *
         FROM
           (SELECT "events"."time", "events"."user_id", "events"."value_2"
            FROM events_table as "events"
            WHERE user_id > 10 and user_id < 40 and  event_type IN (40, 41, 42, 43, 44, 45) ) "temp_data_queries"
            inner  JOIN
           (SELECT 
              user_where_1_1.real_user_id
            FROM
              (SELECT "users"."user_id" as real_user_id
               FROM users_table as "users"
               WHERE  user_id > 10 and user_id < 40 and value_2 > 50 ) user_where_1_1
            INNER JOIN
              (SELECT "users"."user_id"
               FROM users_table as "users"
               WHERE user_id > 10 and user_id < 40 and value_3 > 50 ) user_where_1_join_1 
           ON ("user_where_1_1".real_user_id = "user_where_1_join_1".user_id)) "user_filters_1"
           ON ("temp_data_queries".user_id = "user_filters_1".real_user_id)) "eventQuery") "pushedDownQuery") "pushedDownQuery"
           GROUP BY "generated_group_field" ORDER BY generated_group_field DESC, value DESC;

-- not supported since the first inner join is not on the partition key
SELECT 
 count(*) AS value, "generated_group_field" 
 FROM
  (SELECT DISTINCT "pushedDownQuery"."real_user_id", "generated_group_field"
    FROM
     (SELECT "eventQuery"."real_user_id", "eventQuery"."time", random(), ("eventQuery"."value_2") AS "generated_group_field"
      FROM
        (SELECT *
         FROM
           (SELECT "events"."time", "events"."user_id", "events"."value_2"
            FROM events_table as "events"
            WHERE user_id > 10 and user_id < 40 and  event_type IN (40, 41, 42, 43, 44, 45) ) "temp_data_queries"
            inner  JOIN
           (SELECT 
              user_where_1_1.real_user_id
            FROM
              (SELECT "users"."user_id" as real_user_id
               FROM users_table as "users"
               WHERE  user_id > 10 and user_id < 40 and value_2 > 50 ) user_where_1_1
            INNER JOIN
              (SELECT "users"."user_id"
               FROM users_table as "users"
               WHERE user_id > 10 and user_id < 40 and value_3 > 50 ) user_where_1_join_1 
           ON ("user_where_1_1".real_user_id = "user_where_1_join_1".user_id)) "user_filters_1"
           ON ("temp_data_queries".value_2 = "user_filters_1".real_user_id)) "eventQuery") "pushedDownQuery") "pushedDownQuery"
           GROUP BY "generated_group_field" ORDER BY generated_group_field DESC, value DESC;

-- not supported since the first inner join is not an equi join
SELECT 
 count(*) AS value, "generated_group_field" 
 FROM
  (SELECT DISTINCT "pushedDownQuery"."real_user_id", "generated_group_field"
    FROM
     (SELECT "eventQuery"."real_user_id", "eventQuery"."time", random(), ("eventQuery"."value_2") AS "generated_group_field"
      FROM
        (SELECT *
         FROM
           (SELECT "events"."time", "events"."user_id", "events"."value_2"
            FROM events_table as "events"
            WHERE user_id > 10 and user_id < 40 and  event_type IN (40, 41, 42, 43, 44, 45) ) "temp_data_queries"
            inner  JOIN
           (SELECT 
              user_where_1_1.real_user_id
            FROM
              (SELECT "users"."user_id" as real_user_id
               FROM users_table as "users"
               WHERE  user_id > 10 and user_id < 40 and value_2 > 50 ) user_where_1_1
            INNER JOIN
              (SELECT "users"."user_id"
               FROM users_table as "users"
               WHERE user_id > 10 and user_id < 40 and value_3 > 50 ) user_where_1_join_1 
           ON ("user_where_1_1".real_user_id >= "user_where_1_join_1".user_id)) "user_filters_1"
           ON ("temp_data_queries".user_id = "user_filters_1".real_user_id)) "eventQuery") "pushedDownQuery") "pushedDownQuery"
           GROUP BY "generated_group_field" ORDER BY generated_group_field DESC, value DESC;

-- single level inner joins
SELECT 
  "value_3", count(*) AS cnt 
FROM
	(SELECT "value_3", "user_id",  random()
  	 FROM
    	(SELECT users_in_segment_1.user_id, value_3 
         FROM
      		(SELECT user_id, value_3 * 2 as value_3
        	 FROM
         		(SELECT user_id, value_3 
          		 FROM
           			(SELECT "users"."user_id", value_3
                	 FROM users_table as "users"
                  	 WHERE user_id > 10 and user_id < 40 and value_2 > 30
                ) simple_user_where_1
            ) all_buckets_1
        ) users_in_segment_1
        JOIN
        (SELECT "users"."user_id"
         FROM users_table as "users"
         WHERE user_id > 10 and user_id < 40 and value_2 > 60
        ) some_users_data
        ON ("users_in_segment_1".user_id = "some_users_data".user_id)
    ) segmentalias_1) "tempQuery" 
GROUP BY "value_3"
ORDER BY cnt, value_3 DESC LIMIT 10;


-- not supported since there is no partition column equality at all
SELECT 
  "value_3", count(*) AS cnt 
FROM
(SELECT "value_3", "user_id",  random()
  FROM
    (SELECT users_in_segment_1.user_id, value_3 
        FROM
      (SELECT user_id, value_3 * 2 as value_3
        FROM
         (SELECT user_id, value_3 
          FROM
           (SELECT "users"."user_id", value_3
                FROM users_table as "users"
                  WHERE user_id > 10 and user_id < 40 and value_2 > 30) simple_user_where_1) all_buckets_1) users_in_segment_1
      JOIN
                          (SELECT "users"."user_id"
                            FROM users_table as "users"
                            WHERE user_id > 10 and user_id < 40 and value_2 > 60) some_users_data

                          ON (true)) segmentalias_1) "tempQuery" 
                        GROUP BY "value_3" ORDER BY cnt, value_3 DESC LIMIT 10;

-- nested LATERAL JOINs
SELECT *
FROM
  (SELECT "some_users_data".user_id,
          "some_recent_users".value_3
   FROM
     (SELECT filter_users_1.user_id,
             value_3
      FROM
        (SELECT "users"."user_id"
         FROM users_table as "users"
         WHERE user_id > 20 and user_id < 70 and users.value_2 = 200) filter_users_1
      JOIN LATERAL
        (SELECT user_id,
                value_3
         FROM events_table as "events"
         WHERE  user_id > 20 and user_id < 70
           AND ("events".user_id = "filter_users_1".user_id)
         ORDER BY value_3 DESC
         LIMIT 1) "last_events_1" ON TRUE
      ORDER BY value_3 DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table as "users"
      WHERE "users"."user_id" = "some_recent_users"."user_id"
        AND users.value_2 > 200
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY value_3 DESC
   LIMIT 10) "some_users"
  order BY 
  value_3 DESC 
  limit 10;

-- nested lateral join at top most level
SELECT "some_users_data".user_id, "some_recent_users".value_3
FROM
	(SELECT filter_users_1.user_id, value_3
     FROM
     	(SELECT "users"."user_id"
         FROM users_table as "users"
         WHERE user_id > 20 and user_id < 70 and users.value_2 = 200
        ) filter_users_1
        JOIN LATERAL
        (SELECT user_id, value_3
         FROM events_table as "events"
         WHERE  user_id > 20 and user_id < 70
           AND ("events".user_id = "filter_users_1".user_id)
         ORDER BY value_3 DESC
         LIMIT 1
        ) "last_events_1" ON TRUE
     ORDER BY value_3 DESC
     LIMIT 10
   ) "some_recent_users"
   JOIN LATERAL
   (SELECT "users".user_id
    FROM users_table as "users"
    WHERE "users"."user_id" = "some_recent_users"."user_id"
        AND users.value_2 > 200
    LIMIT 1
   ) "some_users_data" ON TRUE
ORDER BY value_3 DESC, user_id ASC
LIMIT 10;

-- longer nested lateral joins
SELECT *
FROM
  (SELECT "some_users_data".user_id,
          "some_recent_users".value_3
   FROM
     (SELECT filter_users_1.user_id,
             value_3
      FROM
        (SELECT "users"."user_id"
         FROM users_table as "users"
         WHERE user_id > 20 and user_id < 70 and users.value_2 = 200) filter_users_1
      JOIN LATERAL
        (SELECT user_id,
                value_3
         FROM events_table as "events"
         WHERE  user_id > 20 and user_id < 70
           AND ("events".user_id = "filter_users_1".user_id)
         ORDER BY value_3 DESC
         LIMIT 1) "last_events_1" ON TRUE
      ORDER BY value_3 DESC
      LIMIT 10) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table as "users"
      WHERE "users"."user_id" = "some_recent_users"."user_id"
        AND users.value_2 > 200
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY value_3 DESC
   LIMIT 10) "some_users"
  order BY 
  value_3 DESC 
  limit 10;

-- longer nested lateral join wth top level join
SELECT "some_users_data".user_id, "some_recent_users".value_3
FROM
	(SELECT filter_users_1.user_id, value_3
     FROM
     	(SELECT "users"."user_id"
         FROM users_table as "users"
         WHERE user_id > 20 and user_id < 70 and users.value_2 = 200
        ) filter_users_1
        JOIN LATERAL
        (SELECT user_id, value_3
         FROM events_table as "events"
         WHERE  user_id > 20 and user_id < 70
           AND ("events".user_id = "filter_users_1".user_id)
         ORDER BY value_3 DESC
         LIMIT 1
        ) "last_events_1" ON TRUE
     ORDER BY value_3 DESC
     LIMIT 10
    ) "some_recent_users"
    JOIN LATERAL
    (SELECT "users".user_id
     FROM users_table as "users"
     WHERE "users"."user_id" = "some_recent_users"."user_id"
        AND users.value_2 > 200
     LIMIT 1
    ) "some_users_data" ON TRUE
ORDER BY value_3 DESC
LIMIT 10;

-- LEFT JOINs used with INNER JOINs
SELECT
count(*) AS cnt, "generated_group_field"
 FROM
  (SELECT "eventQuery"."user_id", random(), generated_group_field
   FROM
     (SELECT "multi_group_wrapper_1".*, generated_group_field, random()
      FROM
        (SELECT *
         FROM
           (SELECT "events"."time", "events"."user_id" as event_user_id
            FROM events_table as "events"
            WHERE user_id > 80) "temp_data_queries"
           INNER  JOIN
           (SELECT "users"."user_id"
            FROM users_table as "users"
            WHERE user_id > 80 and value_2 = 5) "user_filters_1" 
           on ("temp_data_queries".event_user_id = "user_filters_1".user_id)) AS "multi_group_wrapper_1"
        LEFT JOIN
        (SELECT "users"."user_id" AS "user_id", value_2 AS "generated_group_field"
         FROM users_table as "users") "left_group_by_1"
        on ("left_group_by_1".user_id = "multi_group_wrapper_1".event_user_id)) "eventQuery") "pushedDownQuery" 
  group BY
    "generated_group_field"
  ORDER BY cnt DESC, generated_group_field ASC
  LIMIT 10;

-- single table subquery, no JOINS involved
SELECT
count(*) AS cnt, user_id
FROM
  (SELECT "eventQuery"."user_id", random()
   FROM
     (SELECT "events"."user_id"
      FROM events_table "events"
      WHERE event_type IN (10, 20, 30, 40, 50, 60, 70, 80, 90)) "eventQuery") "pushedDownQuery"
  GROUP BY
   "user_id"
   ORDER BY cnt DESC, user_id DESC
   LIMIT 10;

-- lateral joins in the nested manner
SELECT *
FROM
  (SELECT "some_users_data".user_id, value_2
   FROM
     (SELECT user_id,
             Max(value_2) AS value_2
      FROM
        (SELECT user_id,
                value_2
         FROM
           (SELECT user_id,
                   value_2
            FROM events_table as "events"
            WHERE user_id > 10 and user_id < 20) "events_1"
         ORDER BY value_2 DESC
         LIMIT 10000) "recent_events_1"
      GROUP BY user_id
      ORDER BY max(value_2) DESC) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table as "users"
      WHERE "users"."user_id" = "some_recent_users"."user_id"
        AND value_2 > 75
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY value_2 DESC
   LIMIT 10) "some_users"
  order BY value_2 DESC, user_id DESC
  limit 10;

-- not supported since join is not on the partition key
SELECT *
FROM
  (SELECT "some_users_data".user_id, value_2
   FROM
     (SELECT user_id,
             Max(value_2) AS value_2
      FROM
        (SELECT user_id,
                value_2
         FROM
           (SELECT user_id,
                   value_2
            FROM events_table as "events"
            WHERE user_id > 10 and user_id < 20) "events_1"
         ORDER BY value_2 DESC
         LIMIT 10000) "recent_events_1"
      GROUP BY user_id
      ORDER BY max(value_2) DESC) "some_recent_users"
   JOIN LATERAL
     (SELECT "users".user_id
      FROM users_table as "users"
      WHERE "users"."value_2" = "some_recent_users"."user_id"
        AND value_2 > 75
      LIMIT 1) "some_users_data" ON TRUE
   ORDER BY value_2 DESC
   LIMIT 10) "some_users"
  order BY value_2 DESC, user_id DESC
  limit 10;

-- lets test some unsupported set operations

-- not supported since we use INTERSECT
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT "events"."user_id",
                          "events"."time",
                         0 AS event
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               INTERSECT 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- not supported due to offset
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  ( SELECT *, random()
   FROM
     ( SELECT "t"."user_id",
              "t"."time",
              unnest("t"."collected_events") AS "event_types"
      FROM
        ( SELECT "t1"."user_id",
                 min("t1"."time") AS "time",
                 array_agg(("t1"."event")
                           ORDER BY TIME ASC, event DESC) AS collected_events
         FROM (
                 (SELECT *
                  FROM
                   (SELECT "events"."user_id",
                          "events"."time",
                         0 AS event
                 FROM events_table as  "events"
                WHERE event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 1 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 2 AS event
                     FROM events_table as  "events"
                      WHERE event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT "events"."user_id", "events"."time", 3 AS event
                     FROM events_table as "events"
                     WHERE event_type IN (26, 27, 28, 29, 30, 13) ) events_subquery_4) OFFSET 3 ) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;

-- not supported due to window functions
SELECT   user_id, 
         some_vals 
FROM     ( 
                  SELECT   * , 
                           Row_number() over (PARTITION BY "user_id" ORDER BY "user_id") AS "some_vals",
                           Random() 
                  FROM     users_table 
                 ) user_id 
ORDER BY 1, 
         2 limit 10;

SET citus.subquery_pushdown TO FALSE;
SET citus.enable_router_execution TO TRUE;
