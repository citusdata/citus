--
-- multi subquery complex queries aims to expand existing subquery pushdown
-- regression tests to cover more caeses
-- the tables that are used depends to multi_insert_select_behavioral_analytics_create_table.sql
--

-- We don't need shard id sequence here, so commented out to prevent conflicts with concurrent tests
-- ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1400000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1400000;
 
SET citus.enable_router_execution TO FALSE;

CREATE TABLE events_reference_table as SELECT * FROM events_table;
CREATE TABLE users_reference_table as SELECT * FROM users_table;

SELECT create_reference_table('events_reference_table');
SELECT create_reference_table('users_reference_table');

-- LATERAL JOINs used with INNER JOINs with reference tables
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
              users_reference_table as "users"
            WHERE 
              user_id > 12 and user_id < 16 and value_1 > 20) user_where_1_1
         INNER JOIN
           (SELECT 
              "users"."user_id"
            FROM 
              users_reference_table as "users"
            WHERE 
              user_id > 12 and user_id < 16 and value_2 > 60) user_where_1_join_1 
           ON ("user_where_1_1".user_id = "user_where_1_join_1".user_id)) 
        filter_users_1 
      JOIN LATERAL
        (SELECT 
            user_id, time
         FROM 
          events_reference_table as "events"
         WHERE
           user_id > 12 and user_id < 16 AND 
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
        users_reference_table  as "users"
      WHERE 
        "users"."user_id" = "some_recent_users"."user_id" AND 
        "users"."value_2" > 70
      LIMIT 1) "some_users_data" 
    ON TRUE
   ORDER BY 
      lastseen DESC
   LIMIT 10) "some_users"
ORDER BY 
  user_id DESC
LIMIT 10;
SET citus.subquery_pushdown to OFF;

-- NESTED INNER JOINs with reference tables
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
              events_reference_table as "events"
            WHERE 
              user_id > 10 and user_id < 40 AND event_type IN (40, 41, 42, 43, 44, 45) ) "temp_data_queries"
            INNER  JOIN
           (SELECT 
              user_where_1_1.real_user_id
            FROM
              (SELECT 
                  "users"."user_id" as real_user_id
               FROM  
                users_reference_table as "users"
               WHERE
                 user_id > 10 and user_id < 40 and value_2 > 50 ) user_where_1_1
            INNER JOIN
              (SELECT 
                  "users"."user_id"
               FROM 
                users_reference_table as "users"
               WHERE 
                user_id > 10 and user_id < 40 and value_3 > 50 ) user_where_1_join_1 
           ON ("user_where_1_1".real_user_id = "user_where_1_join_1".user_id)) "user_filters_1"
      ON ("temp_data_queries".user_id = "user_filters_1".real_user_id)) "eventQuery") "pushedDownQuery") "pushedDownQuery"
GROUP BY 
  "generated_group_field" 
ORDER BY 
  generated_group_field DESC, value DESC;

-- single level inner joins with reference tables
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
                  users_reference_table as "users"
                 WHERE 
                  user_id > 10 and user_id < 40 and value_2 > 30
                ) simple_user_where_1
            ) all_buckets_1
        ) users_in_segment_1
        JOIN
        (SELECT 
            "users"."user_id"
         FROM 
          users_reference_table as "users"
         WHERE 
          user_id > 10 and user_id < 40 and value_2 > 60
        ) some_users_data
        ON ("users_in_segment_1".user_id = "some_users_data".user_id)
    ) segmentalias_1) "tempQuery" 
GROUP BY "value_3"
ORDER BY cnt, value_3 DESC LIMIT 10;

-- nested LATERAL JOINs with reference tables
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
          users_reference_table as "users"
         WHERE 
          user_id > 20 and user_id < 70 and users.value_2 = 200) filter_users_1
      JOIN LATERAL
        (SELECT 
            user_id, value_3
         FROM 
          events_reference_table as "events"
         WHERE
           user_id > 20 and user_id < 70 AND 
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
        users_reference_table as "users"
      WHERE 
        "users"."user_id" = "some_recent_users"."user_id" AND 
        users.value_2 > 200
      LIMIT 1) "some_users_data" ON true
   ORDER BY 
    value_3 DESC
LIMIT 10) "some_users"
ORDER BY 
  value_3 DESC 
LIMIT 10;
SET citus.subquery_pushdown to OFF;

-- LEFT JOINs used with INNER JOINs should error out since reference table exist in the
-- left side of the LEFT JOIN.
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
              user_id > 80) "temp_data_queries"
           INNER JOIN
           (SELECT 
              "users"."user_id"
            FROM 
              users_reference_table as "users"
            WHERE 
              user_id > 80 and value_2 = 5) "user_filters_1" 
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

  -- RIGHT JOINs used with INNER JOINs should error out since reference table exist in the
-- right side of the RIGHT JOIN.
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
              user_id > 80) "temp_data_queries"
           INNER JOIN
           (SELECT 
              "users"."user_id"
            FROM 
              users_table as "users"
            WHERE 
              user_id > 80 and value_2 = 5) "user_filters_1" 
           ON ("temp_data_queries".event_user_id = "user_filters_1".user_id)) AS "multi_group_wrapper_1"
        RIGHT JOIN
        (SELECT 
            "users"."user_id" AS "user_id", value_2 AS "generated_group_field"
         FROM 
          users_reference_table as "users") "right_group_by_1"
        ON ("right_group_by_1".user_id = "multi_group_wrapper_1".event_user_id)) "eventQuery") "pushedDownQuery" 
  group BY
    "generated_group_field"
  ORDER BY 
    cnt DESC, generated_group_field ASC
  LIMIT 10;

  -- Outer subquery with reference table
SELECT "some_users_data".user_id, lastseen
FROM
     (SELECT user_id, max(time) AS lastseen
      FROM
        (SELECT user_id, time
         FROM
           (SELECT 
              user_id, time
            FROM 
              events_reference_table as "events"
            WHERE 
              user_id > 10 and user_id < 40) "events_1"
         ORDER BY
           time DESC) "recent_events_1"
      GROUP BY 
        user_id
      ORDER BY 
        max(TIME) DESC) "some_recent_users"
   FULL JOIN
     (SELECT 
        "users".user_id
      FROM 
        users_table as "users"
      WHERE 
        users.value_2 > 50 and users.value_2 < 55) "some_users_data" 
     ON "some_users_data"."user_id" = "some_recent_users"."user_id"
ORDER BY 
  user_id
limit 50;

 -- 
 -- UNIONs and JOINs with reference tables, shoukld error out
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
                      events_reference_table as  "events"
                    WHERE 
                      event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                 UNION 
                 (SELECT 
                    *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 1 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                      event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION 
                 (SELECT 
                    *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 2 AS event
                     FROM 
                        events_table as  "events"
                     WHERE 
                      event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
                UNION 
                  (SELECT 
                      *
                   FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 3 AS event
                     FROM 
                      events_table as "events"
                     WHERE 
                      event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT 
        "users"."user_id"
      FROM 
        users_table as "users"
      WHERE 
        value_1 > 50 and value_1 < 70) AS t 
    ON (t.user_id = q.user_id)) as final_query
GROUP BY 
  types
ORDER BY 
  types;

  -- reference table exist in the subquery of union, should error out 
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
                      event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
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
                                events_reference_table as  "events", users_table as "users"
                              WHERE 
                                events.user_id = users.user_id AND
                                event_type IN (10, 11, 12, 13, 14, 15)
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
                      event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION 
                 (SELECT *
                  FROM
                    (SELECT
                       "events"."time", 3 AS event, "events"."user_id"
                     FROM 
                      events_table as "events"
                     WHERE 
                      event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)
                 ) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT 
        "users"."user_id"
      FROM 
        users_table as "users"
      WHERE 
        value_1 > 50 and value_1 < 70) AS t 
     ON (t.user_id = q.user_id)) as final_query
GROUP BY 
  types
ORDER BY 
  types;

-- 
-- Should error out with UNION ALL Queries on reference tables
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
                      event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                UNION ALL
                 (SELECT *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 1 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (15, 16, 17, 18, 19) ) events_subquery_2)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 2 AS event
                     FROM 
                        events_reference_table as  "events"
                     WHERE 
                        event_type IN (20, 21, 22, 23, 24, 25) ) events_subquery_3)
               UNION ALL
                 (SELECT *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 3 AS event
                     FROM 
                        events_table as "events"
                     WHERE 
                        event_type IN (26, 27, 28, 29, 30, 13)) events_subquery_4)) t1
         GROUP BY "t1"."user_id") AS t) "q" 
INNER JOIN
     (SELECT "users"."user_id"
      FROM users_table as "users"
      WHERE value_1 > 50 and value_1 < 70) AS t ON (t.user_id = q.user_id)) as final_query
GROUP BY types
ORDER BY types;