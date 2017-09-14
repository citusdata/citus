--
-- multi subquery complex queries aims to expand existing subquery pushdown
-- regression tests to cover more caeses
-- the tables that are used depends to multi_insert_select_behavioral_analytics_create_table.sql
--

-- We don't need shard id sequence here, so commented out to prevent conflicts with concurrent tests
-- ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1400000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1400000;
 
SET citus.enable_router_execution TO FALSE;

CREATE TABLE user_buy_test_table(user_id int, item_id int, buy_count int);
SELECT create_distributed_table('user_buy_test_table', 'user_id');
INSERT INTO user_buy_test_table VALUES(1,2,1);
INSERT INTO user_buy_test_table VALUES(2,3,4);
INSERT INTO user_buy_test_table VALUES(3,4,2);
INSERT INTO user_buy_test_table VALUES(7,5,2);

CREATE TABLE users_return_test_table(user_id int, item_id int, buy_count int);
SELECT create_distributed_table('users_return_test_table', 'user_id');
INSERT INTO users_return_test_table VALUES(4,1,1);
INSERT INTO users_return_test_table VALUES(1,3,1);
INSERT INTO users_return_test_table VALUES(3,2,2);

CREATE TABLE users_ref_test_table(id int, it_name varchar(25), k_no int);
SELECT create_reference_table('users_ref_test_table');
INSERT INTO users_ref_test_table VALUES(1,'User_1',45);
INSERT INTO users_ref_test_table VALUES(2,'User_2',46);
INSERT INTO users_ref_test_table VALUES(3,'User_3',47);
INSERT INTO users_ref_test_table VALUES(4,'User_4',48);
INSERT INTO users_ref_test_table VALUES(5,'User_5',49);
INSERT INTO users_ref_test_table VALUES(6,'User_6',50);

-- Simple Join test with reference table
SELECT count(*) FROM
  (SELECT random() FROM user_buy_test_table JOIN users_ref_test_table
  ON user_buy_test_table.user_id = users_ref_test_table.id) subquery_1;

-- Should work, reference table at the inner side is allowed
SELECT count(*) FROM
  (SELECT random(), k_no FROM user_buy_test_table LEFT JOIN users_ref_test_table
  ON user_buy_test_table.user_id = users_ref_test_table.id) subquery_1 WHERE k_no = 47;

-- Should work, although no equality between partition column and reference table
SELECT subquery_1.item_id FROM
  (SELECT user_buy_test_table.item_id, random() FROM user_buy_test_table LEFT JOIN users_ref_test_table
  ON user_buy_test_table.item_id = users_ref_test_table.id) subquery_1
ORDER BY 1;

-- Should work, although no equality between partition column and reference table
SELECT subquery_1.user_id FROM
  (SELECT user_buy_test_table.user_id, random() FROM user_buy_test_table LEFT JOIN users_ref_test_table
  ON user_buy_test_table.user_id > users_ref_test_table.id) subquery_1
ORDER BY 1;

-- Shouldn't work, reference table at the outer side is not allowed
SELECT * FROM
  (SELECT random() FROM users_ref_test_table LEFT JOIN user_buy_test_table
  ON users_ref_test_table.id = user_buy_test_table.user_id) subquery_1;

-- Should work, reference table at the inner side is allowed
SELECT count(*) FROM
  (SELECT random() FROM users_ref_test_table RIGHT JOIN user_buy_test_table
  ON user_buy_test_table.user_id = users_ref_test_table.id) subquery_1;

-- Shouldn't work, reference table at the outer side is not allowed
SELECT * FROM
  (SELECT random() FROM user_buy_test_table RIGHT JOIN users_ref_test_table
  ON user_buy_test_table.user_id = users_ref_test_table.id) subquery_1;

-- Equi join test with reference table on non-partition keys
SELECT count(*) FROM
  (SELECT random() FROM user_buy_test_table JOIN users_ref_test_table
  ON user_buy_test_table.item_id = users_ref_test_table.id) subquery_1;

-- Non-equi join test with reference table on non-partition keys
SELECT count(*) FROM
  (SELECT random() FROM user_buy_test_table JOIN users_ref_test_table
  ON user_buy_test_table.item_id > users_ref_test_table.id) subquery_1;

-- Non-equi left joins with reference tables on non-partition keys
SELECT count(*) FROM
  (SELECT random() FROM user_buy_test_table LEFT JOIN users_ref_test_table
  ON user_buy_test_table.item_id > users_ref_test_table.id) subquery_1;

-- Should pass since reference table locates in the inner part of each left join
SELECT count(*) FROM
  (SELECT tt1.user_id, random() FROM user_buy_test_table AS tt1 JOIN users_return_test_table as tt2
    ON tt1.user_id = tt2.user_id) subquery_1
  LEFT JOIN
  (SELECT tt1.user_id, random() FROM user_buy_test_table as tt1 LEFT JOIN users_ref_test_table as ref
   ON tt1.user_id = ref.id) subquery_2 ON subquery_1.user_id = subquery_2.user_id;

-- two subqueries, each include joins with reference table
-- also, two hash distributed tables are joined on partition keys
SELECT count(*) FROM
  (SELECT DISTINCT user_buy_test_table.user_id, random() FROM user_buy_test_table LEFT JOIN users_ref_test_table
  ON user_buy_test_table.item_id > users_ref_test_table.id AND users_ref_test_table.k_no > 88 AND user_buy_test_table.item_id < 88) subquery_1,
(SELECT DISTINCT user_buy_test_table.user_id, random() FROM user_buy_test_table LEFT JOIN users_ref_test_table
  ON user_buy_test_table.user_id > users_ref_test_table.id AND users_ref_test_table.k_no > 44 AND user_buy_test_table.user_id > 44) subquery_2
WHERE subquery_1.user_id = subquery_2.user_id ;

  -- Should be able to push down since reference tables are inner joined 
  -- with hash distributed tables, the results of those joins are the parts of
  -- an outer join
SELECT subquery_2.id FROM
  (SELECT tt1.user_id, random() FROM user_buy_test_table AS tt1 JOIN users_return_test_table as tt2
    ON tt1.user_id = tt2.user_id) subquery_1
  RIGHT JOIN
  (SELECT tt1.user_id, ref.id, random() FROM user_buy_test_table as tt1 JOIN users_ref_test_table as ref
   ON tt1.user_id = ref.id) subquery_2 ON subquery_1.user_id = subquery_2.user_id ORDER BY 1 DESC LIMIT 5;

-- the same query as the above, but this Citus fails to pushdown the query
-- since the outer part of the right join doesn't include any joins
SELECT * FROM
  (SELECT tt1.user_id, random() FROM user_buy_test_table AS tt1 JOIN users_return_test_table as tt2
    ON tt1.user_id = tt2.user_id) subquery_1
  RIGHT JOIN
  (SELECT *, random() FROM (SELECT tt1.user_id, random() FROM user_buy_test_table as tt1 JOIN users_ref_test_table as ref
   ON tt1.user_id = ref.id) subquery_2_inner) subquery_2 ON subquery_1.user_id = subquery_2.user_id;

-- should be able to pushdown since reference table is in the
-- inner part of the left join
SELECT 
  user_id, sum(value_1) 
FROM 
  (SELECT 
      users_table.user_id, users_table.value_1, random()
    FROM 
      users_table LEFT JOIN events_table ON (users_table.user_id = events_table.user_id) 
      INNER JOIN events_reference_table ON (events_reference_table.value_2 = users_table.user_id)
  ) as foo 
  GROUP BY user_id ORDER BY 2 DESC LIMIT 10;

-- same query as above, reference table is wrapped into a subquery
SELECT 
  user_id, sum(value_1) 
FROM 
  (SELECT 
      users_table.user_id, users_table.value_1, random()
    FROM 
      users_table LEFT JOIN events_table ON (users_table.user_id = events_table.user_id) 
      INNER JOIN (SELECT *, random() FROM events_reference_table) as ref_all ON (ref_all.value_2 = users_table.user_id)
  ) as foo 
  GROUP BY user_id ORDER BY 2 DESC LIMIT 10;

-- should be able to pushdown since reference table is in the
-- inner part of the left join
SELECT 
  user_id, sum(value_1) 
FROM 
  (SELECT 
      users_table.user_id, users_table.value_1, random()
    FROM 
      users_table LEFT JOIN events_table ON (users_table.user_id = events_table.user_id) 
      LEFT JOIN events_reference_table ON (events_reference_table.value_2 = users_table.user_id)
  ) as foo 
  GROUP BY user_id ORDER BY 2 DESC LIMIT 10;

-- should not be able to pushdown since reference table is in the
-- direct outer part of the left join
SELECT 
  user_id, sum(value_1) 
FROM 
  (SELECT 
      users_table.user_id, users_table.value_1, random()
    FROM 
      events_reference_table LEFT JOIN  users_table ON (users_table.user_id = events_reference_table.value_2) 
      LEFT JOIN events_table ON (events_table.user_id = users_table.user_id)
  ) as foo 
  GROUP BY user_id ORDER BY 2 DESC LIMIT 10;  

-- should not be able to pushdown since reference table is in the
-- direct outer part of the left join wrapped into a subquery
SELECT
    *
FROM
    (SELECT *, random() FROM events_reference_table) as ref_all LEFT JOIN users_table 
    ON (users_table.user_id = ref_all.value_2);

-- should not be able to pushdown since reference table is in the
-- outer part of the left join
SELECT 
  user_id, sum(value_1) 
FROM 
  (SELECT 
      users_table.user_id, users_table.value_1, random()
    FROM 
      events_reference_table LEFT JOIN  users_table ON (users_table.user_id = events_reference_table.value_2) 
      LEFT JOIN events_table ON (events_table.user_id = users_table.user_id)
  ) as foo 
  GROUP BY user_id ORDER BY 2 DESC LIMIT 10;  

-- should be able to pushdown since reference table is in the
-- inner part of the left join
SELECT * FROM
(
  SELECT DISTINCT foo.user_id
         FROM
           ((SELECT 
              "events"."time", "events"."user_id" as event_user_id, value_2 as event_val_2, random()
            FROM 
              events_reference_table as "events"
            WHERE 
              event_type > 80) as "temp_data_queries"
           INNER JOIN
           (SELECT 
              "users"."user_id"
            FROM 
              users_table as "users"
            WHERE 
              user_id > 80 and value_2 = 5) as foo_in ON (event_val_2 = user_id)) as foo LEFT JOIN
           (SELECT user_id as user_user_id FROM users_table) as fooo ON (user_id = user_user_id)) as bar;

-- the same query but this time reference table is in the outer part of the query
SELECT * FROM
(
  SELECT DISTINCT foo.user_id
         FROM
           ((SELECT 
              "events"."time", "events"."user_id" as event_user_id, value_2 as event_val_2, random()
            FROM 
              events_reference_table as "events"
            WHERE 
              event_type > 80) as "temp_data_queries"
           LEFT JOIN
           (SELECT 
              "users"."user_id"
            FROM 
              users_table as "users"
            WHERE 
              user_id > 80 and value_2 = 5) as foo_in ON (event_val_2 = user_id)) as foo LEFT JOIN
           (SELECT user_id as user_user_id FROM users_table) as fooo ON (user_id = user_user_id)) as bar;

-- we could even suuport the following where the subquery 
-- on the outer part of the left join contains a reference table 
SELECT max(events_all.cnt), events_all.usr_id
FROM
  (SELECT users_table.user_id as usr_id,
          count(*) as cnt
   FROM events_reference_table
   INNER JOIN users_table ON (users_table.user_id = events_reference_table.user_id) GROUP BY users_table.user_id) AS events_all
LEFT JOIN events_table ON (events_all.usr_id = events_table.user_id) GROUP BY 2 ORDER BY 1 DESC, 2 DESC LIMIT 5;

-- but, we fail to pushdown the following query where join that reference table appears
-- wrapped into a subquery
SELECT max(events_all.cnt),
       events_all.usr_id 
       FROM(
SELECT *, random() FROM
                (SELECT users_table.user_id AS usr_id, count(*) AS cnt
                 FROM events_reference_table
                 INNER JOIN users_table ON (users_table.user_id = events_reference_table.user_id)
                 GROUP BY users_table.user_id) AS events_all_inner) AS events_all
LEFT JOIN events_table ON (events_all.usr_id = events_table.user_id)
GROUP BY 2
ORDER BY 1 DESC,
         2 DESC
LIMIT 5;

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
      DISTINCT "pushedDownQuery"."user_id", "generated_group_field"
    FROM
     (SELECT 
        "eventQuery"."user_id", "eventQuery"."time", random(), ("eventQuery"."value_2") AS "generated_group_field"
      FROM
        (SELECT 
            *
         FROM
           (SELECT 
              "events"."time", "events"."user_id", "events"."value_2"
            FROM 
              events_table as "events"
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

-- LEFT JOINs used with INNER JOINs should not error out since reference table joined
-- with hash table that Citus can push down
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

-- right join where the inner part of the join includes a reference table
-- joined with hash partitioned table using non-equi join 
SELECT user_id, sum(array_length(events_table, 1)), length(hasdone_event), hasdone_event
FROM (
  SELECT
    t1.user_id,
    array_agg(event ORDER BY time) AS events_table,
    COALESCE(hasdone_event, 'Has not done event') AS hasdone_event
  FROM (
    (
      SELECT u.user_id, 'step=>1'::text AS event, e.time
      FROM users_table AS u,
          events_reference_table AS e
      WHERE  u.user_id > e.user_id
      AND u.user_id >= 10
      AND u.user_id <= 25
      AND e.event_type IN (100, 101, 102)
    )
  ) t1 RIGHT JOIN (
      SELECT DISTINCT user_id,
        'Has done event'::TEXT AS hasdone_event
      FROM  events_table AS e
      WHERE  e.user_id >= 10
      AND e.user_id <= 25
      AND e.event_type IN (106, 107, 108)
  ) t2 ON (t1.user_id = t2.user_id)
  GROUP BY  t1.user_id, hasdone_event
) t GROUP BY user_id, hasdone_event
ORDER BY user_id;

-- a similar query as the above, with non-partition key comparison
SELECT user_id, sum(array_length(events_table, 1)), length(hasdone_event), hasdone_event
FROM (
  SELECT
    t1.user_id,
    array_agg(event ORDER BY time) AS events_table,
    COALESCE(hasdone_event, 'Has not done event') AS hasdone_event
  FROM (
    (
      SELECT u.user_id, 'step=>1'::text AS event, e.time
      FROM users_table AS u,
          events_reference_table AS e
      WHERE  u.value_1 > e.user_id
      AND u.user_id >= 10
      AND u.user_id <= 25
      AND e.event_type >= 125 AND e.event_type < 130
    )
  ) t1 RIGHT JOIN (
      SELECT DISTINCT user_id,
        'Has done event'::TEXT AS hasdone_event
      FROM  events_table AS e
      WHERE  e.user_id >= 10
      AND e.user_id <= 25
      AND e.event_type >= 130 AND e.event_type < 135
  ) t2 ON (t1.user_id = t2.user_id)
  GROUP BY  t1.user_id, hasdone_event
) t GROUP BY user_id, hasdone_event
ORDER BY user_id;


-- LEFT JOINs used with INNER JOINs
-- events_table and users_reference_table joined 
-- with event_table.non_part_key < reference_table.any_key
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
           ON ("temp_data_queries".event_user_id < "user_filters_1".user_id)) AS "multi_group_wrapper_1"
        RIGHT JOIN
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
 -- UNIONs and JOINs with reference tables, should error out
 --
SELECT ("final_query"."event_types") as types
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
                      event_type IN (10, 11, 12, 13, 14, 15) ) events_subquery_1) 
                 UNION 
                 (SELECT 
                    *
                  FROM
                    (SELECT 
                        "events"."user_id", "events"."time", 1 AS event
                     FROM 
                        events_reference_table as "events"
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
                                max("users"."time"),
                                0 AS event,
                                "users"."user_id"
                              FROM 
                                events_reference_table as  "events", users_table as "users"
                              WHERE 
                                events.user_id = users.user_id AND
                                event_type IN (10, 11, 12, 13, 14, 15)
                                GROUP BY   "users"."user_id"
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

-- just a sanity check that we don't allow this if the reference table is on the 
-- left part of the left join
SELECT count(*) FROM
  (SELECT random() FROM users_ref_test_table LEFT JOIN user_buy_test_table
  ON user_buy_test_table.item_id > users_ref_test_table.id) subquery_1;

-- we don't allow non equi join among hash partitioned tables
SELECT count(*) FROM
  (SELECT user_buy_test_table.user_id, random() FROM user_buy_test_table LEFT JOIN users_ref_test_table
  ON user_buy_test_table.item_id > users_ref_test_table.id) subquery_1,
(SELECT user_buy_test_table.user_id, random() FROM user_buy_test_table LEFT JOIN users_ref_test_table
  ON user_buy_test_table.user_id > users_ref_test_table.id) subquery_2
WHERE subquery_1.user_id != subquery_2.user_id ;

-- we cannot push this query since hash partitioned tables
-- are not joined on partition keys with equality
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
           ON ("temp_data_queries".event_user_id < "user_filters_1".user_id)) AS "multi_group_wrapper_1"
        RIGHT JOIN
        (SELECT 
            "users"."user_id" AS "user_id", value_2 AS "generated_group_field"
         FROM 
          users_table as "users") "left_group_by_1"
        ON ("left_group_by_1".user_id > "multi_group_wrapper_1".event_user_id)) "eventQuery") "pushedDownQuery" 
  group BY
    "generated_group_field"
  ORDER BY 
    cnt DESC, generated_group_field ASC
  LIMIT 10;

-- two hash partitioned relations are not joined
-- on partiton keys although reference table is fine
-- to push down
SELECT 
  u1.user_id, count(*)
FROM 
  events_table as e1, users_table as u1
WHERE
  event_type IN
            (SELECT 
                event_type
             FROM 
              events_reference_table as e2
             WHERE
              value_2 = 15 AND
              value_3 > 25 AND
              e1.value_2 > e2.value_2
            ) 
            AND u1.user_id > e1.user_id
GROUP BY 1
ORDER BY 2 DESC, 1 DESC
LIMIT 5;

SELECT foo.user_id FROM
(
  SELECT m.user_id, random() FROM users_table m JOIN events_reference_table r ON int4eq(m.user_id, r.user_id)
  WHERE event_type > 100000
) as foo;

-- not supported since group by is on the reference table column
SELECT foo.user_id FROM
(
  SELECT r.user_id, random() FROM users_table m JOIN events_reference_table r ON int4eq(m.user_id, r.user_id)
  GROUP BY r.user_id
) as foo;

-- not supported since distinct is on the reference table column
SELECT foo.user_id FROM
(
  SELECT DISTINCT r.user_id, random() FROM users_table m JOIN events_reference_table r ON int4eq(m.user_id, r.user_id)
) as foo;

-- not supported since distinct on is on the reference table column
SELECT foo.user_id FROM
(
  SELECT DISTINCT ON(r.user_id) r.user_id, random() FROM users_table m JOIN events_reference_table r ON int4eq(m.user_id, r.user_id)
) as foo;

DROP TABLE user_buy_test_table;
DROP TABLE users_ref_test_table;
DROP TABLE users_return_test_table;
