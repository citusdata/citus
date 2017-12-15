--
-- multi subquery complex queries aims to expand existing subquery pushdown
-- regression tests to cover more caeses
-- the tables that are used depends to multi_insert_select_behavioral_analytics_create_table.sql
--

-- We don't need shard id sequence here, so commented out to prevent conflicts with concurrent tests
-- SET citus.next_shard_id TO 1400000;
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

-- table function can be the inner relationship in a join
SELECT count(*) FROM
  (SELECT random() FROM user_buy_test_table JOIN generate_series(1,10) AS users_ref_test_table(id)
  ON user_buy_test_table.item_id > users_ref_test_table.id) subquery_1;

-- table function cannot be used without subquery pushdown
SELECT count(*) FROM user_buy_test_table JOIN generate_series(1,10) AS users_ref_test_table(id)
  ON user_buy_test_table.item_id = users_ref_test_table.id;

-- table function can be the inner relationship in an outer join
SELECT count(*) FROM
  (SELECT random() FROM user_buy_test_table LEFT JOIN generate_series(1,10) AS users_ref_test_table(id)
  ON user_buy_test_table.item_id > users_ref_test_table.id) subquery_1;

SELECT count(*) FROM user_buy_test_table LEFT JOIN (SELECT * FROM generate_series(1,10) id) users_ref_test_table
ON user_buy_test_table.item_id = users_ref_test_table.id;

-- table function cannot be the outer relationship in an outer join
SELECT count(*) FROM
  (SELECT random() FROM user_buy_test_table RIGHT JOIN generate_series(1,10) AS users_ref_test_table(id)
  ON user_buy_test_table.item_id > users_ref_test_table.id) subquery_1;

SELECT count(*) FROM user_buy_test_table RIGHT JOIN (SELECT * FROM generate_series(1,10) id) users_ref_test_table
ON user_buy_test_table.item_id = users_ref_test_table.id;

-- volatile functions cannot be used as table expressions
SELECT count(*) FROM
  (SELECT random() FROM user_buy_test_table JOIN random() AS users_ref_test_table(id)
  ON user_buy_test_table.item_id > users_ref_test_table.id) subquery_1;

-- cannot sneak in a volatile function as a parameter
SELECT count(*) FROM
  (SELECT random() FROM user_buy_test_table JOIN generate_series(random()::int,10) AS users_ref_test_table(id)
  ON user_buy_test_table.item_id > users_ref_test_table.id) subquery_1;

-- cannot perform a union with table function
SELECT count(*) FROM
  (SELECT user_id FROM user_buy_test_table
   UNION ALL
   SELECT id FROM generate_series(1,10) AS users_ref_test_table(id)) subquery_1;

-- subquery without FROM can be the inner relationship in a join
SELECT count(*) FROM
  (SELECT random() FROM user_buy_test_table JOIN (SELECT 4 AS id) users_ref_test_table
  ON user_buy_test_table.item_id > users_ref_test_table.id) subquery_1;

-- subquery without FROM triggers subquery pushdown
SELECT count(*) FROM user_buy_test_table JOIN (SELECT 5 AS id) users_ref_test_table
ON user_buy_test_table.item_id = users_ref_test_table.id;

-- subquery without FROM can be the inner relationship in an outer join
SELECT count(*) FROM user_buy_test_table LEFT JOIN (SELECT 5 AS id) users_ref_test_table
ON user_buy_test_table.item_id = users_ref_test_table.id;

-- subquery without FROM cannot be the outer relationship in an outer join
SELECT count(*) FROM user_buy_test_table RIGHT JOIN (SELECT 5 AS id) users_ref_test_table
ON user_buy_test_table.item_id = users_ref_test_table.id;

-- cannot perform a union with subquery without FROM
SELECT count(*) FROM
  (SELECT user_id FROM user_buy_test_table
   UNION ALL
   SELECT id FROM (SELECT 5 AS id) users_ref_test_table) subquery_1;

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
              event_type > 2) as "temp_data_queries"
           INNER JOIN
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 2 and value_2 = 1) as foo_in ON (event_val_2 = user_id)) as foo LEFT JOIN
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
              event_type > 2) as "temp_data_queries"
           LEFT JOIN
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 2 and value_2 = 1) as foo_in ON (event_val_2 = user_id)) as foo LEFT JOIN
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
              user_id > 0 and user_id < 5 and value_1 > 1) user_where_1_1
         INNER JOIN
           (SELECT
              "users"."user_id"
            FROM
              users_reference_table as "users"
            WHERE
              user_id > 0 and user_id < 5 and value_2 > 2) user_where_1_join_1
           ON ("user_where_1_1".user_id = "user_where_1_join_1".user_id))
        filter_users_1
      JOIN LATERAL
        (SELECT
            user_id, time
         FROM
          events_reference_table as "events"
         WHERE
           user_id > 0 and user_id < 5 AND
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
        "users"."value_2" > 2
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
              user_id > 0 and user_id < 4 AND event_type IN (4, 5) ) "temp_data_queries"
            INNER  JOIN
           (SELECT
              user_where_1_1.real_user_id
            FROM
              (SELECT
                  "users"."user_id" as real_user_id
               FROM
                users_reference_table as "users"
               WHERE
                 user_id > 0 and user_id < 4 and value_2 > 3 ) user_where_1_1
            INNER JOIN
              (SELECT
                  "users"."user_id"
               FROM
                users_reference_table as "users"
               WHERE
                user_id > 0 and user_id < 4 and value_3 > 3 ) user_where_1_join_1
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
                  user_id > 1 and user_id < 4 and value_2 > 2
                ) simple_user_where_1
            ) all_buckets_1
        ) users_in_segment_1
        JOIN
        (SELECT
            "users"."user_id"
         FROM
          users_reference_table as "users"
         WHERE
          user_id > 1 and user_id < 4 and value_2 > 3
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
          user_id > 2 and user_id < 5 and users.value_2 = 3) filter_users_1
      JOIN LATERAL
        (SELECT
            user_id, value_3
         FROM
          events_reference_table as "events"
         WHERE
           user_id > 2 and user_id < 5 AND
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
        users.value_2 > 3
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
              user_id > 4) "temp_data_queries"
           INNER JOIN
           (SELECT
              "users"."user_id"
            FROM
              users_reference_table as "users"
            WHERE
              user_id > 2 and value_2 = 5) "user_filters_1"
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
              user_id > 2) "temp_data_queries"
           INNER JOIN
           (SELECT
              "users"."user_id"
            FROM
              users_table as "users"
            WHERE
              user_id > 2 and value_2 = 5) "user_filters_1"
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
      AND u.user_id >= 1
      AND u.user_id <= 3
      AND e.event_type IN (1, 2)
    )
  ) t1 RIGHT JOIN (
      SELECT DISTINCT user_id,
        'Has done event'::TEXT AS hasdone_event
      FROM  events_table AS e
      WHERE  e.user_id >= 1
      AND e.user_id <= 3
      AND e.event_type IN (3, 4)
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
      AND u.user_id >= 1
      AND u.user_id <= 3
      AND e.event_type >= 2 AND e.event_type < 3
    )
  ) t1 RIGHT JOIN (
      SELECT DISTINCT user_id,
        'Has done event'::TEXT AS hasdone_event
      FROM  events_table AS e
      WHERE  e.user_id >= 1
      AND e.user_id <= 3
      AND e.event_type >= 3 AND e.event_type < 4
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
              user_id > 2) "temp_data_queries"
           INNER JOIN
           (SELECT
              "users"."user_id"
            FROM
              users_reference_table as "users"
            WHERE
              user_id > 2 and value_2 = 5) "user_filters_1"
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
              user_id > 1 and user_id < 4) "events_1"
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
        users.value_2 > 2 and users.value_2 < 4) "some_users_data"
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
                      event_type IN (1, 2) ) events_subquery_1)
                 UNION
                 (SELECT
                    *
                  FROM
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        events_reference_table as "events"
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
        value_1 > 2 and value_1 < 4) AS t
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
                      event_type IN (1, 2) ) events_subquery_1)
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
                                event_type IN (1, 2)
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
        value_1 > 2 and value_1 < 4) AS t
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
                        events_reference_table as  "events"
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
      WHERE value_1 > 2 and value_1 < 4) AS t ON (t.user_id = q.user_id)) as final_query
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
              user_id > 2) "temp_data_queries"
           INNER JOIN
           (SELECT
              "users"."user_id"
            FROM
              users_reference_table as "users"
            WHERE
              user_id > 2 and value_2 = 5) "user_filters_1"
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
              value_2 = 1 AND
              value_3 > 3 AND
              e1.value_2 > e2.value_2
            )
            AND u1.user_id > e1.user_id
GROUP BY 1
ORDER BY 2 DESC, 1 DESC
LIMIT 5;

SELECT foo.user_id FROM
(
  SELECT m.user_id, random() FROM users_table m JOIN events_reference_table r ON int4eq(m.user_id, r.user_id)
  WHERE event_type > 100
) as foo;

-- not pushdownable since group by is on the reference table column
-- recursively planned, but hits unsupported clause type error on the top level query
SELECT foo.user_id FROM
(
  SELECT r.user_id, random() FROM users_table m JOIN events_reference_table r ON int4eq(m.user_id, r.user_id)
  GROUP BY r.user_id
) as foo;

-- not pushdownable since the group by contains at least one distributed table
-- recursively planned, but hits unsupported clause type error on the top level query
SELECT foo.user_id FROM
(
  SELECT r.user_id, random() FROM users_table m JOIN events_reference_table r ON int4eq(m.user_id, r.user_id)
  GROUP BY r.user_id, m.user_id
) as foo
ORDER BY 1 LIMIT 3;

-- not pushdownable since distinct is on the reference table column
-- recursively planned, but hits unsupported clause type error on the top level query
SELECT foo.user_id FROM
(
  SELECT DISTINCT r.user_id, random() FROM users_table m JOIN events_reference_table r ON int4eq(m.user_id, r.user_id)
) as foo;

-- not supported since distinct on is on the reference table column
SELECT foo.user_id FROM
(
  SELECT DISTINCT ON(r.user_id) r.user_id, random() FROM users_table m JOIN events_reference_table r ON int4eq(m.user_id, r.user_id)
) as foo;

-- supported since the distinct on contains at least one distributed table
SELECT foo.user_id FROM
(
  SELECT DISTINCT ON(r.user_id, m.user_id) r.user_id, random() FROM users_table m JOIN events_reference_table r ON int4eq(m.user_id, r.user_id)
) as foo
ORDER BY 1 LIMIT 3;

-- should be able to pushdown since one of the subqueries has distinct on reference tables
-- and there is only reference table in that subquery
SELECT 
  distinct_users, event_type, time
FROM
(SELECT user_id, time, event_type FROM events_table) as events_dist INNER JOIN
(SELECT DISTINCT user_id as distinct_users FROM users_reference_table) users_ref ON (events_dist.user_id = users_ref.distinct_users)
ORDER BY time DESC
LIMIT 5
OFFSET 0;

-- the same query wuth multiple reference tables in the subquery
SELECT 
  distinct_users, event_type, time
FROM
(SELECT user_id, time, event_type FROM events_table) as events_dist INNER JOIN
(SELECT DISTINCT users_reference_table.user_id as distinct_users FROM users_reference_table, events_reference_table 
 WHERE events_reference_table.user_id =  users_reference_table.user_id AND events_reference_table.event_type IN (1,2,3,4)) users_ref
ON (events_dist.user_id = users_ref.distinct_users)
ORDER BY time DESC
LIMIT 5
OFFSET 0;

-- similar query as the above, but with group bys
SELECT 
  distinct_users, event_type, time
FROM
(SELECT user_id, time, event_type FROM events_table) as events_dist INNER JOIN
(SELECT user_id as distinct_users FROM users_reference_table GROUP BY distinct_users) users_ref ON (events_dist.user_id = users_ref.distinct_users)
ORDER BY time DESC
LIMIT 5
OFFSET 0;

-- should not push down this query since there is a distributed table (i.e., events_table)
-- which is not in the DISTINCT clause. Recursive planning also fails since router execution
-- is disabled
SELECT * FROM
(
  SELECT DISTINCT users_reference_table.user_id FROM users_reference_table, events_table WHERE users_reference_table.user_id = events_table.value_4
) as foo;

SELECT * FROM
(
  SELECT users_reference_table.user_id FROM users_reference_table, events_table WHERE users_reference_table.user_id = events_table.value_4
  GROUP BY 1
) as foo;

-- similiar to the above examples, this time there is a subquery 
-- whose output is not in the DISTINCT clause
SELECT * FROM
(
  SELECT DISTINCT users_reference_table.user_id FROM users_reference_table, (SELECT user_id, random() FROM events_table) as us_events WHERE users_reference_table.user_id = us_events.user_id
) as foo;

-- the following query is safe to push down since the DISTINCT clause include distribution column
SELECT * FROM
(
  SELECT DISTINCT users_reference_table.user_id, us_events.user_id FROM users_reference_table, (SELECT user_id, random() FROM events_table WHERE event_type IN (2,3)) as us_events WHERE users_reference_table.user_id = us_events.user_id
) as foo 
ORDER BY 1 DESC 
LIMIT 4;

-- should not pushdown since there is a non partition column on the DISTINCT clause
-- Recursive planning also fails since router execution
-- is disabled
SELECT * FROM
(
  SELECT 
    DISTINCT users_reference_table.user_id, us_events.value_4 
  FROM 
    users_reference_table, 
    (SELECT user_id, value_4, random() FROM events_table WHERE event_type IN (2,3)) as us_events 
  WHERE 
    users_reference_table.user_id = us_events.user_id
) as foo 
ORDER BY 1 DESC 
LIMIT 4;



-- test the read_intermediate_result() for GROUP BYs
BEGIN;
 
SELECT broadcast_intermediate_result('squares', 'SELECT s, s*s FROM generate_series(1,200) s');

-- single appereance of read_intermediate_result
SELECT 
  DISTINCT user_id 
FROM 
  users_table 
JOIN 
(SELECT 
  max(res.val) as mx 
FROM 
  read_intermediate_result('squares', 'binary') AS res (val int, val_square int) 
GROUP BY res.val_square) squares
 ON (mx = user_id)
ORDER BY 1
LIMIT 5;

-- similar to the above, with DISTINCT on intermediate result
SELECT DISTINCT user_id
FROM users_table
JOIN
  (SELECT DISTINCT res.val AS mx
   FROM read_intermediate_result('squares', 'binary') AS res (val int, val_square int)) squares ON (mx = user_id)
ORDER BY 1
LIMIT 5;

-- single appereance of read_intermediate_result but inside a subquery
SELECT 
  DISTINCT user_id 
FROM 
  users_table 
JOIN (
  SELECT *,random() FROM (SELECT 
    max(res.val) as mx 
  FROM 
      (SELECT val, val_square FROM read_intermediate_result('squares', 'binary') AS res (val int, val_square int)) res 
  GROUP BY res.val_square) foo)
squares
 ON (mx = user_id)
ORDER BY 1
LIMIT 5;

-- multiple read_intermediate_results in the same subquery is OK
SELECT 
  DISTINCT user_id 
FROM 
  users_table 
JOIN 
(SELECT 
  max(res.val) as mx 
FROM 
  read_intermediate_result('squares', 'binary') AS res (val int, val_square int),
  read_intermediate_result('squares', 'binary') AS res2 (val int, val_square int) 
WHERE res.val = res2.val_square
GROUP BY res2.val_square) squares
 ON (mx = user_id)
ORDER BY 1
LIMIT 5;

-- mixed recurring tuples should be supported
SELECT 
  DISTINCT user_id 
FROM 
  users_table 
JOIN 
(SELECT 
  max(res.val) as mx 
FROM 
  read_intermediate_result('squares', 'binary') AS res (val int, val_square int), 
  generate_series(0, 10) i
  WHERE
  res.val = i
  GROUP BY 
    i) squares
 ON (mx = user_id)
ORDER BY 1
LIMIT 5;

-- should recursively plan since
-- there are no columns on the GROUP BY from the distributed table
SELECT 
  DISTINCT user_id 
FROM 
  users_reference_table
JOIN 
  (SELECT 
    max(val_square) as mx 
  FROM 
    read_intermediate_result('squares', 'binary') AS res (val int, val_square int), events_table 
  WHERE 
    events_table.user_id = res.val GROUP BY res.val) squares
 ON (mx = user_id)
ORDER BY 1
LIMIT 5;

ROLLBACK;

-- should work since we're using an immutable function as recurring tuple
SELECT 
  DISTINCT user_id 
FROM 
  users_table 
JOIN 
(SELECT 
  max(i+5)as mx 
FROM 
  generate_series(0, 10) as i GROUP BY i) squares
 ON (mx = user_id)
ORDER BY 1
LIMIT 5;


-- should recursively plan since we're 
-- using an immutable function as recurring tuple
-- along with a distributed table, where GROUP BY is 
-- on the recurring tuple
SELECT 
  DISTINCT user_id 
FROM 
  users_reference_table
JOIN 
  (SELECT 
    max(i+5)as mx 
  FROM 
     generate_series(0, 10) as i, events_table 
  WHERE 
    events_table.user_id = i GROUP BY i) squares
 ON (mx = user_id)
ORDER BY 1
LIMIT 5;

DROP TABLE user_buy_test_table;
DROP TABLE users_ref_test_table;
DROP TABLE users_return_test_table;
