--
-- MULTI_INSERT_SELECT
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 13300000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 13300000;

-- create co-located tables
SET citus.shard_count = 4;
SET citus.shard_replication_factor = 2;

CREATE TABLE raw_events_first (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint, UNIQUE(user_id, value_1));
SELECT create_distributed_table('raw_events_first', 'user_id');

CREATE TABLE raw_events_second (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint, UNIQUE(user_id, value_1));
SELECT create_distributed_table('raw_events_second', 'user_id');

CREATE TABLE agg_events (user_id int, value_1_agg int, value_2_agg int, value_3_agg float, value_4_agg bigint, agg_time timestamp, UNIQUE(user_id, value_1_agg));
SELECT create_distributed_table('agg_events', 'user_id');;

-- create the reference table as well
CREATE TABLE reference_table (user_id int);
SELECT create_reference_table('reference_table');

-- set back to the defaults
SET citus.shard_count = DEFAULT;
SET citus.shard_replication_factor = DEFAULT;

INSERT INTO raw_events_first (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (1, now(), 10, 100, 1000.1, 10000);
INSERT INTO raw_events_first (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (2, now(), 20, 200, 2000.1, 20000);
INSERT INTO raw_events_first (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (3, now(), 30, 300, 3000.1, 30000);
INSERT INTO raw_events_first (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (4, now(), 40, 400, 4000.1, 40000);
INSERT INTO raw_events_first (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (5, now(), 50, 500, 5000.1, 50000);
INSERT INTO raw_events_first (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (6, now(), 60, 600, 6000.1, 60000);

SET client_min_messages TO DEBUG4;

-- raw table to raw table
INSERT INTO raw_events_second  SELECT * FROM raw_events_first;

-- see that our first multi shard INSERT...SELECT works expected
SET client_min_messages TO INFO;
SELECT
   raw_events_first.user_id
FROM
   raw_events_first, raw_events_second 
WHERE
   raw_events_first.user_id = raw_events_second.user_id;

-- see that we get unique vialitons
INSERT INTO raw_events_second  SELECT * FROM raw_events_first;

-- add one more row
INSERT INTO raw_events_first (user_id, time) VALUES
                         (7, now());

-- try a single shard query
SET client_min_messages TO DEBUG4;
INSERT INTO raw_events_second (user_id, time) SELECT user_id, time FROM raw_events_first WHERE user_id = 7;


SET client_min_messages TO INFO;

-- add one more row
INSERT INTO raw_events_first (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (8, now(), 80, 800, 8000, 80000);


-- reorder columns
SET client_min_messages TO DEBUG4;
INSERT INTO raw_events_second (value_2, value_1, value_3, value_4, user_id, time) 
SELECT 
   value_2, value_1, value_3, value_4, user_id, time 
FROM 
   raw_events_first
WHERE
   user_id = 8;

-- a zero shard select
INSERT INTO raw_events_second (value_2, value_1, value_3, value_4, user_id, time) 
SELECT 
   value_2, value_1, value_3, value_4, user_id, time 
FROM 
   raw_events_first
WHERE
   false;


-- another zero shard select
INSERT INTO raw_events_second (value_2, value_1, value_3, value_4, user_id, time) 
SELECT 
   value_2, value_1, value_3, value_4, user_id, time 
FROM 
   raw_events_first
WHERE
   0 != 0;

-- add one more row
SET client_min_messages TO INFO;
INSERT INTO raw_events_first (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (9, now(), 90, 900, 9000, 90000);


-- show that RETURNING also works
SET client_min_messages TO DEBUG4;
INSERT INTO raw_events_second (user_id, value_1, value_3) 
SELECT 
   user_id, value_1, value_3
FROM
   raw_events_first 
WHERE
   value_3 = 9000 
RETURNING *;

-- hits two shards
INSERT INTO raw_events_second (user_id, value_1, value_3) 
SELECT 
   user_id, value_1, value_3
FROM
   raw_events_first 
WHERE
   user_id = 9 OR user_id = 16 
RETURNING *;


-- now do some aggregations
INSERT INTO agg_events 
SELECT
   user_id, sum(value_1), avg(value_2), sum(value_3), count(value_4) 
FROM
   raw_events_first
GROUP BY
   user_id;

-- group by column not exists on the SELECT target list
INSERT INTO agg_events (value_3_agg, value_4_agg, value_1_agg, user_id) 
SELECT
   sum(value_3), count(value_4), sum(value_1), user_id
FROM
   raw_events_first
GROUP BY
   value_2, user_id
RETURNING *;


-- some subquery tests
INSERT INTO agg_events 
            (value_1_agg, 
             user_id) 
SELECT SUM(value_1), 
       id 
FROM   (SELECT raw_events_second.user_id AS id, 
               raw_events_second.value_1 
        FROM   raw_events_first, 
               raw_events_second 
        WHERE  raw_events_first.user_id = raw_events_second.user_id) AS foo 
GROUP  BY id; 


-- subquery one more level depth 
INSERT INTO agg_events 
            (value_4_agg, 
             value_1_agg, 
             user_id) 
SELECT v4, 
       v1, 
       id 
FROM   (SELECT SUM(raw_events_second.value_4) AS v4, 
               SUM(raw_events_first.value_1) AS v1, 
               raw_events_second.user_id      AS id 
        FROM   raw_events_first, 
               raw_events_second 
        WHERE  raw_events_first.user_id = raw_events_second.user_id 
        GROUP  BY raw_events_second.user_id) AS foo; 

-- join between subqueries
INSERT INTO agg_events
            (user_id)
SELECT f2.id FROM

(SELECT
      id
FROM   (SELECT reference_table.user_id      AS id
        FROM   raw_events_first,
               reference_table
        WHERE  raw_events_first.user_id = reference_table.user_id ) AS foo) as f
INNER JOIN
(SELECT v4,
       v1,
       id
FROM   (SELECT SUM(raw_events_second.value_4) AS v4,
               SUM(raw_events_first.value_1) AS v1,
               raw_events_second.user_id      AS id
        FROM   raw_events_first,
               raw_events_second
        WHERE  raw_events_first.user_id = raw_events_second.user_id
        GROUP  BY raw_events_second.user_id
        HAVING SUM(raw_events_second.value_4) > 10) AS foo2 ) as f2
ON (f.id = f2.id);

-- add one more level subqueris on top of subquery JOINs
INSERT INTO agg_events
            (user_id, value_4_agg)
SELECT
  outer_most.id, max(outer_most.value)
FROM
(
  SELECT f2.id as id, f2.v4 as value FROM
    (SELECT
          id
      FROM   (SELECT reference_table.user_id      AS id
               FROM   raw_events_first,
                      reference_table
            WHERE  raw_events_first.user_id = reference_table.user_id ) AS foo) as f
  INNER JOIN
    (SELECT v4,
          v1,
          id
    FROM   (SELECT SUM(raw_events_second.value_4) AS v4,
               SUM(raw_events_first.value_1) AS v1,
               raw_events_second.user_id      AS id
            FROM   raw_events_first,
                    raw_events_second
            WHERE  raw_events_first.user_id = raw_events_second.user_id
            GROUP  BY raw_events_second.user_id
            HAVING SUM(raw_events_second.value_4) > 10) AS foo2 ) as f2
ON (f.id = f2.id)) as outer_most
GROUP BY
  outer_most.id;

-- subqueries in WHERE clause
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id IN (SELECT user_id
                   FROM   raw_events_second
                   WHERE  user_id = 2);

-- some UPSERTS
INSERT INTO agg_events AS ae 
            (
                        user_id,
                        value_1_agg,
                        agg_time
            ) 
SELECT user_id,
       value_1,
       time
FROM   raw_events_first
ON conflict (user_id, value_1_agg)
DO UPDATE
   SET    agg_time = EXCLUDED.agg_time 
   WHERE  ae.agg_time < EXCLUDED.agg_time;

-- upserts with returning
INSERT INTO agg_events AS ae 
            ( 
                        user_id, 
                        value_1_agg, 
                        agg_time 
            ) 
SELECT user_id, 
       value_1, 
       time 
FROM   raw_events_first 
ON conflict (user_id, value_1_agg)
DO UPDATE
   SET    agg_time = EXCLUDED.agg_time 
   WHERE  ae.agg_time < EXCLUDED.agg_time
RETURNING user_id, value_1_agg;


INSERT INTO agg_events (user_id, value_1_agg)
SELECT
   user_id, sum(value_1 + value_2)
FROM
   raw_events_first GROUP BY user_id;

--  FILTER CLAUSE
INSERT INTO agg_events (user_id, value_1_agg)
SELECT
   user_id, sum(value_1 + value_2) FILTER (where value_3 = 15)
FROM
   raw_events_first GROUP BY user_id;

-- a test with reference table JOINs
INSERT INTO
  agg_events (user_id, value_1_agg)
SELECT
  raw_events_first.user_id, sum(value_1)
FROM
  reference_table, raw_events_first
WHERE
  raw_events_first.user_id = reference_table.user_id
GROUP BY
  raw_events_first.user_id;

-- a note on the outer joins is that
-- we filter out outer join results
-- where partition column returns
-- NULL. Thus, we could INSERT less rows
-- than we expect from subquery result.
-- see the following tests

SET client_min_messages TO INFO;

-- we don't want to see constraint vialotions, so truncate first
TRUNCATE agg_events;
-- add a row to first table to make table contents different
INSERT INTO raw_events_second (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (10, now(), 100, 10000, 10000, 100000);

DELETE FROM raw_events_second WHERE user_id = 2;

-- we select 11 rows
SELECT t1.user_id AS col1,
         t2.user_id AS col2
  FROM   raw_events_first t1
         FULL JOIN raw_events_second t2
                ON t1.user_id = t2.user_id
  ORDER  BY t1.user_id,
            t2.user_id;

SET client_min_messages TO DEBUG4;
-- we insert 10 rows since we filtered out
-- NULL partition column values
INSERT INTO agg_events (user_id, value_1_agg)
SELECT t1.user_id AS col1,
       t2.user_id AS col2
FROM   raw_events_first t1
       FULL JOIN raw_events_second t2
              ON t1.user_id = t2.user_id;

SET client_min_messages TO INFO;
-- see that the results are different from the SELECT query
SELECT 
  user_id, value_1_agg
FROM 
  agg_events 
ORDER BY
  user_id, value_1_agg;

-- we don't want to see constraint vialotions, so truncate first
SET client_min_messages TO INFO;
TRUNCATE agg_events;
SET client_min_messages TO DEBUG4;

-- DISTINCT clause
INSERT INTO agg_events (value_1_agg, user_id)
  SELECT
    DISTINCT value_1, user_id
  FROM
    raw_events_first;

-- we don't want to see constraint vialotions, so truncate first
SET client_min_messages TO INFO;
truncate agg_events;
SET client_min_messages TO DEBUG4;

-- we do not support DISTINCT ON clauses
INSERT INTO agg_events (value_1_agg, user_id)
  SELECT
    DISTINCT ON (value_1) value_1, user_id
  FROM
    raw_events_first;

-- We do not support some CTEs
WITH fist_table_agg AS
  (SELECT sum(value_1) as v1_agg, user_id FROM raw_events_first GROUP BY user_id)
INSERT INTO agg_events
            (value_1_agg, user_id)
            SELECT
              v1_agg, user_id
            FROM
              fist_table_agg;

-- We do support some CTEs
INSERT INTO agg_events
  WITH sub_cte AS (SELECT 1)
  SELECT
    raw_events_first.user_id, (SELECT * FROM sub_cte)
  FROM
    raw_events_first;

-- We do not support any set operations
INSERT INTO
  raw_events_first(user_id)
SELECT
  user_id
FROM
  ((SELECT user_id FROM raw_events_first) UNION
   (SELECT user_id FROM raw_events_second)) as foo;

-- We do not support any set operations
INSERT INTO
  raw_events_first(user_id)
  (SELECT user_id FROM raw_events_first) INTERSECT
  (SELECT user_id FROM raw_events_first);

-- We do not support any set operations
INSERT INTO
  raw_events_first(user_id)
SELECT
  user_id
FROM
  ((SELECT user_id FROM raw_events_first WHERE user_id = 15) EXCEPT
   (SELECT user_id FROM raw_events_second where user_id = 17)) as foo;

-- unsupported JOIN
INSERT INTO agg_events
            (value_4_agg,
             value_1_agg,
             user_id)
SELECT v4,
       v1,
       id
FROM   (SELECT SUM(raw_events_second.value_4) AS v4,
               SUM(raw_events_first.value_1) AS v1,
               raw_events_second.user_id      AS id
        FROM   raw_events_first,
               raw_events_second
        WHERE  raw_events_first.user_id != raw_events_second.user_id
        GROUP  BY raw_events_second.user_id) AS foo;


-- INSERT partition column does not match with SELECT partition column
INSERT INTO agg_events
            (value_4_agg,
             value_1_agg,
             user_id)
SELECT v4,
       v1,
       id
FROM   (SELECT SUM(raw_events_second.value_4) AS v4,
               SUM(raw_events_first.value_1) AS v1,
               raw_events_second.value_3      AS id
        FROM   raw_events_first,
               raw_events_second
        WHERE  raw_events_first.user_id = raw_events_second.user_id
        GROUP  BY raw_events_second.value_3) AS foo;

-- error cases
-- no part column at all
INSERT INTO raw_events_second
            (value_1)
SELECT value_1
FROM   raw_events_first;

INSERT INTO raw_events_second
            (value_1)
SELECT user_id
FROM   raw_events_first;

INSERT INTO raw_events_second
            (user_id)
SELECT value_1
FROM   raw_events_first;

INSERT INTO raw_events_second
            (user_id)
SELECT user_id * 2
FROM   raw_events_first;

INSERT INTO raw_events_second
            (user_id)
SELECT user_id :: bigint
FROM   raw_events_first;

INSERT INTO agg_events
            (value_3_agg,
             value_4_agg,
             value_1_agg,
             value_2_agg,
             user_id)
SELECT SUM(value_3),
       Count(value_4),
       user_id,
       SUM(value_1),
       Avg(value_2)
FROM   raw_events_first
GROUP  BY user_id;

INSERT INTO agg_events
            (value_3_agg,
             value_4_agg,
             value_1_agg,
             value_2_agg,
             user_id)
SELECT SUM(value_3),
       Count(value_4),
       user_id,
       SUM(value_1), 
       value_2
FROM   raw_events_first
GROUP  BY user_id,
          value_2;

-- tables should be co-located
INSERT INTO agg_events (user_id)
SELECT
  user_id
FROM
  reference_table;

-- unsupported joins between subqueries
-- we do not return bare partition column on the inner query
INSERT INTO agg_events
            (user_id)
SELECT f2.id FROM
(SELECT
      id
FROM   (SELECT reference_table.user_id      AS id
        FROM   raw_events_first,
               reference_table
        WHERE  raw_events_first.user_id = reference_table.user_id ) AS foo) as f
INNER JOIN
(SELECT v4,
       v1,
       id
FROM   (SELECT SUM(raw_events_second.value_4) AS v4,
               raw_events_second.value_1 AS v1,
               SUM(raw_events_second.user_id)      AS id
        FROM   raw_events_first,
               raw_events_second
        WHERE  raw_events_first.user_id = raw_events_second.user_id
        GROUP  BY raw_events_second.value_1
        HAVING SUM(raw_events_second.value_4) > 10) AS foo2 ) as f2
ON (f.id = f2.id);


-- the second part of the query is not routable since
-- no GROUP BY on the partition column
INSERT INTO agg_events
            (user_id)
SELECT f.id FROM
(SELECT
      id
FROM   (SELECT raw_events_first.user_id      AS id
        FROM   raw_events_first,
               reference_table
        WHERE  raw_events_first.user_id = reference_table.user_id ) AS foo) as f
INNER JOIN
(SELECT v4,
       v1,
       id
FROM   (SELECT SUM(raw_events_second.value_4) AS v4,
               raw_events_second.value_1 AS v1,
               SUM(raw_events_second.user_id)      AS id
        FROM   raw_events_first,
               raw_events_second
        WHERE  raw_events_first.user_id = raw_events_second.user_id
        GROUP  BY raw_events_second.value_1
        HAVING SUM(raw_events_second.value_4) > 10) AS foo2 ) as f2
ON (f.id = f2.id);

-- cannot pushdown the query since the JOIN is not equi JOIN
INSERT INTO agg_events
            (user_id, value_4_agg)
SELECT
outer_most.id, max(outer_most.value)
 FROM
(
  SELECT f2.id as id, f2.v4 as value FROM
    (SELECT
          id
      FROM   (SELECT reference_table.user_id      AS id
               FROM   raw_events_first,
                      reference_table
            WHERE  raw_events_first.user_id = reference_table.user_id ) AS foo) as f
  INNER JOIN
    (SELECT v4,
          v1,
          id
    FROM   (SELECT SUM(raw_events_second.value_4) AS v4,
               SUM(raw_events_first.value_1) AS v1,
               raw_events_second.user_id      AS id
            FROM   raw_events_first,
                    raw_events_second
            WHERE  raw_events_first.user_id = raw_events_second.user_id
            GROUP  BY raw_events_second.user_id
            HAVING SUM(raw_events_second.value_4) > 10) AS foo2 ) as f2
ON (f.id != f2.id)) as outer_most
GROUP BY outer_most.id;

-- we currently not support grouping sets
INSERT INTO agg_events
            (user_id,
             value_1_agg,
             value_2_agg)
SELECT user_id,
       Sum(value_1) AS sum_val1,
       Sum(value_2) AS sum_val2
FROM   raw_events_second
GROUP  BY grouping sets ( ( user_id ), ( value_1 ), ( user_id, value_1 ), ( ) );

-- set back to INFO
SET client_min_messages TO INFO;

-- Views does not work
CREATE VIEW test_view AS SELECT * FROM raw_events_first;
INSERT INTO raw_events_second SELECT * FROM test_view;
