--
-- MULTI_INSERT_SELECT
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 13300000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 13300000;


CREATE TABLE raw_events_first (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint, UNIQUE(user_id, value_1));
SELECT master_create_distributed_table('raw_events_first', 'user_id', 'hash');
SELECT master_create_worker_shards('raw_events_first', 4, 2);

CREATE TABLE raw_events_second (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint, UNIQUE(user_id, value_1));
SELECT master_create_distributed_table('raw_events_second', 'user_id', 'hash');
SELECT master_create_worker_shards('raw_events_second', 4, 2);

CREATE TABLE agg_events (user_id int, value_1_agg int, value_2_agg int, value_3_agg float, value_4_agg bigint, agg_time timestamp, UNIQUE(user_id, value_1_agg));
SELECT master_create_distributed_table('agg_events', 'user_id', 'hash');
SELECT master_create_worker_shards('agg_events', 4, 2);

-- make tables as co-located
UPDATE pg_dist_partition SET colocationid = 100000 WHERE logicalrelid IN ('raw_events_first', 'raw_events_second', 'agg_events');


CREATE TABLE reference_table (user_id int);
SELECT master_create_distributed_table('reference_table', 'user_id', 'hash');
SELECT master_create_worker_shards('reference_table', 1, 2);

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
-- TODO: there is a bug on RETURNING 
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



-- TODO:: add hll and date_trunc 
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

-- TODO: UUIDs

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

-- We do not support CTEs
WITH fist_table_agg AS
  (SELECT sum(value_1) as v1_agg, user_id FROM raw_events_first GROUP BY user_id)
INSERT INTO agg_events
            (value_1_agg, user_id)
            SELECT
              v1_agg, user_id
            FROM
              fist_table_agg;

-- We do not support CTEs in the INSERT as well
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
INSERT INTO agg_events (user_id) SELECT user_id FROM reference_table;
