--
-- MULTI_INSERT_SELECT
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 13300000;

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

CREATE TABLE insert_select_varchar_test (key varchar, value int);
SELECT create_distributed_table('insert_select_varchar_test', 'key', 'hash');

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

SET client_min_messages TO DEBUG2;

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

-- stable functions should be allowed
INSERT INTO raw_events_second (user_id, time)
SELECT
  user_id, now()
FROM
  raw_events_first
WHERE
  user_id < 0;

INSERT INTO raw_events_second (user_id)
SELECT
  user_id
FROM
  raw_events_first
WHERE
  time > now() + interval '1 day';

-- hide version-dependent PL/pgSQL context messages
\set VERBOSITY terse

-- make sure we evaluate stable functions on the master, once
CREATE OR REPLACE FUNCTION evaluate_on_master()
RETURNS int LANGUAGE plpgsql STABLE
AS $function$
BEGIN
  RAISE NOTICE 'evaluating on master';
  RETURN 0;
END;
$function$;

INSERT INTO raw_events_second (user_id, value_1)
SELECT
  user_id, evaluate_on_master()
FROM
  raw_events_first
WHERE
  user_id < 0;

-- make sure we don't evaluate stable functions with column arguments
CREATE OR REPLACE FUNCTION evaluate_on_master(x int)
RETURNS int LANGUAGE plpgsql STABLE
AS $function$
BEGIN
  RAISE NOTICE 'evaluating on master';
  RETURN x;
END;
$function$;

INSERT INTO raw_events_second (user_id, value_1)
SELECT
  user_id, evaluate_on_master(value_1)
FROM
  raw_events_first
WHERE
  user_id = 0;

\set VERBOSITY default

-- volatile functions should be disallowed
INSERT INTO raw_events_second (user_id, value_1)
SELECT
  user_id, (random()*10)::int
FROM
  raw_events_first;

INSERT INTO raw_events_second (user_id, value_1)
WITH sub_cte AS (SELECT (random()*10)::int)
SELECT
  user_id, (SELECT * FROM sub_cte)
FROM
  raw_events_first;


-- add one more row
INSERT INTO raw_events_first (user_id, time) VALUES
                         (7, now());

-- try a single shard query
SET client_min_messages TO DEBUG2;
INSERT INTO raw_events_second (user_id, time) SELECT user_id, time FROM raw_events_first WHERE user_id = 7;


SET client_min_messages TO INFO;

-- add one more row
INSERT INTO raw_events_first (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (8, now(), 80, 800, 8000, 80000);


-- reorder columns
SET client_min_messages TO DEBUG2;
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
SET client_min_messages TO DEBUG2;
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
GROUP  BY id
ORDER  BY id;


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
        GROUP  BY raw_events_second.user_id) AS foo
ORDER  BY id;

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

INSERT INTO raw_events_second
             (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id IN (SELECT user_id
                   FROM   raw_events_second
                   WHERE  user_id != 2 AND value_1 = 2000)
ON conflict (user_id, value_1) DO NOTHING;

INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id IN (SELECT user_id
                   FROM  raw_events_second WHERE false);

INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id IN (SELECT user_id
                  FROM   raw_events_second
                   WHERE value_1 = 1000 OR value_1 = 2000 OR value_1 = 3000);

-- lets mix subqueries in FROM clause and subqueries in WHERE
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
        HAVING SUM(raw_events_second.value_4) > 1000) AS foo2 ) as f2
ON (f.id = f2.id)
WHERE f.id IN (SELECT user_id
               FROM   raw_events_second);

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

SET client_min_messages TO DEBUG2;
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
SET client_min_messages TO DEBUG2;

-- DISTINCT clause
INSERT INTO agg_events (value_1_agg, user_id)
  SELECT
    DISTINCT value_1, user_id
  FROM
    raw_events_first;

-- we don't want to see constraint vialotions, so truncate first
SET client_min_messages TO INFO;
truncate agg_events;
SET client_min_messages TO DEBUG2;

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

-- We don't support CTEs that consist of const values as well
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

-- some supported LEFT joins
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first  LEFT JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.user_id;
 
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_second.user_id
 FROM
   reference_table LEFT JOIN raw_events_second ON reference_table.user_id = raw_events_second.user_id;
 
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first LEFT JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.user_id
   WHERE raw_events_first.user_id = 10;
 
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first LEFT JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.user_id
   WHERE raw_events_second.user_id = 10 OR raw_events_second.user_id = 11;
 
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first INNER JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.user_id
   WHERE raw_events_first.user_id = 10 AND raw_events_first.user_id = 20;
 
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first LEFT JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.user_id
   WHERE raw_events_first.user_id = 10 AND raw_events_second.user_id = 20;
 
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first LEFT JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.user_id
   WHERE raw_events_first.user_id IN (19, 20, 21);
 
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first INNER JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.user_id
   WHERE raw_events_second.user_id IN (19, 20, 21);
 
 -- the following is a very tricky query for Citus
 -- although we do not support pushing down JOINs on non-partition
 -- columns here it is safe to push it down given that we're looking for
 -- a specific value (i.e., value_1 = 12) on the joining column.
 -- Note that the query always hits the same shard on raw_events_second
 -- and this query wouldn't have worked if we're to use different worker
 -- count or shard replication factor
 INSERT INTO agg_events 
             (user_id) 
 SELECT raw_events_first.user_id 
 FROM   raw_events_first, 
        raw_events_second 
 WHERE  raw_events_second.user_id = raw_events_first.value_1 
        AND raw_events_first.value_1 = 12; 
 
 -- some unsupported LEFT/INNER JOINs
 -- JOIN on one table with partition column other is not
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first LEFT JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.value_1;
 
 -- same as the above with INNER JOIN
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first INNER JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.value_1;
 
 -- a not meaningful query
 INSERT INTO agg_events 
             (user_id) 
 SELECT raw_events_second.user_id 
 FROM   raw_events_first, 
        raw_events_second 
 WHERE  raw_events_first.user_id = raw_events_first.value_1; 
 
 -- both tables joined on non-partition columns
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first LEFT JOIN raw_events_second ON raw_events_first.value_1 = raw_events_second.value_1;
 
 -- same as the above with INNER JOIN
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first INNER JOIN raw_events_second ON raw_events_first.value_1 = raw_events_second.value_1;
 
-- even if there is a filter on the partition key, since the join is not on the partition key we reject
-- this query
INSERT INTO agg_events (user_id)
SELECT
  raw_events_first.user_id
FROM
  raw_events_first LEFT JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.value_1
WHERE 
  raw_events_first.user_id = 10;
 
 -- same as the above with INNER JOIN
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first INNER JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.value_1
 WHERE raw_events_first.user_id = 10;
 
 -- make things a bit more complicate with IN clauses
 INSERT INTO agg_events (user_id)
 SELECT
   raw_events_first.user_id
 FROM
   raw_events_first INNER JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.value_1
   WHERE raw_events_first.value_1 IN (10, 11,12) OR raw_events_second.user_id IN (1,2,3,4);
 
 -- implicit join on non partition column should also not be pushed down
 INSERT INTO agg_events 
             (user_id) 
 SELECT raw_events_first.user_id 
 FROM   raw_events_first, 
        raw_events_second 
 WHERE  raw_events_second.user_id = raw_events_first.value_1; 
 
 -- the following is again a tricky query for Citus
 -- if the given filter was on value_1 as shown in the above, Citus could
 -- push it down. But here the query is refused
 INSERT INTO agg_events 
             (user_id) 
 SELECT raw_events_first.user_id 
 FROM   raw_events_first, 
        raw_events_second 
 WHERE  raw_events_second.user_id = raw_events_first.value_1 
        AND raw_events_first.value_2 = 12;
 
 -- lets do some unsupported query tests with subqueries
 -- foo is not joined on the partition key so the query is not 
 -- pushed down
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
                FROM   raw_events_first LEFT JOIN
                       reference_table
             ON (raw_events_first.value_1 = reference_table.user_id)) AS foo) as f
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

 -- if the given filter was on value_1 as shown in the above, Citus could
 -- push it down. But here the query is refused
 INSERT INTO agg_events 
             (user_id) 
 SELECT raw_events_first.user_id 
 FROM   raw_events_first, 
        raw_events_second 
 WHERE  raw_events_second.user_id = raw_events_first.value_1 
        AND raw_events_first.value_2 = 12;

 -- lets do some unsupported query tests with subqueries
 -- foo is not joined on the partition key so the query is not 
 -- pushed down
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
                FROM   raw_events_first LEFT JOIN
                       reference_table
             ON (raw_events_first.value_1 = reference_table.user_id)) AS foo) as f
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
-- GROUP BY not on the partition column (i.e., value_1) and thus join
-- on f.id = f2.id is not on the partition key (instead on the sum of partition key)
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


-- cannot pushdown since foo2 is not join on partition key
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
            WHERE  raw_events_first.user_id = raw_events_second.value_1
            GROUP  BY raw_events_second.user_id
            HAVING SUM(raw_events_second.value_4) > 10) AS foo2 ) as f2
ON (f.id = f2.id)) as outer_most
GROUP BY
  outer_most.id;

-- cannot push down since foo doesn't have en equi join
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
            WHERE  raw_events_first.user_id != reference_table.user_id ) AS foo) as f
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


-- some unsupported LATERAL JOINs
-- join on averages is not on the partition key
INSERT INTO agg_events (user_id, value_4_agg)
SELECT
  averages.user_id, avg(averages.value_4)
FROM
    (SELECT
      raw_events_second.user_id
    FROM
      reference_table JOIN raw_events_second on (reference_table.user_id = raw_events_second.user_id)
    ) reference_ids
  JOIN LATERAL
    (SELECT
      user_id, value_4
    FROM
      raw_events_first WHERE
      value_4 = reference_ids.user_id) as averages ON true
    GROUP BY averages.user_id;

-- join among reference_ids and averages is not on the partition key
INSERT INTO agg_events (user_id, value_4_agg)
SELECT
  averages.user_id, avg(averages.value_4)
FROM
    (SELECT
      raw_events_second.user_id
    FROM
      reference_table JOIN raw_events_second on (reference_table.user_id = raw_events_second.user_id)
    ) reference_ids
  JOIN LATERAL
    (SELECT
      user_id, value_4
    FROM
      raw_events_first) as averages ON averages.value_4 = reference_ids.user_id
    GROUP BY averages.user_id;

-- join among the agg_ids and averages is not on the partition key
INSERT INTO agg_events (user_id, value_4_agg)
SELECT
  averages.user_id, avg(averages.value_4)
FROM
    (SELECT
      raw_events_second.user_id
    FROM
      reference_table JOIN raw_events_second on (reference_table.user_id = raw_events_second.user_id)
    ) reference_ids
  JOIN LATERAL
    (SELECT
      user_id, value_4
    FROM
      raw_events_first) as averages ON averages.user_id = reference_ids.user_id
JOIN LATERAL
    (SELECT user_id, value_4 FROM agg_events) as agg_ids ON (agg_ids.value_4 = averages.user_id)
    GROUP BY averages.user_id;

-- not supported subqueries in WHERE clause
-- since the selected value in the WHERE is not
-- partition key  
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id IN (SELECT value_1
                   FROM   raw_events_second);

-- same as above but slightly more complex
-- since it also includes subquery in FROM as well
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
ON (f.id = f2.id)
WHERE f.id IN (SELECT value_1
               FROM   raw_events_second);

-- some more semi-anti join tests

-- join in where
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id IN (SELECT raw_events_second.user_id
                   FROM   raw_events_second, raw_events_first
                   WHERE  raw_events_second.user_id = raw_events_first.user_id AND raw_events_first.user_id = 200);

-- we cannot push this down since it is NOT IN
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id NOT IN (SELECT raw_events_second.user_id
                   FROM   raw_events_second, raw_events_first
                   WHERE  raw_events_second.user_id = raw_events_first.user_id AND raw_events_first.user_id = 200);


-- safe to push down
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  EXISTS (SELECT 1
                   FROM   raw_events_second
                   WHERE  raw_events_second.user_id =raw_events_first.user_id);

-- we cannot push down
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  NOT EXISTS (SELECT 1
                   FROM   raw_events_second
                   WHERE  raw_events_second.user_id =raw_events_first.user_id);


-- more complex LEFT JOINs 
 INSERT INTO agg_events
             (user_id, value_4_agg)
 SELECT
   outer_most.id, max(outer_most.value)
 FROM
 (
   SELECT f2.id as id, f2.v4 as value FROM
     (SELECT
           id
       FROM   (SELECT raw_events_first.user_id      AS id
                FROM   raw_events_first LEFT JOIN
                       reference_table
             ON (raw_events_first.user_id = reference_table.user_id)) AS foo) as f
   LEFT JOIN
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


-- cannot push down since the f.id IN is matched with value_1
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id IN (
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
ON (f.id = f2.id)
WHERE f.id IN (SELECT value_1
               FROM   raw_events_second));

-- same as above, but this time is it safe to push down since
-- f.id IN is matched with user_id
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id IN (
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
ON (f.id = f2.id)
WHERE f.id IN (SELECT user_id
               FROM   raw_events_second));

-- cannot push down since top level user_id is matched with NOT IN
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id NOT IN (
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
ON (f.id = f2.id)
WHERE f.id IN (SELECT user_id
               FROM   raw_events_second));

-- cannot push down since join is not equi join (f.id > f2.id)
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id IN (
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
ON (f.id > f2.id)
WHERE f.id IN (SELECT user_id
               FROM   raw_events_second));

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

-- avoid constraint violations
TRUNCATE raw_events_first;

-- we don't support LIMIT even if it exists in the subqueries 
-- in where clause
INSERT INTO agg_events(user_id)
SELECT user_id 
FROM   users_table 
WHERE  user_id 
  IN (SELECT 
      user_id 
        FROM  (
                (
                  SELECT 
                    user_id 
                    FROM
                    (
                      SELECT 
                        e1.user_id 
                      FROM 
                        users_table u1, events_table e1 
                      WHERE 
                        e1.user_id = u1.user_id LIMIT 3
                     ) as f_inner
                  )
          ) AS f2); 

-- Altering a table and selecting from it using a multi-shard statement
-- in the same transaction is allowed because we will use the same
-- connections for all co-located placements.
BEGIN;
ALTER TABLE raw_events_second DROP COLUMN value_4;
INSERT INTO raw_events_first SELECT * FROM raw_events_second; 
ROLLBACK;

-- Alterating a table and selecting from it using a single-shard statement
-- in the same transaction is disallowed because we will use a different
-- connection.
BEGIN;
ALTER TABLE raw_events_second DROP COLUMN value_4;
INSERT INTO raw_events_first SELECT * FROM raw_events_second WHERE user_id = 100; 
ROLLBACK;

-- Insert after copy is currently disallowed because of the way the 
-- transaction modification state is currently handled. Copy is also
-- rolled back.
BEGIN;
COPY raw_events_second (user_id, value_1) FROM STDIN DELIMITER ',';
100,100
\.
INSERT INTO raw_events_first SELECT * FROM raw_events_second;
ROLLBACK;

-- Insert after copy is currently allowed for single-shard operation.
-- Both insert and copy are rolled back successfully.
BEGIN;
COPY raw_events_second (user_id, value_1) FROM STDIN DELIMITER ',';
101,101
\.
INSERT INTO raw_events_first SELECT * FROM raw_events_second WHERE user_id = 101;
SELECT user_id FROM raw_events_first WHERE user_id = 101;
ROLLBACK;

-- Copy after insert is currently disallowed.
BEGIN;
INSERT INTO raw_events_first SELECT * FROM raw_events_second;
COPY raw_events_first (user_id, value_1) FROM STDIN DELIMITER ',';
102,102
\.
ROLLBACK;

BEGIN;
INSERT INTO raw_events_first SELECT * FROM raw_events_second WHERE user_id = 100;
COPY raw_events_first (user_id, value_1) FROM STDIN DELIMITER ',';
103,103
\.
ROLLBACK;

-- selecting from views works
CREATE VIEW test_view AS SELECT * FROM raw_events_first;
INSERT INTO raw_events_first (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (16, now(), 60, 600, 6000.1, 60000);
SELECT count(*) FROM raw_events_second;
INSERT INTO raw_events_second SELECT * FROM test_view;
INSERT INTO raw_events_first (user_id, time, value_1, value_2, value_3, value_4) VALUES
                         (17, now(), 60, 600, 6000.1, 60000);
INSERT INTO raw_events_second SELECT * FROM test_view WHERE user_id = 17 GROUP BY 1,2,3,4,5,6;
SELECT count(*) FROM raw_events_second;

-- inserting into views does not
INSERT INTO test_view SELECT * FROM raw_events_second;

-- we need this in our next test
truncate raw_events_first;

SET client_min_messages TO DEBUG2;

-- first show that the query works now
INSERT INTO raw_events_first SELECT * FROM raw_events_second;

SET client_min_messages TO INFO;
truncate raw_events_first;
SET client_min_messages TO DEBUG2;

-- now show that it works for a single shard query as well
INSERT INTO raw_events_first SELECT * FROM raw_events_second WHERE user_id = 5;

SET client_min_messages TO INFO;

-- if a single shard of the SELECT is unhealty, the query should fail
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 13300004 AND nodeport = :worker_1_port;
truncate raw_events_first;
SET client_min_messages TO DEBUG2;

-- this should fail
INSERT INTO raw_events_first SELECT * FROM raw_events_second;

-- this should also fail
INSERT INTO raw_events_first SELECT * FROM raw_events_second WHERE user_id = 5;

-- but this should work given that it hits different shard
INSERT INTO raw_events_first SELECT * FROM raw_events_second WHERE user_id = 6;

SET client_min_messages TO INFO;

-- mark the unhealthy placement as healthy again for the next tests
UPDATE pg_dist_shard_placement SET shardstate = 1 WHERE shardid = 13300004 AND nodeport = :worker_1_port;

-- now that we should show that it works if one of the target shard interval is not healthy
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 13300000 AND nodeport = :worker_1_port;
truncate raw_events_first;
SET client_min_messages TO DEBUG2;

-- this should work
INSERT INTO raw_events_first SELECT * FROM raw_events_second;

SET client_min_messages TO INFO;
truncate raw_events_first;
SET client_min_messages TO DEBUG2;

-- this should also work
INSERT INTO raw_events_first SELECT * FROM raw_events_second WHERE user_id = 5;

SET client_min_messages TO INFO;

-- now do some tests with varchars
INSERT INTO insert_select_varchar_test VALUES ('test_1', 10);
INSERT INTO insert_select_varchar_test VALUES ('test_2', 30);

INSERT INTO insert_select_varchar_test (key, value)
SELECT *, 100
FROM   (SELECT f1.key
        FROM   (SELECT key
                FROM   insert_select_varchar_test 
                GROUP  BY 1
                HAVING Count(key) < 3) AS f1, 
               (SELECT key 
                FROM   insert_select_varchar_test 
                GROUP  BY 1 
                HAVING Sum(COALESCE(insert_select_varchar_test.value, 0)) > 
                       20.0) 
               AS f2 
        WHERE  f1.key = f2.key 
        GROUP  BY 1) AS foo; 

SELECT * FROM insert_select_varchar_test;

-- some tests with DEFAULT columns and constant values
-- this test is mostly importantly intended for deparsing the query correctly
-- but still it is preferable to have this test here instead of multi_deparse_shard_query
CREATE TABLE table_with_defaults
( 
  store_id int,
  first_name text,
  default_1 int DEFAULT 1,
  last_name text,
  default_2 text DEFAULT '2'
);

-- we don't need many shards
SET citus.shard_count = 2;
SELECT create_distributed_table('table_with_defaults', 'store_id');

-- let's see the queries
SET client_min_messages TO DEBUG2;

-- a very simple query
INSERT INTO table_with_defaults SELECT * FROM table_with_defaults;

-- see that defaults are filled
INSERT INTO table_with_defaults (store_id, first_name)
SELECT
  store_id, first_name
FROM
  table_with_defaults;

-- shuffle one of the defaults and skip the other
INSERT INTO table_with_defaults (default_2, store_id, first_name)
SELECT
  default_2, store_id, first_name
FROM
  table_with_defaults;

-- shuffle both defaults
INSERT INTO table_with_defaults (default_2, store_id, default_1, first_name)
SELECT
  default_2, store_id, default_1, first_name
FROM
  table_with_defaults;

-- use constants instead of non-default column
INSERT INTO table_with_defaults (default_2, last_name, store_id, first_name)
SELECT
  default_2, 'Freund', store_id, 'Andres'
FROM
  table_with_defaults;

-- use constants instead of non-default column and skip both defauls
INSERT INTO table_with_defaults (last_name, store_id, first_name)
SELECT
  'Freund', store_id, 'Andres'
FROM
  table_with_defaults;

-- use constants instead of default columns
INSERT INTO table_with_defaults (default_2, last_name, store_id, first_name, default_1)
SELECT
  20, last_name, store_id, first_name, 10
FROM
  table_with_defaults;

-- use constants instead of both default columns and non-default columns
INSERT INTO table_with_defaults (default_2, last_name, store_id, first_name, default_1)
SELECT
  20, 'Freund', store_id, 'Andres', 10
FROM
  table_with_defaults;

-- some of the the ultimate queries where we have constants,
-- defaults and group by entry is not on the target entry
INSERT INTO table_with_defaults (default_2, store_id, first_name)
SELECT
  '2000', store_id, 'Andres'
FROM
  table_with_defaults
GROUP BY
  last_name, store_id;

INSERT INTO table_with_defaults (default_1, store_id, first_name, default_2)
SELECT
  1000, store_id, 'Andres', '2000'
FROM
  table_with_defaults
GROUP BY
  last_name, store_id, first_name;


INSERT INTO table_with_defaults (default_1, store_id, first_name, default_2)
SELECT
  1000, store_id, 'Andres', '2000'
FROM
  table_with_defaults
GROUP BY
  last_name, store_id, first_name, default_2;

INSERT INTO table_with_defaults (default_1, store_id, first_name)
SELECT
  1000, store_id, 'Andres'
FROM
  table_with_defaults
GROUP BY
  last_name, store_id, first_name, default_2;

RESET client_min_messages;

-- Stable function in default should be allowed
ALTER TABLE table_with_defaults ADD COLUMN t timestamptz DEFAULT now();

INSERT INTO table_with_defaults (store_id, first_name, last_name)
SELECT
  store_id, 'first '||store_id, 'last '||store_id
FROM
  table_with_defaults
GROUP BY
  store_id, first_name, last_name;

-- Volatile function in default should be disallowed
CREATE TABLE table_with_serial (
  store_id int,
  s bigserial
);
SELECT create_distributed_table('table_with_serial', 'store_id');

INSERT INTO table_with_serial (store_id)
SELECT
  store_id
FROM
  table_with_defaults
GROUP BY
  store_id;

-- do some more error/error message checks
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
CREATE TABLE text_table (part_col text, val int);
CREATE TABLE char_table (part_col char[], val int);
create table table_with_starts_with_defaults (a int DEFAULT 5, b int, c int);
SELECT create_distributed_table('text_table', 'part_col');
SELECT create_distributed_table('char_table','part_col');
SELECT create_distributed_table('table_with_starts_with_defaults', 'c');

INSERT INTO text_table (part_col) 
  SELECT 
    CASE WHEN part_col = 'onder' THEN 'marco'
      END 
FROM text_table ;



INSERT INTO text_table (part_col) SELECT COALESCE(part_col, 'onder') FROM text_table;
INSERT INTO text_table (part_col) SELECT GREATEST(part_col, 'jason') FROM text_table;
INSERT INTO text_table (part_col) SELECT LEAST(part_col, 'andres') FROM text_table;
INSERT INTO text_table (part_col) SELECT NULLIF(part_col, 'metin') FROM text_table;
INSERT INTO text_table (part_col) SELECT part_col isnull FROM text_table;
INSERT INTO text_table (part_col) SELECT part_col::text from char_table;
INSERT INTO text_table (part_col) SELECT (part_col = 'burak') is true FROM text_table;
INSERT INTO text_table (part_col) SELECT val FROM text_table;
INSERT INTO text_table (part_col) SELECT val::text FROM text_table;
insert into table_with_starts_with_defaults (b,c) select b,c FROM table_with_starts_with_defaults;

-- Test on partition column without native hash function 
CREATE TABLE raw_table
(
    id BIGINT,
    time DATE
);

CREATE TABLE summary_table
(
    time DATE,
    count BIGINT
);

SELECT create_distributed_table('raw_table', 'time');
SELECT create_distributed_table('summary_table', 'time');

INSERT INTO raw_table VALUES(1, '11-11-1980');
INSERT INTO summary_table SELECT time, COUNT(*) FROM raw_table GROUP BY time;

SELECT * FROM summary_table;

DROP TABLE raw_table;
DROP TABLE summary_table;
DROP TABLE raw_events_first CASCADE;
DROP TABLE raw_events_second;
DROP TABLE reference_table;
DROP TABLE agg_events;
DROP TABLE table_with_defaults;
DROP TABLE table_with_serial;
DROP TABLE text_table;
DROP TABLE char_table;
DROP TABLE table_with_starts_with_defaults;
