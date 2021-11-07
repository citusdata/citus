--
-- multi shard update delete
-- this file is intended to test multi shard update/delete queries
--
SET citus.next_shard_id TO 1440000;

SET citus.shard_replication_factor to 1;
SET citus.multi_shard_modify_mode to 'parallel';

CREATE TABLE users_test_table(user_id int, value_1 int, value_2 int, value_3 int);
SELECT create_distributed_table('users_test_table', 'user_id');
\COPY users_test_table FROM STDIN DELIMITER AS ',';
1, 5, 6, 7
2, 12, 7, 18
3, 23, 8, 25
4, 42, 9, 23
5, 35, 10, 21
6, 21, 11, 25
7, 27, 12, 18
8, 18, 13, 4
7, 38, 14, 22
6, 43, 15, 22
5, 61, 16, 17
4, 6, 17, 8
3, 16, 18, 44
2, 25, 19, 38
1, 55, 20, 17
\.

CREATE TABLE events_test_table (user_id int, value_1 int, value_2 int, value_3 int);
SELECT create_distributed_table('events_test_table', 'user_id');
\COPY events_test_table FROM STDIN DELIMITER AS ',';
1, 5, 7, 7
3, 11, 78, 18
5, 22, 9, 25
7, 41, 10, 23
9, 34, 11, 21
1, 20, 12, 25
3, 26, 13, 18
5, 17, 14, 4
7, 37, 15, 22
9, 42, 16, 22
1, 60, 17, 17
3, 5, 18, 8
5, 15, 19, 44
7, 24, 20, 38
9, 54, 21, 17
\.

CREATE TABLE events_reference_copy_table (like events_test_table);
SELECT create_reference_table('events_reference_copy_table');
INSERT INTO events_reference_copy_table SELECT * FROM events_test_table;

CREATE TABLE users_reference_copy_table (like users_test_table);
SELECT create_reference_table('users_reference_copy_table');
INSERT INTO users_reference_copy_table SELECT * FROM users_test_table;

-- Run multi shard updates and deletes without transaction on hash distributed tables
UPDATE users_test_table SET value_1 = 1;
SELECT COUNT(*), SUM(value_1) FROM users_test_table;

SELECT COUNT(*), SUM(value_2) FROM users_test_table WHERE user_id = 1 or user_id = 3;
UPDATE users_test_table SET value_2 = value_2 + 1 WHERE user_id = 1 or user_id = 3;
SELECT COUNT(*), SUM(value_2) FROM users_test_table WHERE user_id = 1 or user_id = 3;

UPDATE users_test_table SET value_3 = 0 WHERE user_id <> 5;
SELECT SUM(value_3) FROM users_test_table WHERE user_id <> 5;

SELECT COUNT(*) FROM users_test_table WHERE user_id = 3 or user_id = 5;
DELETE FROM users_test_table WHERE user_id = 3 or user_id = 5;
SELECT COUNT(*) FROM users_test_table WHERE user_id = 3 or user_id = 5;

-- Run multi shard update delete queries within transactions
BEGIN;
UPDATE users_test_table SET value_3 = 0;
END;
SELECT SUM(value_3) FROM users_test_table;

-- Update can also be rollbacked
BEGIN;
UPDATE users_test_table SET value_3 = 1;
ROLLBACK;
SELECT SUM(value_3) FROM users_test_table;

-- Run with inserts (we need to set citus.multi_shard_modify_mode to sequential)
BEGIN;
INSERT INTO users_test_table (user_id, value_3) VALUES(20, 15);
INSERT INTO users_test_table (user_id, value_3) VALUES(16,1), (20,16), (7,1), (20,17);
SET citus.multi_shard_modify_mode to sequential;
UPDATE users_test_table SET value_3 = 1;
END;
SELECT SUM(value_3) FROM users_test_table;

SET citus.multi_shard_modify_mode to 'sequential';
-- Run multiple multi shard updates (with sequential executor)
BEGIN;
UPDATE users_test_table SET value_3 = 5;
UPDATE users_test_table SET value_3 = 0;
END;
SELECT SUM(value_3) FROM users_copy_table;

-- Run multiple multi shard updates (with parallel executor)
SET citus.multi_shard_modify_mode to 'parallel';
UPDATE users_test_table SET value_3 = 5;
BEGIN;
UPDATE users_test_table SET value_3 = 2;
UPDATE users_test_table SET value_3 = 0;
END;
SELECT SUM(value_3) FROM users_test_table;

-- Check with kind of constraints
UPDATE users_test_table SET value_3 = 1 WHERE user_id = 3 or true;
SELECT COUNT(*), SUM(value_3) FROM users_test_table;
UPDATE users_test_table SET value_3 = 0 WHERE user_id = 20 and false;
SELECT COUNT(*), SUM(value_3) FROM users_test_table;

-- Run multi shard updates with prepared statements
PREPARE foo_plan(int,int) AS UPDATE users_test_table SET value_1 = $1, value_3 = $2;

EXECUTE foo_plan(1,5);
EXECUTE foo_plan(3,15);
EXECUTE foo_plan(5,25);
EXECUTE foo_plan(7,35);
EXECUTE foo_plan(9,45);
EXECUTE foo_plan(0,0);

SELECT SUM(value_1), SUM(value_3) FROM users_test_table;

-- Test on append table (set executor mode to sequential, since with the append
-- distributed tables parallel executor may create tons of connections)
SET citus.multi_shard_modify_mode to sequential;
CREATE TABLE append_stage_table(id int, col_2 int);
INSERT INTO append_stage_table VALUES(1,3);
INSERT INTO append_stage_table VALUES(3,2);
INSERT INTO append_stage_table VALUES(5,4);

CREATE TABLE append_stage_table_2(id int, col_2 int);
INSERT INTO append_stage_table_2 VALUES(8,3);
INSERT INTO append_stage_table_2 VALUES(9,2);
INSERT INTO append_stage_table_2 VALUES(10,4);

CREATE TABLE test_append_table(id int, col_2 int);
SELECT create_distributed_table('test_append_table','id','append');
SELECT master_create_empty_shard('test_append_table') AS shardid \gset
COPY test_append_table FROM STDIN WITH (format 'csv', append_to_shard :shardid);
1,3
3,2
5,4
\.
SELECT master_create_empty_shard('test_append_table') AS shardid \gset
COPY test_append_table FROM STDIN WITH (format 'csv', append_to_shard :shardid);
8,3
9,2
10,4
\.
UPDATE test_append_table SET col_2 = 5;
SELECT * FROM test_append_table ORDER BY 1 DESC, 2 DESC;

DROP TABLE append_stage_table;
DROP TABLE append_stage_table_2;
DROP TABLE test_append_table;

-- Update multi shard of partitioned distributed table
SET citus.multi_shard_modify_mode to 'parallel';
SET citus.shard_replication_factor to 1;
CREATE TABLE tt1(id int, col_2 int) partition by range (col_2);
CREATE TABLE tt1_510 partition of tt1 for VALUES FROM (5) to (10);
CREATE TABLE tt1_1120 partition of tt1 for VALUES FROM (11) to (20);
INSERT INTO tt1 VALUES (1,11), (3,15), (5,17), (6,19), (8,17), (2,12);
SELECT create_distributed_table('tt1','id');
UPDATE tt1 SET col_2 = 13;
DELETE FROM tt1 WHERE id = 1 or id = 3 or id = 5;
SELECT * FROM tt1 ORDER BY 1 DESC, 2 DESC;

-- Partitioned distributed table within transaction
INSERT INTO tt1 VALUES(4,6);
INSERT INTO tt1 VALUES(7,7);
INSERT INTO tt1 VALUES(9,8);
BEGIN;
-- Update rows from partititon tt1_1120
UPDATE tt1 SET col_2 = 12 WHERE col_2 > 10 and col_2 < 20;
-- Update rows from partititon tt1_510
UPDATE tt1 SET col_2 = 7 WHERE col_2 < 10 and col_2 > 5;
COMMIT;
SELECT * FROM tt1 ORDER BY id;

-- Modify main table and partition table within same transaction
BEGIN;
UPDATE tt1 SET col_2 = 12 WHERE col_2 > 10 and col_2 < 20;
UPDATE tt1 SET col_2 = 7 WHERE col_2 < 10 and col_2 > 5;
DELETE FROM tt1_510;
DELETE FROM tt1_1120;
COMMIT;
SELECT * FROM tt1 ORDER BY id;
DROP TABLE tt1;

-- Update and copy in the same transaction
CREATE TABLE tt2(id int, col_2 int);
SELECT create_distributed_table('tt2','id');

BEGIN;
\COPY tt2 FROM STDIN DELIMITER AS ',';
1, 10
3, 15
7, 14
9, 75
2, 42
\.
UPDATE tt2 SET col_2 = 1;
COMMIT;
SELECT * FROM tt2 ORDER BY id;

-- Test returning with both type of executors
UPDATE tt2 SET col_2 = 5 RETURNING id, col_2;
SET citus.multi_shard_modify_mode to sequential;
UPDATE tt2 SET col_2 = 3 RETURNING id, col_2;
DROP TABLE tt2;

-- Multiple RTEs are only supported if subquery is pushdownable
SET citus.multi_shard_modify_mode to DEFAULT;

-- To test colocation between tables in modify query
SET citus.shard_count to 6;

CREATE TABLE events_test_table_2 (user_id int, value_1 int, value_2 int, value_3 int);
SELECT create_distributed_table('events_test_table_2', 'user_id');
\COPY events_test_table_2 FROM STDIN DELIMITER AS ',';
1, 5, 7, 7
3, 11, 78, 18
5, 22, 9, 25
7, 41, 10, 23
9, 34, 11, 21
1, 20, 12, 25
3, 26, 13, 18
5, 17, 14, 4
7, 37, 15, 22
9, 42, 16, 22
1, 60, 17, 17
3, 5, 18, 8
5, 15, 19, 44
7, 24, 20, 38
9, 54, 21, 17
\.

CREATE TABLE events_test_table_local (user_id int, value_1 int, value_2 int, value_3 int);
\COPY events_test_table_local FROM STDIN DELIMITER AS ',';
1, 5, 7, 7
3, 11, 78, 18
5, 22, 9, 25
7, 41, 10, 23
9, 34, 11, 21
1, 20, 12, 25
3, 26, 13, 18
5, 17, 14, 4
7, 37, 15, 22
9, 42, 16, 22
1, 60, 17, 17
3, 5, 18, 8
5, 15, 19, 44
7, 24, 20, 38
9, 54, 21, 17
\.

CREATE TABLE test_table_1(id int, date_col timestamptz, col_3 int);
INSERT INTO test_table_1 VALUES(1, '2014-04-05 08:32:12', 5);
INSERT INTO test_table_1 VALUES(2, '2015-02-01 08:31:16', 7);
INSERT INTO test_table_1 VALUES(3, '2111-01-12 08:35:19', 9);
SELECT create_distributed_table('test_table_1', 'id');

-- We can pushdown query if there is partition key equality
UPDATE users_test_table
SET    value_2 = 5
FROM   events_test_table
WHERE  users_test_table.user_id = events_test_table.user_id;

DELETE FROM users_test_table
USING  events_test_table
WHERE  users_test_table.user_id = events_test_table.user_id;

UPDATE users_test_table
SET    value_1 = 3
WHERE  user_id IN (SELECT user_id
              FROM   events_test_table);

DELETE FROM users_test_table
WHERE  user_id IN (SELECT user_id
                   FROM events_test_table);

DELETE FROM events_test_table_2
WHERE now() > (SELECT max(date_col)
               FROM test_table_1
               WHERE test_table_1.id = events_test_table_2.user_id
               GROUP BY id)
RETURNING *;

UPDATE users_test_table
SET    value_1 = 5
FROM   events_test_table
WHERE  users_test_table.user_id = events_test_table.user_id
       AND events_test_table.user_id > 5;

UPDATE users_test_table
SET    value_1 = 4
WHERE  user_id IN (SELECT user_id
              FROM   users_test_table
              UNION
              SELECT user_id
              FROM   events_test_table);

UPDATE users_test_table
SET    value_1 = 4
WHERE  user_id IN (SELECT user_id
              FROM   users_test_table
              UNION
              SELECT user_id
              FROM   events_test_table) returning value_3;

UPDATE users_test_table
SET    value_1 = 4
WHERE  user_id IN (SELECT user_id
              FROM   users_test_table
              UNION ALL
              SELECT user_id
              FROM   events_test_table) returning value_3;

UPDATE users_test_table
SET value_1 = 5
WHERE
  value_2 >
          (SELECT
              max(value_2)
           FROM
              events_test_table
           WHERE
              users_test_table.user_id = events_test_table.user_id
           GROUP BY
              user_id
          );

UPDATE users_test_table
SET value_3 = 1
WHERE
  value_2 >
          (SELECT
              max(value_2)
           FROM
              events_test_table
           WHERE
              users_test_table.user_id = events_test_table.user_id AND
              users_test_table.value_2 > events_test_table.value_2
           GROUP BY
              user_id
          );

UPDATE users_test_table
SET value_2 = 4
WHERE
  value_1 > 1 AND value_1 < 3
  AND value_2 >= 1
  AND user_id IN
  (
    SELECT
      e1.user_id
    FROM (
      SELECT
        user_id,
        1 AS view_homepage
      FROM events_test_table
      WHERE
         value_1 IN (0, 1)
    ) e1 LEFT JOIN LATERAL (
      SELECT
        user_id,
        1 AS use_demo
      FROM events_test_table
      WHERE
        user_id = e1.user_id
    ) e2 ON true
);

UPDATE users_test_table
SET value_3 = 5
WHERE value_2 IN (SELECT AVG(value_1) OVER (PARTITION BY user_id) FROM events_test_table WHERE events_test_table.user_id = users_test_table.user_id);

-- Test it within transaction
BEGIN;

INSERT INTO users_test_table
SELECT * FROM events_test_table
WHERE events_test_table.user_id = 1 OR events_test_table.user_id = 5;

SELECT SUM(value_2) FROM users_test_table;

UPDATE users_test_table
SET value_2 = 1
FROM events_test_table
WHERE users_test_table.user_id = events_test_table.user_id;

SELECT SUM(value_2) FROM users_test_table;

COMMIT;

-- Test with schema
CREATE SCHEMA sec_schema;
CREATE TABLE sec_schema.tt1(id int, value_1 int);
SELECT create_distributed_table('sec_schema.tt1','id');
INSERT INTO sec_schema.tt1 values(1,1),(2,2),(7,7),(9,9);

UPDATE sec_schema.tt1
SET value_1 = 11
WHERE id < (SELECT max(value_2) FROM events_test_table_2
             WHERE sec_schema.tt1.id = events_test_table_2.user_id
             GROUP BY user_id)
RETURNING *;

DROP SCHEMA sec_schema CASCADE;

-- We don't need partition key equality with reference tables
UPDATE events_test_table
SET    value_2 = 5
FROM   users_reference_copy_table
WHERE  users_reference_copy_table.user_id = events_test_table.value_1;

-- Both reference tables and hash distributed tables can be used in subquery
UPDATE events_test_table as ett
SET    value_2 = 6
WHERE ett.value_3 IN (SELECT utt.value_3
                                    FROM users_test_table as utt, users_reference_copy_table as uct
                                    WHERE utt.user_id = uct.user_id AND utt.user_id = ett.user_id);

-- We don't need equality check with constant values in sub-select
UPDATE users_reference_copy_table
SET    value_2 = 6
WHERE  user_id IN (SELECT 2);

UPDATE users_reference_copy_table
SET    value_2 = 6
WHERE  value_1 IN (SELECT 2);

UPDATE users_test_table
SET    value_2 = 6
WHERE  user_id IN (SELECT 2);

UPDATE users_test_table
SET    value_2 = 6
WHERE  value_1 IN (SELECT 2);

-- Function calls in subqueries will be recursively planned
UPDATE test_table_1
SET    col_3 = 6
WHERE  date_col IN (SELECT now());

-- Test with prepared statements
SELECT COUNT(*) FROM users_test_table WHERE value_1 = 0;
PREPARE foo_plan_2(int,int) AS UPDATE users_test_table
                               SET value_1 = $1, value_3 = $2
                               FROM events_test_table
                               WHERE users_test_table.user_id = events_test_table.user_id;

EXECUTE foo_plan_2(1,5);
EXECUTE foo_plan_2(3,15);
EXECUTE foo_plan_2(5,25);
EXECUTE foo_plan_2(7,35);
EXECUTE foo_plan_2(9,45);
EXECUTE foo_plan_2(0,0);
SELECT COUNT(*) FROM users_test_table WHERE value_1 = 0;

-- Test with varying WHERE expressions
UPDATE users_test_table
SET value_1 = 7
FROM events_test_table
WHERE users_test_table.user_id = events_test_table.user_id OR FALSE;

UPDATE users_test_table
SET value_1 = 7
FROM events_test_table
WHERE users_test_table.user_id = events_test_table.user_id AND TRUE;

-- Test with inactive shard-placement
-- manually set shardstate of one placement of users_test_table as inactive
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 1440000;
UPDATE users_test_table
SET    value_2 = 5
FROM   events_test_table
WHERE  users_test_table.user_id = events_test_table.user_id;

-- manually set shardstate of one placement of events_test_table as inactive
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE shardid = 1440004;
UPDATE users_test_table
SET    value_2 = 5
FROM   events_test_table
WHERE  users_test_table.user_id = events_test_table.user_id;

UPDATE pg_dist_shard_placement SET shardstate = 1 WHERE shardid = 1440000;
UPDATE pg_dist_shard_placement SET shardstate = 1 WHERE shardid = 1440004;

-- Subquery must return single value to use it with comparison operators
UPDATE users_test_table as utt
SET    value_1 = 3
WHERE value_2 > (SELECT value_3 FROM events_test_table as ett WHERE utt.user_id = ett.user_id);

-- We can not pushdown a query if the target relation is reference table
UPDATE users_reference_copy_table
SET    value_2 = 5
FROM   events_test_table
WHERE  users_reference_copy_table.user_id = events_test_table.user_id;

-- We cannot push down it if the query has outer join and using
UPDATE events_test_table
SET value_2 = users_test_table.user_id
FROM users_test_table
FULL OUTER JOIN events_test_table e2 USING (user_id)
WHERE e2.user_id = events_test_table.user_id RETURNING events_test_table.value_2;

-- Non-pushdownable subqueries, but will be handled through recursive planning
UPDATE users_test_table
SET    value_1 = 1
WHERE  user_id IN (SELECT Count(value_1)
              FROM   events_test_table
              GROUP  BY user_id);

UPDATE users_test_table
SET    value_1 = (SELECT Count(*)
                  FROM   events_test_table);

UPDATE users_test_table
SET    value_1 = 4
WHERE  user_id IN (SELECT user_id
              FROM   users_test_table
              UNION
              SELECT value_1
              FROM   events_test_table);

UPDATE users_test_table
SET    value_1 = 4
WHERE  user_id IN (SELECT user_id
              FROM   users_test_table
              INTERSECT
              SELECT Sum(value_1)
              FROM   events_test_table
              GROUP  BY user_id);

UPDATE users_test_table
SET    value_2 = (SELECT value_3
                  FROM   users_test_table);

UPDATE users_test_table
SET value_2 = 2
WHERE
  value_2 >
          (SELECT
              max(value_2)
           FROM
              events_test_table
           WHERE
              users_test_table.user_id > events_test_table.user_id AND
              users_test_table.value_1 = events_test_table.value_1
           GROUP BY
              user_id
          );

UPDATE users_test_table
SET (value_1, value_2) = (2,1)
WHERE  user_id IN
       (SELECT user_id
        FROM   users_test_table
        INTERSECT
        SELECT user_id
        FROM   events_test_table);

-- Reference tables can not locate on the outer part of the outer join
UPDATE users_test_table
SET value_1 = 4
WHERE user_id IN
    (SELECT DISTINCT e2.user_id
     FROM users_reference_copy_table
     LEFT JOIN users_test_table e2 ON (e2.user_id = users_reference_copy_table.value_1)) RETURNING *;

-- Volatile functions are also not supported
UPDATE users_test_table
SET    value_2 = 5
FROM   events_test_table
WHERE  users_test_table.user_id = events_test_table.user_id * random();

UPDATE users_test_table
SET    value_2 = 5 * random()
FROM   events_test_table
WHERE  users_test_table.user_id = events_test_table.user_id;

UPDATE users_test_table
SET    value_1 = 3
WHERE  user_id = 1 AND value_1 IN (SELECT value_1
                                   FROM users_test_table
                                   WHERE user_id = 1 AND value_2 > random());

-- Recursive modify planner does not take care of following test because the query
-- is fully pushdownable, yet not allowed because it would lead to inconsistent replicas.
UPDATE users_test_table
SET    value_2 = subquery.random FROM (SELECT user_id, random()
                                       FROM events_test_table) subquery
WHERE  users_test_table.user_id = subquery.user_id;

-- Volatile functions in a subquery are recursively planned
UPDATE users_test_table
SET    value_2 = 5
WHERE  users_test_table.user_id IN (SELECT user_id * random() FROM events_test_table);

UPDATE users_test_table
SET    value_2 = subquery.random FROM (SELECT user_id, random()
                                       FROM events_test_table) subquery;

UPDATE users_test_table
SET    value_2 = subquery.random FROM (SELECT user_id, random()
                                       FROM events_test_table OFFSET 0) subquery
WHERE  users_test_table.user_id = subquery.user_id;

-- Make following tests consistent
UPDATE users_test_table SET value_2 = 0;

-- Joins with tables not supported
UPDATE users_test_table
SET    value_2 = 5
FROM   events_test_table_local
WHERE  users_test_table.user_id = events_test_table_local.user_id;

UPDATE events_test_table_local
SET    value_2 = 5
FROM   users_test_table
WHERE  events_test_table_local.user_id = users_test_table.user_id;

-- Local tables in a subquery are supported through recursive planning
UPDATE users_test_table
SET    value_2 = 5
WHERE  users_test_table.user_id IN(SELECT user_id FROM events_test_table_local);

-- Shard counts of tables must be equal to pushdown the query
UPDATE users_test_table
SET    value_2 = 5
FROM   events_test_table_2
WHERE  users_test_table.user_id = events_test_table_2.user_id;

-- Should error out due to multiple row return from subquery, but we can not get this information within
-- subquery pushdown planner. This query will be sent to worker with recursive planner.
\set VERBOSITY terse
DELETE FROM users_test_table
WHERE  users_test_table.user_id = (SELECT user_id
                                   FROM   events_test_table);
\set VERBOSITY default

-- Cursors are not supported
BEGIN;
DECLARE test_cursor CURSOR FOR SELECT * FROM users_test_table ORDER BY user_id;
FETCH test_cursor;
UPDATE users_test_table SET value_2 = 5 WHERE CURRENT OF test_cursor;
ROLLBACK;

-- Stable functions are supported
SELECT * FROM test_table_1 ORDER BY 1 DESC, 2 DESC, 3 DESC;
UPDATE test_table_1 SET col_3 = 3 WHERE date_col < now();
SELECT * FROM test_table_1 ORDER BY 1 DESC, 2 DESC, 3 DESC;
DELETE FROM test_table_1 WHERE date_col < current_timestamp;
SELECT * FROM test_table_1 ORDER BY 1 DESC, 2 DESC, 3 DESC;

DROP TABLE test_table_1;

-- Volatile functions are not supported
CREATE TABLE test_table_2(id int, double_col double precision);
INSERT INTO test_table_2 VALUES(1, random());
INSERT INTO test_table_2 VALUES(2, random());
INSERT INTO test_table_2 VALUES(3, random());
SELECT create_distributed_table('test_table_2', 'id');

UPDATE test_table_2 SET double_col = random();

DROP TABLE test_table_2;

-- Run multi shard updates and deletes without transaction on reference tables
SELECT COUNT(*) FROM users_reference_copy_table;
UPDATE users_reference_copy_table SET value_1 = 1;
SELECT SUM(value_1) FROM users_reference_copy_table;

SELECT COUNT(*), SUM(value_2) FROM users_reference_copy_table WHERE user_id = 3 or user_id = 5;
UPDATE users_reference_copy_table SET value_2 = value_2 + 1 WHERE user_id = 3 or user_id = 5;
SELECT COUNT(*), SUM(value_2) FROM users_reference_copy_table WHERE user_id = 3 or user_id = 5;

UPDATE users_reference_copy_table SET value_3 = 0 WHERE user_id <> 3;
SELECT SUM(value_3) FROM users_reference_copy_table WHERE user_id <> 3;

DELETE FROM users_reference_copy_table WHERE user_id = 3 or user_id = 5;
SELECT COUNT(*) FROM users_reference_copy_table WHERE user_id = 3 or user_id = 5;

-- Do some tests by changing shard replication factor
DROP TABLE users_test_table;

SET citus.shard_replication_factor to 2;

CREATE TABLE users_test_table(user_id int, value_1 int, value_2 int, value_3 int);
SELECT create_distributed_table('users_test_table', 'user_id');
\COPY users_test_table FROM STDIN DELIMITER AS ',';
1, 5, 6, 7
2, 12, 7, 18
3, 23, 8, 25
4, 42, 9, 23
5, 35, 10, 21
6, 21, 11, 25
7, 27, 12, 18
8, 18, 13, 4
7, 38, 14, 22
6, 43, 15, 22
5, 61, 16, 17
4, 6, 17, 8
3, 16, 18, 44
2, 25, 19, 38
1, 55, 20, 17
\.

-- Run multi shard updates and deletes without transaction on hash distributed tables
UPDATE users_test_table SET value_1 = 1;
SELECT COUNT(*), SUM(value_1) FROM users_test_table;

SELECT COUNT(*), SUM(value_2) FROM users_test_table WHERE user_id = 1 or user_id = 3;
UPDATE users_test_table SET value_2 = value_2 + 1 WHERE user_id = 1 or user_id = 3;
SELECT COUNT(*), SUM(value_2) FROM users_test_table WHERE user_id = 1 or user_id = 3;

UPDATE users_test_table SET value_3 = 0 WHERE user_id <> 5;
SELECT SUM(value_3) FROM users_test_table WHERE user_id <> 5;

SELECT COUNT(*) FROM users_test_table WHERE user_id = 3 or user_id = 5;
DELETE FROM users_test_table WHERE user_id = 3 or user_id = 5;
SELECT COUNT(*) FROM users_test_table WHERE user_id = 3 or user_id = 5;

DROP TABLE users_test_table;
DROP TABLE events_test_table;
DROP TABLE events_reference_copy_table;
DROP TABLE users_reference_copy_table;
