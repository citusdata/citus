--
-- Distributed Partitioned Table Tests
--
ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1660000;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

--
-- Distributed Partitioned Table Creation Tests
--

-- 1-) Distributing partitioned table
-- create partitioned table
CREATE TABLE partitioning_test(id int, time date) PARTITION BY RANGE (time);
 
-- create its partitions
CREATE TABLE partitioning_test_2009 PARTITION OF partitioning_test FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE partitioning_test_2010 PARTITION OF partitioning_test FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');

-- load some data and distribute tables
INSERT INTO partitioning_test VALUES (1, '2009-06-06');
INSERT INTO partitioning_test VALUES (2, '2010-07-07');

INSERT INTO partitioning_test_2009 VALUES (3, '2009-09-09');
INSERT INTO partitioning_test_2010 VALUES (4, '2010-03-03');

-- distribute partitioned table
SELECT create_distributed_table('partitioning_test', 'id');

-- see the data is loaded to shards
SELECT * FROM partitioning_test ORDER BY 1;

-- see partitioned table and its partitions are distributed
SELECT 
	logicalrelid 
FROM 
	pg_dist_partition 
WHERE 
	logicalrelid IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')
ORDER BY 1;

SELECT 
	logicalrelid, count(*) 
FROM pg_dist_shard 
	WHERE logicalrelid IN ('partitioning_test', 'partitioning_test_2009', 'partitioning_test_2010')
GROUP BY
	logicalrelid
ORDER BY
	1,2;

-- 2-) Creating partition of a distributed table
CREATE TABLE partitioning_test_2011 PARTITION OF partitioning_test FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');

-- new partition is automatically distributed as well
SELECT 
	logicalrelid 
FROM 
	pg_dist_partition 
WHERE 
	logicalrelid IN ('partitioning_test', 'partitioning_test_2011')
ORDER BY 1;

SELECT 
	logicalrelid, count(*) 
FROM pg_dist_shard 
	WHERE logicalrelid IN ('partitioning_test', 'partitioning_test_2011')
GROUP BY
	logicalrelid
ORDER BY
	1,2;

-- 3-) Attaching non distributed table to a distributed table
CREATE TABLE partitioning_test_2012(id int, time date);

-- load some data
INSERT INTO partitioning_test_2012 VALUES (5, '2012-06-06');
INSERT INTO partitioning_test_2012 VALUES (6, '2012-07-07');

ALTER TABLE partitioning_test ATTACH PARTITION partitioning_test_2012 FOR VALUES FROM ('2012-01-01') TO ('2013-01-01');

-- attached partition is distributed as well
SELECT 
	logicalrelid 
FROM 
	pg_dist_partition 
WHERE 
	logicalrelid IN ('partitioning_test', 'partitioning_test_2012')
ORDER BY 1;

SELECT 
	logicalrelid, count(*) 
FROM pg_dist_shard 
	WHERE logicalrelid IN ('partitioning_test', 'partitioning_test_2012')
GROUP BY
	logicalrelid
ORDER BY
	1,2;

-- see the data is loaded to shards
SELECT * FROM partitioning_test ORDER BY 1;

-- 4-) Attaching distributed table to distributed table
CREATE TABLE partitioning_test_2013(id int, time date);
SELECT create_distributed_table('partitioning_test_2013', 'id');

-- load some data
INSERT INTO partitioning_test_2013 VALUES (7, '2013-06-06');
INSERT INTO partitioning_test_2013 VALUES (8, '2013-07-07');

ALTER TABLE partitioning_test ATTACH PARTITION partitioning_test_2013 FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');

-- see the data is loaded to shards
SELECT * FROM partitioning_test ORDER BY 1;

-- 5-) Failure cases while creating distributed partitioned tables
-- cannot distribute a partition if its parent is not distributed
CREATE TABLE partitioning_test_failure(id int, time date) PARTITION BY RANGE (time);
CREATE TABLE partitioning_test_failure_2009 PARTITION OF partitioning_test_failure FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
SELECT create_distributed_table('partitioning_test_failure_2009', 'id');

-- only hash distributed tables can have partitions
SELECT create_distributed_table('partitioning_test_failure', 'id', 'append');
SELECT create_distributed_table('partitioning_test_failure', 'id', 'range');
SELECT create_reference_table('partitioning_test_failure');

-- replication factor > 1 is not allowed in distributed partitioned tables
SET citus.shard_replication_factor TO 2;
SELECT create_distributed_table('partitioning_test_failure', 'id');
SET citus.shard_replication_factor TO 1;

-- non-distributed tables cannot have distributed partitions;
DROP TABLE partitioning_test_failure_2009;
CREATE TABLE partitioning_test_failure_2009(id int, time date);
SELECT create_distributed_table('partitioning_test_failure_2009', 'id');
ALTER TABLE partitioning_test_failure ATTACH PARTITION partitioning_test_failure_2009 FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');

-- multi-level partitioning is not allowed
DROP TABLE partitioning_test_failure_2009;
CREATE TABLE partitioning_test_failure_2009 PARTITION OF partitioning_test_failure FOR VALUES FROM ('2009-01-01') TO ('2010-01-01') PARTITION BY RANGE (time);
SELECT create_distributed_table('partitioning_test_failure', 'id');

-- multi-level partitioning is not allowed in different order
DROP TABLE partitioning_test_failure_2009;
SELECT create_distributed_table('partitioning_test_failure', 'id');
CREATE TABLE partitioning_test_failure_2009 PARTITION OF partitioning_test_failure FOR VALUES FROM ('2009-01-01') TO ('2010-01-01') PARTITION BY RANGE (time);


--
-- DMLs in distributed partitioned tables
--

-- test COPY
-- COPY data to partitioned table
COPY partitioning_test FROM STDIN WITH CSV;
9,2009-01-01
10,2010-01-01
11,2011-01-01
12,2012-01-01
\.


-- COPY data to partition directly
COPY partitioning_test_2009 FROM STDIN WITH CSV;
13,2009-01-02
14,2009-01-03
\.

-- see the data is loaded to shards
SELECT * FROM partitioning_test WHERE id >= 9 ORDER BY 1;

-- test INSERT
-- INSERT INTO the partitioned table
INSERT INTO partitioning_test VALUES(15, '2009-02-01');
INSERT INTO partitioning_test VALUES(16, '2010-02-01');
INSERT INTO partitioning_test VALUES(17, '2011-02-01');
INSERT INTO partitioning_test VALUES(18, '2012-02-01');

-- INSERT INTO the partitions directly table
INSERT INTO partitioning_test VALUES(19, '2009-02-02');
INSERT INTO partitioning_test VALUES(20, '2010-02-02');

-- see the data is loaded to shards
SELECT * FROM partitioning_test WHERE id >= 15 ORDER BY 1;

-- test INSERT/SELECT
-- INSERT/SELECT from partition to partitioned table
INSERT INTO partitioning_test SELECT * FROM partitioning_test_2011;

-- INSERT/SELECT from partitioned table to partition
INSERT INTO partitioning_test_2012 SELECT * FROM partitioning_test WHERE time >= '2012-01-01' AND time < '2013-01-01';

-- see the data is loaded to shards (rows in the given range should be duplicated)
SELECT * FROM partitioning_test WHERE time >= '2011-01-01' AND time < '2013-01-01' ORDER BY 1;

-- test UPDATE
-- UPDATE partitioned table
UPDATE partitioning_test SET time = '2013-07-07' WHERE id = 7;

-- UPDATE partition directly
UPDATE partitioning_test_2013 SET time = '2013-08-08' WHERE id = 8;

-- see the data is updated
SELECT * FROM partitioning_test WHERE id = 7 OR id = 8 ORDER BY 1;

-- UPDATE that tries to move a row to a non-existing partition (this should fail)
UPDATE partitioning_test SET time = '2020-07-07' WHERE id = 7;

-- UPDATE with subqueries on partitioned table
UPDATE
    partitioning_test
SET
    time = time + INTERVAL '1 day'
WHERE
    id IN (SELECT id FROM partitioning_test WHERE id = 1);

-- UPDATE with subqueries on partition
UPDATE
    partitioning_test_2009
SET
    time = time + INTERVAL '1 month'
WHERE
    id IN (SELECT id FROM partitioning_test WHERE id = 2);

-- see the data is updated
SELECT * FROM partitioning_test WHERE id = 1 OR id = 2 ORDER BY 1;

-- test DELETE
-- DELETE from partitioned table
DELETE FROM partitioning_test WHERE id = 9;

-- DELETE from partition directly
DELETE FROM partitioning_test_2010 WHERE id = 10;

-- see the data is deleted
SELECT * FROM partitioning_test WHERE id = 9 OR id = 10 ORDER BY 1;

-- test master_modify_multiple_shards
-- master_modify_multiple_shards on partitioned table
SELECT master_modify_multiple_shards('UPDATE partitioning_test SET time = time + INTERVAL ''1 day''');

-- see rows are UPDATED
SELECT * FROM partitioning_test ORDER BY 1;

-- master_modify_multiple_shards on partition directly
SELECT master_modify_multiple_shards('UPDATE partitioning_test_2009 SET time = time + INTERVAL ''1 day''');

-- see rows are UPDATED
SELECT * FROM partitioning_test_2009 ORDER BY 1;

-- test master_modify_multiple_shards which fails in workers (updated value is outside of partition bounds)
SELECT master_modify_multiple_shards('UPDATE partitioning_test_2009 SET time = time + INTERVAL ''6 month''');

--
-- DDL in distributed partitioned tables
--

-- test CREATE INDEX
-- CREATE INDEX on partitioned table - this will error out
CREATE INDEX partitioning_index ON partitioning_test(id);

-- CREATE INDEX on partition
CREATE INDEX partitioning_2009_index ON partitioning_test_2009(id);

-- CREATE INDEX CONCURRENTLY on partition
CREATE INDEX CONCURRENTLY partitioned_2010_index ON partitioning_test_2010(id);

-- see index is created
SELECT tablename, indexname FROM pg_indexes WHERE tablename LIKE 'partitioning_test%' ORDER BY indexname;

-- test add COLUMN
-- add COLUMN to partitioned table
ALTER TABLE partitioning_test ADD new_column int;

-- add COLUMN to partition - this will error out
ALTER TABLE partitioning_test_2010 ADD new_column_2 int;

-- see additional column is created
SELECT name, type FROM table_attrs WHERE relid = 'partitioning_test'::regclass ORDER BY 1;
SELECT name, type FROM table_attrs WHERE relid = 'partitioning_test_2010'::regclass ORDER BY 1;

-- test add PRIMARY KEY
-- add PRIMARY KEY to partitioned table - this will error out
ALTER TABLE partitioning_test ADD CONSTRAINT partitioning_primary PRIMARY KEY (id);

-- ADD PRIMARY KEY to partition
ALTER TABLE partitioning_test_2009 ADD CONSTRAINT partitioning_2009_primary PRIMARY KEY (id);

-- see PRIMARY KEY is created
SELECT
    table_name,
    constraint_name,
    constraint_type
FROM
    information_schema.table_constraints
WHERE
    table_name = 'partitioning_test_2009' AND 
    constraint_name = 'partitioning_2009_primary';

-- test ADD FOREIGN CONSTRAINT
-- add FOREIGN CONSTRAINT to partitioned table -- this will error out
ALTER TABLE partitioning_test ADD CONSTRAINT partitioning_foreign FOREIGN KEY (id) REFERENCES partitioning_test_2009 (id);

-- add FOREIGN CONSTRAINT to partition
INSERT INTO partitioning_test_2009 VALUES (5, '2009-06-06');
INSERT INTO partitioning_test_2009 VALUES (6, '2009-07-07');
INSERT INTO partitioning_test_2009 VALUES(12, '2009-02-01');
INSERT INTO partitioning_test_2009 VALUES(18, '2009-02-01');
ALTER TABLE partitioning_test_2012 ADD CONSTRAINT partitioning_2012_foreign FOREIGN KEY (id) REFERENCES partitioning_test_2009 (id) ON DELETE CASCADE;

-- see FOREIGN KEY is created
SELECT "Constraint" FROM table_fkeys WHERE relid = 'partitioning_test_2012'::regclass ORDER BY 1;

-- test ON DELETE CASCADE works
DELETE FROM partitioning_test_2009 WHERE id = 5;

-- see that element is deleted from both partitions
SELECT * FROM partitioning_test_2009 WHERE id = 5 ORDER BY 1;
SELECT * FROM partitioning_test_2012 WHERE id = 5 ORDER BY 1;

-- test DETACH partition
ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_2009;

-- see DETACHed partitions content is not accessible from partitioning_test;
SELECT * FROM partitioning_test WHERE time >= '2009-01-01' AND time < '2010-01-01' ORDER BY 1;

--
-- Transaction tests
--

-- DDL in transaction
BEGIN;
ALTER TABLE partitioning_test ADD newer_column int;

-- see additional column is created
SELECT name, type FROM table_attrs WHERE relid = 'partitioning_test'::regclass ORDER BY 1;

ROLLBACK;

-- see rollback is successful
SELECT name, type FROM table_attrs WHERE relid = 'partitioning_test'::regclass ORDER BY 1;

-- COPY in transaction
BEGIN;
COPY partitioning_test FROM STDIN WITH CSV;
22,2010-01-01,22
23,2011-01-01,23
24,2013-01-01,24
\.

-- see the data is loaded to shards
SELECT * FROM partitioning_test WHERE id = 22 ORDER BY 1;
SELECT * FROM partitioning_test WHERE id = 23 ORDER BY 1;
SELECT * FROM partitioning_test WHERE id = 24 ORDER BY 1;

ROLLBACK;

-- see rollback is successful
SELECT * FROM partitioning_test WHERE id >= 22 ORDER BY 1;

-- DML in transaction
BEGIN;

-- INSERT in transaction
INSERT INTO partitioning_test VALUES(25, '2010-02-02');

-- see the data is loaded to shards
SELECT * FROM partitioning_test WHERE id = 25 ORDER BY 1;

-- INSERT/SELECT in transaction
INSERT INTO partitioning_test SELECT * FROM partitioning_test WHERE id = 25;

-- see the data is loaded to shards
SELECT * FROM partitioning_test WHERE id = 25 ORDER BY 1;

-- UPDATE in transaction
UPDATE partitioning_test SET time = '2010-10-10' WHERE id = 25;

-- see the data is updated
SELECT * FROM partitioning_test WHERE id = 25 ORDER BY 1;

-- perform operations on partition and partioned tables together
INSERT INTO partitioning_test VALUES(26, '2010-02-02', 26);
INSERT INTO partitioning_test_2010 VALUES(26, '2010-02-02', 26);
COPY partitioning_test FROM STDIN WITH CSV;
26,2010-02-02,26
\.
COPY partitioning_test_2010 FROM STDIN WITH CSV;
26,2010-02-02,26
\.

-- see the data is loaded to shards (we should see 4 rows with same content)
SELECT * FROM partitioning_test WHERE id = 26 ORDER BY 1;

ROLLBACK;

-- see rollback is successful
SELECT * FROM partitioning_test WHERE id = 26 ORDER BY 1;

-- DETACH and DROP in a transaction
BEGIN;
ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_2011;
DROP TABLE partitioning_test_2011;
COMMIT;

-- see DROPed partitions content is not accessible
SELECT * FROM partitioning_test WHERE time >= '2011-01-01' AND time < '2012-01-01' ORDER BY 1;

--
-- Misc tests
--

-- test TRUNCATE
-- test TRUNCATE partition
TRUNCATE partitioning_test_2012;

-- see partition is TRUNCATEd
SELECT * FROM partitioning_test_2012 ORDER BY 1;

-- test TRUNCATE partitioned table
TRUNCATE partitioning_test;

-- see partitioned table is TRUNCATEd
SELECT * FROM partitioning_test ORDER BY 1;

-- test DROP
-- test DROP partition
INSERT INTO partitioning_test_2010 VALUES(27, '2010-02-01');
DROP TABLE partitioning_test_2010;

-- see DROPped partitions content is not accessible from partitioning_test;
SELECT * FROM partitioning_test WHERE time >= '2010-01-01' AND time < '2011-01-01' ORDER BY 1;

-- test DROP partitioned table
DROP TABLE partitioning_test;

-- dropping the parent should CASCADE to the children as well
SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'partitioning_test%' ORDER BY 1;

-- test distributing partitioned table colocated with non-partitioned table
CREATE TABLE partitioned_users_table (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint) PARTITION BY RANGE (time);
CREATE TABLE partitioned_events_table (user_id int, time timestamp, event_type int, value_2 int, value_3 float, value_4 bigint) PARTITION BY RANGE (time);

SELECT create_distributed_table('partitioned_users_table', 'user_id', colocate_with => 'users_table');
SELECT create_distributed_table('partitioned_events_table', 'user_id', colocate_with => 'events_table');

-- INSERT/SELECT from regular table to partitioned table
CREATE TABLE partitioned_users_table_2009 PARTITION OF partitioned_users_table FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
CREATE TABLE partitioned_events_table_2009 PARTITION OF partitioned_events_table FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');

INSERT INTO partitioned_events_table SELECT * FROM events_table;
INSERT INTO partitioned_users_table_2009 SELECT * FROM users_table;

--
-- Complex JOINs, subqueries, UNIONs etc...
--

-- subquery with UNIONs on partitioned table
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
                    partitioned_events_table as  "events"
                   WHERE 
                    event_type IN (10, 11, 12, 13, 14, 15))
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 1 AS event
                     FROM 
                        partitioned_events_table as "events"
                     WHERE 
                        event_type IN (15, 16, 17, 18, 19) )
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 2 AS event
                     FROM 
                        partitioned_events_table as  "events"
                     WHERE 
                        event_type IN (20, 21, 22, 23, 24, 25) )
               UNION 
                    (SELECT 
                        "events"."user_id", "events"."time", 3 AS event
                     FROM 
                        partitioned_events_table as "events"
                     WHERE 
                        event_type IN (26, 27, 28, 29, 30, 13))) t1
         GROUP BY "t1"."user_id") AS t) "q" 
) AS final_query
GROUP BY types
ORDER BY types;

-- UNION and JOIN on both partitioned and regular tables
SELECT ("final_query"."event_types") as types, count(*) AS sumOfEventType
FROM
  (SELECT 
      *, random()
    FROM
     (SELECT 
        "t"."user_id", "t"."time", unnest("t"."collected_events") AS "event_types"
      FROM
        (SELECT 
            "t1"."user_id", min("t1"."time") AS "time", array_agg(("t1"."event") ORDER BY TIME ASC, event DESC) AS collected_events
          FROM (
                (SELECT 
                    *
                 FROM
                   (SELECT 
                          "events"."time", 0 AS event, "events"."user_id"
                    FROM 
                      partitioned_events_table as "events"
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
                    (SELECT 
                        "events"."time", 2 AS event, "events"."user_id"
                     FROM 
                        partitioned_events_table as  "events"
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
        partitioned_users_table as "users"
      WHERE 
        value_1 > 50 and value_1 < 70) AS t 
     ON (t.user_id = q.user_id)) as final_query
GROUP BY 
  types
ORDER BY 
  types;

-- test LIST partitioning
CREATE TABLE list_partitioned_events_table (user_id int, time date, event_type int, value_2 int, value_3 float, value_4 bigint) PARTITION BY LIST (time);
CREATE TABLE list_partitioned_events_table_2014_01_01_05 PARTITION OF list_partitioned_events_table FOR VALUES IN ('2014-01-01', '2014-01-02', '2014-01-03', '2014-01-04', '2014-01-05');
CREATE TABLE list_partitioned_events_table_2014_01_06_10 PARTITION OF list_partitioned_events_table FOR VALUES IN ('2014-01-06', '2014-01-07', '2014-01-08', '2014-01-09', '2014-01-10');
CREATE TABLE list_partitioned_events_table_2014_01_11_15 PARTITION OF list_partitioned_events_table FOR VALUES IN ('2014-01-11', '2014-01-12', '2014-01-13', '2014-01-14', '2014-01-15');

-- test distributing partitioned table colocated with another partitioned table
SELECT create_distributed_table('list_partitioned_events_table', 'user_id', colocate_with => 'partitioned_events_table');

-- INSERT/SELECT from partitioned table to partitioned table
INSERT INTO
    list_partitioned_events_table
SELECT
    user_id,
    date_trunc('day', time) as time,
    event_type,
    value_2,
    value_3,
    value_4
FROM
    events_table
WHERE
    time >= '2014-01-01' AND
    time <= '2014-01-15';

-- LEFT JOINs used with INNER JOINs on range partitioned table, list partitioned table and non-partitioned table
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
              "list_partitioned_events_table"."time", "list_partitioned_events_table"."user_id" as event_user_id
            FROM 
             list_partitioned_events_table as "list_partitioned_events_table"
            WHERE 
              user_id > 80) "temp_data_queries"
           INNER JOIN
           (SELECT 
              "users"."user_id"
            FROM 
              partitioned_users_table as "users"
            WHERE 
              user_id > 80 and value_2 = 5) "user_filters_1" 
           ON ("temp_data_queries".event_user_id = "user_filters_1".user_id)) AS "multi_group_wrapper_1"
        LEFT JOIN
        (SELECT 
            "users"."user_id" AS "user_id", value_2 AS "generated_group_field"
         FROM 
          partitioned_users_table as "users") "left_group_by_1"
        ON ("left_group_by_1".user_id = "multi_group_wrapper_1".event_user_id)) "eventQuery") "pushedDownQuery" 
  GROUP BY
    "generated_group_field"
  ORDER BY 
    cnt DESC, generated_group_field ASC
  LIMIT 10;

--
-- Additional partitioning features
--

-- test multi column partitioning
CREATE TABLE multi_column_partitioning(c1 int, c2 int) PARTITION BY RANGE (c1, c2);
CREATE TABLE multi_column_partitioning_0_0_10_0 PARTITION OF multi_column_partitioning FOR VALUES FROM (0, 0) TO (10, 0);
SELECT create_distributed_table('multi_column_partitioning', 'c1');

-- test INSERT to multi-column partitioned table
INSERT INTO multi_column_partitioning VALUES(1, 1);
INSERT INTO multi_column_partitioning_0_0_10_0 VALUES(5, -5);

-- test INSERT to multi-column partitioned table where no suitable partition exists
INSERT INTO multi_column_partitioning VALUES(10, 1);

-- test with MINVALUE/MAXVALUE
CREATE TABLE multi_column_partitioning_10_max_20_min PARTITION OF multi_column_partitioning FOR VALUES FROM (10, MAXVALUE) TO (20, MINVALUE);

-- test INSERT to partition with MINVALUE/MAXVALUE bounds
INSERT INTO multi_column_partitioning VALUES(11, -11);
INSERT INTO multi_column_partitioning_10_max_20_min VALUES(19, -19);

-- test INSERT to multi-column partitioned table where no suitable partition exists
INSERT INTO multi_column_partitioning VALUES(20, -20);

-- see data is loaded to multi-column partitioned table
SELECT * FROM multi_column_partitioning ORDER BY 1, 2;

--
-- Tests for locks on partitioned tables
--
CREATE TABLE partitioning_locks(id int, ref_id int, time date) PARTITION BY RANGE (time);

-- create its partitions
CREATE TABLE partitioning_locks_2009 PARTITION OF partitioning_locks FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE partitioning_locks_2010 PARTITION OF partitioning_locks FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');

-- distribute partitioned table
SELECT create_distributed_table('partitioning_locks', 'id');

-- test locks on router SELECT
BEGIN;
SELECT * FROM partitioning_locks WHERE id = 1 ORDER BY 1, 2;
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;

-- test locks on real-time SELECT
BEGIN;
SELECT * FROM partitioning_locks ORDER BY 1, 2;
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;

-- test locks on task-tracker SELECT
SET citus.task_executor_type TO 'task-tracker';
BEGIN;
SELECT * FROM partitioning_locks AS pl1 JOIN partitioning_locks AS pl2 ON pl1.id = pl2.ref_id ORDER BY 1, 2;
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;
SET citus.task_executor_type TO 'real-time';

-- test locks on INSERT
BEGIN;
INSERT INTO partitioning_locks VALUES(1, 1, '2009-01-01');
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;

-- test locks on UPDATE
BEGIN;
UPDATE partitioning_locks SET time = '2009-02-01' WHERE id = 1;
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;

-- test locks on DELETE
BEGIN;
DELETE FROM partitioning_locks WHERE id = 1;
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;

-- test locks on INSERT/SELECT
CREATE TABLE partitioning_locks_for_select(id int, ref_id int, time date);
SELECT create_distributed_table('partitioning_locks_for_select', 'id');
BEGIN;
INSERT INTO partitioning_locks SELECT * FROM partitioning_locks_for_select;
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;

-- test locks on coordinator INSERT/SELECT
BEGIN;
INSERT INTO partitioning_locks SELECT * FROM partitioning_locks_for_select LIMIT 5;
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;

-- test locks on master_modify_multiple_shards
BEGIN;
SELECT master_modify_multiple_shards('UPDATE partitioning_locks SET time = ''2009-03-01''');
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;

-- test locks on DDL
BEGIN;
ALTER TABLE partitioning_locks ADD COLUMN new_column int;
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;

-- test locks on TRUNCATE
BEGIN;
TRUNCATE partitioning_locks;
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;

-- test shard resource locks with master_modify_multiple_shards
BEGIN;
SELECT master_modify_multiple_shards('UPDATE partitioning_locks_2009 SET time = ''2009-03-01''');

-- see the locks on parent table
SELECT
    logicalrelid,
    locktype,
    mode
FROM
    pg_locks AS l JOIN pg_dist_shard AS s
ON
    l.objid = s.shardid
WHERE
    logicalrelid IN ('partitioning_locks', 'partitioning_locks_2009', 'partitioning_locks_2010') AND
    pid = pg_backend_pid()
ORDER BY
    1, 2, 3;
COMMIT;

-- test shard resource locks with TRUNCATE
BEGIN;
TRUNCATE partitioning_locks_2009;

-- see the locks on parent table
SELECT
    logicalrelid,
    locktype,
    mode
FROM
    pg_locks AS l JOIN pg_dist_shard AS s
ON
    l.objid = s.shardid
WHERE
    logicalrelid IN ('partitioning_locks', 'partitioning_locks_2009', 'partitioning_locks_2010') AND
    pid = pg_backend_pid()
ORDER BY
    1, 2, 3;
COMMIT;

-- test shard resource locks with INSERT/SELECT
BEGIN;
INSERT INTO partitioning_locks_2009 SELECT * FROM partitioning_locks WHERE time >= '2009-01-01' AND time < '2010-01-01';

-- see the locks on parent table
SELECT
    logicalrelid,
    locktype,
    mode
FROM
    pg_locks AS l JOIN pg_dist_shard AS s
ON
    l.objid = s.shardid
WHERE
    logicalrelid IN ('partitioning_locks', 'partitioning_locks_2009', 'partitioning_locks_2010') AND
    pid = pg_backend_pid()
ORDER BY
    1, 2, 3;
COMMIT;

DROP TABLE
IF EXISTS
    partitioning_test_2012,
    partitioning_test_2013,
    partitioned_events_table,
    partitioned_users_table,
    list_partitioned_events_table,
    multi_column_partitioning,
    partitioning_locks,
    partitioning_locks_for_select;
