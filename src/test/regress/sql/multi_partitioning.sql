--
-- Distributed Partitioned Table Tests
--
SET citus.next_shard_id TO 1660000;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

--
-- Distributed Partitioned Table Creation Tests
--

-- 1-) Distributing partitioned table
-- create partitioned table
CREATE TABLE partitioning_test(id int, time date) PARTITION BY RANGE (time);

CREATE TABLE partitioning_hash_test(id int, subid int) PARTITION BY HASH(subid);

-- create its partitions
CREATE TABLE partitioning_test_2009 PARTITION OF partitioning_test FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE partitioning_test_2010 PARTITION OF partitioning_test FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');

CREATE TABLE partitioning_hash_test_0 PARTITION OF partitioning_hash_test FOR VALUES WITH (MODULUS 3, REMAINDER 0);
CREATE TABLE partitioning_hash_test_1 PARTITION OF partitioning_hash_test FOR VALUES WITH (MODULUS 3, REMAINDER 1);

-- load some data and distribute tables
INSERT INTO partitioning_test VALUES (1, '2009-06-06');
INSERT INTO partitioning_test VALUES (2, '2010-07-07');

INSERT INTO partitioning_test_2009 VALUES (3, '2009-09-09');
INSERT INTO partitioning_test_2010 VALUES (4, '2010-03-03');

INSERT INTO partitioning_hash_test VALUES (1, 2);
INSERT INTO partitioning_hash_test VALUES (2, 13);
INSERT INTO partitioning_hash_test VALUES (3, 7);
INSERT INTO partitioning_hash_test VALUES (4, 4);

-- distribute partitioned table
SELECT create_distributed_table('partitioning_test', 'id');

SELECT create_distributed_table('partitioning_hash_test', 'id');

-- see the data is loaded to shards
SELECT * FROM partitioning_test ORDER BY 1;

SELECT * FROM partitioning_hash_test ORDER BY 1;

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

SELECT
	logicalrelid
FROM
	pg_dist_partition
WHERE
	logicalrelid IN ('partitioning_hash_test', 'partitioning_hash_test_0', 'partitioning_hash_test_1')
ORDER BY 1;

SELECT
	logicalrelid, count(*)
FROM pg_dist_shard
	WHERE logicalrelid IN ('partitioning_hash_test', 'partitioning_hash_test_0', 'partitioning_hash_test_1')
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

-- try to insert a new data to hash partitioned table
-- no partition is defined for value 5
INSERT INTO partitioning_hash_test VALUES (8, 5);
INSERT INTO partitioning_hash_test VALUES (9, 12);

CREATE TABLE partitioning_hash_test_2 (id int, subid int);
INSERT INTO partitioning_hash_test_2 VALUES (8, 5);

ALTER TABLE partitioning_hash_test ATTACH PARTITION partitioning_hash_test_2 FOR VALUES WITH (MODULUS 3, REMAINDER 2);

INSERT INTO partitioning_hash_test VALUES (9, 12);

-- see the data is loaded to shards
SELECT * FROM partitioning_test ORDER BY 1;

SELECT * FROM partitioning_hash_test ORDER BY 1;

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

-- create default partition
CREATE TABLE partitioning_test_default PARTITION OF partitioning_test DEFAULT;

\d+ partitioning_test

INSERT INTO partitioning_test VALUES(21, '2014-02-02');
INSERT INTO partitioning_test VALUES(22, '2015-04-02');

-- see they are inserted into default partition
SELECT * FROM partitioning_test WHERE id > 20 ORDER BY 1, 2;
SELECT * FROM partitioning_test_default ORDER BY 1, 2;

-- create a new partition (will fail)
CREATE TABLE partitioning_test_2014 PARTITION OF partitioning_test FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');

BEGIN;
ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_default;
CREATE TABLE partitioning_test_2014 PARTITION OF partitioning_test FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
INSERT INTO partitioning_test SELECT * FROM partitioning_test_default WHERE time >= '2014-01-01' AND time < '2015-01-01';
DELETE FROM partitioning_test_default WHERE time >= '2014-01-01' AND time < '2015-01-01';
ALTER TABLE partitioning_test ATTACH PARTITION partitioning_test_default DEFAULT;
END;

-- see data is in the table, but some moved out from default partition
SELECT * FROM partitioning_test WHERE id > 20 ORDER BY 1, 2;
SELECT * FROM partitioning_test_default ORDER BY 1, 2;

-- multi-shard UPDATE on partitioned table
UPDATE partitioning_test SET time = time + INTERVAL '1 day';

-- see rows are UPDATED
SELECT * FROM partitioning_test ORDER BY 1;

-- multi-shard UPDATE on partition directly
UPDATE partitioning_test_2009 SET time = time + INTERVAL '1 day';

-- see rows are UPDATED
SELECT * FROM partitioning_test_2009 ORDER BY 1;

-- test multi-shard UPDATE which fails in workers (updated value is outside of partition bounds)
UPDATE partitioning_test_2009 SET time = time + INTERVAL '6 month';

--
-- DDL in distributed partitioned tables
--

-- test CREATE INDEX
-- CREATE INDEX on partitioned table - this will error out
-- on earlier versions of postgres earlier than 11.
CREATE INDEX partitioning_index ON partitioning_test(id);

-- CREATE INDEX on partition
CREATE INDEX partitioning_2009_index ON partitioning_test_2009(id);

-- CREATE INDEX CONCURRENTLY on partition
CREATE INDEX CONCURRENTLY partitioned_2010_index ON partitioning_test_2010(id);

-- see index is created
SELECT tablename, indexname FROM pg_indexes WHERE tablename LIKE 'partitioning_test_%' ORDER BY indexname;

-- test drop
-- indexes created on parent table can only be dropped on parent table
-- ie using the same index name
-- following will fail
DROP INDEX partitioning_test_2009_id_idx;

-- but dropping index on parent table will succeed
DROP INDEX partitioning_index;

-- this index was already created on partition table
DROP INDEX partitioning_2009_index;

-- test drop index on non-distributed, partitioned table
CREATE TABLE non_distributed_partitioned_table(a int, b int) PARTITION BY RANGE (a);
CREATE TABLE non_distributed_partitioned_table_1 PARTITION OF non_distributed_partitioned_table
FOR VALUES FROM (0) TO (10);
CREATE INDEX non_distributed_partitioned_table_index ON non_distributed_partitioned_table(a);

-- see index is created
SELECT tablename, indexname FROM pg_indexes WHERE tablename LIKE 'non_distributed_partitioned_table_%' ORDER BY indexname;

-- drop the index and see it is dropped
DROP INDEX non_distributed_partitioned_table_index;
SELECT tablename, indexname FROM pg_indexes WHERE tablename LIKE 'non_distributed%' ORDER BY indexname;

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

-- however, you can add primary key if it contains both distribution and partition key
ALTER TABLE partitioning_hash_test ADD CONSTRAINT partitioning_hash_primary PRIMARY KEY (id, subid);

-- see PRIMARY KEY is created
SELECT
    table_name,
    constraint_name,
    constraint_type
FROM
    information_schema.table_constraints
WHERE
    table_name LIKE 'partitioning_hash_test%' AND
    constraint_type = 'PRIMARY KEY'
ORDER BY 1;

-- test ADD FOREIGN CONSTRAINT
-- add FOREIGN CONSTRAINT to partitioned table -- this will error out (it is a self reference)
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

-- delete from default partition
DELETE FROM partitioning_test WHERE time >= '2015-01-01';
SELECT * FROM partitioning_test_default;


-- create a reference table for foreign key test
CREATE TABLE partitioning_test_reference(id int PRIMARY KEY, subid int);
INSERT INTO partitioning_test_reference SELECT a, a FROM generate_series(1, 50) a;

SELECT create_reference_table('partitioning_test_reference');

ALTER TABLE partitioning_test ADD CONSTRAINT partitioning_reference_fkey FOREIGN KEY (id) REFERENCES partitioning_test_reference(id) ON DELETE CASCADE;

CREATE TABLE partitioning_test_foreign_key(id int PRIMARY KEY, value int);
SELECT create_distributed_table('partitioning_test_foreign_key', 'id');
INSERT INTO partitioning_test_foreign_key SELECT * FROM partitioning_test_reference;

ALTER TABLE partitioning_hash_test ADD CONSTRAINT partitioning_reference_fk_test FOREIGN KEY (id) REFERENCES partitioning_test_foreign_key(id) ON DELETE CASCADE;

-- check foreign keys on partitions
SELECT
	table_name, constraint_name, constraint_type FROm information_schema.table_constraints
WHERE
	table_name LIKE 'partitioning_hash_test%' AND
	constraint_type = 'FOREIGN KEY'
ORDER BY
	1,2;

-- check foreign keys on partition shards
-- there is some text ordering issue regarding table name
-- forcing integer sort by extracting shardid
CREATE TYPE foreign_key_details AS (table_name text, constraint_name text, constraint_type text);
SELECT right(table_name, 7)::int as shardid, * FROM (
	SELECT (json_populate_record(NULL::foreign_key_details,
	json_array_elements_text(result::json)::json )).*
	FROM run_command_on_workers($$
		SELECT
			COALESCE(json_agg(row_to_json(q)), '[]'::json)
		FROM (
			SELECT
				table_name, constraint_name, constraint_type
			FROM information_schema.table_constraints
			WHERE
				table_name LIKE 'partitioning_hash_test%' AND
				constraint_type = 'FOREIGN KEY'
			ORDER BY 1, 2, 3
			) q
		$$) ) w
ORDER BY 1, 2, 3, 4;

DROP TYPE  foreign_key_details;

-- set replication factor back to 1 since it gots reset
-- after connection re-establishment
SET citus.shard_replication_factor TO 1;

SELECT * FROM partitioning_test WHERE id = 11 or id = 12;
DELETE FROM partitioning_test_reference WHERE id = 11 or id = 12;

SELECT * FROM partitioning_hash_test ORDER BY 1, 2;
DELETE FROM partitioning_test_foreign_key WHERE id = 2 OR id = 9;
-- see data is deleted from referencing table
SELECT * FROM partitioning_test WHERE id = 11 or id = 12;
SELECT * FROM partitioning_hash_test ORDER BY 1, 2;


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
DROP TABLE partitioning_test_reference;

-- dropping the parent should CASCADE to the children as well
SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'partitioning_test%' ORDER BY 1;

-- test distributing partitioned table colocated with non-partitioned table
CREATE TABLE partitioned_users_table (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint) PARTITION BY RANGE (time);
CREATE TABLE partitioned_events_table (user_id int, time timestamp, event_type int, value_2 int, value_3 float, value_4 bigint) PARTITION BY RANGE (time);

SELECT create_distributed_table('partitioned_users_table', 'user_id', colocate_with => 'users_table');
SELECT create_distributed_table('partitioned_events_table', 'user_id', colocate_with => 'events_table');

-- INSERT/SELECT from regular table to partitioned table
CREATE TABLE partitioned_users_table_2009 PARTITION OF partitioned_users_table FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');
CREATE TABLE partitioned_events_table_2009 PARTITION OF partitioned_events_table FOR VALUES FROM ('2017-01-01') TO ('2018-01-01');

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
                    event_type IN (1, 2) )
               UNION
                    (SELECT
                        "events"."user_id", "events"."time", 1 AS event
                     FROM
                        partitioned_events_table as "events"
                     WHERE
                        event_type IN (3, 4) )
               UNION
                    (SELECT
                        "events"."user_id", "events"."time", 2 AS event
                     FROM
                        partitioned_events_table as  "events"
                     WHERE
                        event_type IN (5, 6) )
               UNION
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                        partitioned_events_table as "events"
                     WHERE
                        event_type IN (1, 6))) t1
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
                      event_type IN (1, 2)) events_subquery_1)
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
                        partitioned_events_table as  "events"
                     WHERE
                      event_type IN (3, 4)) events_subquery_3)
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
        partitioned_users_table as "users"
      WHERE
        value_1 > 2 and value_1 < 5) AS t
     ON (t.user_id = q.user_id)) as final_query
GROUP BY
  types
ORDER BY
  types;

-- test LIST partitioning
CREATE TABLE list_partitioned_events_table (user_id int, time date, event_type int, value_2 int, value_3 float, value_4 bigint) PARTITION BY LIST (time);
CREATE TABLE list_partitioned_events_table_2014_01_01_05 PARTITION OF list_partitioned_events_table FOR VALUES IN ('2017-11-21', '2017-11-22', '2017-11-23', '2017-11-24', '2017-11-25');
CREATE TABLE list_partitioned_events_table_2014_01_06_10 PARTITION OF list_partitioned_events_table FOR VALUES IN ('2017-11-26', '2017-11-27', '2017-11-28', '2017-11-29', '2017-11-30');
CREATE TABLE list_partitioned_events_table_2014_01_11_15 PARTITION OF list_partitioned_events_table FOR VALUES IN ('2017-12-01', '2017-12-02', '2017-12-03', '2017-12-04', '2017-12-05');

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
    time >= '2017-11-21' AND
    time <= '2017-12-01';

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
              user_id > 2) "temp_data_queries"
           INNER JOIN
           (SELECT
              "users"."user_id"
            FROM
              partitioned_users_table as "users"
            WHERE
              user_id > 2 and value_2 = 1) "user_filters_1"
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
RESET citus.task_executor_type;

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

-- test locks on multi-shard UPDATE
BEGIN;
UPDATE partitioning_locks SET time = '2009-03-01';
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

CREATE VIEW lockinfo AS
    SELECT
        logicalrelid,
        CASE
            WHEN l.objsubid = 5 THEN 'shard'
            WHEN l.objsubid = 4 THEN 'shard_metadata'
            ELSE 'colocated_shards_metadata'
        END AS locktype,
        mode
    FROM
        pg_locks AS l JOIN (select row_number() over (partition by logicalrelid order by shardminvalue) -1 as shardintervalindex, * from pg_dist_shard) AS s
    ON
        (l.objsubid IN (4, 5) AND l.objid = s.shardid )
        OR (l.objsubid = 8
            AND l.objid IN (select colocationid from pg_dist_partition AS p where p.logicalrelid = s.logicalrelid)
            AND l.classid = shardintervalindex
        )
    WHERE
        logicalrelid IN ('partitioning_locks', 'partitioning_locks_2009', 'partitioning_locks_2010')
        AND pid = pg_backend_pid()
        AND l.locktype = 'advisory'
    ORDER BY
        1, 2, 3;

-- test shard resource locks with multi-shard UPDATE
BEGIN;
UPDATE partitioning_locks_2009 SET time = '2009-03-01';

-- see the locks on parent table
SELECT * FROM lockinfo;
COMMIT;

-- test shard resource locks with TRUNCATE
BEGIN;
TRUNCATE partitioning_locks_2009;

-- see the locks on parent table
SELECT * FROM lockinfo;
COMMIT;

-- test shard resource locks with INSERT/SELECT
BEGIN;
INSERT INTO partitioning_locks_2009 SELECT * FROM partitioning_locks WHERE time >= '2009-01-01' AND time < '2010-01-01';

-- see the locks on parent table
SELECT * FROM lockinfo;
COMMIT;

-- test partition-wise join
CREATE TABLE partitioning_hash_join_test(id int, subid int) PARTITION BY HASH(subid);
CREATE TABLE partitioning_hash_join_test_0 PARTITION OF partitioning_hash_join_test FOR VALUES WITH (MODULUS 3, REMAINDER 0);
CREATE TABLE partitioning_hash_join_test_1 PARTITION OF partitioning_hash_join_test FOR VALUES WITH (MODULUS 3, REMAINDER 1);
CREATE TABLE partitioning_hash_join_test_2 PARTITION OF partitioning_hash_join_test FOR VALUES WITH (MODULUS 3, REMAINDER 2);

SELECT create_distributed_table('partitioning_hash_join_test', 'id');

SELECT success FROM run_command_on_workers('alter system set enable_mergejoin to off');
SELECT success FROM run_command_on_workers('alter system set enable_nestloop to off');
SELECT success FROM run_command_on_workers('alter system set enable_indexscan to off');
SELECT success FROM run_command_on_workers('alter system set enable_indexonlyscan to off');
SELECT success FROM run_command_on_workers('alter system set enable_partitionwise_join to off');
SELECT success FROM run_command_on_workers('select pg_reload_conf()');

EXPLAIN (COSTS OFF)
SELECT * FROM partitioning_hash_test JOIN partitioning_hash_join_test USING (id, subid);

-- set partition-wise join on and parallel to off
SELECT success FROM run_command_on_workers('alter system set enable_partitionwise_join to on');
SELECT success FROM run_command_on_workers('select pg_reload_conf()');

SET enable_partitionwise_join TO on;
ANALYZE partitioning_hash_test, partitioning_hash_join_test;

EXPLAIN (COSTS OFF)
SELECT * FROM partitioning_hash_test JOIN partitioning_hash_join_test USING (id, subid);

-- note that partition-wise joins only work when partition key is in the join
-- following join does not have that, therefore join will not be pushed down to
-- partitions
EXPLAIN (COSTS OFF)
SELECT * FROM partitioning_hash_test JOIN partitioning_hash_join_test USING (id);

-- reset partition-wise join
SELECT success FROM run_command_on_workers('alter system reset enable_partitionwise_join');
SELECT success FROM run_command_on_workers('alter system reset enable_mergejoin');
SELECT success FROM run_command_on_workers('alter system reset enable_nestloop');
SELECT success FROM run_command_on_workers('alter system reset enable_indexscan');
SELECT success FROM run_command_on_workers('alter system reset enable_indexonlyscan');
SELECT success FROM run_command_on_workers('select pg_reload_conf()');

RESET enable_partitionwise_join;

DROP VIEW lockinfo;
DROP TABLE
IF EXISTS
    partitioning_test_2009,
    partitioned_events_table,
    partitioned_users_table,
    list_partitioned_events_table,
    multi_column_partitioning,
    partitioning_locks,
    partitioning_locks_for_select;

-- make sure we can create a partitioned table with streaming replication
SET citus.replication_model TO 'streaming';
CREATE TABLE partitioning_test(id int, time date) PARTITION BY RANGE (time);
CREATE TABLE partitioning_test_2009 PARTITION OF partitioning_test FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
SELECT create_distributed_table('partitioning_test', 'id');
DROP TABLE partitioning_test;

-- make sure we can attach partitions to a distributed table in a schema
CREATE SCHEMA partitioning_schema;
CREATE TABLE partitioning_schema."schema-test"(id int, time date) PARTITION BY RANGE (time);
SELECT create_distributed_table('partitioning_schema."schema-test"', 'id');
CREATE TABLE partitioning_schema."schema-test_2009"(id int, time date);
ALTER TABLE partitioning_schema."schema-test" ATTACH PARTITION partitioning_schema."schema-test_2009" FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');

-- attached partition is distributed as well
SELECT
	logicalrelid
FROM
	pg_dist_partition
WHERE
	logicalrelid IN ('partitioning_schema."schema-test"'::regclass, 'partitioning_schema."schema-test_2009"'::regclass)
ORDER BY 1;

SELECT
	logicalrelid, count(*)
FROM
    pg_dist_shard
WHERE
    logicalrelid IN ('partitioning_schema."schema-test"'::regclass, 'partitioning_schema."schema-test_2009"'::regclass)
GROUP BY
	logicalrelid
ORDER BY
	1,2;

DROP TABLE partitioning_schema."schema-test";

-- make sure we can create partition of a distributed table in a schema
CREATE TABLE partitioning_schema."schema-test"(id int, time date) PARTITION BY RANGE (time);
SELECT create_distributed_table('partitioning_schema."schema-test"', 'id');
CREATE TABLE partitioning_schema."schema-test_2009" PARTITION OF partitioning_schema."schema-test" FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');

-- newly created partition is distributed as well
SELECT
	logicalrelid
FROM
	pg_dist_partition
WHERE
	logicalrelid IN ('partitioning_schema."schema-test"'::regclass, 'partitioning_schema."schema-test_2009"'::regclass)
ORDER BY 1;

SELECT
	logicalrelid, count(*)
FROM
    pg_dist_shard
WHERE
    logicalrelid IN ('partitioning_schema."schema-test"'::regclass, 'partitioning_schema."schema-test_2009"'::regclass)
GROUP BY
	logicalrelid
ORDER BY
	1,2;

DROP TABLE partitioning_schema."schema-test";

-- make sure creating partitioned tables works while search_path is set
CREATE TABLE partitioning_schema."schema-test"(id int, time date) PARTITION BY RANGE (time);
SET search_path = partitioning_schema;
SELECT create_distributed_table('"schema-test"', 'id');
CREATE TABLE partitioning_schema."schema-test_2009" PARTITION OF "schema-test" FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');

-- newly created partition is distributed as well
SELECT
	logicalrelid
FROM
	pg_dist_partition
WHERE
	logicalrelid IN ('partitioning_schema."schema-test"'::regclass, 'partitioning_schema."schema-test_2009"'::regclass)
ORDER BY 1;

SELECT
	logicalrelid, count(*)
FROM
    pg_dist_shard
WHERE
    logicalrelid IN ('partitioning_schema."schema-test"'::regclass, 'partitioning_schema."schema-test_2009"'::regclass)
GROUP BY
	logicalrelid
ORDER BY
	1,2;


-- test we don't deadlock when attaching and detaching partitions from partitioned
-- tables with foreign keys
CREATE TABLE reference_table(id int PRIMARY KEY);
SELECT create_reference_table('reference_table');

CREATE TABLE reference_table_2(id int PRIMARY KEY);
SELECT create_reference_table('reference_table_2');

CREATE TABLE partitioning_test(id int, time date) PARTITION BY RANGE (time);
CREATE TABLE partitioning_test_2008 PARTITION OF partitioning_test FOR VALUES FROM ('2008-01-01') TO ('2009-01-01');
CREATE TABLE partitioning_test_2009 (LIKE partitioning_test);
CREATE TABLE partitioning_test_2010 (LIKE partitioning_test);
CREATE TABLE partitioning_test_2011 (LIKE partitioning_test);

-- distributing partitioning_test will also distribute partitioning_test_2008
SELECT create_distributed_table('partitioning_test', 'id');
SELECT create_distributed_table('partitioning_test_2009', 'id');
SELECT create_distributed_table('partitioning_test_2010', 'id');
SELECT create_distributed_table('partitioning_test_2011', 'id');

ALTER TABLE partitioning_test ADD CONSTRAINT partitioning_reference_fkey
    FOREIGN KEY (id) REFERENCES reference_table(id) ON DELETE CASCADE;
ALTER TABLE partitioning_test_2009 ADD CONSTRAINT partitioning_reference_fkey_2009
    FOREIGN KEY (id) REFERENCES reference_table(id) ON DELETE CASCADE;

INSERT INTO partitioning_test_2010 VALUES (1, '2010-02-01');
-- This should fail because of foreign key constraint violation
ALTER TABLE partitioning_test ATTACH PARTITION partitioning_test_2010
      FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');
-- Truncate, so attaching again won't fail
TRUNCATE partitioning_test_2010;

-- Attach a table which already has the same constraint
ALTER TABLE partitioning_test ATTACH PARTITION partitioning_test_2009
      FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
-- Attach a table which doesn't have the constraint
ALTER TABLE partitioning_test ATTACH PARTITION partitioning_test_2010
      FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');
-- Attach a table which has a different constraint
ALTER TABLE partitioning_test ATTACH PARTITION partitioning_test_2011
      FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');

ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_2008;
ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_2009;
ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_2010;
ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_2011;

DROP TABLE partitioning_test, partitioning_test_2008, partitioning_test_2009,
           partitioning_test_2010, partitioning_test_2011,
           reference_table, reference_table_2;

DROP SCHEMA partitioning_schema CASCADE;
RESET SEARCH_PATH;
DROP TABLE IF EXISTS
	partitioning_hash_test,
	partitioning_hash_join_test,
	partitioning_test_failure,
	non_distributed_partitioned_table,
	partitioning_test_foreign_key;
