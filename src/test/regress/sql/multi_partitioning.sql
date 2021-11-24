--
-- Distributed Partitioned Table Tests
--
SET citus.next_shard_id TO 1660000;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.enable_repartition_joins to ON;

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

-- should not return results when only querying parent
SELECT * FROM ONLY partitioning_test ORDER BY 1;
SELECT * FROM ONLY partitioning_test WHERE id = 1;
SELECT * FROM ONLY partitioning_test a JOIN partitioning_hash_test b ON (a.id = b.subid) ORDER BY 1;

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
-- (1) UPDATE partitioned table
UPDATE partitioning_test SET time = '2013-07-07' WHERE id = 7;

-- (2) UPDATE partition directly
UPDATE partitioning_test_2013 SET time = '2013-08-08' WHERE id = 8;

-- (3) UPDATE only the parent (noop)
UPDATE ONLY partitioning_test SET time = '2013-09-09' WHERE id = 7;

-- (4) DELETE from only the parent (noop)
DELETE FROM ONLY partitioning_test WHERE id = 7;

-- see that only (1) and (2) had an effect
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
				table_name SIMILAR TO 'partitioning_hash_test%\d{2,}' AND
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

-- perform operations on partition and partitioned tables together
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

-- test distributed partitions are indeed colocated with the parent table
CREATE TABLE sensors(measureid integer, eventdatetime date, measure_data jsonb, PRIMARY KEY (measureid, eventdatetime, measure_data))
PARTITION BY RANGE(eventdatetime);

CREATE TABLE sensors_old PARTITION OF sensors FOR VALUES FROM ('2000-01-01') TO ('2020-01-01');
CREATE TABLE sensors_2020_01_01 PARTITION OF sensors FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
CREATE TABLE sensors_new PARTITION OF sensors DEFAULT;

SELECT create_distributed_table('sensors', 'measureid', colocate_with:='none');

SELECT count(DISTINCT colocationid) FROM pg_dist_partition
WHERE logicalrelid IN ('sensors'::regclass, 'sensors_old'::regclass, 'sensors_2020_01_01'::regclass, 'sensors_new'::regclass);

CREATE TABLE local_sensors(measureid integer, eventdatetime date, measure_data jsonb, PRIMARY KEY (measureid, eventdatetime, measure_data))
PARTITION BY RANGE(eventdatetime);

CREATE TABLE local_sensors_old PARTITION OF local_sensors FOR VALUES FROM ('2000-01-01') TO ('2020-01-01');
CREATE TABLE local_sensors_2020_01_01 PARTITION OF local_sensors FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
CREATE TABLE local_sensors_new PARTITION OF local_sensors DEFAULT;

SELECT create_distributed_table('local_sensors', 'measureid', colocate_with:='sensors');

SELECT count(DISTINCT colocationid) FROM pg_dist_partition
WHERE logicalrelid IN ('sensors'::regclass, 'sensors_old'::regclass, 'sensors_2020_01_01'::regclass, 'sensors_new'::regclass,
'local_sensors'::regclass, 'local_sensors_old'::regclass, 'local_sensors_2020_01_01'::regclass, 'local_sensors_new'::regclass);

DROP TABLE sensors;
DROP TABLE local_sensors;

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

-- subquery with UNIONs on partitioned table, but only scan (empty) parent for some
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
                        ONLY partitioned_events_table as  "events"
                     WHERE
                        event_type IN (5, 6) )
               UNION
                    (SELECT
                        "events"."user_id", "events"."time", 3 AS event
                     FROM
                        ONLY partitioned_events_table as "events"
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

BEGIN;
SELECT * FROM partitioning_locks AS pl1 JOIN partitioning_locks AS pl2 ON pl1.id = pl2.ref_id ORDER BY 1, 2;
SELECT relation::regclass, locktype, mode FROM pg_locks WHERE relation::regclass::text LIKE 'partitioning_locks%' AND pid = pg_backend_pid() ORDER BY 1, 2, 3;
COMMIT;

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
SET citus.shard_replication_factor TO 1;
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

SET citus.next_shard_id TO 1660300;

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

SELECT parent_table, partition_column, partition, from_value, to_value FROM time_partitions;

-- create the same partition to verify it behaves like in plain PG
CREATE TABLE partitioning_test_2011 PARTITION OF partitioning_test FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');
CREATE TABLE IF NOT EXISTS partitioning_test_2011 PARTITION OF partitioning_test FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');

-- verify we can create a partition that doesn't already exist with IF NOT EXISTS
CREATE TABLE IF NOT EXISTS partitioning_test_2013 PARTITION OF partitioning_test FOR VALUES FROM ('2013-01-01') TO ('2014-01-01');
SELECT logicalrelid FROM pg_dist_partition WHERE logicalrelid IN ('partitioning_test', 'partitioning_test_2013') ORDER BY 1;

-- create the same table but that is not a partition and verify it behaves like in plain PG
CREATE TABLE not_partition(time date);
CREATE TABLE not_partition PARTITION OF partitioning_test FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');
CREATE TABLE IF NOT EXISTS not_partition PARTITION OF partitioning_test FOR VALUES FROM ('2011-01-01') TO ('2012-01-01');
DROP TABLE not_partition;

-- verify it skips when the partition with the same name belongs to another table
CREATE TABLE another_table(id int, time date) PARTITION BY RANGE (time);
CREATE TABLE partition_of_other_table PARTITION OF another_table FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
CREATE TABLE partition_of_other_table PARTITION OF partitioning_test FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
CREATE TABLE IF NOT EXISTS partition_of_other_table PARTITION OF partitioning_test FOR VALUES FROM ('2014-01-01') TO ('2015-01-01');
ALTER TABLE another_table DETACH PARTITION partition_of_other_table;
DROP TABLE another_table, partition_of_other_table;

-- test fix_pre_citus10_partitioned_table_constraint_names udf
SELECT fix_pre_citus10_partitioned_table_constraint_names('partitioning_test');
SELECT fix_pre_citus10_partitioned_table_constraint_names();

-- the following should fail
SELECT fix_pre_citus10_partitioned_table_constraint_names('public.non_distributed_partitioned_table');
SELECT fix_pre_citus10_partitioned_table_constraint_names('reference_table');

ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_2008;
ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_2009;
ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_2010;
ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_2011;
ALTER TABLE partitioning_test DETACH PARTITION partitioning_test_2013;

DROP TABLE partitioning_test_2008, partitioning_test_2009, partitioning_test_2010,
           partitioning_test_2011, partitioning_test_2013, reference_table_2;
-- verify this doesn't crash and gives a debug message for dropped table
SET client_min_messages TO DEBUG1;
DROP TABLE partitioning_test, reference_table;
RESET client_min_messages;

RESET SEARCH_PATH;

-- not timestamp partitioned
CREATE TABLE not_time_partitioned (x int, y int) PARTITION BY RANGE (x);
CREATE TABLE not_time_partitioned_p0 PARTITION OF not_time_partitioned DEFAULT;
CREATE TABLE not_time_partitioned_p1 PARTITION OF not_time_partitioned FOR VALUES FROM (1) TO (2);
SELECT parent_table, partition_column, partition, from_value, to_value FROM time_partitions;
SELECT * FROM time_partition_range('not_time_partitioned_p1');
DROP TABLE not_time_partitioned;

-- multi-column partitioned
CREATE TABLE multi_column_partitioned (x date, y date) PARTITION BY RANGE (x, y);
CREATE TABLE multi_column_partitioned_p1 PARTITION OF multi_column_partitioned  FOR VALUES FROM ('2020-01-01', '2020-01-01') TO ('2020-12-31','2020-12-31');
SELECT parent_table, partition_column, partition, from_value, to_value FROM time_partitions;
SELECT * FROM time_partition_range('multi_column_partitioned_p1');
DROP TABLE multi_column_partitioned;

-- not-range-partitioned
CREATE TABLE list_partitioned (x date, y date) PARTITION BY LIST (x);
CREATE TABLE list_partitioned_p1 PARTITION OF list_partitioned FOR VALUES IN ('2020-01-01');
SELECT parent_table, partition_column, partition, from_value, to_value FROM time_partitions;
SELECT * FROM time_partition_range('list_partitioned_p1');
DROP TABLE list_partitioned;

-- error out when inheriting a distributed table
CREATE TABLE test_inheritance(a int, b int);
SELECT create_distributed_table('test_inheritance','a');
CREATE TABLE local_inheritance (k int) INHERITS (test_inheritance);
DROP TABLE test_inheritance;

-- test worker partitioned table size functions
CREATE TABLE "events.Energy Added" (user_id int, time timestamp with time zone, data jsonb, PRIMARY KEY (user_id, time )) PARTITION BY RANGE ("time");
SELECT create_distributed_table('"events.Energy Added"', 'user_id', colocate_with:='none');
CREATE TABLE "Energy Added_17634"  PARTITION OF "events.Energy Added" FOR VALUES  FROM ('2018-04-13 00:00:00+00') TO ('2018-04-14 00:00:00+00');

-- test shard cost by disk size function
SET client_min_messages TO DEBUG1;
SELECT citus_shard_cost_by_disk_size(shardid) FROM pg_dist_shard WHERE logicalrelid = '"events.Energy Added"'::regclass ORDER BY shardid LIMIT 1;
RESET client_min_messages;
CREATE INDEX idx_btree_hobbies ON "events.Energy Added" USING BTREE ((data->>'location'));
 \c - - - :worker_1_port
-- should not be zero because of TOAST, vm, fms
SELECT worker_partitioned_table_size(oid) FROM pg_class WHERE relname LIKE '%events.Energy Added%' ORDER BY relname LIMIT 1;
-- should be zero since no data
SELECT worker_partitioned_relation_size(oid) FROM pg_class WHERE relname LIKE '%events.Energy Added%' ORDER BY relname LIMIT 1;
-- should not be zero because of indexes + pg_table_size()
SELECT worker_partitioned_relation_total_size(oid) FROM pg_class WHERE relname LIKE '%events.Energy Added%' ORDER BY relname LIMIT 1;
 \c - - - :master_port
DROP TABLE "events.Energy Added";

-- test we skip the foreign key validation query on coordinator
-- that happens when attaching a non-distributed partition to a distributed-partitioned table
-- with a foreign key to another distributed table
SET search_path = partitioning_schema;
SET citus.shard_replication_factor = 1;
CREATE TABLE another_distributed_table (x int primary key, y int);
SELECT create_distributed_table('another_distributed_table','x');
CREATE TABLE distributed_parent_table (
  event_id serial NOT NULL REFERENCES another_distributed_table (x),
  event_time timestamptz NOT NULL DEFAULT now())
  PARTITION BY RANGE (event_time);
SELECT create_distributed_table('distributed_parent_table', 'event_id');
CREATE TABLE non_distributed_child_1 (event_id int NOT NULL, event_time timestamptz NOT NULL DEFAULT now());
ALTER TABLE distributed_parent_table ATTACH PARTITION non_distributed_child_1 FOR VALUES FROM ('2021-06-30') TO ('2021-07-01');
-- check DEFAULT partition behaves as expected
CREATE TABLE non_distributed_child_2 (event_id int NOT NULL, event_time timestamptz NOT NULL DEFAULT now());
ALTER TABLE distributed_parent_table ATTACH PARTITION non_distributed_child_2 DEFAULT;
-- check adding another partition when default partition exists
CREATE TABLE non_distributed_child_3 (event_id int NOT NULL, event_time timestamptz NOT NULL DEFAULT now());
ALTER TABLE distributed_parent_table ATTACH PARTITION non_distributed_child_3 FOR VALUES FROM ('2021-07-30') TO ('2021-08-01');

-- Test time partition utility UDFs
-- a) test get_missing_time_partition_ranges
-- 1) test get_missing_time_partition_ranges with date partitioned table
CREATE TABLE date_partitioned_table(
 measureid integer,
 eventdate date,
 measure_data jsonb) PARTITION BY RANGE(eventdate);

SELECT create_distributed_table('date_partitioned_table','measureid');

-- test interval must be multiple days for date partitioned table
SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '6 hours', '2022-01-01', '2021-01-01');
SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '1 week 1 day 1 hour', '2022-01-01', '2021-01-01');

-- test with various intervals
SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '1 day', '2021-02-01', '2021-01-01');
SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '1 week', '2022-01-01', '2021-01-01');
SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '1 month', '2022-01-01', '2021-01-01');
SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '3 months', '2022-01-01', '2021-01-01');
SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '6 months', '2022-01-01', '2021-01-01');

-- test with from_date > to_date
SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '1 day', '2021-01-01', '2021-02-01');

-- test with existing partitions
BEGIN;
  CREATE TABLE date_partitioned_table_2021_01_01 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2021-01-02');
  SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '1 day', '2021-01-05', '2020-12-30');
  SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '2 days', '2021-01-05', '2020-12-30');
ROLLBACK;

BEGIN;
  CREATE TABLE date_partitioned_table_2021_01_01 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2021-01-02');
  CREATE TABLE date_partitioned_table_2021_01_02 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-02') TO ('2021-01-03');
  SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '1 day', '2021-01-05', '2020-12-30');
  SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '2 days', '2021-01-05', '2020-12-30');
ROLLBACK;

BEGIN;
  CREATE TABLE date_partitioned_table_2021_01_01 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2021-01-03');
  CREATE TABLE date_partitioned_table_2021_01_02 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-05') TO ('2021-01-07');
  SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '2 days', '2021-01-15', '2020-12-30');
ROLLBACK;

BEGIN;
  CREATE TABLE date_partitioned_table_2021_01_01 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2021-01-02');
  CREATE TABLE date_partitioned_table_2021_01_02 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-02') TO ('2021-01-04');
  SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '2 days', '2021-01-05', '2020-12-30');
ROLLBACK;

BEGIN;
  CREATE TABLE date_partitioned_table_2021_01_01 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2021-01-03');
  CREATE TABLE date_partitioned_table_2021_01_02 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-04') TO ('2021-01-06');
  SELECT * FROM get_missing_time_partition_ranges('date_partitioned_table', INTERVAL '2 days', '2021-01-15', '2020-12-30');
ROLLBACK;

DROP TABLE date_partitioned_table;

-- 2) test timestamp with time zone partitioend table
CREATE TABLE tstz_partitioned_table(
 measureid integer,
 eventdatetime timestamp with time zone,
 measure_data jsonb) PARTITION BY RANGE(eventdatetime);

SELECT create_distributed_table('tstz_partitioned_table','measureid');

-- test with various intervals
SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '30 minutes', '2021-01-01 12:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '1 hour', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '6 hours', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '12 hours', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '1 week', '2021-01-15 00:00:00', '2021-01-01 00:00:00');

-- test with from_date > to_date
SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '1 day', '2021-01-01 00:00:00', '2021-01-05 00:00:00');

-- test with existing partitions
BEGIN;
  CREATE TABLE tstz_partitioned_table_2021_01_01 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tstz_partitioned_table_2021_01_01 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  CREATE TABLE tstz_partitioned_table_2021_01_02 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-02 00:00:00') TO ('2021-01-03 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tstz_partitioned_table_2021_01_01 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-03 00:00:00');
  CREATE TABLE tstz_partitioned_table_2021_01_02 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-05 00:00:00') TO ('2021-01-07 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '2 days', '2021-01-15 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tstz_partitioned_table_2021_01_01 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  CREATE TABLE tstz_partitioned_table_2021_01_02 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-02 00:00:00') TO ('2021-01-04 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tstz_partitioned_table_2021_01_01 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-03 00:00:00');
  CREATE TABLE tstz_partitioned_table_2021_01_02 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-04 00:00:00') TO ('2021-01-06 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '2 days', '2021-01-15 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

DROP TABLE tstz_partitioned_table;

-- 3) test timestamp without time zone partitioend table
CREATE TABLE tswtz_partitioned_table(
 measureid integer,
 eventdatetime timestamp without time zone,
 measure_data jsonb) PARTITION BY RANGE(eventdatetime);

SELECT create_distributed_table('tswtz_partitioned_table','measureid');

-- test with various intervals
SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '30 minutes', '2021-01-01 12:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '1 hour', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '6 hours', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '12 hours', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '1 week', '2021-01-15 00:00:00', '2021-01-01 00:00:00');

-- test with from_date > to_date
SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '1 day', '2021-01-01 00:00:00', '2021-01-05 00:00:00');

-- test with existing partitions
BEGIN;
  CREATE TABLE tswtz_partitioned_table_2021_01_01 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tswtz_partitioned_table_2021_01_01 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  CREATE TABLE tswtz_partitioned_table_2021_01_02 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-02 00:00:00') TO ('2021-01-03 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tswtz_partitioned_table_2021_01_01 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-03 00:00:00');
  CREATE TABLE tswtz_partitioned_table_2021_01_02 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-05 00:00:00') TO ('2021-01-07 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '2 days', '2021-01-15 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tswtz_partitioned_table_2021_01_01 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  CREATE TABLE tswtz_partitioned_table_2021_01_02 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-02 00:00:00') TO ('2021-01-04 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tswtz_partitioned_table_2021_01_01 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-03 00:00:00');
  CREATE TABLE tswtz_partitioned_table_2021_01_02 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-04 00:00:00') TO ('2021-01-06 00:00:00');
  SELECT * FROM get_missing_time_partition_ranges('tswtz_partitioned_table', INTERVAL '2 days', '2021-01-15 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

DROP TABLE tswtz_partitioned_table;

-- 4) test with weird name
CREATE TABLE "test !/ \n _dist_123_table"(
 measureid integer,
 eventdatetime timestamp without time zone,
 measure_data jsonb) PARTITION BY RANGE(eventdatetime);

SELECT create_distributed_table('"test !/ \n _dist_123_table"','measureid');

-- test with various intervals
SELECT * FROM get_missing_time_partition_ranges('"test !/ \n _dist_123_table"', INTERVAL '30 minutes', '2021-01-01 12:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('"test !/ \n _dist_123_table"', INTERVAL '1 hour', '2021-01-01 12:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('"test !/ \n _dist_123_table"', INTERVAL '6 hours', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
SELECT * FROM get_missing_time_partition_ranges('"test !/ \n _dist_123_table"', INTERVAL '1 day', '2021-01-03 00:00:00', '2021-01-01 00:00:00');

DROP TABLE "test !/ \n _dist_123_table";

-- b) test create_time_partitions
-- 1) test create_time_partitions with date partitioned table
CREATE TABLE date_partitioned_table(
 measureid integer,
 eventdate date,
 measure_data jsonb) PARTITION BY RANGE(eventdate);

-- test interval must be multiple days for date partitioned table
SELECT create_time_partitions('date_partitioned_table', INTERVAL '6 hours', '2022-01-01', '2021-01-01');
SELECT create_time_partitions('date_partitioned_table', INTERVAL '1 week 1 day 1 hour', '2022-01-01', '2021-01-01');

-- test with various intervals
BEGIN;
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '1 day', '2021-02-01', '2021-01-01');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '1 week', '2022-01-01', '2021-01-01');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '1 month', '2022-01-01', '2021-01-01');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '3 months', '2022-01-01', '2021-01-01');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '6 months', '2022-01-01', '2021-01-01');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

-- test with from_date > to_date
SELECT * FROM create_time_partitions('date_partitioned_table', INTERVAL '1 day', '2021-01-01', '2021-02-01');

-- test with existing partitions
BEGIN;
  CREATE TABLE date_partitioned_table_2021_01_01 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2021-01-02');
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '1 day', '2021-01-05', '2020-12-30');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_table'::regclass ORDER BY 3;
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '2 days', '2021-01-15', '2020-12-25');
ROLLBACK;

BEGIN;
  CREATE TABLE date_partitioned_table_2021_01_01 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2021-01-02');
  CREATE TABLE date_partitioned_table_2021_01_02 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-02') TO ('2021-01-03');
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '1 day', '2021-01-05', '2020-12-30');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_table'::regclass ORDER BY 3;
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '2 days', '2021-01-05', '2020-12-30');
ROLLBACK;

BEGIN;
  CREATE TABLE date_partitioned_table_2021_01_01 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2021-01-03');
  CREATE TABLE date_partitioned_table_2021_01_02 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-05') TO ('2021-01-07');
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '2 days', '2021-01-15', '2020-12-30');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  CREATE TABLE date_partitioned_table_2021_01_01 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2021-01-02');
  CREATE TABLE date_partitioned_table_2020_01_02 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-02') TO ('2021-01-04');
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '1 day', '2021-01-05', '2020-12-30');
ROLLBACK;

BEGIN;
  CREATE TABLE date_partitioned_table_2021_01_01 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-01') TO ('2021-01-03');
  CREATE TABLE date_partitioned_table_2021_01_02 PARTITION OF date_partitioned_table FOR VALUES FROM ('2021-01-04') TO ('2021-01-06');
  SELECT create_time_partitions('date_partitioned_table', INTERVAL '2 days', '2021-01-15', '2020-12-30');
ROLLBACK;

DROP TABLE date_partitioned_table;

-- 2) test timestamp with time zone partitioend table
CREATE TABLE tstz_partitioned_table(
 measureid integer,
 eventdatetime timestamp with time zone,
 measure_data jsonb) PARTITION BY RANGE(eventdatetime);

-- test with various intervals
BEGIN;
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '30 minutes', '2021-01-01 12:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tstz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '1 hour', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tstz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '6 hours', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tstz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '12 hours', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tstz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tstz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '1 week', '2021-01-15 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tstz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

-- test with from_date > to_date
SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '1 day', '2021-01-01 00:00:00', '2021-01-05 00:00:00');

-- test with existing partitions
BEGIN;
  CREATE TABLE tstz_partitioned_table_2021_01_01 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tstz_partitioned_table'::regclass ORDER BY 3;
  SELECT * FROM get_missing_time_partition_ranges('tstz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tstz_partitioned_table_2021_01_01 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  CREATE TABLE tstz_partitioned_table_2021_01_02 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-02 00:00:00') TO ('2021-01-03 00:00:00');
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tstz_partitioned_table'::regclass ORDER BY 3;
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tstz_partitioned_table_2021_01_01 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-03 00:00:00');
  CREATE TABLE tstz_partitioned_table_2021_01_02 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-05 00:00:00') TO ('2021-01-07 00:00:00');
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '2 days', '2021-01-15 00:00:00', '2020-12-30 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tstz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  CREATE TABLE tstz_partitioned_table_2021_01_01 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  CREATE TABLE tstz_partitioned_table_2021_01_02 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-02 00:00:00') TO ('2021-01-04 00:00:00');
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tstz_partitioned_table_2021_01_01 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-03 00:00:00');
  CREATE TABLE tstz_partitioned_table_2021_01_02 PARTITION OF tstz_partitioned_table FOR VALUES FROM ('2021-01-04 00:00:00') TO ('2021-01-06 00:00:00');
  SELECT create_time_partitions('tstz_partitioned_table', INTERVAL '2 days', '2021-01-15 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

DROP TABLE tstz_partitioned_table;

-- 3) test timestamp without time zone partitioend table
CREATE TABLE tswtz_partitioned_table(
 measureid integer,
 eventdatetime timestamp without time zone,
 measure_data jsonb) PARTITION BY RANGE(eventdatetime);

SELECT create_distributed_table('tswtz_partitioned_table','measureid');

BEGIN;
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '30 minutes', '2021-01-01 12:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tswtz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '1 hour', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tswtz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '6 hours', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tswtz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '12 hours', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tswtz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tswtz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '1 week', '2021-01-15 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tswtz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

-- test with from_date > to_date
SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '1 day', '2021-01-01 00:00:00', '2021-01-05 00:00:00');

-- test with existing partitions
BEGIN;
  CREATE TABLE tswtz_partitioned_table_2021_01_01 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tswtz_partitioned_table'::regclass ORDER BY 3;
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tswtz_partitioned_table_2021_01_01 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  CREATE TABLE tswtz_partitioned_table_2021_01_02 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-02 00:00:00') TO ('2021-01-03 00:00:00');
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '1 day', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tswtz_partitioned_table'::regclass ORDER BY 3;
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tswtz_partitioned_table_2020_01_01 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-03 00:00:00');
  CREATE TABLE tswtz_partitioned_table_2020_01_02 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-05 00:00:00') TO ('2021-01-07 00:00:00');
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '2 days', '2021-01-15 00:00:00', '2020-12-30 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = 'tswtz_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  CREATE TABLE tswtz_partitioned_table_2020_01_01 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-02 00:00:00');
  CREATE TABLE tswtz_partitioned_table_2020_01_02 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-02 00:00:00') TO ('2021-01-04 00:00:00');
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '2 days', '2021-01-05 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

BEGIN;
  CREATE TABLE tswtz_partitioned_table_2020_01_01 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-01 00:00:00') TO ('2021-01-03 00:00:00');
  CREATE TABLE tswtz_partitioned_table_2020_01_02 PARTITION OF tswtz_partitioned_table FOR VALUES FROM ('2021-01-04 00:00:00') TO ('2021-01-06 00:00:00');
  SELECT create_time_partitions('tswtz_partitioned_table', INTERVAL '2 days', '2021-01-15 00:00:00', '2020-12-30 00:00:00');
ROLLBACK;

DROP TABLE tswtz_partitioned_table;

-- 4) test with weird name
CREATE TABLE "test !/ \n _dist_123_table"(
 measureid integer,
 eventdatetime timestamp without time zone,
 measure_data jsonb) PARTITION BY RANGE(eventdatetime);

SELECT create_distributed_table('"test !/ \n _dist_123_table"','measureid');

-- test with various intervals
BEGIN;
  SELECT create_time_partitions('"test !/ \n _dist_123_table"', INTERVAL '30 minutes', '2021-01-01 12:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = '"test !/ \n _dist_123_table"'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('"test !/ \n _dist_123_table"', INTERVAL '6 hours', '2021-01-02 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = '"test !/ \n _dist_123_table"'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('"test !/ \n _dist_123_table"', INTERVAL '1 day', '2021-01-03 00:00:00', '2021-01-01 00:00:00');
  SELECT * FROM time_partitions WHERE parent_table = '"test !/ \n _dist_123_table"'::regclass ORDER BY 3;
ROLLBACK;

DROP TABLE "test !/ \n _dist_123_table";

-- 5) test with distributed table
CREATE TABLE date_distributed_partitioned_table(
 measureid integer,
 eventdate date,
 measure_data jsonb) PARTITION BY RANGE(eventdate);

SELECT create_distributed_table('date_distributed_partitioned_table', 'measureid');

-- test interval must be multiple days for date partitioned table
SELECT create_time_partitions('date_distributed_partitioned_table', INTERVAL '6 hours', '2022-01-01', '2021-01-01');
SELECT create_time_partitions('date_distributed_partitioned_table', INTERVAL '1 week 1 day 1 hour', '2022-01-01', '2021-01-01');

-- test with various intervals
BEGIN;
  SELECT create_time_partitions('date_distributed_partitioned_table', INTERVAL '1 day', '2021-02-01', '2021-01-01');
  SELECT * FROM time_partitions WHERE parent_table = 'date_distributed_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('date_distributed_partitioned_table', INTERVAL '1 week', '2022-01-01', '2021-01-01');
  SELECT * FROM time_partitions WHERE parent_table = 'date_distributed_partitioned_table'::regclass ORDER BY 3;
ROLLBACK;

DROP TABLE date_distributed_partitioned_table;

-- pi test with parameter names
CREATE TABLE pi_table(
  event_id bigserial,
  event_time timestamptz default now(),
  payload text) PARTITION BY RANGE (event_time);

BEGIN;
  SELECT create_time_partitions('pi_table', start_from := '2021-08-01', end_at := '2021-10-01', partition_interval := pi() * interval '1 day');
  SELECT * FROM time_partitions WHERE parent_table = 'pi_table'::regclass ORDER BY 3;
ROLLBACK;

DROP TABLE pi_table;

-- 6) test with citus local table
select 1 from citus_add_node('localhost', :master_port, groupid=>0);
CREATE TABLE date_partitioned_citus_local_table(
 measureid integer,
 eventdate date,
 measure_data jsonb) PARTITION BY RANGE(eventdate);

SELECT citus_add_local_table_to_metadata('date_partitioned_citus_local_table');

-- test interval must be multiple days for date partitioned table
SELECT create_time_partitions('date_partitioned_citus_local_table', INTERVAL '6 hours', '2022-01-01', '2021-01-01');
SELECT create_time_partitions('date_partitioned_citus_local_table', INTERVAL '1 week 1 day 1 hour', '2022-01-01', '2021-01-01');

-- test with various intervals
BEGIN;
  SELECT create_time_partitions('date_partitioned_citus_local_table', INTERVAL '1 day', '2021-02-01', '2021-01-01');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_citus_local_table'::regclass ORDER BY 3;
ROLLBACK;

BEGIN;
  SELECT create_time_partitions('date_partitioned_citus_local_table', INTERVAL '1 week', '2022-01-01', '2021-01-01');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_citus_local_table'::regclass ORDER BY 3;
ROLLBACK;

set client_min_messages to error;
DROP TABLE date_partitioned_citus_local_table;
-- also test with foreign key
CREATE TABLE date_partitioned_citus_local_table(
 measureid integer,
 eventdate date,
 measure_data jsonb, PRIMARY KEY (measureid, eventdate)) PARTITION BY RANGE(eventdate);

SELECT citus_add_local_table_to_metadata('date_partitioned_citus_local_table');

-- test interval must be multiple days for date partitioned table
SELECT create_time_partitions('date_partitioned_citus_local_table', INTERVAL '1 day', '2021-02-01', '2021-01-01');

CREATE TABLE date_partitioned_citus_local_table_2(
 measureid integer,
 eventdate date,
 measure_data jsonb, PRIMARY KEY (measureid, eventdate)) PARTITION BY RANGE(eventdate);

SELECT citus_add_local_table_to_metadata('date_partitioned_citus_local_table_2');
ALTER TABLE date_partitioned_citus_local_table_2 ADD CONSTRAINT fkey_1 FOREIGN KEY (measureid, eventdate) REFERENCES date_partitioned_citus_local_table(measureid, eventdate);
SELECT create_time_partitions('date_partitioned_citus_local_table_2', INTERVAL '1 day', '2021-02-01', '2021-01-01');
-- after the above work, these should also work for creating new partitions
BEGIN;
  SELECT create_time_partitions('date_partitioned_citus_local_table', INTERVAL '1 day', '2021-03-01', '2021-02-01');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_citus_local_table'::regclass ORDER BY 3;
ROLLBACK;
BEGIN;
  SELECT create_time_partitions('date_partitioned_citus_local_table_2', INTERVAL '1 day', '2021-03-01', '2021-02-01');
  SELECT * FROM time_partitions WHERE parent_table = 'date_partitioned_citus_local_table'::regclass ORDER BY 3;
ROLLBACK;
set client_min_messages to notice;
-- c) test drop_old_time_partitions
-- 1) test with date partitioned table
CREATE TABLE date_partitioned_table_to_exp (event_date date, event int) partition by range (event_date);
SELECT create_distributed_table('date_partitioned_table_to_exp', 'event');

CREATE TABLE date_partitioned_table_to_exp_d00 PARTITION OF date_partitioned_table_to_exp FOR VALUES FROM ('2000-01-01') TO ('2009-12-31');
CREATE TABLE date_partitioned_table_to_exp_d10 PARTITION OF date_partitioned_table_to_exp FOR VALUES FROM ('2010-01-01') TO ('2019-12-31');
CREATE TABLE date_partitioned_table_to_exp_d20 PARTITION OF date_partitioned_table_to_exp FOR VALUES FROM ('2020-01-01') TO ('2029-12-31');
INSERT INTO date_partitioned_table_to_exp VALUES ('2005-01-01', 1);
INSERT INTO date_partitioned_table_to_exp VALUES ('2015-01-01', 2);
INSERT INTO date_partitioned_table_to_exp VALUES ('2025-01-01', 3);

\set VERBOSITY terse

-- expire no partitions
CALL drop_old_time_partitions('date_partitioned_table_to_exp', '1999-01-01');
SELECT partition FROM time_partitions WHERE parent_table = 'date_partitioned_table_to_exp'::regclass ORDER BY partition::text;

-- expire 2 old partitions
CALL drop_old_time_partitions('date_partitioned_table_to_exp', '2021-01-01');
SELECT partition FROM time_partitions WHERE parent_table = 'date_partitioned_table_to_exp'::regclass ORDER BY partition::text;

\set VERBOSITY default
DROP TABLE date_partitioned_table_to_exp;

-- 2) test with timestamptz partitioned table
CREATE TABLE tstz_partitioned_table_to_exp (event_time timestamptz, event int) partition by range (event_time);
SELECT create_distributed_table('tstz_partitioned_table_to_exp', 'event');

CREATE TABLE tstz_partitioned_table_to_exp_d0 PARTITION OF tstz_partitioned_table_to_exp FOR VALUES FROM ('2021-01-01 02:00:00+00') TO ('2021-01-01 06:00:00+00');
CREATE TABLE tstz_partitioned_table_to_exp_d1 PARTITION OF tstz_partitioned_table_to_exp FOR VALUES FROM ('2021-01-01 06:00:00+00') TO ('2021-01-01 10:00:00+00');
CREATE TABLE tstz_partitioned_table_to_exp_d2 PARTITION OF tstz_partitioned_table_to_exp FOR VALUES FROM ('2021-01-01 10:00:00+00') TO ('2021-01-01 14:00:00+00');
INSERT INTO tstz_partitioned_table_to_exp VALUES ('2021-01-01 03:00:00+00', 1);
INSERT INTO tstz_partitioned_table_to_exp VALUES ('2021-01-01 09:00:00+00', 2);
INSERT INTO tstz_partitioned_table_to_exp VALUES ('2021-01-01 13:00:00+00', 3);

\set VERBOSITY terse

-- expire no partitions
CALL drop_old_time_partitions('tstz_partitioned_table_to_exp', '2021-01-01 01:00:00+00');
SELECT partition FROM time_partitions WHERE parent_table = 'tstz_partitioned_table_to_exp'::regclass ORDER BY partition::text;

-- expire 2 old partitions
CALL drop_old_time_partitions('tstz_partitioned_table_to_exp', '2021-01-01 12:00:00+00');
SELECT partition FROM time_partitions WHERE parent_table = 'tstz_partitioned_table_to_exp'::regclass ORDER BY partition::text;

\set VERBOSITY default
DROP TABLE tstz_partitioned_table_to_exp;

-- 3) test with weird table name
CREATE TABLE "test !/ \n _dist_123_table_exp" (event_time timestamptz, event int) partition by range (event_time);
SELECT create_distributed_table('"test !/ \n _dist_123_table_exp"', 'event');

CREATE TABLE tstz_partitioned_table_to_exp_d0 PARTITION OF "test !/ \n _dist_123_table_exp" FOR VALUES FROM ('2021-01-01 02:00:00+00') TO ('2021-01-01 06:00:00+00');
CREATE TABLE tstz_partitioned_table_to_exp_d1 PARTITION OF "test !/ \n _dist_123_table_exp" FOR VALUES FROM ('2021-01-01 06:00:00+00') TO ('2021-01-01 10:00:00+00');
CREATE TABLE tstz_partitioned_table_to_exp_d2 PARTITION OF "test !/ \n _dist_123_table_exp" FOR VALUES FROM ('2021-01-01 10:00:00+00') TO ('2021-01-01 14:00:00+00');
INSERT INTO "test !/ \n _dist_123_table_exp" VALUES ('2021-01-01 03:00:00+00', 1);
INSERT INTO "test !/ \n _dist_123_table_exp" VALUES ('2021-01-01 09:00:00+00', 2);
INSERT INTO "test !/ \n _dist_123_table_exp" VALUES ('2021-01-01 13:00:00+00', 3);

\set VERBOSITY terse

-- expire no partitions
CALL drop_old_time_partitions('"test !/ \n _dist_123_table_exp"', '2021-01-01 01:00:00+00');
SELECT partition FROM time_partitions WHERE parent_table = '"test !/ \n _dist_123_table_exp"'::regclass ORDER BY partition::text;

-- expire 2 old partitions
CALL drop_old_time_partitions('"test !/ \n _dist_123_table_exp"', '2021-01-01 12:00:00+00');
SELECT partition FROM time_partitions WHERE parent_table = '"test !/ \n _dist_123_table_exp"'::regclass ORDER BY partition::text;

\set VERBOSITY default
DROP TABLE "test !/ \n _dist_123_table_exp";

-- 4) test with citus local tables
CREATE TABLE date_partitioned_table_to_exp (event_date date, event int) partition by range (event_date);
SELECT citus_add_local_table_to_metadata('date_partitioned_table_to_exp');

CREATE TABLE date_partitioned_table_to_exp_d00 PARTITION OF date_partitioned_table_to_exp FOR VALUES FROM ('2000-01-01') TO ('2009-12-31');
CREATE TABLE date_partitioned_table_to_exp_d10 PARTITION OF date_partitioned_table_to_exp FOR VALUES FROM ('2010-01-01') TO ('2019-12-31');
CREATE TABLE date_partitioned_table_to_exp_d20 PARTITION OF date_partitioned_table_to_exp FOR VALUES FROM ('2020-01-01') TO ('2029-12-31');

\set VERBOSITY terse

-- expire no partitions
CALL drop_old_time_partitions('date_partitioned_table_to_exp', '1999-01-01');
SELECT partition FROM time_partitions WHERE parent_table = 'date_partitioned_table_to_exp'::regclass ORDER BY partition::text;

-- expire 2 old partitions
CALL drop_old_time_partitions('date_partitioned_table_to_exp', '2021-01-01');
SELECT partition FROM time_partitions WHERE parent_table = 'date_partitioned_table_to_exp'::regclass ORDER BY partition::text;

\set VERBOSITY default
set client_min_messages to error;
DROP TABLE date_partitioned_table_to_exp;
DROP TABLE date_partitioned_citus_local_table CASCADE;
DROP TABLE date_partitioned_citus_local_table_2;
set client_min_messages to notice;

SELECT citus_remove_node('localhost', :master_port);

-- d) invalid tables for helper UDFs
CREATE TABLE multiple_partition_column_table(
  event_id bigserial,
  event_time timestamptz,
  payload text) PARTITION BY RANGE (event_time, event_id);

SELECT create_time_partitions('multiple_partition_column_table', INTERVAL '1 month', now() + INTERVAL '1 year');
CALL drop_old_time_partitions('multiple_partition_column_table', now());
DROP TABLE multiple_partition_column_table;

CREATE TABLE invalid_partition_column_table(
  event_id bigserial,
  event_time bigint,
  payload text) PARTITION BY RANGE (event_time);

SELECT create_time_partitions('invalid_partition_column_table', INTERVAL '1 month', now() + INTERVAL '1 year');
CALL drop_old_time_partitions('invalid_partition_column_table', now());
DROP TABLE invalid_partition_column_table;

CREATE TABLE non_partitioned_table(
  event_id bigserial,
  event_time timestamptz,
  payload text);

SELECT create_time_partitions('non_partitioned_table', INTERVAL '1 month', now() + INTERVAL '1 year');
CALL drop_old_time_partitions('non_partitioned_table', now());
DROP TABLE non_partitioned_table;

-- https://github.com/citusdata/citus/issues/4962
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 361168;
CREATE TABLE part_table_with_very_long_name (
    dist_col integer,
    long_named_integer_col integer,
    long_named_part_col timestamp
) PARTITION BY RANGE (long_named_part_col);

CREATE TABLE part_table_with_long_long_long_long_name
PARTITION OF part_table_with_very_long_name
FOR VALUES FROM ('2010-01-01') TO ('2015-01-01');

SELECT create_distributed_table('part_table_with_very_long_name', 'dist_col');

CREATE INDEX ON part_table_with_very_long_name
USING btree (long_named_integer_col, long_named_part_col);

-- index is created
SELECT tablename, indexname FROM pg_indexes
WHERE schemaname = 'partitioning_schema' AND tablename ilike '%part_table_with_%' ORDER BY 1, 2;

-- should work properly - no names clashes
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

\c - - - :worker_1_port
-- check that indexes are named properly
SELECT tablename, indexname FROM pg_indexes
WHERE schemaname = 'partitioning_schema' AND tablename ilike '%part_table_with_%' ORDER BY 1, 2;

\c - - - :master_port
DROP SCHEMA partitioning_schema CASCADE;
RESET search_path;
DROP TABLE IF EXISTS
	partitioning_hash_test,
	partitioning_test_failure,
	non_distributed_partitioned_table,
	partitioning_test_foreign_key;
