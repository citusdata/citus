-- this test file aims to test UPSERT feature on Citus


SET citus.next_shard_id TO 980000;

CREATE SCHEMA upsert_test;
SET search_path TO upsert_test;

CREATE TABLE upsert_test
(
	part_key int UNIQUE,
	other_col int,
	third_col int
);

-- distribute the table and create shards
SELECT create_distributed_table('upsert_test', 'part_key', 'hash');

-- do a regular insert
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1), (2, 2);

-- observe that there is a conflict and the following query does nothing
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT DO NOTHING;

-- same as the above with different syntax
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key) DO NOTHING;

--again the same query with another syntax
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT ON CONSTRAINT upsert_test_part_key_key DO NOTHING;

-- now, update the columns
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1)
	ON CONFLICT (part_key) DO UPDATE SET other_col = 2,  third_col = 4;

-- see the results
SELECT * FROM upsert_test ORDER BY part_key ASC;

-- do a multi-row DO NOTHING insert
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1), (2, 2)
ON CONFLICT DO NOTHING;

-- do a multi-row DO UPDATE insert
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 10), (2, 20), (3, 30)
ON CONFLICT (part_key) DO
UPDATE SET other_col = EXCLUDED.other_col WHERE upsert_test.part_key != 1;

-- see the results
SELECT * FROM upsert_test ORDER BY part_key ASC;

DELETE FROM upsert_test WHERE part_key = 2;
DELETE FROM upsert_test WHERE part_key = 3;

-- use a WHERE clause, so that SET doesn't have an affect
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key)
	DO UPDATE SET other_col = 30 WHERE upsert_test.other_col = 3;

-- see the results
SELECT * FROM upsert_test;

-- use a WHERE clause, that hits the row and updates it
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key)
	DO UPDATE SET other_col = 30 WHERE upsert_test.other_col = 2;

-- see the results
SELECT * FROM upsert_test;

-- use two elements in the WHERE, that doesn't hit the row and updates it
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key)
	DO UPDATE SET other_col = 30 WHERE upsert_test.other_col = 2 AND upsert_test.other_col = 3;

-- use EXCLUDED keyword
INSERT INTO upsert_test (part_key, other_col, third_col) VALUES (1, 1, 100) ON CONFLICT (part_key)
	DO UPDATE SET other_col = EXCLUDED.third_col;

-- see the results
SELECT * FROM upsert_test;

-- now update multiple columns with ALIAS table and reference to the row itself
INSERT INTO upsert_test as ups_test (part_key) VALUES (1)
	ON CONFLICT (part_key) DO UPDATE SET other_col = ups_test.other_col + 50, third_col = 200;

-- see the results
SELECT * FROM upsert_test;

-- now, do some more complex assignments
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key)
		DO UPDATE SET other_col = upsert_test.other_col + 1,
		third_col = upsert_test.third_col + (EXCLUDED.part_key + EXCLUDED.other_col) + 670;

-- see the results
SELECT * FROM upsert_test;

-- now, WHERE clause also has table reference
INSERT INTO upsert_test as ups_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key)
		DO UPDATE SET other_col = (ups_test.other_col + ups_test.third_col + (EXCLUDED.part_key + EXCLUDED.other_col)) % 15
		WHERE ups_test.third_col < 1000 + ups_test.other_col;

-- see the results
SELECT * FROM upsert_test;

-- Test upsert, with returning:
INSERT INTO upsert_test (part_key, other_col) VALUES (2, 2)
	ON CONFLICT (part_key) DO UPDATE SET other_col = 3
        RETURNING *;

INSERT INTO upsert_test (part_key, other_col) VALUES (2, 2)
	ON CONFLICT (part_key) DO UPDATE SET other_col = 3
        RETURNING *;

-- create another table
CREATE TABLE upsert_test_2
(
	part_key int,
	other_col int,
	third_col int,
	PRIMARY KEY (part_key, other_col)
);

-- distribute the table and create shards
SELECT create_distributed_table('upsert_test_2', 'part_key', 'hash');

-- now show that Citus works with multiple columns as the PRIMARY KEY, including the partiton key
INSERT INTO upsert_test_2 (part_key, other_col) VALUES (1, 1);
INSERT INTO upsert_test_2 (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key, other_col) DO NOTHING;

-- this errors out since there is no unique constraint on partition key
INSERT INTO upsert_test_2 (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key) DO NOTHING;

-- create another table
CREATE TABLE upsert_test_3
(
	part_key int,
	count int
);

-- note that this is not a unique index
CREATE INDEX idx_ups_test ON upsert_test_3(part_key);

-- distribute the table and create shards
SELECT create_distributed_table('upsert_test_3', 'part_key', 'hash');

-- since there are no unique indexes, error-out
INSERT INTO upsert_test_3 VALUES (1, 0) ON CONFLICT(part_key) DO UPDATE SET count = upsert_test_3.count + 1;

-- create another table
CREATE TABLE upsert_test_4
(
	part_key int UNIQUE,
	count int
);

-- distribute the table and create shards
SELECT create_distributed_table('upsert_test_4', 'part_key', 'hash');

-- a single row insert
INSERT INTO upsert_test_4 VALUES (1, 0);

-- show a simple count example use case
INSERT INTO upsert_test_4 VALUES (1, 0) ON CONFLICT(part_key) DO UPDATE SET count = upsert_test_4.count + 1;
INSERT INTO upsert_test_4 VALUES (1, 0) ON CONFLICT(part_key) DO UPDATE SET count = upsert_test_4.count + 1;
INSERT INTO upsert_test_4 VALUES (1, 0) ON CONFLICT(part_key) DO UPDATE SET count = upsert_test_4.count + 1;
INSERT INTO upsert_test_4 VALUES (1, 0) ON CONFLICT(part_key) DO UPDATE SET count = upsert_test_4.count + 1;
INSERT INTO upsert_test_4 VALUES (1, 0) ON CONFLICT(part_key) DO UPDATE SET count = upsert_test_4.count + 1;
INSERT INTO upsert_test_4 VALUES (1, 0) ON CONFLICT(part_key) DO UPDATE SET count = upsert_test_4.count + 1;

-- now see the results
SELECT * FROM upsert_test_4;

-- now test dropped columns
SET citus.shard_replication_factor TO 1;
CREATE TABLE dropcol_distributed(key int primary key, drop1 int, keep1 text, drop2 numeric, keep2 float);
SELECT create_distributed_table('dropcol_distributed', 'key', 'hash');

INSERT INTO dropcol_distributed AS dropcol (key, keep1, keep2) VALUES (1, '5', 5) ON CONFLICT(key)
	DO UPDATE SET keep1 = dropcol.keep1;

ALTER TABLE dropcol_distributed DROP COLUMN drop2;

INSERT INTO dropcol_distributed (key, keep1, keep2) VALUES (1, '5', 5) ON CONFLICT(key)
	DO UPDATE SET keep1 = dropcol_distributed.keep1;

ALTER TABLE dropcol_distributed DROP COLUMN keep2;

INSERT INTO dropcol_distributed AS dropcol (key, keep1) VALUES (1, '5') ON CONFLICT(key)
	DO UPDATE SET keep1 = dropcol.keep1;

ALTER TABLE dropcol_distributed DROP COLUMN drop1;

INSERT INTO dropcol_distributed AS dropcol (key, keep1) VALUES (1, '5') ON CONFLICT(key)
	DO UPDATE SET keep1 = dropcol.keep1;

-- below we test the cases that Citus does not support
-- subquery in the SET clause
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key) DO
	UPDATE SET other_col = (SELECT count(*) from upsert_test);

-- non mutable function call in the SET
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key) DO
	UPDATE SET other_col = random()::int;

-- non mutable function call in the WHERE
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key) DO
	UPDATE SET other_col = 5 WHERE upsert_test.other_col = random()::int;

-- non mutable function call in the arbiter WHERE
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key)  WHERE part_key = random()::int
	DO UPDATE SET other_col = 5;

-- error out on attempt to update the partition key
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key) DO
	UPDATE SET part_key = 15;

SET client_min_messages TO WARNING;
DROP SCHEMA upsert_test CASCADE;
