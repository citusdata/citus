-- tests behaviour of INSERT INTO ... SELECT with repartitioning
CREATE SCHEMA insert_select_repartition;
SET search_path TO 'insert_select_repartition';

SET citus.next_shard_id TO 4213581;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';

-- Test 1
-- 4 shards, hash distributed.
-- Negate distribution column value.
SET citus.shard_count TO 4;
CREATE TABLE source_table(a int);
SELECT create_distributed_table('source_table', 'a');
INSERT INTO source_table SELECT * FROM generate_series(1, 10);
CREATE TABLE target_table(a int);
SELECT create_distributed_table('target_table', 'a');

SET client_min_messages TO DEBUG2;
INSERT INTO target_table SELECT -a FROM source_table;
RESET client_min_messages;

SELECT * FROM target_table WHERE a=-1 OR a=-3 OR a=-7 ORDER BY a;

DROP TABLE source_table, target_table;

--
-- Test 2.
-- range partitioning, composite distribution column
CREATE TYPE composite_key_type AS (f1 int, f2 text);

-- source
CREATE TABLE source_table(f1 int, key composite_key_type, value int, mapped_key composite_key_type);
SELECT create_distributed_table('source_table', 'key', 'range');
CALL public.create_range_partitioned_shards('source_table', '{"(0,a)","(25,a)"}','{"(24,z)","(49,z)"}');

INSERT INTO source_table VALUES (0, (0, 'a'), 1, (0, 'a'));
INSERT INTO source_table VALUES (1, (1, 'b'), 2, (26, 'b'));
INSERT INTO source_table VALUES (2, (2, 'c'), 3, (3, 'c'));
INSERT INTO source_table VALUES (3, (4, 'd'), 4, (27, 'd'));
INSERT INTO source_table VALUES (4, (30, 'e'), 5, (30, 'e'));
INSERT INTO source_table VALUES (5, (31, 'f'), 6, (31, 'f'));
INSERT INTO source_table VALUES (6, (32, 'g'), 50, (8, 'g'));

-- target
CREATE TABLE target_table(f1 int DEFAULT 0, value int, key composite_key_type);
SELECT create_distributed_table('target_table', 'key', 'range');
CALL public.create_range_partitioned_shards('target_table', '{"(0,a)","(25,a)"}','{"(24,z)","(49,z)"}');

SET client_min_messages TO DEBUG2;
INSERT INTO target_table SELECT f1, value, mapped_key FROM source_table;
RESET client_min_messages;

SELECT * FROM target_table ORDER BY key;
SELECT * FROM target_table WHERE key = (26, 'b')::composite_key_type;

-- with explicit column names
TRUNCATE target_table;
SET client_min_messages TO DEBUG2;
INSERT INTO target_table(value, key) SELECT value, mapped_key FROM source_table;
RESET client_min_messages;
SELECT * FROM target_table ORDER BY key;

-- missing value for a column
TRUNCATE target_table;
SET client_min_messages TO DEBUG2;
INSERT INTO target_table(key) SELECT mapped_key AS key_renamed FROM source_table;
RESET client_min_messages;
SELECT * FROM target_table ORDER BY key;

-- missing value for distribution column
INSERT INTO target_table(value) SELECT value FROM source_table;

DROP TABLE source_table, target_table;

SET client_min_messages TO WARNING;
DROP SCHEMA insert_select_repartition CASCADE;
