-- tests behaviour of INSERT INTO ... SELECT with repartitioning
CREATE SCHEMA insert_select_repartition;
SET search_path TO 'insert_select_repartition';

SET citus.next_shard_id TO 4213581;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO 'streaming';

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
-- range partitioning, composite distribution column
--
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
CREATE TABLE target_table(f1 int DEFAULT 0, value int, key composite_key_type PRIMARY KEY);
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

-- ON CONFLICT
SET client_min_messages TO DEBUG2;
INSERT INTO target_table(key)
SELECT mapped_key AS key_renamed FROM source_table
WHERE (mapped_key).f1 % 2 = 1
ON CONFLICT (key) DO UPDATE SET f1=1;
RESET client_min_messages;
SELECT * FROM target_table ORDER BY key;

-- missing value for distribution column
INSERT INTO target_table(value) SELECT value FROM source_table;
DROP TABLE source_table, target_table;

-- different column types
-- verifies that we add necessary casts, otherwise even shard routing won't
-- work correctly and we will see 2 values for the same primary key.
CREATE TABLE target_table(col_1 int primary key, col_2 int);
SELECT create_distributed_table('target_table','col_1');
INSERT INTO target_table VALUES (1,2), (2,3), (3,4), (4,5), (5,6);

CREATE TABLE source_table(col_1 numeric, col_2 numeric, col_3 numeric);
SELECT create_distributed_table('source_table','col_1');
INSERT INTO source_table VALUES (1,1,1), (3,3,3), (5,5,5);

SET client_min_messages TO DEBUG2;
INSERT INTO target_table
SELECT
	col_1, col_2
FROM
	source_table
ON CONFLICT(col_1) DO UPDATE SET col_2 = EXCLUDED.col_2;
RESET client_min_messages;

SELECT * FROM target_table ORDER BY 1;

DROP TABLE source_table, target_table;

--
-- array coercion
--
SET citus.shard_count TO 3;
CREATE TABLE source_table(a int, mapped_key int, c float[]);
SELECT create_distributed_table('source_table', 'a');
INSERT INTO source_table VALUES (1, -1, ARRAY[1.1, 2.2, 3.3]), (2, -2, ARRAY[4.5, 5.8]),
                                (3, -3, ARRAY[]::float[]), (4, -4, ARRAY[3.3]);

SET citus.shard_count TO 2;
CREATE TABLE target_table(a int, b int[]);
SELECT create_distributed_table('target_table', 'a');

SET client_min_messages TO DEBUG2;
INSERT INTO target_table SELECT mapped_key, c FROM source_table;
RESET client_min_messages;

SELECT * FROM target_table ORDER BY a;

--
-- worker queries can have more columns than necessary. ExpandWorkerTargetEntry()
-- might add additional columns to the target list.
--
TRUNCATE target_table;
\set VERBOSITY TERSE

-- first verify that the SELECT query below fetches 3 projected columns from workers
SET citus.log_remote_commands TO true; SET client_min_messages TO DEBUG;
   CREATE TABLE results AS SELECT max(-a), array_agg(mapped_key) FROM source_table GROUP BY a;
RESET citus.log_remote_commands; RESET client_min_messages;
DROP TABLE results;

-- now verify that we don't write the extra columns to the intermediate result files and
-- insertion to the target works fine.
SET client_min_messages TO DEBUG2;
INSERT INTO target_table SELECT max(-a), array_agg(mapped_key) FROM source_table GROUP BY a;
RESET client_min_messages;

SELECT * FROM target_table ORDER BY a;

--
-- repartitioned INSERT/SELECT followed by other DML in stame transaction
--

-- case 1. followed by DELETE
TRUNCATE target_table;
BEGIN;
INSERT INTO target_table SELECT mapped_key, c FROM source_table;
SELECT * FROM target_table ORDER BY a;
DELETE FROM target_table;
END;
SELECT * FROM target_table ORDER BY a;

-- case 2. followed by UPDATE
TRUNCATE target_table;
BEGIN;
INSERT INTO target_table SELECT mapped_key, c FROM source_table;
SELECT * FROM target_table ORDER BY a;
UPDATE target_table SET b=array_append(b, a);
END;
SELECT * FROM target_table ORDER BY a;

-- case 3. followed by multi-row INSERT
TRUNCATE target_table;
BEGIN;
INSERT INTO target_table SELECT mapped_key, c FROM source_table;
SELECT * FROM target_table ORDER BY a;
INSERT INTO target_table VALUES (-5, ARRAY[10,11]), (-6, ARRAY[11,12]), (-7, ARRAY[999]);
END;
SELECT * FROM target_table ORDER BY a;

-- case 4. followed by distributed INSERT/SELECT
TRUNCATE target_table;
BEGIN;
INSERT INTO target_table SELECT mapped_key, c FROM source_table;
SELECT * FROM target_table ORDER BY a;
INSERT INTO target_table SELECT * FROM target_table;
END;
SELECT * FROM target_table ORDER BY a;

SET client_min_messages TO WARNING;
DROP SCHEMA insert_select_repartition CASCADE;
