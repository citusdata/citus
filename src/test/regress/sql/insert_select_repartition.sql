--
-- INSERT_SELECT_REPARTITION
--

-- tests behaviour of INSERT INTO ... SELECT with repartitioning
CREATE SCHEMA insert_select_repartition;
SET search_path TO 'insert_select_repartition';

SET citus.next_shard_id TO 4213581;
SET citus.shard_replication_factor TO 1;

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

SET client_min_messages TO DEBUG1;
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
SET client_min_messages TO DEBUG1;
INSERT INTO target_table SELECT max(-a), array_agg(mapped_key) FROM source_table GROUP BY a;
RESET client_min_messages;

SELECT * FROM target_table ORDER BY a;

--
-- repartitioned INSERT/SELECT followed/preceded by other DML in same transaction
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

-- case 5. preceded by DELETE
TRUNCATE target_table;
BEGIN;
DELETE FROM target_table;
INSERT INTO target_table SELECT mapped_key, c FROM source_table;
END;
SELECT * FROM target_table ORDER BY a;

-- case 6. preceded by UPDATE
TRUNCATE target_table;
BEGIN;
UPDATE target_table SET b=array_append(b, a);
INSERT INTO target_table SELECT mapped_key, c FROM source_table;
END;
SELECT * FROM target_table ORDER BY a;

-- case 7. preceded by multi-row INSERT
TRUNCATE target_table;
BEGIN;
INSERT INTO target_table VALUES (-5, ARRAY[10,11]), (-6, ARRAY[11,12]), (-7, ARRAY[999]);
INSERT INTO target_table SELECT mapped_key, c FROM source_table;
END;
SELECT * FROM target_table ORDER BY a;

-- case 8. preceded by distributed INSERT/SELECT
TRUNCATE target_table;
INSERT INTO target_table SELECT mapped_key, c FROM source_table;
BEGIN;
INSERT INTO target_table SELECT * FROM target_table;
INSERT INTO target_table SELECT mapped_key, c FROM source_table;
END;
SELECT * FROM target_table ORDER BY a;

--
-- repartitioned INSERT/SELECT with RETURNING
--
TRUNCATE target_table;
SET client_min_messages TO DEBUG1;
WITH c AS (
    INSERT INTO target_table
    SELECT mapped_key, c FROM source_table
    RETURNING *)
SELECT * FROM c ORDER by a;
RESET client_min_messages;

--
-- in combination with CTEs
--
TRUNCATE target_table;
SET client_min_messages TO DEBUG1;
WITH t AS (
    SELECT mapped_key, a, c FROM source_table
    WHERE a > floor(random())
)
INSERT INTO target_table
SELECT mapped_key, c FROM t NATURAL JOIN source_table;
RESET client_min_messages;
SELECT * FROM target_table ORDER BY a;

DROP TABLE source_table, target_table;

--
-- The case where select query has a GROUP BY ...
--

SET citus.shard_count TO 4;
CREATE TABLE source_table(a int, b int);
SELECT create_distributed_table('source_table', 'a');

SET citus.shard_count TO 3;
CREATE TABLE target_table(a int, b int);
SELECT create_distributed_table('target_table', 'a');

INSERT INTO source_table SELECT floor(i/4), i*i FROM generate_series(1, 20) i;

SET client_min_messages TO DEBUG1;
INSERT INTO target_table SELECT a, max(b) FROM source_table GROUP BY a;
RESET client_min_messages;

SELECT * FROM target_table ORDER BY a;

--
-- EXPLAIN output should specify repartitioned INSERT/SELECT
--

EXPLAIN INSERT INTO target_table SELECT a, max(b) FROM source_table GROUP BY a;

--
-- EXPLAIN ANALYZE is currently not supported
--

EXPLAIN ANALYZE INSERT INTO target_table SELECT a, max(b) FROM source_table GROUP BY a;

--
-- Duplicate names in target list
--

TRUNCATE target_table;

SET client_min_messages TO DEBUG2;
INSERT INTO target_table
 SELECT max(b), max(b) FROM source_table GROUP BY a;
RESET client_min_messages;

SELECT * FROM target_table ORDER BY a;

--
-- Prepared INSERT/SELECT
--
TRUNCATE target_table;
PREPARE insert_plan(int, int) AS
INSERT INTO target_table
  SELECT a, max(b) FROM source_table
  WHERE a BETWEEN $1 AND $2 GROUP BY a;

SET client_min_messages TO DEBUG1;
EXECUTE insert_plan(0, 2);
EXECUTE insert_plan(0, 2);
EXECUTE insert_plan(0, 2);
EXECUTE insert_plan(0, 2);
EXECUTE insert_plan(0, 2);
EXECUTE insert_plan(0, 2);

EXECUTE insert_plan(2, 4);
EXECUTE insert_plan(2, 4);
EXECUTE insert_plan(2, 4);
EXECUTE insert_plan(2, 4);
EXECUTE insert_plan(2, 4);
EXECUTE insert_plan(2, 4);
RESET client_min_messages;

SELECT a, count(*), count(distinct b) distinct_values FROM target_table GROUP BY a ORDER BY a;

DEALLOCATE insert_plan;

--
-- Prepared router INSERT/SELECT. We currently use pull to coordinator when the
-- distributed query has a single task.
--
TRUNCATE target_table;

PREPARE insert_plan(int) AS
INSERT INTO target_table
  SELECT a, max(b) FROM source_table
  WHERE a=$1 GROUP BY a;

SET client_min_messages TO DEBUG1;
EXECUTE insert_plan(0);
EXECUTE insert_plan(0);
EXECUTE insert_plan(0);
EXECUTE insert_plan(0);
EXECUTE insert_plan(0);
EXECUTE insert_plan(0);
EXECUTE insert_plan(0);
RESET client_min_messages;

SELECT a, count(*), count(distinct b) distinct_values FROM target_table GROUP BY a ORDER BY a;

DEALLOCATE insert_plan;

--
-- Prepared INSERT/SELECT with no parameters.
--

TRUNCATE target_table;

PREPARE insert_plan AS
INSERT INTO target_table
  SELECT a, max(b) FROM source_table
  WHERE a BETWEEN 1 AND 2 GROUP BY a;

SELECT public.coordinator_plan($Q$
EXPLAIN EXECUTE insert_plan;
$Q$);

SET client_min_messages TO DEBUG1;
EXECUTE insert_plan;
EXECUTE insert_plan;
EXECUTE insert_plan;
EXECUTE insert_plan;
EXECUTE insert_plan;
EXECUTE insert_plan;
EXECUTE insert_plan;
RESET client_min_messages;

SELECT a, count(*), count(distinct b) distinct_values FROM target_table GROUP BY a ORDER BY a;

DEALLOCATE insert_plan;

--
-- INSERT/SELECT in CTE
--

TRUNCATE target_table;

SET client_min_messages TO DEBUG2;
SET citus.enable_non_colocated_router_query_pushdown TO ON;
WITH r AS (
  INSERT INTO target_table SELECT * FROM source_table RETURNING *
)
INSERT INTO target_table SELECT source_table.a, max(source_table.b) FROM source_table NATURAL JOIN r GROUP BY source_table.a;
RESET citus.enable_non_colocated_router_query_pushdown;
RESET client_min_messages;

SELECT * FROM target_table ORDER BY a, b;

DROP TABLE source_table, target_table;

--
-- Constraint failure and rollback
--

SET citus.shard_count TO 4;
CREATE TABLE source_table(a int, b int);
SELECT create_distributed_table('source_table', 'a');
INSERT INTO source_table SELECT i, i * i FROM generate_series(1, 10) i;
UPDATE source_table SET b = NULL where b IN (9, 4);

SET citus.shard_replication_factor TO 2;
CREATE TABLE target_table(a int, b int not null);
SELECT create_distributed_table('target_table', 'a', 'range');
CALL public.create_range_partitioned_shards('target_table', '{0,3,6,9}','{2,5,8,50}');

INSERT INTO target_table VALUES (11,9), (22,4);

EXPLAIN (costs off) INSERT INTO target_table SELECT * FROM source_table;
EXPLAIN (costs off) INSERT INTO target_table SELECT * FROM source_table WHERE b IS NOT NULL;

BEGIN;
SAVEPOINT s1;
INSERT INTO target_table SELECT * FROM source_table;
ROLLBACK TO SAVEPOINT s1;
INSERT INTO target_table SELECT * FROM source_table WHERE b IS NOT NULL;
END;

SELECT * FROM target_table ORDER BY b;

-- verify that values have been replicated to both replicas
SELECT * FROM run_command_on_placements('target_table', 'select count(*) from %s') ORDER BY shardid, nodeport;

--
-- Multiple casts in the SELECT query
--

TRUNCATE target_table;

SET client_min_messages TO DEBUG2;
INSERT INTO target_table SELECT 1.12, b::bigint FROM source_table WHERE b IS NOT NULL;
RESET client_min_messages;

SELECT * FROM target_table ORDER BY a, b;

--
-- ROLLBACK after out of range error
--

TRUNCATE target_table;

BEGIN;
INSERT INTO target_table SELECT a * 10, b FROM source_table WHERE b IS NOT NULL;
END;

SELECT max(result) FROM run_command_on_placements('target_table', 'select count(*) from %s');

DROP TABLE source_table, target_table;

--
-- Range partitioned target's ranges doesn't cover the whole range
--

SET citus.shard_replication_factor TO 2;
SET citus.shard_count TO 4;
CREATE TABLE source_table(a int, b int);
SELECT create_distributed_table('source_table', 'a');
INSERT INTO source_table SELECT i, i * i FROM generate_series(1, 10) i;

SET citus.shard_replication_factor TO 2;
CREATE TABLE target_table(b int not null, a float);
SELECT create_distributed_table('target_table', 'a', 'range');
CALL public.create_range_partitioned_shards('target_table', '{0.0,3.5,6.5,9.5}','{2.9,5.9,8.9,50.0}');

INSERT INTO target_table SELECT b, a+0.6 FROM source_table;
SELECT * FROM target_table ORDER BY a;

-- verify that values have been replicated to both replicas, and that each
-- replica has received correct number of rows
SELECT * FROM run_command_on_placements('target_table', 'select count(*) from %s') ORDER BY shardid, nodeport;

DROP TABLE source_table, target_table;

--
-- Select column names should be unique
--

SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;
CREATE TABLE source_table(a int, b int);
SELECT create_distributed_table('source_table', 'a');

SET citus.shard_count TO 3;
CREATE TABLE target_table(a int, b int, c int, d int, e int, f int);
SELECT create_distributed_table('target_table', 'a');

INSERT INTO source_table SELECT i, i * i FROM generate_series(1, 10) i;

SET client_min_messages TO DEBUG2;
INSERT INTO target_table SELECT a AS aa, b AS aa, 1 AS aa, 2 AS aa FROM source_table;
RESET client_min_messages;

SELECT count(*) FROM target_table;

--
-- Disable repartitioned insert/select
--

TRUNCATE target_table;
SET citus.enable_repartitioned_insert_select TO OFF;

EXPLAIN (costs off) INSERT INTO target_table SELECT a AS aa, b AS aa, 1 AS aa, 2 AS aa FROM source_table;

SET client_min_messages TO DEBUG2;
INSERT INTO target_table SELECT a AS aa, b AS aa, 1 AS aa, 2 AS aa FROM source_table;
RESET client_min_messages;

SELECT count(*) FROM target_table;

SET citus.enable_repartitioned_insert_select TO ON;
EXPLAIN (costs off) INSERT INTO target_table SELECT a AS aa, b AS aa, 1 AS aa, 2 AS aa FROM source_table;

DROP TABLE source_table, target_table;

--
-- Don't use INSERT/SELECT repartition with repartition joins
--

create table test(x int, y int);
select create_distributed_table('test', 'x');
set citus.enable_repartition_joins to true;

INSERT INTO test SELECT i, i FROM generate_series(1, 10) i;

EXPLAIN (costs off) INSERT INTO test(y, x) SELECT a.x, b.y FROM test a JOIN test b USING (y);

SET client_min_messages TO DEBUG1;
INSERT INTO test(y, x) SELECT a.x, b.y FROM test a JOIN test b USING (y);
RESET client_min_messages;

SELECT count(*) FROM test;

TRUNCATE test;
INSERT INTO test SELECT i, i FROM generate_series(1, 10) i;

EXPLAIN (costs off) INSERT INTO test SELECT a.* FROM test a JOIN test b USING (y);

SET client_min_messages TO DEBUG1;
INSERT INTO test SELECT a.* FROM test a JOIN test b USING (y);
RESET client_min_messages;

SELECT count(*) FROM test;

--
-- In the following case we coerce some columns and move uncoerced versions to the
-- end of SELECT list. The following case verifies that we rename those columns so
-- we don't get "column reference is ambiguous" errors.
--

CREATE TABLE target_table(
    c1 int,
    c2 int,
    c3 timestamp,
    a int,
    b int,
    c int,
    c4 int,
    c5 int,
    c6 int[],
    cardinality int,
    sum int,
    PRIMARY KEY (c1, c2, c3, c4, c5, c6)
);

SET citus.shard_count TO 5;
SELECT create_distributed_table('target_table', 'c1');

CREATE TABLE source_table(
    c1 int,
    c2 int,
    c3 date,
    c4 int,
    cardinality int,
    sum int
);

SET citus.shard_count TO 4;
SELECT create_distributed_table('source_table', 'c1');

CREATE OR REPLACE FUNCTION dist_func(a int, b int) RETURNS int[]
AS $$
BEGIN
 RETURN array_fill(a, ARRAY[b]);
END;
$$
LANGUAGE plpgsql STABLE;

SELECT create_distributed_function('dist_func(int, int)');

SET client_min_messages TO DEBUG;
SET citus.enable_unique_job_ids TO off;
INSERT INTO source_table VALUES (1,2, '2020-02-02', 3, 4, 5);
INSERT INTO source_table VALUES (1,2, '2020-02-02', 3, 4, 5);
INSERT INTO source_table VALUES (3,4, '2020-02-02', 3, 4, 5);

INSERT INTO target_table AS enriched(c1, c2, c3, c4, c5, c6, cardinality, sum)
SELECT c1, c2, c3, c4, -1::float AS c5,
       dist_func(c1, 4) c6,
       sum(cardinality),
       sum(sum)
FROM source_table
GROUP BY c1, c2, c3, c4, c6
ON CONFLICT(c1, c2, c3, c4, c5, c6)
DO UPDATE SET
 cardinality = enriched.cardinality + excluded.cardinality,
 sum = enriched.sum + excluded.sum;

RESET client_min_messages;

EXPLAIN (COSTS OFF) INSERT INTO target_table AS enriched(c1, c2, c3, c4, c5, c6, cardinality, sum)
SELECT c1, c2, c3, c4, -1::float AS c5,
       dist_func(c1, 4) c6,
       sum(cardinality),
       sum(sum)
FROM source_table
GROUP BY c1, c2, c3, c4, c6
ON CONFLICT(c1, c2, c3, c4, c5, c6)
DO UPDATE SET
 cardinality = enriched.cardinality + excluded.cardinality,
 sum = enriched.sum + excluded.sum;


-- verify that we don't report repartitioned insert/select for tables
-- with sequences. See https://github.com/citusdata/citus/issues/3936
create table table_with_sequences (x int, y int, z bigserial);
insert into table_with_sequences values (1,1);
select create_distributed_table('table_with_sequences','x');
explain (costs off) insert into table_with_sequences select y, x from table_with_sequences;

-- verify that we don't report repartitioned insert/select for tables
-- with user-defined sequences.
CREATE SEQUENCE user_defined_sequence;
create table table_with_user_sequences (x int, y int, z bigint default nextval('user_defined_sequence'));
insert into table_with_user_sequences values (1,1);
select create_distributed_table('table_with_user_sequences','x');
explain (costs off) insert into table_with_user_sequences select y, x from table_with_user_sequences;

CREATE TABLE dist_table_1(id int);
SELECT create_distributed_table('dist_table_1','id');
CREATE TABLE dist_table_2(id int);
SELECT create_distributed_table('dist_table_2','id');

-- verify that insert select with union can be repartitioned. We cannot push down the query
-- since UNION clause has no FROM clause at top level query.
SELECT public.coordinator_plan($$
  EXPLAIN (COSTS FALSE) INSERT INTO dist_table_1(id) SELECT id FROM dist_table_1 UNION SELECT id FROM dist_table_2;
$$);

-- clean-up
SET client_min_messages TO WARNING;
DROP SCHEMA insert_select_repartition CASCADE;
