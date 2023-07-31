CREATE SCHEMA insert_select_single_shard_table;
SET search_path TO insert_select_single_shard_table;

SET citus.next_shard_id TO 1820000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

SET client_min_messages TO NOTICE;

CREATE TABLE nullkey_c1_t1(a int, b int);
CREATE TABLE nullkey_c1_t2(a int, b int);
SELECT create_distributed_table('nullkey_c1_t1', null, colocate_with=>'none');
SELECT create_distributed_table('nullkey_c1_t2', null, colocate_with=>'nullkey_c1_t1');

CREATE TABLE nullkey_c2_t1(a int, b int);
SELECT create_distributed_table('nullkey_c2_t1', null, colocate_with=>'none');

CREATE TABLE reference_table(a int, b int);
SELECT create_reference_table('reference_table');

CREATE TABLE distributed_table_c1_t1(a int, b int);
SELECT create_distributed_table('distributed_table_c1_t1', 'a');

CREATE TABLE distributed_table_c1_t2(a int, b int);
SELECT create_distributed_table('distributed_table_c1_t2', 'a');

CREATE TABLE distributed_table_c2_t1(a int, b int);
SELECT create_distributed_table('distributed_table_c2_t1', 'a', colocate_with=>'none');

CREATE TABLE citus_local_table(a int, b int);
SELECT citus_add_local_table_to_metadata('citus_local_table');

CREATE TABLE postgres_local_table(a int, b int);

CREATE FUNCTION reload_tables() RETURNS void AS $$
    BEGIN
		SET LOCAL client_min_messages TO WARNING;

		TRUNCATE nullkey_c1_t1, nullkey_c1_t2, nullkey_c2_t1, reference_table, distributed_table_c1_t1,
                 distributed_table_c1_t2, distributed_table_c2_t1, citus_local_table, postgres_local_table;

        INSERT INTO nullkey_c1_t1 SELECT i, i FROM generate_series(1, 8) i;
        INSERT INTO nullkey_c1_t2 SELECT i, i FROM generate_series(2, 7) i;
        INSERT INTO nullkey_c2_t1 SELECT i, i FROM generate_series(2, 7) i;
        INSERT INTO reference_table SELECT i, i FROM generate_series(0, 5) i;
        INSERT INTO distributed_table_c1_t1 SELECT i, i FROM generate_series(3, 8) i;
        INSERT INTO distributed_table_c1_t2 SELECT i, i FROM generate_series(2, 9) i;
        INSERT INTO distributed_table_c2_t1 SELECT i, i FROM generate_series(5, 10) i;
        INSERT INTO citus_local_table SELECT i, i FROM generate_series(0, 10) i;
        INSERT INTO postgres_local_table SELECT i, i FROM generate_series(5, 10) i;
    END;
$$ LANGUAGE plpgsql;

SELECT reload_tables();

CREATE TABLE append_table (a int, b int);
SELECT create_distributed_table('append_table', 'a', 'append');
SELECT master_create_empty_shard('append_table') AS shardid1 \gset
SELECT master_create_empty_shard('append_table') AS shardid2 \gset
SELECT master_create_empty_shard('append_table') AS shardid3 \gset

COPY append_table (a, b) FROM STDIN WITH (format 'csv', append_to_shard :shardid1);
1, 40
2, 42
3, 44
4, 46
5, 48
\.

COPY append_table (a, b) FROM STDIN WITH (format 'csv', append_to_shard :shardid2);
6, 50
7, 52
8, 54
9, 56
10, 58
\.

CREATE TABLE range_table(a int, b int);
SELECT create_distributed_table('range_table', 'a', 'range');
CALL public.create_range_partitioned_shards('range_table', '{"0","25"}','{"24","49"}');
INSERT INTO range_table VALUES (0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 50);

CREATE MATERIALIZED VIEW matview AS SELECT b*2+a AS a, a*a AS b FROM nullkey_c1_t1;

SET client_min_messages TO DEBUG2;

-- Test inserting into a distributed table by selecting from a combination of
-- different table types together with single-shard tables.

-- use a single-shard table
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1;

-- use a reference table
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN reference_table USING (a);
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 RIGHT JOIN reference_table USING (b) WHERE reference_table.a >= 1 AND reference_table.a <= 5;
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t2.a, nullkey_c1_t2.b FROM nullkey_c1_t2 LEFT JOIN reference_table USING (b);
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 INTERSECT SELECT * FROM reference_table;

-- use a colocated single-shard table
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN nullkey_c1_t2 USING (b);
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 FULL JOIN nullkey_c1_t2 USING (a);
INSERT INTO distributed_table_c1_t1 SELECT COALESCE(nullkey_c1_t1.a, 1), nullkey_c1_t1.b FROM nullkey_c1_t1 FULL JOIN matview USING (a);
INSERT INTO distributed_table_c1_t1 SELECT * FROM nullkey_c1_t1 UNION SELECT * FROM nullkey_c1_t2;

-- use a non-colocated single-shard table
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t2.a, nullkey_c1_t2.b FROM nullkey_c1_t2 LEFT JOIN nullkey_c2_t1 USING (a);
INSERT INTO distributed_table_c1_t1 SELECT * FROM nullkey_c1_t1 UNION SELECT * FROM nullkey_c2_t1;

SET client_min_messages TO DEBUG1;
SET citus.enable_repartition_joins TO ON;

-- use a distributed table that is colocated with the target table, with repartition joins enabled
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN distributed_table_c1_t2 USING (a);
INSERT INTO distributed_table_c1_t1 SELECT distributed_table_c1_t2.a, distributed_table_c1_t2.b FROM nullkey_c1_t1 JOIN distributed_table_c1_t2 USING (a);
INSERT INTO distributed_table_c1_t1 SELECT distributed_table_c1_t2.a, distributed_table_c1_t2.b FROM nullkey_c1_t1 JOIN distributed_table_c1_t2 USING (b);
INSERT INTO distributed_table_c1_t1 SELECT distributed_table_c1_t2.a, distributed_table_c1_t2.b FROM nullkey_c1_t1 JOIN distributed_table_c1_t2 USING (a) WHERE distributed_table_c1_t2.a = 1;

-- use a distributed table that is not colocated with the target table, with repartition joins enabled
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t2.a, nullkey_c1_t2.b FROM nullkey_c1_t2 JOIN distributed_table_c2_t1 USING (a);

RESET citus.enable_repartition_joins;
SET client_min_messages TO DEBUG2;

-- use a citus local table
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN citus_local_table USING (a);

-- use a postgres local table
INSERT INTO distributed_table_c1_t1 SELECT nullkey_c1_t2.a, nullkey_c1_t2.b FROM nullkey_c1_t2 JOIN postgres_local_table USING (a);

-- use append / range distributed tables
INSERT INTO range_table SELECT * FROM nullkey_c1_t1;
INSERT INTO append_table SELECT * FROM nullkey_c1_t1;

SELECT avg(a), avg(b) FROM distributed_table_c1_t1 ORDER BY 1, 2;
TRUNCATE distributed_table_c1_t1;
INSERT INTO distributed_table_c1_t1 SELECT i, i FROM generate_series(3, 8) i;

-- Test inserting into a reference table by selecting from a combination of
-- different table types together with single-shard tables.

-- use a single-shard table
INSERT INTO reference_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1;

-- use a reference table
INSERT INTO reference_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN reference_table USING (a);
INSERT INTO reference_table SELECT nullkey_c1_t2.a, nullkey_c1_t2.b FROM nullkey_c1_t2 LEFT JOIN reference_table USING (b);
INSERT INTO reference_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 UNION SELECT * FROM reference_table;
INSERT INTO reference_table SELECT nullkey_c1_t2.a, nullkey_c1_t2.b FROM nullkey_c1_t2 LEFT JOIN reference_table USING (b) WHERE b IN (SELECT b FROM matview);

-- use a colocated single-shard table
INSERT INTO reference_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN nullkey_c1_t2 USING (b);
INSERT INTO reference_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 FULL JOIN nullkey_c1_t2 USING (a);

-- use a non-colocated single-shard table
INSERT INTO reference_table SELECT nullkey_c1_t2.a, nullkey_c1_t2.b FROM nullkey_c1_t2 LEFT JOIN nullkey_c2_t1 USING (a);

-- use a distributed table

SET client_min_messages TO DEBUG1;
SET citus.enable_repartition_joins TO ON;

INSERT INTO reference_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN distributed_table_c1_t2 USING (a);
INSERT INTO reference_table SELECT distributed_table_c1_t2.a, distributed_table_c1_t2.b FROM nullkey_c1_t1 JOIN distributed_table_c1_t2 USING (a);
INSERT INTO reference_table SELECT distributed_table_c1_t2.a, distributed_table_c1_t2.b FROM nullkey_c1_t1 JOIN distributed_table_c1_t2 USING (b);
INSERT INTO reference_table SELECT distributed_table_c1_t2.a, distributed_table_c1_t2.b FROM nullkey_c1_t1 JOIN distributed_table_c1_t2 USING (a) WHERE distributed_table_c1_t2.a = 1;

RESET citus.enable_repartition_joins;
SET client_min_messages TO DEBUG2;

-- use a citus local table
INSERT INTO reference_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN citus_local_table USING (a);

-- use a postgres local table
INSERT INTO reference_table SELECT nullkey_c1_t2.a, nullkey_c1_t2.b FROM nullkey_c1_t2 JOIN postgres_local_table USING (a);

SELECT avg(a), avg(b) FROM reference_table ORDER BY 1, 2;
TRUNCATE reference_table;
INSERT INTO reference_table SELECT i, i FROM generate_series(0, 5) i;

-- Test inserting into a citus local table by selecting from a combination of
-- different table types together with single-shard tables.

-- use a single-shard table
INSERT INTO citus_local_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1;

-- use a reference table
INSERT INTO citus_local_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN reference_table USING (a);

-- use a colocated single-shard table
INSERT INTO citus_local_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN nullkey_c1_t2 USING (b);

-- use a distributed table
SET client_min_messages TO DEBUG1;
SET citus.enable_repartition_joins TO ON;
INSERT INTO citus_local_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN distributed_table_c1_t2 USING (a);
RESET citus.enable_repartition_joins;
SET client_min_messages TO DEBUG2;

-- use a citus local table
INSERT INTO citus_local_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN citus_local_table USING (a);

-- use a postgres local table
INSERT INTO citus_local_table SELECT nullkey_c1_t2.a, nullkey_c1_t2.b FROM nullkey_c1_t2 JOIN postgres_local_table USING (a);

SELECT avg(a), avg(b) FROM citus_local_table ORDER BY 1, 2;
TRUNCATE citus_local_table;
INSERT INTO citus_local_table SELECT i, i FROM generate_series(0, 10) i;

-- Test inserting into a single-shard table by selecting from a combination of
-- different table types, together with or without single-shard tables.

-- use a postgres local table
INSERT INTO nullkey_c1_t1 SELECT postgres_local_table.a, postgres_local_table.b FROM postgres_local_table;
INSERT INTO nullkey_c1_t1 SELECT postgres_local_table.a, postgres_local_table.b FROM postgres_local_table JOIN reference_table USING (a);
INSERT INTO nullkey_c1_t1 SELECT postgres_local_table.a, postgres_local_table.b FROM postgres_local_table LEFT JOIN nullkey_c1_t1 USING (a);

-- use a citus local table
INSERT INTO nullkey_c1_t1 SELECT citus_local_table.a, citus_local_table.b FROM citus_local_table;
INSERT INTO nullkey_c1_t1 SELECT citus_local_table.a, citus_local_table.b FROM citus_local_table JOIN reference_table USING (a) JOIN postgres_local_table USING (a) ORDER BY 1,2 OFFSET 7;
INSERT INTO nullkey_c1_t1 SELECT citus_local_table.a, citus_local_table.b FROM citus_local_table JOIN nullkey_c1_t1 USING (a);

-- use a distributed table
INSERT INTO nullkey_c1_t1 SELECT distributed_table_c1_t2.a, distributed_table_c1_t2.b FROM distributed_table_c1_t2;
INSERT INTO nullkey_c1_t1 SELECT distributed_table_c1_t2.a, distributed_table_c1_t2.b FROM distributed_table_c1_t2 JOIN reference_table USING (a);

SET client_min_messages TO DEBUG1;
SET citus.enable_repartition_joins TO ON;
INSERT INTO nullkey_c1_t1 SELECT distributed_table_c1_t2.a, distributed_table_c1_t2.b FROM distributed_table_c1_t2 JOIN nullkey_c1_t1 USING (a);
RESET citus.enable_repartition_joins;
SET client_min_messages TO DEBUG2;

-- use a non-colocated single-shard table
INSERT INTO nullkey_c2_t1 SELECT q.* FROM (SELECT reference_table.* FROM reference_table LEFT JOIN nullkey_c1_t1 USING (a)) q JOIN nullkey_c1_t2 USING (a);

-- use a materialized view
INSERT INTO nullkey_c1_t1 SELECT * FROM matview;
INSERT INTO nullkey_c1_t1 SELECT reference_table.a, reference_table.b FROM reference_table JOIN matview ON (reference_table.a = matview.a);
INSERT INTO nullkey_c1_t1 SELECT q.* FROM (SELECT reference_table.* FROM reference_table JOIN nullkey_c1_t1 USING (a)) q JOIN matview USING (a);

-- use append / range distributed tables
INSERT INTO nullkey_c1_t1 SELECT * FROM range_table;
INSERT INTO nullkey_c1_t1 SELECT * FROM append_table;

SELECT avg(a), avg(b) FROM nullkey_c1_t1 ORDER BY 1, 2;
SELECT avg(a), avg(b) FROM nullkey_c2_t1 ORDER BY 1, 2;
TRUNCATE nullkey_c1_t1, nullkey_c2_t1;
INSERT INTO nullkey_c1_t1 SELECT i, i FROM generate_series(1, 8) i;
INSERT INTO nullkey_c2_t1 SELECT i, i FROM generate_series(2, 7) i;

-- Test inserting into a local table by selecting from a combination of
-- different table types, together with or without single-shard tables.

INSERT INTO postgres_local_table SELECT nullkey_c1_t1.a, nullkey_c1_t1.b FROM nullkey_c1_t1 JOIN reference_table USING (a);

INSERT INTO postgres_local_table SELECT * FROM nullkey_c1_t1 ORDER BY 1,2 OFFSET 3 LIMIT 2;

WITH cte_1 AS (
  DELETE FROM nullkey_c1_t1 WHERE a >= 1 and a <= 4 RETURNING *
)
INSERT INTO postgres_local_table SELECT cte_1.* FROM cte_1 LEFT JOIN nullkey_c1_t2 USING (a) WHERE nullkey_c1_t2.a IS NULL;

INSERT INTO postgres_local_table SELECT * FROM nullkey_c1_t1 EXCEPT SELECT * FROM postgres_local_table;

SELECT avg(a), avg(b) FROM postgres_local_table ORDER BY 1, 2;
TRUNCATE postgres_local_table;
INSERT INTO postgres_local_table SELECT i, i FROM generate_series(5, 10) i;

-- Try slightly more complex queries.

SET client_min_messages TO DEBUG1;

WITH cte_1 AS (
  SELECT nullkey_c1_t1.a, reference_table.b FROM nullkey_c1_t1 JOIN reference_table USING (a)
),
cte_2 AS (
  SELECT reference_table.a, postgres_local_table.b FROM postgres_local_table LEFT JOIN reference_table USING (b)
)
INSERT INTO distributed_table_c1_t1
SELECT cte_1.* FROM cte_1 JOIN cte_2 USING (a) JOIN distributed_table_c1_t2 USING (a) ORDER BY 1,2;

SET client_min_messages TO DEBUG2;

WITH cte_1 AS (
  SELECT nullkey_c1_t1.a, reference_table.b FROM nullkey_c1_t1 JOIN reference_table USING (a)
),
cte_2 AS (
  SELECT * FROM nullkey_c1_t2 WHERE EXISTS (
    SELECT 1 FROM reference_table WHERE reference_table.a = nullkey_c1_t2.a
  )
  ORDER BY 1,2 OFFSET 1 LIMIT 4
)
INSERT INTO distributed_table_c1_t1
SELECT * FROM cte_1 UNION SELECT * FROM cte_2 EXCEPT SELECT * FROM reference_table;

INSERT INTO distributed_table_c1_t1 (a, b)
SELECT t1.a, t2.b
FROM nullkey_c1_t1 t1
JOIN (
  SELECT b FROM nullkey_c1_t2 ORDER BY b DESC LIMIT 1
) t2
ON t1.b < t2.b;

INSERT INTO distributed_table_c1_t1 (a, b)
WITH cte AS (
  SELECT a, b,
    (SELECT a FROM nullkey_c1_t2 WHERE b = t.b) AS d1,
    (SELECT a FROM reference_table WHERE b = t.b) AS d2
  FROM nullkey_c1_t1 t
)
SELECT d1, COALESCE(d2, a) FROM cte WHERE d1 IS NOT NULL AND d2 IS NOT NULL;

INSERT INTO citus_local_table (a, b)
SELECT t1.a, t2.b
FROM nullkey_c1_t1 t1
CROSS JOIN (
  SELECT b FROM nullkey_c2_t1 ORDER BY b LIMIT 1
) t2;

INSERT INTO distributed_table_c1_t1 (a, b)
SELECT t1.a, t2.b
FROM reference_table t1
LEFT JOIN (
  SELECT b, ROW_NUMBER() OVER (ORDER BY b DESC) AS rn
  FROM nullkey_c1_t1
) t2 ON t1.b = t2.b
WHERE t2.rn > 0;

INSERT INTO nullkey_c1_t1 (a, b)
SELECT t1.a, t2.b
FROM nullkey_c1_t1 t1
JOIN (
  SELECT rn, b
  FROM (
    SELECT b, ROW_NUMBER() OVER (ORDER BY b DESC) AS rn
    FROM distributed_table_c2_t1
  ) q
) t2 ON t1.b = t2.b
WHERE t2.rn > 2;

INSERT INTO distributed_table_c1_t1 (a, b)
SELECT t1.a, t2.b
FROM nullkey_c1_t1 t1
JOIN (
  SELECT sum_val, b
  FROM (
    SELECT b, SUM(a) OVER (PARTITION BY b) AS sum_val
    FROM nullkey_c1_t1
  ) q
) t2 ON t1.b = t2.b
WHERE t2.sum_val > 2;

-- Temporaryly reduce the verbosity to avoid noise
-- in the output of the next query.
SET client_min_messages TO DEBUG1;

INSERT INTO nullkey_c1_t1 SELECT DISTINCT ON (a) a, b FROM nullkey_c1_t2;

-- keep low verbosity as PG15 and PG14 produces slightly different outputs
INSERT INTO nullkey_c1_t1 SELECT b, SUM(a) OVER (ORDER BY b) AS sum_val FROM nullkey_c1_t1;

SET client_min_messages TO DEBUG2;
INSERT INTO nullkey_c2_t1
SELECT t2.a, t2.b
FROM nullkey_c1_t1 AS t2
JOIN reference_table AS t3 ON (t2.a = t3.a)
WHERE NOT EXISTS (
  SELECT 1 FROM nullkey_c1_t2 AS t1 WHERE t1.b = t3.b
);

INSERT INTO distributed_table_c1_t1
SELECT t1.a, t1.b
FROM nullkey_c1_t1 AS t1
WHERE t1.a NOT IN (
  SELECT DISTINCT t2.a FROM distributed_table_c1_t2 AS t2
);

INSERT INTO distributed_table_c1_t1
SELECT t1.a, t1.b
FROM reference_table AS t1
JOIN (
  SELECT t2.a FROM (
    SELECT a FROM nullkey_c1_t1
    UNION
    SELECT a FROM nullkey_c1_t2
  ) AS t2
) AS t3 ON t1.a = t3.a;

-- Temporaryly reduce the verbosity to avoid noise
-- in the output of the next query.
SET client_min_messages TO DEBUG1;

INSERT INTO nullkey_c1_t1
SELECT t1.a, t1.b
FROM reference_table AS t1
WHERE t1.a IN (
  SELECT t2.a FROM (
    SELECT t3.a FROM (
      SELECT a FROM distributed_table_c1_t1 WHERE b > 4
    ) AS t3
    JOIN (
      SELECT a FROM distributed_table_c1_t2 WHERE b < 7
    ) AS t4 ON t3.a = t4.a
  ) AS t2
);

SET client_min_messages TO DEBUG2;

-- test upsert with plain INSERT query

CREATE TABLE upsert_test_1
(
	unique_col int UNIQUE,
	other_col int,
	third_col int
);
SELECT create_distributed_table('upsert_test_1', null);

CREATE TABLE upsert_test_2(key int primary key, value text);
SELECT create_distributed_table('upsert_test_2', null);

INSERT INTO upsert_test_2 AS upsert_test_2_alias (key, value) VALUES (1, '5') ON CONFLICT(key)
	DO UPDATE SET value = (upsert_test_2_alias.value::int * 2)::text;

INSERT INTO upsert_test_2 (key, value) VALUES (1, '5') ON CONFLICT(key)
	DO UPDATE SET value = (upsert_test_2.value::int * 3)::text;

INSERT INTO upsert_test_1 (unique_col, other_col) VALUES (1, 1) ON CONFLICT (unique_col)
	DO UPDATE SET other_col = (SELECT count(*) from upsert_test_1);

INSERT INTO upsert_test_1 (unique_col, other_col) VALUES (1, 1) ON CONFLICT (unique_col)
	DO UPDATE SET other_col = random()::int;

INSERT INTO upsert_test_1 (unique_col, other_col) VALUES (1, 1) ON CONFLICT (unique_col)
	DO UPDATE SET other_col = 5 WHERE upsert_test_1.other_col = random()::int;

INSERT INTO upsert_test_1 VALUES (3, 5, 7);

INSERT INTO upsert_test_1 (unique_col, other_col) VALUES (1, 1) ON CONFLICT (unique_col) WHERE unique_col = random()::int
	DO UPDATE SET other_col = 5;

CREATE TABLE upsert_test_3 (key_1 int, key_2 bigserial, value text DEFAULT 'default_value', PRIMARY KEY (key_1, key_2));
SELECT create_distributed_table('upsert_test_3', null);

INSERT INTO upsert_test_3 VALUES (1, DEFAULT, '1') RETURNING *;
INSERT INTO upsert_test_3 VALUES (5, DEFAULT, DEFAULT) RETURNING *;

SET client_min_messages TO DEBUG1;
INSERT INTO upsert_test_3 SELECT 7, other_col, 'harcoded_text_value' FROM upsert_test_1 RETURNING *;
SET client_min_messages TO DEBUG2;

-- test upsert with INSERT .. SELECT queries

SET client_min_messages TO DEBUG1;
INSERT INTO upsert_test_1 (unique_col, other_col) SELECT unique_col, other_col FROM upsert_test_1 ON CONFLICT (unique_col)
    DO UPDATE SET other_col = upsert_test_1.other_col + 1;
-- Fails due to https://github.com/citusdata/citus/issues/6826.
INSERT INTO upsert_test_1 (unique_col, other_col) SELECT unique_col, other_col FROM upsert_test_1 ON CONFLICT (unique_col)
	DO UPDATE SET other_col = (SELECT count(*) from upsert_test_1);
SET client_min_messages TO DEBUG2;

INSERT INTO upsert_test_1 (unique_col, other_col) SELECT unique_col, other_col FROM upsert_test_1 ON CONFLICT (unique_col)
	DO UPDATE SET other_col = random()::int;

INSERT INTO upsert_test_1 (unique_col, other_col) SELECT unique_col, other_col FROM upsert_test_1 ON CONFLICT (unique_col)
	DO UPDATE SET other_col = 5 WHERE upsert_test_1.other_col = random()::int;

SELECT reload_tables();

ALTER TABLE nullkey_c1_t1 ADD PRIMARY KEY (a);
ALTER TABLE distributed_table_c1_t1 ADD PRIMARY KEY (a,b);

INSERT INTO nullkey_c1_t1 AS t1 (a, b) SELECT t3.a, t3.b FROM nullkey_c1_t2 t2 JOIN reference_table t3 ON (t2.a = t3.a) ON CONFLICT (a)
    DO UPDATE SET a = t1.a + 10;

SET client_min_messages TO DEBUG1;
INSERT INTO distributed_table_c1_t1 AS t1 (a, b) SELECT t3.a, t3.b FROM nullkey_c1_t2 t2 JOIN reference_table t3 ON (t2.a = t3.a) ON CONFLICT (a, b)
    DO UPDATE SET b = t1.b + 10;
INSERT INTO nullkey_c1_t1 AS t1 (a, b) SELECT t3.a, t3.b FROM distributed_table_c1_t1 t2 JOIN reference_table t3 ON (t2.a = t3.a) ON CONFLICT (a)
    DO UPDATE SET a = t1.a + 10;
-- This also fails due to https://github.com/citusdata/citus/issues/6826.
INSERT INTO nullkey_c1_t1 AS t1 (a, b) SELECT t3.a, t3.b FROM distributed_table_c1_t1 t2 JOIN reference_table t3 ON (t2.a = t3.a) WHERE t2.a = 3 ON CONFLICT (a)
    DO UPDATE SET a = (SELECT max(b)+1 FROM distributed_table_c1_t1 WHERE a = 3);
SET client_min_messages TO DEBUG2;

SELECT avg(a), avg(b) FROM distributed_table_c1_t1;
SELECT avg(a), avg(b) FROM nullkey_c1_t1;
SELECT avg(a), avg(b) FROM nullkey_c1_t2;
SELECT * FROM upsert_test_1 ORDER BY unique_col;
SELECT * FROM upsert_test_2 ORDER BY key;
SELECT * FROM upsert_test_3 ORDER BY key_1, key_2;

SET client_min_messages TO WARNING;
DROP SCHEMA insert_select_single_shard_table CASCADE;
