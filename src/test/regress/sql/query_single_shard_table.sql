CREATE SCHEMA query_single_shard_table;
SET search_path TO query_single_shard_table;

SET citus.next_shard_id TO 1620000;
SET citus.shard_count TO 32;

SET client_min_messages TO NOTICE;

CREATE TABLE nullkey_c1_t1(a int, b int);
CREATE TABLE nullkey_c1_t2(a int, b int);
SELECT create_distributed_table('nullkey_c1_t1', null, colocate_with=>'none');
SELECT create_distributed_table('nullkey_c1_t2', null, colocate_with=>'nullkey_c1_t1');
INSERT INTO nullkey_c1_t1 SELECT i, i FROM generate_series(1, 8) i;
INSERT INTO nullkey_c1_t2 SELECT i, i FROM generate_series(2, 7) i;

CREATE TABLE nullkey_c2_t1(a int, b int);
CREATE TABLE nullkey_c2_t2(a int, b int);
SELECT create_distributed_table('nullkey_c2_t1', null, colocate_with=>'none');
SELECT create_distributed_table('nullkey_c2_t2', null, colocate_with=>'nullkey_c2_t1', distribution_type=>null);
INSERT INTO nullkey_c2_t1 SELECT i, i FROM generate_series(2, 7) i;
INSERT INTO nullkey_c2_t2 SELECT i, i FROM generate_series(1, 8) i;

CREATE TABLE nullkey_c3_t1(a int, b int);
SELECT create_distributed_table('nullkey_c3_t1', null, colocate_with=>'none');
INSERT INTO nullkey_c3_t1 SELECT i, i FROM generate_series(1, 8) i;

CREATE TABLE reference_table(a int, b int);
SELECT create_reference_table('reference_table');
INSERT INTO reference_table SELECT i, i FROM generate_series(0, 5) i;

CREATE TABLE distributed_table(a int, b int);
SELECT create_distributed_table('distributed_table', 'a');
INSERT INTO distributed_table SELECT i, i FROM generate_series(3, 8) i;

CREATE TABLE citus_local_table(a int, b int);
SELECT citus_add_local_table_to_metadata('citus_local_table');
INSERT INTO citus_local_table SELECT i, i FROM generate_series(0, 10) i;

CREATE TABLE postgres_local_table(a int, b int);
INSERT INTO postgres_local_table SELECT i, i FROM generate_series(5, 10) i;

CREATE TABLE articles_hash (
	id bigint NOT NULL,
	author_id bigint NOT NULL,
	title varchar(20) NOT NULL,
	word_count integer
);

INSERT INTO articles_hash VALUES ( 4,  4, 'altdorfer', 14551),( 5,  5, 'aruru', 11389),
								 (13,  3, 'aseyev', 2255),(15,  5, 'adversa', 3164),
								 (18,  8, 'assembly', 911),(19,  9, 'aubergiste', 4981),
								 (28,  8, 'aerophyte', 5454),(29,  9, 'amateur', 9524),
								 (42,  2, 'ausable', 15885),(43,  3, 'affixal', 12723),
								 (49,  9, 'anyone', 2681),(50, 10, 'anjanette', 19519);

SELECT create_distributed_table('articles_hash', null, colocate_with=>'none');

CREATE TABLE raw_events_first (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint, UNIQUE(user_id, value_1));
SELECT create_distributed_table('raw_events_first', null, colocate_with=>'none', distribution_type=>null);

CREATE TABLE raw_events_second (user_id int, time timestamp, value_1 int, value_2 int, value_3 float, value_4 bigint, UNIQUE(user_id, value_1));
SELECT create_distributed_table('raw_events_second', null, colocate_with=>'raw_events_first', distribution_type=>null);

CREATE TABLE agg_events (user_id int, value_1_agg int, value_2_agg int, value_3_agg float, value_4_agg bigint, agg_time timestamp, UNIQUE(user_id, value_1_agg));
SELECT create_distributed_table('agg_events', null, colocate_with=>'raw_events_first', distribution_type=>null);

CREATE TABLE users_ref_table (user_id int);
SELECT create_reference_table('users_ref_table');

INSERT INTO raw_events_first VALUES (1, '1970-01-01', 10, 100, 1000.1, 10000), (3, '1971-01-01', 30, 300, 3000.1, 30000),
                                    (5, '1972-01-01', 50, 500, 5000.1, 50000), (2, '1973-01-01', 20, 200, 2000.1, 20000),
                                    (4, '1974-01-01', 40, 400, 4000.1, 40000), (6, '1975-01-01', 60, 600, 6000.1, 60000);

CREATE TABLE modify_fast_path(key int, value_1 int, value_2 text);
SELECT create_distributed_table('modify_fast_path', null);

CREATE TABLE modify_fast_path_reference(key int, value_1 int, value_2 text);
SELECT create_reference_table('modify_fast_path_reference');

CREATE TABLE bigserial_test (x int, y int, z bigserial);
SELECT create_distributed_table('bigserial_test', null);

CREATE TABLE append_table (text_col text, a int);
SELECT create_distributed_table('append_table', 'a', 'append');
SELECT master_create_empty_shard('append_table') AS shardid1 \gset
SELECT master_create_empty_shard('append_table') AS shardid2 \gset
SELECT master_create_empty_shard('append_table') AS shardid3 \gset

COPY append_table (text_col, a) FROM STDIN WITH (format 'csv', append_to_shard :shardid1);
abc,234
bcd,123
bcd,234
cde,345
def,456
efg,234
\.

COPY append_table (text_col, a) FROM STDIN WITH (format 'csv', append_to_shard :shardid2);
abc,123
efg,123
hij,123
hij,234
ijk,1
jkl,0
\.

CREATE TABLE range_table(a int, b int);
SELECT create_distributed_table('range_table', 'a', 'range');
CALL public.create_range_partitioned_shards('range_table', '{"0","25"}','{"24","49"}');
INSERT INTO range_table VALUES (0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 50);

SET client_min_messages to DEBUG2;

-- simple insert
INSERT INTO nullkey_c1_t1 VALUES (1,2), (2,2), (3,4);
INSERT INTO nullkey_c1_t2 VALUES (1,3), (3,4), (5,1), (6,2);

INSERT INTO nullkey_c2_t1 VALUES (1,0), (2,5), (4,3), (5,2);
INSERT INTO nullkey_c2_t2 VALUES (2,4), (3,2), (5,2), (7,4);

-- simple select
SELECT * FROM nullkey_c1_t1 ORDER BY 1,2;

-- for update / share
SELECT * FROM modify_fast_path WHERE key = 1 FOR UPDATE;
SELECT * FROM modify_fast_path WHERE key = 1 FOR SHARE;
SELECT * FROM modify_fast_path FOR UPDATE;
SELECT * FROM modify_fast_path FOR SHARE;

-- cartesian product with different table types

--    with other table types
SELECT COUNT(*) FROM distributed_table d1, nullkey_c1_t1;
SELECT COUNT(*) FROM reference_table d1, nullkey_c1_t1;
SELECT COUNT(*) FROM citus_local_table d1, nullkey_c1_t1;
SELECT COUNT(*) FROM postgres_local_table d1, nullkey_c1_t1;

--    with a colocated single-shard table
SELECT COUNT(*) FROM nullkey_c1_t1 d1, nullkey_c1_t2;

--    with a non-colocated single-shard table
SELECT COUNT(*) FROM nullkey_c1_t1 d1, nullkey_c2_t1;

-- First, show that nullkey_c1_t1 and nullkey_c3_t1 are not colocated.
SELECT
  (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'query_single_shard_table.nullkey_c1_t1'::regclass) !=
  (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'query_single_shard_table.nullkey_c3_t1'::regclass);

-- Now verify that we can join them via router planner because it doesn't care
-- about whether two tables are colocated or not but physical location of shards
-- when citus.enable_non_colocated_router_query_pushdown is set to on.

SET citus.enable_non_colocated_router_query_pushdown TO ON;

SELECT COUNT(*) FROM nullkey_c1_t1 JOIN nullkey_c3_t1 USING(a);

SET citus.enable_non_colocated_router_query_pushdown TO OFF;

SELECT COUNT(*) FROM nullkey_c1_t1 JOIN nullkey_c3_t1 USING(a);

RESET citus.enable_non_colocated_router_query_pushdown;

-- colocated join between single-shard tables
SELECT COUNT(*) FROM nullkey_c1_t1 JOIN nullkey_c1_t2 USING(a);
SELECT COUNT(*) FROM nullkey_c1_t1 LEFT JOIN nullkey_c1_t2 USING(a);
SELECT COUNT(*) FROM nullkey_c1_t1 FULL JOIN nullkey_c1_t2 USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
LEFT JOIN LATERAL (
    SELECT * FROM nullkey_c1_t2 t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
JOIN LATERAL (
    SELECT * FROM nullkey_c1_t2 t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE EXISTS (
    SELECT * FROM nullkey_c1_t2 t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b IN (
    SELECT b+1 FROM nullkey_c1_t2 t2 WHERE t2.b = t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b NOT IN (
    SELECT a FROM nullkey_c1_t2 t2 WHERE t2.b > t1.a
);

-- non-colocated inner joins between single-shard tables
SELECT * FROM nullkey_c1_t1 JOIN nullkey_c2_t1 USING(a) ORDER BY 1,2,3;

SELECT COUNT(*) FROM nullkey_c1_t1 t1
JOIN LATERAL (
    SELECT * FROM nullkey_c2_t2 t2 WHERE t2.b > t1.a
) q USING(a);

-- non-colocated outer joins between single-shard tables
SELECT * FROM nullkey_c1_t1 LEFT JOIN nullkey_c2_t2 USING(a) ORDER BY 1,2,3 LIMIT 4;
SELECT * FROM nullkey_c1_t1 FULL JOIN nullkey_c2_t2 USING(a) ORDER BY 1,2,3 LIMIT 4;
SELECT * FROM nullkey_c1_t1 t1
LEFT JOIN LATERAL (
    SELECT * FROM nullkey_c2_t2 t2 WHERE t2.b > t1.a
) q USING(a) ORDER BY 1,2,3 OFFSET 3 LIMIT 4;

SELECT COUNT(*) FROM nullkey_c1_t1 t1
LEFT JOIN LATERAL (
    SELECT * FROM nullkey_c2_t2 t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE EXISTS (
    SELECT * FROM nullkey_c2_t2 t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b IN (
    SELECT b+1 FROM nullkey_c2_t2 t2 WHERE t2.b = t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b NOT IN (
    SELECT a FROM nullkey_c2_t2 t2 WHERE t2.b > t1.a
);

-- join with a reference table
SELECT COUNT(*) FROM nullkey_c1_t1, reference_table WHERE nullkey_c1_t1.a = reference_table.a;

WITH cte_1 AS
	(SELECT * FROM nullkey_c1_t1, reference_table WHERE nullkey_c1_t1.a = reference_table.a ORDER BY 1,2,3,4 FOR UPDATE)
SELECT COUNT(*) FROM cte_1;

-- join with postgres / citus local tables
SELECT * FROM nullkey_c1_t1 JOIN postgres_local_table USING(a);
SELECT * FROM nullkey_c1_t1 JOIN citus_local_table USING(a);

-- join with a distributed table
SELECT * FROM distributed_table d1 JOIN nullkey_c1_t1 USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
JOIN LATERAL (
    SELECT * FROM distributed_table t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM distributed_table t1
JOIN LATERAL (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
) q USING(a);

-- outer joins with different table types
SELECT COUNT(*) FROM nullkey_c1_t1 LEFT JOIN reference_table USING(a);
SELECT COUNT(*) FROM reference_table LEFT JOIN nullkey_c1_t1 USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 LEFT JOIN citus_local_table USING(a);
SELECT COUNT(*) FROM citus_local_table LEFT JOIN nullkey_c1_t1 USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 LEFT JOIN postgres_local_table USING(a);
SELECT COUNT(*) FROM postgres_local_table LEFT JOIN nullkey_c1_t1 USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 FULL JOIN citus_local_table USING(a);
SELECT COUNT(*) FROM nullkey_c1_t1 FULL JOIN postgres_local_table USING(a);
SELECT COUNT(*) FROM nullkey_c1_t1 FULL JOIN reference_table USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 JOIN append_table USING(a);
SELECT COUNT(*) FROM nullkey_c1_t1 JOIN range_table USING(a);

SET citus.enable_non_colocated_router_query_pushdown TO ON;

SELECT COUNT(*) FROM nullkey_c1_t1 JOIN range_table USING(a) WHERE range_table.a = 20;

SET citus.enable_non_colocated_router_query_pushdown TO OFF;

SELECT COUNT(*) FROM nullkey_c1_t1 JOIN range_table USING(a) WHERE range_table.a = 20;

RESET citus.enable_non_colocated_router_query_pushdown;

-- lateral / semi / anti joins with different table types

--    with a reference table
SELECT COUNT(*) FROM nullkey_c1_t1 t1
LEFT JOIN LATERAL (
    SELECT * FROM reference_table t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE EXISTS (
    SELECT * FROM reference_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE NOT EXISTS (
    SELECT * FROM reference_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b IN (
    SELECT b+1 FROM reference_table t2 WHERE t2.b = t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b NOT IN (
    SELECT a FROM reference_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
JOIN LATERAL (
    SELECT * FROM reference_table t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM reference_table t1
LEFT JOIN LATERAL (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM reference_table t1
WHERE EXISTS (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM reference_table t1
WHERE t1.b IN (
    SELECT b+1 FROM nullkey_c1_t1 t2 WHERE t2.b = t1.a
);

SELECT COUNT(*) FROM reference_table t1
WHERE t1.b NOT IN (
    SELECT a FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM reference_table t1
JOIN LATERAL (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
) q USING(a);

--    with a distributed table
SELECT COUNT(*) FROM nullkey_c1_t1 t1
LEFT JOIN LATERAL (
    SELECT * FROM distributed_table t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE EXISTS (
    SELECT * FROM distributed_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE NOT EXISTS (
    SELECT * FROM distributed_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b IN (
    SELECT b+1 FROM distributed_table t2 WHERE t2.b = t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b NOT IN (
    SELECT a FROM distributed_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM distributed_table t1
LEFT JOIN LATERAL (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM distributed_table t1
WHERE EXISTS (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM distributed_table t1
WHERE t1.b IN (
    SELECT b+1 FROM nullkey_c1_t1 t2 WHERE t2.b = t1.a
);

SELECT COUNT(*) FROM distributed_table t1
WHERE t1.b NOT IN (
    SELECT a FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
);

--    with postgres / citus local tables
SELECT COUNT(*) FROM nullkey_c1_t1 t1
LEFT JOIN LATERAL (
    SELECT * FROM citus_local_table t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE EXISTS (
    SELECT * FROM citus_local_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE NOT EXISTS (
    SELECT * FROM citus_local_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b IN (
    SELECT b+1 FROM citus_local_table t2 WHERE t2.b = t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b NOT IN (
    SELECT a FROM citus_local_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
JOIN LATERAL (
    SELECT * FROM citus_local_table t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM citus_local_table t1
LEFT JOIN LATERAL (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM postgres_local_table t1
LEFT JOIN LATERAL (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM citus_local_table t1
WHERE EXISTS (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM citus_local_table t1
WHERE t1.b IN (
    SELECT b+1 FROM nullkey_c1_t1 t2 WHERE t2.b = t1.a
);

SELECT COUNT(*) FROM citus_local_table t1
WHERE t1.b NOT IN (
    SELECT a FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM citus_local_table t1
JOIN LATERAL (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
LEFT JOIN LATERAL (
    SELECT * FROM postgres_local_table t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE EXISTS (
    SELECT * FROM postgres_local_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE NOT EXISTS (
    SELECT * FROM postgres_local_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b IN (
    SELECT b+1 FROM postgres_local_table t2 WHERE t2.b = t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
WHERE t1.b NOT IN (
    SELECT a FROM postgres_local_table t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM nullkey_c1_t1 t1
JOIN LATERAL (
    SELECT * FROM postgres_local_table t2 WHERE t2.b > t1.a
) q USING(a);

SELECT COUNT(*) FROM postgres_local_table t1
WHERE EXISTS (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM postgres_local_table t1
WHERE t1.b IN (
    SELECT b+1 FROM nullkey_c1_t1 t2 WHERE t2.b = t1.a
);

SELECT COUNT(*) FROM postgres_local_table t1
WHERE t1.b NOT IN (
    SELECT a FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
);

SELECT COUNT(*) FROM postgres_local_table t1
JOIN LATERAL (
    SELECT * FROM nullkey_c1_t1 t2 WHERE t2.b > t1.a
) q USING(a);

-- insert .. select

--    between two colocated single-shard tables

--    The target list of "distributed statement"s that we send to workers
--    differ(*) in Postgres versions < 15. For this reason, we temporarily
--    disable debug messages here and run the EXPLAIN'ed version of the
--    command.
--
--    (*):  < SELECT a, b > vs  < SELECT table_name.a, table_name.b >
SET client_min_messages TO WARNING;
EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, SUMMARY FALSE, VERBOSE FALSE)
INSERT INTO nullkey_c1_t1 SELECT * FROM nullkey_c1_t2;
SET client_min_messages TO DEBUG2;

--    between two non-colocated single-shard tables
INSERT INTO nullkey_c1_t1 SELECT * FROM nullkey_c2_t1;

--    between a single-shard table and a table of different type
SET client_min_messages TO WARNING;
EXPLAIN (ANALYZE TRUE, TIMING FALSE, COSTS FALSE, SUMMARY FALSE, VERBOSE FALSE)
INSERT INTO nullkey_c1_t1 SELECT * FROM reference_table;
SET client_min_messages TO DEBUG2;

INSERT INTO nullkey_c1_t1 SELECT * FROM distributed_table;
INSERT INTO nullkey_c1_t1 SELECT * FROM citus_local_table;
INSERT INTO nullkey_c1_t1 SELECT * FROM postgres_local_table;

INSERT INTO reference_table SELECT * FROM nullkey_c1_t1;
INSERT INTO distributed_table SELECT * FROM nullkey_c1_t1;
INSERT INTO citus_local_table SELECT * FROM nullkey_c1_t1;
INSERT INTO postgres_local_table SELECT * FROM nullkey_c1_t1;

-- test subquery
SELECT count(*) FROM
(
	SELECT * FROM (SELECT * FROM nullkey_c1_t2) as subquery_inner
) AS subquery_top;

-- test cte inlining
WITH cte_nullkey_c1_t1 AS (SELECT * FROM nullkey_c1_t1),
     cte_postgres_local_table AS (SELECT * FROM postgres_local_table),
     cte_distributed_table AS (SELECT * FROM distributed_table)
SELECT COUNT(*) FROM cte_distributed_table, cte_nullkey_c1_t1, cte_postgres_local_table
WHERE cte_nullkey_c1_t1.a > 3 AND cte_distributed_table.a < 5;

-- test recursive ctes
WITH level_0 AS (
  WITH level_1 AS (
    WITH RECURSIVE level_2_recursive(x) AS (
        VALUES (1)
      UNION ALL
        SELECT a + 1 FROM nullkey_c1_t1 JOIN level_2_recursive ON (a = x) WHERE a < 2
    )
    SELECT * FROM level_2_recursive RIGHT JOIN reference_table ON (level_2_recursive.x = reference_table.a)
  )
  SELECT * FROM level_1
)
SELECT COUNT(*) FROM level_0;

WITH level_0 AS (
  WITH level_1 AS (
    WITH RECURSIVE level_2_recursive(x) AS (
        VALUES (1)
      UNION ALL
        SELECT a + 1 FROM nullkey_c1_t1 JOIN level_2_recursive ON (a = x) WHERE a < 100
    )
    SELECT * FROM level_2_recursive JOIN distributed_table ON (level_2_recursive.x = distributed_table.a)
  )
  SELECT * FROM level_1
)
SELECT COUNT(*) FROM level_0;

-- grouping set
SELECT
	id, substring(title, 2, 1) AS subtitle, count(*)
	FROM articles_hash
	WHERE author_id = 1 or author_id = 2
	GROUP BY GROUPING SETS ((id),(subtitle))
	ORDER BY id, subtitle;

-- subquery in SELECT clause
SELECT a.title AS name, (SELECT a2.id FROM articles_hash a2 WHERE a.id = a2.id  LIMIT 1)
						 AS special_price FROM articles_hash a
ORDER BY 1,2;

-- test prepared statements

-- prepare queries can be router plannable
PREPARE author_1_articles as
	SELECT *
	FROM articles_hash
	WHERE author_id = 1;

EXECUTE author_1_articles;
EXECUTE author_1_articles;
EXECUTE author_1_articles;
EXECUTE author_1_articles;
EXECUTE author_1_articles;
EXECUTE author_1_articles;

-- parametric prepare queries can be router plannable
PREPARE author_articles(int) as
	SELECT *
	FROM articles_hash
	WHERE author_id = $1;

EXECUTE author_articles(1);
EXECUTE author_articles(1);
EXECUTE author_articles(1);
EXECUTE author_articles(1);
EXECUTE author_articles(1);
EXECUTE author_articles(1);

EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);
EXECUTE author_articles(NULL);

PREPARE author_articles_update(int) AS
	UPDATE articles_hash SET title = 'test' WHERE author_id = $1;

EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);
EXECUTE author_articles_update(NULL);

-- More tests with insert .. select.
--
-- The target list of "distributed statement"s that we send to workers
-- might differ(*) in Postgres versions < 15 and they are reported when
-- "log level >= DEBUG2". For this reason, we set log level to DEBUG1 to
-- avoid reporting them.
--
-- DEBUG1 still allows reporting the reason why given INSERT .. SELECT
-- query is not distributed / requires pull-to-coordinator.

SET client_min_messages TO DEBUG1;

INSERT INTO bigserial_test (x, y) SELECT x, y FROM bigserial_test;

INSERT INTO bigserial_test (x, y) SELECT a, a FROM reference_table;

INSERT INTO agg_events
            (user_id)
SELECT f2.id FROM

(SELECT
      id
FROM   (SELECT users_ref_table.user_id      AS id
        FROM   raw_events_first,
               users_ref_table
        WHERE  raw_events_first.user_id = users_ref_table.user_id ) AS foo) as f
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

-- upsert with returning
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

-- using a left join
INSERT INTO agg_events (user_id)
SELECT
  raw_events_first.user_id
FROM
  raw_events_first LEFT JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.user_id
  WHERE raw_events_second.user_id = 10 OR raw_events_second.user_id = 11;

INSERT INTO agg_events (user_id)
SELECT
  users_ref_table.user_id
FROM
  users_ref_table LEFT JOIN raw_events_second ON users_ref_table.user_id = raw_events_second.user_id
  WHERE raw_events_second.user_id = 10 OR raw_events_second.user_id = 11;

INSERT INTO agg_events (user_id)
SELECT COALESCE(raw_events_first.user_id, users_ref_table.user_id)
FROM raw_events_first
     RIGHT JOIN (users_ref_table LEFT JOIN raw_events_second ON users_ref_table.user_id = raw_events_second.user_id)
     ON raw_events_first.user_id = users_ref_table.user_id;

-- using a full join
INSERT INTO agg_events (user_id, value_1_agg)
SELECT t1.user_id AS col1,
       t2.user_id AS col2
FROM   raw_events_first t1
       FULL JOIN raw_events_second t2
              ON t1.user_id = t2.user_id;

-- using semi join
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  user_id IN (SELECT raw_events_second.user_id
                   FROM   raw_events_second, raw_events_first
                   WHERE  raw_events_second.user_id = raw_events_first.user_id AND raw_events_first.user_id = 200);

-- using lateral join
INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   raw_events_first
WHERE  NOT EXISTS (SELECT 1
                   FROM   raw_events_second
                   WHERE  raw_events_second.user_id =raw_events_first.user_id);

INSERT INTO raw_events_second
            (user_id)
SELECT user_id
FROM   users_ref_table
WHERE  NOT EXISTS (SELECT 1
                   FROM   raw_events_second
                   WHERE  raw_events_second.user_id = users_ref_table.user_id);

-- using inner join
INSERT INTO agg_events (user_id)
SELECT raw_events_first.user_id
FROM raw_events_first INNER JOIN raw_events_second ON raw_events_first.user_id = raw_events_second.value_1
WHERE raw_events_first.value_1 IN (10, 11,12) OR raw_events_second.user_id IN (1,2,3,4);

INSERT INTO agg_events (user_id)
SELECT raw_events_first.user_id
FROM raw_events_first INNER JOIN users_ref_table ON raw_events_first.user_id = users_ref_table.user_id
WHERE raw_events_first.value_1 IN (10, 11,12) OR users_ref_table.user_id IN (1,2,3,4);

-- We could relax distributed insert .. select checks to allow pushing
-- down more clauses down to the worker nodes when inserting into a single
-- shard by selecting from a colocated one. We might want to do something
-- like https://github.com/citusdata/citus/pull/6772.
--
--  e.g., insert into null_shard_key_1/citus_local/reference
--        select * from null_shard_key_1/citus_local/reference limit 1
--
-- Below "limit / offset clause" test and some others are examples of this.

-- limit / offset clause
INSERT INTO agg_events (user_id) SELECT raw_events_first.user_id FROM raw_events_first LIMIT 1;
INSERT INTO agg_events (user_id) SELECT raw_events_first.user_id FROM raw_events_first OFFSET 1;
INSERT INTO agg_events (user_id) SELECT users_ref_table.user_id FROM users_ref_table LIMIT 1;

-- using a materialized cte
WITH cte AS MATERIALIZED
  (SELECT max(value_1)+1 as v1_agg, user_id FROM raw_events_first GROUP BY user_id)
INSERT INTO agg_events (value_1_agg, user_id)
SELECT v1_agg, user_id FROM cte;

INSERT INTO raw_events_second
  WITH cte AS MATERIALIZED (SELECT * FROM raw_events_first)
  SELECT user_id * 1000, time, value_1, value_2, value_3, value_4 FROM cte;

INSERT INTO raw_events_second (user_id)
  WITH cte AS MATERIALIZED (SELECT * FROM users_ref_table)
  SELECT user_id FROM cte;

-- using a regular cte
WITH cte AS (SELECT * FROM raw_events_first)
INSERT INTO raw_events_second
  SELECT user_id * 7000, time, value_1, value_2, value_3, value_4 FROM cte;

INSERT INTO raw_events_second
  WITH cte AS (SELECT * FROM raw_events_first)
  SELECT * FROM cte;

INSERT INTO agg_events
  WITH sub_cte AS (SELECT 1)
  SELECT
    raw_events_first.user_id, (SELECT * FROM sub_cte)
  FROM
    raw_events_first;

-- we still support complex joins via INSERT's cte list ..
WITH cte AS (
    SELECT DISTINCT(reference_table.a) AS a, 1 AS b
    FROM distributed_table RIGHT JOIN reference_table USING (a)
)
INSERT INTO raw_events_second (user_id, value_1)
  SELECT (a+5)*-1, b FROM cte;

-- .. and via SELECT's cte list too
INSERT INTO raw_events_second (user_id, value_1)
WITH cte AS (
    SELECT DISTINCT(reference_table.a) AS a, 1 AS b
    FROM distributed_table RIGHT JOIN reference_table USING (a)
)
  SELECT (a+5)*2, b FROM cte;

-- using set operations
INSERT INTO
  raw_events_first(user_id)
  (SELECT user_id FROM raw_events_first) INTERSECT
  (SELECT user_id FROM raw_events_first);

INSERT INTO
  raw_events_first(user_id)
  (SELECT user_id FROM users_ref_table) INTERSECT
  (SELECT user_id FROM raw_events_first);

-- group by clause inside subquery
INSERT INTO agg_events
            (user_id)
SELECT f2.id FROM

(SELECT
      id
FROM   (SELECT raw_events_second.user_id      AS id
        FROM   raw_events_first,
               raw_events_second
        WHERE  raw_events_first.user_id = raw_events_second.user_id ) AS foo) as f
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

-- group by clause inside lateral subquery
INSERT INTO agg_events (user_id, value_4_agg)
SELECT
  averages.user_id, avg(averages.value_4)
FROM
    (SELECT
      t1.user_id
    FROM
      raw_events_second t1 JOIN raw_events_second t2 on (t1.user_id = t2.user_id)
    ) reference_ids
  JOIN LATERAL
    (SELECT
      user_id, value_4
    FROM
      raw_events_first) as averages ON averages.value_4 = reference_ids.user_id
    GROUP BY averages.user_id;

-- using aggregates
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

INSERT INTO agg_events (value_3_agg, value_1_agg)
SELECT AVG(user_id), SUM(user_id)
FROM users_ref_table
GROUP BY user_id;

-- using generate_series
INSERT INTO raw_events_first (user_id, value_1, value_2)
SELECT s, s, s FROM generate_series(1, 5) s;

CREATE SEQUENCE insert_select_test_seq;

-- nextval() expression in select's targetlist
INSERT INTO raw_events_first (user_id, value_1, value_2)
SELECT s, nextval('insert_select_test_seq'), (random()*10)::int
FROM generate_series(100, 105) s;

-- non-immutable function
INSERT INTO modify_fast_path (key, value_1) VALUES (2,1) RETURNING value_1, random() * key;

SET client_min_messages TO DEBUG2;

-- update / delete

UPDATE nullkey_c1_t1 SET a = 1 WHERE b = 5;
UPDATE nullkey_c1_t1 SET a = 1 WHERE a = 5;
UPDATE nullkey_c1_t1 SET a = random();
UPDATE nullkey_c1_t1 SET a = 1 WHERE a = random();

DELETE FROM nullkey_c1_t1 WHERE b = 5;
DELETE FROM nullkey_c1_t1 WHERE a = random();

-- simple update queries between different table types / colocated tables
UPDATE nullkey_c1_t1 SET b = 5 FROM nullkey_c1_t2 WHERE nullkey_c1_t1.b = nullkey_c1_t2.b;
UPDATE nullkey_c1_t1 SET b = 5 FROM nullkey_c2_t1 WHERE nullkey_c1_t1.b = nullkey_c2_t1.b;
UPDATE nullkey_c1_t1 SET b = 5 FROM reference_table WHERE nullkey_c1_t1.b = reference_table.b;
UPDATE nullkey_c1_t1 SET b = 5 FROM distributed_table WHERE nullkey_c1_t1.b = distributed_table.b;
UPDATE nullkey_c1_t1 SET b = 5 FROM distributed_table WHERE nullkey_c1_t1.b = distributed_table.a;
UPDATE nullkey_c1_t1 SET b = 5 FROM citus_local_table WHERE nullkey_c1_t1.b = citus_local_table.b;
UPDATE nullkey_c1_t1 SET b = 5 FROM postgres_local_table WHERE nullkey_c1_t1.b = postgres_local_table.b;

UPDATE reference_table SET b = 5 FROM nullkey_c1_t1 WHERE nullkey_c1_t1.b = reference_table.b;
UPDATE distributed_table SET b = 5 FROM nullkey_c1_t1 WHERE nullkey_c1_t1.b = distributed_table.b;
UPDATE distributed_table SET b = 5 FROM nullkey_c1_t1 WHERE nullkey_c1_t1.b = distributed_table.a;
UPDATE citus_local_table SET b = 5 FROM nullkey_c1_t1 WHERE nullkey_c1_t1.b = citus_local_table.b;
UPDATE postgres_local_table SET b = 5 FROM nullkey_c1_t1 WHERE nullkey_c1_t1.b = postgres_local_table.b;

-- simple delete queries between different table types / colocated tables
DELETE FROM nullkey_c1_t1 USING nullkey_c1_t2 WHERE nullkey_c1_t1.b = nullkey_c1_t2.b;
DELETE FROM nullkey_c1_t1 USING nullkey_c2_t1 WHERE nullkey_c1_t1.b = nullkey_c2_t1.b;
DELETE FROM nullkey_c1_t1 USING reference_table WHERE nullkey_c1_t1.b = reference_table.b;
DELETE FROM nullkey_c1_t1 USING distributed_table WHERE nullkey_c1_t1.b = distributed_table.b;
DELETE FROM nullkey_c1_t1 USING distributed_table WHERE nullkey_c1_t1.b = distributed_table.a;
DELETE FROM nullkey_c1_t1 USING citus_local_table WHERE nullkey_c1_t1.b = citus_local_table.b;
DELETE FROM nullkey_c1_t1 USING postgres_local_table WHERE nullkey_c1_t1.b = postgres_local_table.b;

DELETE FROM reference_table USING nullkey_c1_t1 WHERE nullkey_c1_t1.b = reference_table.b;
DELETE FROM distributed_table USING nullkey_c1_t1 WHERE nullkey_c1_t1.b = distributed_table.b;
DELETE FROM distributed_table USING nullkey_c1_t1 WHERE nullkey_c1_t1.b = distributed_table.a;
DELETE FROM citus_local_table USING nullkey_c1_t1 WHERE nullkey_c1_t1.b = citus_local_table.b;
DELETE FROM postgres_local_table USING nullkey_c1_t1 WHERE nullkey_c1_t1.b = postgres_local_table.b;

-- slightly more complex update queries
UPDATE nullkey_c1_t1 SET b = 5 WHERE nullkey_c1_t1.b IN (SELECT b FROM distributed_table);

WITH cte AS materialized(
    SELECT * FROM distributed_table
)
UPDATE nullkey_c1_t1 SET b = 5 FROM cte WHERE nullkey_c1_t1.b = cte.a;

WITH cte AS (
    SELECT reference_table.a AS a, 1 AS b
    FROM distributed_table RIGHT JOIN reference_table USING (a)
)
UPDATE nullkey_c1_t1 SET b = 5 WHERE nullkey_c1_t1.b IN (SELECT b FROM cte);

UPDATE nullkey_c1_t1 SET b = 5 FROM reference_table WHERE EXISTS (
    SELECT 1 FROM reference_table LEFT JOIN nullkey_c1_t1 USING (a) WHERE nullkey_c1_t1.b IS NULL
);

UPDATE nullkey_c1_t1 tx SET b = (
    SELECT nullkey_c1_t2.b FROM nullkey_c1_t2 JOIN nullkey_c1_t1 ON (nullkey_c1_t1.a != nullkey_c1_t2.a) WHERE nullkey_c1_t1.a = tx.a ORDER BY 1 LIMIT 1
);

UPDATE nullkey_c1_t1 tx SET b = t2.b FROM nullkey_c1_t1 t1 JOIN nullkey_c1_t2 t2 ON (t1.a = t2.a);

WITH cte AS (
    SELECT * FROM nullkey_c1_t2 ORDER BY 1,2 LIMIT 10
)
UPDATE nullkey_c1_t1 SET b = 5 WHERE nullkey_c1_t1.a IN (SELECT b FROM cte);

UPDATE modify_fast_path SET value_1 = value_1 + 12 * value_1 WHERE key = 1;
UPDATE modify_fast_path SET value_1 = NULL WHERE value_1 = 15 AND (key = 1 OR value_2 = 'citus');
UPDATE modify_fast_path SET value_1 = 5 WHERE key = 2 RETURNING value_1 * 15, value_1::numeric * 16;
UPDATE modify_fast_path
	SET value_1 = 1
	FROM modify_fast_path_reference
	WHERE
		modify_fast_path.key = modify_fast_path_reference.key AND
		modify_fast_path.key  = 1 AND
		modify_fast_path_reference.key = 1;

PREPARE p1 (int, int, int) AS
	UPDATE modify_fast_path SET value_1 = value_1 + $1 WHERE key = $2 AND value_1 = $3;
EXECUTE p1(1,1,1);
EXECUTE p1(2,2,2);
EXECUTE p1(3,3,3);
EXECUTE p1(4,4,4);
EXECUTE p1(5,5,5);
EXECUTE p1(6,6,6);
EXECUTE p1(7,7,7);

PREPARE prepared_zero_shard_update(int) AS UPDATE modify_fast_path SET value_1 = 1 WHERE key = $1 AND false;
EXECUTE prepared_zero_shard_update(1);
EXECUTE prepared_zero_shard_update(2);
EXECUTE prepared_zero_shard_update(3);
EXECUTE prepared_zero_shard_update(4);
EXECUTE prepared_zero_shard_update(5);
EXECUTE prepared_zero_shard_update(6);
EXECUTE prepared_zero_shard_update(7);

-- slightly more complex delete queries
DELETE FROM nullkey_c1_t1 WHERE nullkey_c1_t1.b IN (SELECT b FROM distributed_table);

WITH cte AS materialized(
    SELECT * FROM distributed_table
)
DELETE FROM nullkey_c1_t1 USING cte WHERE nullkey_c1_t1.b = cte.a;

WITH cte AS (
    SELECT reference_table.a AS a, 1 AS b
    FROM distributed_table RIGHT JOIN reference_table USING (a)
)
DELETE FROM nullkey_c1_t1 WHERE nullkey_c1_t1.b IN (SELECT b FROM cte);

DELETE FROM nullkey_c1_t1 USING reference_table WHERE EXISTS (
    SELECT 1 FROM reference_table LEFT JOIN nullkey_c1_t1 USING (a) WHERE nullkey_c1_t1.b IS NULL
);

DELETE FROM nullkey_c1_t1 tx USING nullkey_c1_t1 t1 JOIN nullkey_c1_t2 t2 ON (t1.a = t2.a);

WITH cte AS (
    SELECT * FROM nullkey_c1_t2 ORDER BY 1,2 LIMIT 10
)
DELETE FROM nullkey_c1_t1 WHERE nullkey_c1_t1.a IN (SELECT b FROM cte);

DELETE FROM modify_fast_path WHERE value_1 = 15 AND (key = 1 OR value_2 = 'citus');
DELETE FROM modify_fast_path WHERE key = 2 RETURNING value_1 * 15, value_1::numeric * 16;
DELETE FROM modify_fast_path
	USING modify_fast_path_reference
	WHERE
		modify_fast_path.key = modify_fast_path_reference.key AND
		modify_fast_path.key  = 1 AND
		modify_fast_path_reference.key = 1;

PREPARE p2 (int, int, int) AS
	DELETE FROM modify_fast_path WHERE key = ($2)*$1 AND value_1 = $3;
EXECUTE p2(1,1,1);
EXECUTE p2(2,2,2);
EXECUTE p2(3,3,3);
EXECUTE p2(4,4,4);
EXECUTE p2(5,5,5);
EXECUTE p2(6,6,6);
EXECUTE p2(7,7,7);

PREPARE prepared_zero_shard_delete(int) AS DELETE FROM modify_fast_path WHERE key = $1 AND false;
EXECUTE prepared_zero_shard_delete(1);
EXECUTE prepared_zero_shard_delete(2);
EXECUTE prepared_zero_shard_delete(3);
EXECUTE prepared_zero_shard_delete(4);
EXECUTE prepared_zero_shard_delete(5);
EXECUTE prepared_zero_shard_delete(6);
EXECUTE prepared_zero_shard_delete(7);

-- test modifying ctes

WITH cte AS (
    UPDATE modify_fast_path SET value_1 = value_1 + 1 WHERE key = 1 RETURNING *
)
SELECT * FROM cte;

WITH cte AS (
    DELETE FROM modify_fast_path WHERE key = 1 RETURNING *
)
SELECT * FROM modify_fast_path;

WITH cte AS (
    DELETE FROM modify_fast_path WHERE key = 1 RETURNING *
)
SELECT * FROM modify_fast_path_reference WHERE key IN (SELECT key FROM cte);

WITH cte AS (
    DELETE FROM reference_table WHERE a = 1 RETURNING *
)
SELECT * FROM nullkey_c1_t1 WHERE a IN (SELECT a FROM cte);

WITH cte AS (
    DELETE FROM nullkey_c1_t1 WHERE a = 1 RETURNING *
)
SELECT * FROM nullkey_c1_t2 WHERE a IN (SELECT a FROM cte);

WITH cte AS (
    DELETE FROM nullkey_c1_t1 WHERE a = 1 RETURNING *
)
SELECT * FROM nullkey_c2_t1 WHERE a IN (SELECT a FROM cte);

WITH cte AS (
    DELETE FROM nullkey_c1_t1 WHERE a = 1 RETURNING *
)
SELECT * FROM distributed_table WHERE a IN (SELECT a FROM cte);

-- Below two queries fail very late when
-- citus.enable_non_colocated_router_query_pushdown is set to on.

SET citus.enable_non_colocated_router_query_pushdown TO ON;

WITH cte AS (
    DELETE FROM distributed_table WHERE a = 1 RETURNING *
)
SELECT * FROM nullkey_c1_t1 WHERE a IN (SELECT a FROM cte);

WITH cte AS (
    DELETE FROM distributed_table WHERE a = 1 RETURNING *
)
SELECT * FROM nullkey_c1_t1 WHERE b IN (SELECT b FROM cte);

SET citus.enable_non_colocated_router_query_pushdown TO OFF;

WITH cte AS (
    DELETE FROM distributed_table WHERE a = 1 RETURNING *
)
SELECT * FROM nullkey_c1_t1 WHERE a IN (SELECT a FROM cte);

WITH cte AS (
    DELETE FROM distributed_table WHERE a = 1 RETURNING *
)
SELECT * FROM nullkey_c1_t1 WHERE b IN (SELECT b FROM cte);

RESET citus.enable_non_colocated_router_query_pushdown;

WITH cte AS (
    UPDATE modify_fast_path SET value_1 = value_1 + 1 WHERE key = 1 RETURNING *
)
UPDATE modify_fast_path SET value_1 = value_1 + 1 WHERE key = 1;

WITH cte AS (
    DELETE FROM modify_fast_path WHERE key = 1 RETURNING *
)
DELETE FROM modify_fast_path WHERE key = 1;

-- test window functions

SELECT
	user_id, avg(avg(value_3)) OVER (PARTITION BY user_id, MIN(value_2))
FROM
	raw_events_first
GROUP BY
	1
ORDER BY
	2 DESC NULLS LAST, 1 DESC;

SELECT
	user_id, max(value_1) OVER (PARTITION BY user_id, MIN(value_2))
FROM (
	SELECT
		DISTINCT us.user_id, us.value_2, us.value_1, random() as r1
	FROM
		raw_events_first as us, raw_events_second
	WHERE
		us.user_id = raw_events_second.user_id
	ORDER BY
		user_id, value_2
	) s
GROUP BY
	1, value_1
ORDER BY
	2 DESC, 1;

SELECT
	DISTINCT ON (raw_events_second.user_id, rnk) raw_events_second.user_id, rank() OVER my_win AS rnk
FROM
	raw_events_second, raw_events_first
WHERE
	raw_events_first.user_id = raw_events_second.user_id
WINDOW
	my_win AS (PARTITION BY raw_events_second.user_id, raw_events_first.value_1 ORDER BY raw_events_second.time DESC)
ORDER BY
	rnk DESC, 1 DESC
LIMIT 10;

SET client_min_messages TO ERROR;
DROP SCHEMA query_single_shard_table CASCADE;
