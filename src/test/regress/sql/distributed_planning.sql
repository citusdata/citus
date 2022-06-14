SET search_path TO "distributed planning";

-- Confirm the basics work
INSERT INTO test VALUES (1, 2), (3, 4), (5, 6), (2, 7), (4, 5);
INSERT INTO test VALUES (6, 7);
-- Insert two edge case values, the first value hashes to MAX_INT32 and the
-- second hashes to MIN_INT32. This should not break anything, but these cases
-- caused some crashes in the past.
-- See c6c31e0f1fe5b8cc955b0da42264578dcdae16cc and
-- 683279cc366069db9c2ccaca85dfaf8572113cda for details.
--
-- These specific values were found by using the following two queries:
-- select q.i from (select generate_series(0, 10000000000000) i) q where hashint8(q.i) = 2147483647 limit 1;
-- select q.i from (select generate_series(0, 10000000000000) i) q where hashint8(q.i) = -2147483648 limit 1;
INSERT INTO test VALUES (2608474032, 2608474032), (963809240, 963809240);
SELECT * FROM test WHERE x = 1 ORDER BY y, x;
-- Confirm that hash values are as expected
SELECT hashint8(x) FROM test ORDER BY 1;
SELECT t1.x, t2.y FROM test t1 JOIN test t2 USING(x) WHERE t1.x = 1 AND t2.x = 1 ORDER BY t2.y, t1.x;
SELECT * FROM test WHERE x = 1 OR x = 2 ORDER BY y, x;
SELECT count(*) FROM test;
SELECT * FROM test ORDER BY x;
WITH cte_1 AS (UPDATE test SET y = y - 1 RETURNING *) SELECT * FROM cte_1 ORDER BY 1,2;

-- observe that there is a conflict and the following query does nothing
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT DO NOTHING RETURNING *;

-- same as the above with different syntax
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT (part_key) DO NOTHING RETURNING *;

-- again the same query with another syntax
INSERT INTO upsert_test (part_key, other_col) VALUES (1, 1) ON CONFLICT ON CONSTRAINT upsert_test_part_key_key DO NOTHING RETURNING *;

BEGIN;

	-- force local execution if possible
	SELECT count(*) FROM upsert_test WHERE part_key = 1;

	-- multi-shard pushdown query that goes through local execution
	INSERT INTO upsert_test (part_key, other_col) SELECT part_key, other_col FROM upsert_test ON CONFLICT ON CONSTRAINT upsert_test_part_key_key DO NOTHING RETURNING *;
	INSERT INTO upsert_test (part_key, other_col) SELECT part_key, other_col FROM upsert_test ON CONFLICT ON CONSTRAINT upsert_test_part_key_key DO UPDATE SET other_col=EXCLUDED.other_col + 1 RETURNING *;

	-- multi-shard pull-to-coordinator query that goes through local execution
	INSERT INTO upsert_test (part_key, other_col) SELECT part_key, other_col FROM upsert_test LIMIT 100 ON CONFLICT ON CONSTRAINT upsert_test_part_key_key DO NOTHING RETURNING *;
	INSERT INTO upsert_test (part_key, other_col) SELECT part_key, other_col FROM upsert_test LIMIT 100 ON CONFLICT ON CONSTRAINT upsert_test_part_key_key DO UPDATE SET other_col=EXCLUDED.other_col + 1 RETURNING *;

COMMIT;


BEGIN;
	INSERT INTO  test SELECT i,i from generate_series(0,1000)i;

	-- only pulls 1 row, should not hit the limit
	WITH cte_1 AS (SELECT * FROM test LIMIT 1) SELECT count(*) FROM cte_1;

	-- cte_1 only pulls 1 row, but cte_2 all rows
	WITH cte_1 AS (SELECT * FROM test LIMIT 1),
	     cte_2 AS (SELECT * FROM test OFFSET 0)
	SELECT count(*) FROM cte_1, cte_2;
ROLLBACK;



-- single shard and multi-shard delete
-- inside a transaction block
BEGIN;
	DELETE FROM test WHERE y = 5;
	INSERT INTO test VALUES (4, 5);

	DELETE FROM test WHERE x = 1;
	INSERT INTO test VALUES (1, 2);
COMMIT;


-- basic view queries
SET citus.enable_ddl_propagation TO OFF;
CREATE VIEW simple_view AS
	SELECT count(*) as cnt FROM test t1 JOIN test t2 USING (x);
RESET citus.enable_ddl_propagation;
SELECT * FROM simple_view;
SELECT * FROM simple_view, test WHERE test.x = simple_view.cnt;

BEGIN;
	COPY test(x) FROM STDIN;
1
2
3
4
5
6
7
8
9
10
\.
	SELECT count(*) FROM test;
	COPY (SELECT count(DISTINCT x) FROM test) TO STDOUT;
	INSERT INTO test SELECT i,i FROM generate_series(0,100)i;
ROLLBACK;

-- prepared statements with custom types
PREPARE single_node_prepare_p1(int, int, new_type) AS
	INSERT INTO test_2 VALUES ($1, $2, $3);

EXECUTE single_node_prepare_p1(1, 1, (95, 'citus9.5')::new_type);
EXECUTE single_node_prepare_p1(2 ,2, (94, 'citus9.4')::new_type);
EXECUTE single_node_prepare_p1(3 ,2, (93, 'citus9.3')::new_type);
EXECUTE single_node_prepare_p1(4 ,2, (92, 'citus9.2')::new_type);
EXECUTE single_node_prepare_p1(5 ,2, (91, 'citus9.1')::new_type);
EXECUTE single_node_prepare_p1(6 ,2, (90, 'citus9.0')::new_type);
EXECUTE single_node_prepare_p1(6 ,2, (90, 'citus9.0')::new_type);

PREPARE use_local_query_cache(int) AS SELECT count(*) FROM test_2 WHERE x =  $1;

EXECUTE use_local_query_cache(1);
EXECUTE use_local_query_cache(1);
EXECUTE use_local_query_cache(1);
EXECUTE use_local_query_cache(1);
EXECUTE use_local_query_cache(1);
EXECUTE use_local_query_cache(1);
EXECUTE use_local_query_cache(1);


BEGIN;
	INSERT INTO test_2 VALUES (7 ,2, (83, 'citus8.3')::new_type);
	SAVEPOINT s1;
	INSERT INTO test_2 VALUES (9 ,1, (82, 'citus8.2')::new_type);
	SAVEPOINT s2;
	ROLLBACK TO SAVEPOINT s1;
	SELECT * FROM test_2 WHERE z = (83, 'citus8.3')::new_type OR z = (82, 'citus8.2')::new_type;
	RELEASE SAVEPOINT s1;
COMMIT;

SELECT * FROM test_2 WHERE z = (83, 'citus8.3')::new_type OR z = (82, 'citus8.2')::new_type;

WITH cte_1 AS (SELECT * FROM test_2) SELECT * FROM cte_1 ORDER BY 1,2;

-- final query is router query
WITH cte_1 AS (SELECT * FROM test_2) SELECT * FROM cte_1, test_2 WHERE  test_2.x = cte_1.x AND test_2.x = 7 ORDER BY 1,2;

-- final query is a distributed query
WITH cte_1 AS (SELECT * FROM test_2) SELECT * FROM cte_1, test_2 WHERE  test_2.x = cte_1.x AND test_2.y != 2 ORDER BY 1,2;

SELECT count(DISTINCT x) FROM test;
SELECT count(DISTINCT y) FROM test;

-- query pushdown should work
SELECT
	*
FROM
	(SELECT x, count(*) FROM test_2 GROUP BY x) as foo,
	(SELECT x, count(*) FROM test_2 GROUP BY x) as bar
WHERE
	foo.x = bar.x
ORDER BY 1 DESC, 2 DESC, 3 DESC, 4 DESC
LIMIT 1;

-- Check repartition joins are supported
SELECT * FROM test t1, test t2 WHERE t1.x = t2.y ORDER BY t1.x, t2.x, t1.y, t2.y;


-- INSERT SELECT router
BEGIN;
INSERT INTO test(x, y) SELECT x, y FROM test WHERE x = 1;
SELECT count(*) from test;
ROLLBACK;


-- INSERT SELECT pushdown
BEGIN;
INSERT INTO test(x, y) SELECT x, y FROM test;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT analytical query
BEGIN;
INSERT INTO test(x, y) SELECT count(x), max(y) FROM test;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT repartition
BEGIN;
INSERT INTO test(x, y) SELECT y, x FROM test;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT from reference table into distributed
BEGIN;
INSERT INTO test(x, y) SELECT a, b FROM ref;
SELECT count(*) from test;
ROLLBACK;


-- INSERT SELECT from local table into distributed
BEGIN;
INSERT INTO test(x, y) SELECT c, d FROM local;
SELECT count(*) from test;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO ref(a, b) SELECT x, y FROM test;
SELECT count(*) from ref;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO ref(a, b) SELECT c, d FROM local;
SELECT count(*) from ref;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO local(c, d) SELECT x, y FROM test;
SELECT count(*) from local;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO local(c, d) SELECT a, b FROM ref;
SELECT count(*) from local;
ROLLBACK;

-- Confirm that dummy placements work
SELECT count(*) FROM test WHERE false;
SELECT count(*) FROM test WHERE false GROUP BY GROUPING SETS (x,y);

SELECT count(*) FROM test;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO ref(a, b) SELECT x, y FROM test;
SELECT count(*) from ref;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO ref(a, b) SELECT c, d FROM local;
SELECT count(*) from ref;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO local(c, d) SELECT x, y FROM test;
SELECT count(*) from local;
ROLLBACK;

-- INSERT SELECT from distributed table to local table
BEGIN;
INSERT INTO local(c, d) SELECT a, b FROM ref;
SELECT count(*) from local;
ROLLBACK;

-- values materialization test
WITH cte_1 (num,letter) AS (VALUES (1, 'one'), (2, 'two'), (3, 'three'))
SELECT
	count(*)
FROM
	test
WHERE x IN (SELECT num FROM cte_1);


BEGIN;
INSERT INTO ref VALUES (1, 1), (3, 3), (4, 4);
INSERT INTO ref2 VALUES (1, 1), (2, 2), (3, 3), (5, 5);
-- multiple CTEs working together
-- this qeury was reported as a bug by a user
WITH cte_ref AS (
    SELECT a as abc from ref where a IN (1, 2)
),
cte_join1 AS (
    SELECT
       2 as result
    FROM cte_ref
    JOIN ref2 AS reff ON reff.b = cte_ref.abc
),
cte_join2 AS (
    SELECT
       1 as result
    FROM cte_ref
    LEFT JOIN test
    ON test.x = cte_ref.abc
    WHERE test.x IN (1, 2)
      AND x > 0
),
cte_with_subq AS (
    SELECT
      (SELECT result FROM cte_join1 where result::int > 0) as result
    FROM cte_join2
    UNION ALL
    SELECT
      (SELECT result FROM cte_join1 where result::int < 10) as result
    FROM cte_ref
)

SELECT * from cte_with_subq
UNION ALL SELECT 3 FROM cte_join1
UNION ALL SELECT 4 FROM cte_ref
ORDER BY 1;

ROLLBACK;

prepare p1 as INSERT INTO t1(a,c) VALUES (15, 15) ON CONFLICT (c) DO UPDATE SET a=EXCLUDED.a + 10 RETURNING *;
execute p1(5);
execute p1(5);
execute p1(5);
execute p1(5);
execute p1(5);
execute p1(5);
execute p1(5);

prepare p5(int) as INSERT INTO t1(a,c) VALUES (15, $1) ON CONFLICT (c) DO UPDATE SET a=EXCLUDED.a + 10 RETURNING *;
execute p5(5);
execute p5(5);
execute p5(5);
execute p5(5);
execute p5(5);
execute p5(5);
execute p5(5);

INSERT INTO "companies" ("id","meta","name","created_at","updated_at","deleted_at") VALUES (1,'{"test":123}','Name','2016-11-07 17:34:22.101807','2021-05-20 22:16:55.424521',NULL) ON CONFLICT (id) DO UPDATE SET "meta"=EXCLUDED."meta"  RETURNING "id";
INSERT INTO "companies" ("id","meta","name","created_at","updated_at","deleted_at") VALUES (1,'{"test":123}','Name','2016-11-07 17:34:22.101807','2021-05-20 22:16:55.424521',NULL) ON CONFLICT (id) DO UPDATE SET "meta"=EXCLUDED."meta"  RETURNING "id";
PREPARE p6 AS INSERT INTO "companies" ("id","meta","name","created_at","updated_at","deleted_at") VALUES (1,'{"test":123}','Name','2016-11-07 17:34:22.101807','2021-05-20 22:16:55.424521',NULL) ON CONFLICT (id) DO UPDATE SET "meta"=EXCLUDED."meta"  RETURNING "id";;
EXECUTE p6;
EXECUTE p6;
EXECUTE p6;
EXECUTE p6;
EXECUTE p6;
EXECUTE p6;
EXECUTE p6;

prepare insert_select(int) as insert into companies SELECT * FROM companies WHERE id >= $1 ON CONFLICT(id) DO UPDATE SET "meta"=EXCLUDED."meta" RETURNING "id";;
EXECUTE insert_select(1);
EXECUTE insert_select(1);
EXECUTE insert_select(1);
EXECUTE insert_select(1);
EXECUTE insert_select(1);
EXECUTE insert_select(1);
EXECUTE insert_select(1);

prepare insert_select_1 as insert into companies SELECT * FROM companies WHERE id >= 1 ON CONFLICT(id) DO UPDATE SET "meta"=EXCLUDED."meta" RETURNING "id";;
EXECUTE insert_select_1;
EXECUTE insert_select_1;
EXECUTE insert_select_1;
EXECUTE insert_select_1;
EXECUTE insert_select_1;
EXECUTE insert_select_1;
EXECUTE insert_select_1;

-- query fails on the shards should be handled
-- nicely
\set VERBOSITY terse
SELECT x/0 FROM test;

SELECT count(DISTINCT row(key, value)) FROM non_binary_copy_test;
SELECT count(*), event FROM date_part_table GROUP BY event ORDER BY count(*) DESC, event DESC LIMIT 5;
SELECT count(*), event FROM date_part_table WHERE user_id = 1 GROUP BY event ORDER BY count(*) DESC, event DESC LIMIT 5;
SELECT count(*), t1.event FROM date_part_table t1 JOIN date_part_table USING (user_id) WHERE t1.user_id = 1 GROUP BY t1.event ORDER BY count(*) DESC, t1.event DESC LIMIT 5;

SELECT count(*), event FROM date_part_table WHERE event_time > '2020-01-05' GROUP BY event ORDER BY count(*) DESC, event DESC LIMIT 5;
SELECT count(*), event FROM date_part_table WHERE user_id = 12 AND event_time = '2020-01-12 12:00:00' GROUP BY event ORDER BY count(*) DESC, event DESC LIMIT 5;
SELECT count(*), t1.event FROM date_part_table t1 JOIN date_part_table t2 USING (user_id) WHERE t1.user_id = 1 AND t2.event_time > '2020-01-03' GROUP BY t1.event ORDER BY count(*) DESC, t1.event DESC LIMIT 5;

TRUNCATE test;
TRUNCATE ref;
insert into test(x, y) SELECT 1, i FROM generate_series(1, 10) i;
insert into test(x, y) SELECT 3, i FROM generate_series(11, 40) i;
insert into test(x, y) SELECT i, 1 FROM generate_series(1, 10) i;
insert into test(x, y) SELECT i, 3 FROM generate_series(11, 40) i;

insert into ref(a, b) SELECT i, 1 FROM generate_series(1, 10) i;
insert into ref(a, b) SELECT i, 3 FROM generate_series(11, 40) i;
insert into ref(a, b) SELECT 1, i FROM generate_series(1, 10) i;
insert into ref(a, b) SELECT 3, i FROM generate_series(11, 40) i;

SELECT count(*)
FROM test,
    LATERAL (
        SELECT
            ref.a
        FROM ref
        WHERE
            ref.b = test.x
        LIMIT 2
    ) q;

SELECT count(*)
FROM test,
    LATERAL (
        SELECT
            ref.a
        FROM ref
        WHERE
            ref.b = test.y
        LIMIT 2
    ) q;

-- Since the only correlates on the distribution column, this can be safely
-- pushed down. But this is currently considered to hard to detect, so we fail.
--
-- SELECT count(*)
-- FROM ref,
--     LATERAL (
--         SELECT
--             test.x
--         FROM test
--         WHERE
--             test.x = ref.a
--         LIMIT 2
--     ) q;

-- This returns wrong results when pushed down. Instead of returning 2 rows,
-- for each row in the reference table. It would return (2 * number of shards)
-- rows for each row in the reference table.
-- See issue #5327
--
-- SELECT count(*)
-- FROM ref,
--     LATERAL (
--         SELECT
--             test.y
--         FROM test
--         WHERE
--             test.y = ref.a
--         LIMIT 2
--     ) q;

SELECT count(*)
FROM ref,
    LATERAL (
        SELECT
            ref_2.b y
        FROM ref ref_2
        WHERE
            ref_2.b = ref.a
        LIMIT 2
    ) q;

SELECT count(*)
FROM test,
    LATERAL (
        SELECT
            test_2.y
        FROM test test_2
        WHERE
            test_2.x = test.x
        LIMIT 2
    ) q;

SELECT count(*)
FROM ref,
    LATERAL (
        SELECT
            ref_2.b y
        FROM ref ref_2
        WHERE
            ref_2.b = ref.a
        LIMIT 2
    ) q JOIN test ON test.x = q.y;

-- Would require repartitioning to work with subqueries
--
-- SELECT count(*)
-- FROM test,
--     LATERAL (
--         SELECT
--             test_2.x
--         FROM test test_2
--         WHERE
--             test_2.x = test.y
--         LIMIT 2
--     ) q ;

SELECT count(*)
FROM ref JOIN test on ref.b = test.x,
    LATERAL (
        SELECT
            ref_2.b y
        FROM ref ref_2
        WHERE
            ref_2.b = ref.a
        LIMIT 2
    ) q
;

SELECT count(*)
FROM ref JOIN test on ref.b = test.y,
    LATERAL (
        SELECT
            ref_2.b y
        FROM ref ref_2
        WHERE
            ref_2.b = ref.a
        LIMIT 2
    ) q
;

-- Too complex joins for Citus to handle currently
--
-- SELECT count(*)
-- FROM ref JOIN test on ref.b = test.x,
--     LATERAL (
--         SELECT
--             test_2.x
--         FROM test test_2
--         WHERE
--             test_2.x = ref.a
--         LIMIT 2
--     ) q
-- ;

-- Would require repartitioning to work with subqueries
--
-- SELECT count(*)
-- FROM ref JOIN test on ref.b = test.x,
--     LATERAL (
--         SELECT
--             test_2.y
--         FROM test test_2
--         WHERE
--             test_2.y = ref.a
--         LIMIT 2
--     ) q
-- ;

SELECT count(*)
FROM ref JOIN test on ref.b = test.x,
    LATERAL (
        SELECT
            test_2.x
        FROM test test_2
        WHERE
            test_2.x = test.x
        LIMIT 2
    ) q
;

-- Without LIMIT clauses
SELECT count(*)
FROM test,
    LATERAL (
        SELECT
            ref.a
        FROM ref
        WHERE
            ref.b = test.x
    ) q;

SELECT count(*)
FROM test,
    LATERAL (
        SELECT
            ref.a
        FROM ref
        WHERE
            ref.b = test.y
    ) q;

SELECT count(*)
FROM ref,
    LATERAL (
        SELECT
            test.x
        FROM test
        WHERE
            test.x = ref.a
    ) q;

SELECT count(*)
FROM ref,
    LATERAL (
        SELECT
            test.y
        FROM test
        WHERE
            test.y = ref.a
    ) q;

SELECT count(*)
FROM ref,
    LATERAL (
        SELECT
            ref_2.b y
        FROM ref ref_2
        WHERE
            ref_2.b = ref.a
    ) q;

SELECT count(*)
FROM test,
    LATERAL (
        SELECT
            test_2.y
        FROM test test_2
        WHERE
            test_2.x = test.x
    ) q;

SELECT count(*)
FROM test,
    LATERAL (
        SELECT
            test_2.x
        FROM test test_2
        WHERE
            test_2.x = test.y
    ) q ;

SELECT count(*)
FROM ref JOIN test on ref.b = test.x,
    LATERAL (
        SELECT
            ref_2.b y
        FROM ref ref_2
        WHERE
            ref_2.b = ref.a
    ) q
;

SELECT count(*)
FROM ref JOIN test on ref.b = test.y,
    LATERAL (
        SELECT
            ref_2.b y
        FROM ref ref_2
        WHERE
            ref_2.b = ref.a
    ) q
;

SELECT count(*)
FROM ref JOIN test on ref.b = test.x,
    LATERAL (
        SELECT
            test_2.x
        FROM test test_2
        WHERE
            test_2.x = ref.a
    ) q
;

SELECT count(*)
FROM ref JOIN test on ref.b = test.x,
    LATERAL (
        SELECT
            test_2.y
        FROM test test_2
        WHERE
            test_2.y = ref.a
    ) q
;

SELECT count(*)
FROM ref JOIN test on ref.b = test.x,
    LATERAL (
        SELECT
            test_2.x
        FROM test test_2
        WHERE
            test_2.x = test.x
    ) q
;

SELECT count(*)
FROM ref,
    LATERAL (
        SELECT
            ref_2.b y
        FROM ref ref_2
        WHERE
            ref_2.b = ref.a
    ) q JOIN test ON test.x = q.y;

