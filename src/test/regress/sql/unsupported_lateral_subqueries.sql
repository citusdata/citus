CREATE SCHEMA unsupported_lateral_joins;
SET search_path TO unsupported_lateral_joins;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 13354100;

CREATE TABLE test(x bigint, y bigint);
SELECT create_distributed_table('test','x');

CREATE TABLE ref(a bigint, b bigint);
SELECT create_reference_table('ref');

insert into test(x, y) SELECT 1, i FROM generate_series(1, 10) i;
insert into test(x, y) SELECT 3, i FROM generate_series(11, 40) i;
insert into test(x, y) SELECT i, 1 FROM generate_series(1, 10) i;
insert into test(x, y) SELECT i, 3 FROM generate_series(11, 40) i;

insert into ref(a, b) SELECT i, 1 FROM generate_series(1, 10) i;
insert into ref(a, b) SELECT i, 3 FROM generate_series(11, 40) i;
insert into ref(a, b) SELECT 1, i FROM generate_series(1, 10) i;
insert into ref(a, b) SELECT 3, i FROM generate_series(11, 40) i;

-- The following queries return wrong results when pushed down. Instead of
-- returning 2 rows, for each row in ref table. They would return (2 * number
-- of shards) rows for each row in the reference table. See issue #5327
SELECT count(*)
FROM ref,
    LATERAL (
        SELECT
            test.y
        FROM test
        WHERE
            test.y = ref.a
        LIMIT 2
    ) q;

SELECT count(*)
FROM (VALUES (1), (3)) ref(a),
    LATERAL (
        SELECT
            test.y
        FROM test
        WHERE
            test.y = ref.a
        LIMIT 2
    ) q;

WITH ref(a) as (select y from test)
SELECT count(*)
FROM ref,
    LATERAL (
        SELECT
            test.y
        FROM test
        WHERE
            test.y = ref.a
        LIMIT 2
    ) q;

SELECT count(*)
FROM generate_series(1, 3) ref(a),
    LATERAL (
        SELECT
            test.y
        FROM test
        WHERE
            test.y = ref.a
        LIMIT 2
    ) q;

SELECT count(*)
FROM (SELECT generate_series(1, 3)) ref(a),
    LATERAL (
        SELECT
            test.y
        FROM test
        WHERE
            test.y = ref.a
        LIMIT 2
    ) q;


-- make sure right error message is chosen
SELECT count(*)
FROM ref ref_table,
    (VALUES (1), (3)) rec_values(a),
    LATERAL (
        SELECT
            test.y
        FROM test
        WHERE
            test.y = ref_table.a
        LIMIT 2
    ) q;

SELECT count(*)
FROM ref as ref_table,
    (VALUES (1), (3)) ref_values(a),
    LATERAL (
        SELECT
            test.y
        FROM test
        WHERE
            test.y = ref_values.a
        LIMIT 2
    ) q;

SELECT count(*) FROM
    ref ref_outer,
    LATERAL (
        SELECT * FROM
            LATERAL ( SELECT *
            FROM ref ref_inner,
                LATERAL (
                    SELECT
                        test.y
                    FROM test
                    WHERE
                        test.y = ref_outer.a
                    LIMIT 2
                ) q
            ) q2
    ) q3;

SELECT count(*) FROM
    ref ref_outer,
    LATERAL (
        SELECT * FROM
            LATERAL ( SELECT *
            FROM ref ref_inner,
                LATERAL (
                    SELECT
                        test.y
                    FROM test
                    WHERE
                        test.y = ref_inner.a
                    LIMIT 2
                ) q
            ) q2
    ) q3;




-- Since this only correlates on the distribution column, this can be safely
-- pushed down. But this is currently considered to hard to detect, so we fail.
SELECT count(*)
FROM ref,
    LATERAL (
        SELECT
            test.x
        FROM test
        WHERE
            test.x = ref.a
        LIMIT 2
    ) q;

-- Would require repartitioning to work with subqueries
SELECT count(*)
FROM test,
    LATERAL (
        SELECT
            test_2.x
        FROM test test_2
        WHERE
            test_2.x = test.y
        LIMIT 2
    ) q ;

-- Too complex joins for Citus to handle currently
SELECT count(*)
FROM ref JOIN test on ref.b = test.x,
    LATERAL (
        SELECT
            test_2.x
        FROM test test_2
        WHERE
            test_2.x = ref.a
        LIMIT 2
    ) q
;

-- Would require repartitioning to work with subqueries
SELECT count(*)
FROM ref JOIN test on ref.b = test.x,
    LATERAL (
        SELECT
            test_2.y
        FROM test test_2
        WHERE
            test_2.y = ref.a
        LIMIT 2
    ) q
;

SET client_min_messages TO WARNING;
DROP SCHEMA unsupported_lateral_joins CASCADE;
