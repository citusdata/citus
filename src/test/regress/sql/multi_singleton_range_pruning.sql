CREATE SCHEMA singleton_range_pruning;
SET search_path TO singleton_range_pruning;
SET citus.shard_replication_factor TO 1;

-- Regression for #8934: infer integer equality from singleton ranges.
SET citus.shard_count TO 4;
CREATE TABLE singleton_int2 (k smallint, touched boolean DEFAULT false);
CREATE TABLE singleton_int4 (k integer, touched boolean DEFAULT false);
CREATE TABLE singleton_int8 (k bigint, touched boolean DEFAULT false);
SELECT create_distributed_table('singleton_int2', 'k');
SELECT create_distributed_table('singleton_int4', 'k');
SELECT create_distributed_table('singleton_int8', 'k');

INSERT INTO singleton_int2(k) SELECT generate_series(-3, 3);
INSERT INTO singleton_int4(k) SELECT generate_series(-3, 3);
INSERT INTO singleton_int8(k) SELECT generate_series(-3, 3);
CREATE TEMP TABLE singleton_reference AS SELECT generate_series(-3, 3) AS k;

-- Check routing with both planner settings and explicitly matching bound types.
SET citus.enable_fast_path_router_planner TO on;
SELECT public.explain_has_single_task('EXPLAIN (COSTS OFF) SELECT * FROM singleton_int2 WHERE k BETWEEN 2::smallint AND 2::smallint') AS singleton_one_task;
SELECT public.explain_has_single_task('EXPLAIN (COSTS OFF) SELECT * FROM singleton_int4 WHERE k BETWEEN 2::integer AND 2::integer') AS singleton_one_task;
SELECT public.explain_has_single_task('EXPLAIN (COSTS OFF) SELECT * FROM singleton_int8 WHERE k BETWEEN 2::bigint AND 2::bigint') AS singleton_one_task;
SET citus.enable_fast_path_router_planner TO off;
SELECT public.explain_has_single_task('EXPLAIN (COSTS OFF) SELECT * FROM singleton_int2 WHERE k BETWEEN 2::smallint AND 2::smallint') AS singleton_one_task;
SELECT public.explain_has_single_task('EXPLAIN (COSTS OFF) SELECT * FROM singleton_int4 WHERE k BETWEEN 2::integer AND 2::integer') AS singleton_one_task;
SELECT public.explain_has_single_task('EXPLAIN (COSTS OFF) SELECT * FROM singleton_int8 WHERE k BETWEEN 2::bigint AND 2::bigint') AS singleton_one_task;

SELECT public.explain_has_single_task(
    'EXPLAIN (COSTS OFF) SELECT * FROM singleton_int4
     WHERE 2 <= k AND 2 >= k') AS reversed_one_task;
SELECT public.explain_has_single_task(
    'EXPLAIN (COSTS OFF) SELECT * FROM singleton_int4
     WHERE k BETWEEN (1 + 1) AND (4 / 2)') AS folded_one_task;
SELECT public.explain_has_single_task(
    'EXPLAIN (COSTS OFF) SELECT * FROM singleton_int4
     WHERE k BETWEEN 1 AND 2 AND k BETWEEN 2 AND 3') AS intersected_one_task;
SELECT public.explain_has_single_task(
    'EXPLAIN (COSTS OFF) SELECT * FROM singleton_int4
     WHERE k BETWEEN -3 AND 3') AS wider_one_task;
SELECT public.explain_has_single_task(
    'EXPLAIN (COSTS OFF) SELECT * FROM singleton_int4
     WHERE k >= 2') AS lower_only_one_task;

-- A singleton OR branch must not hide rows allowed by another branch.
SELECT array_agg(k ORDER BY k) FROM singleton_int4
WHERE k BETWEEN 2 AND 2 OR k BETWEEN -3 AND -2;
SELECT array_agg(k ORDER BY k) FROM singleton_int4
WHERE k BETWEEN 2 AND 2 OR k = 3;
SELECT array_agg(k ORDER BY k) FROM singleton_int4
WHERE k BETWEEN 2 AND 2 OR k < 0;

-- Compare distributed results with PostgreSQL's local evaluation.
SELECT
    (SELECT array_agg(k ORDER BY k) FROM singleton_int4
     WHERE k BETWEEN -3 AND 3)
    IS NOT DISTINCT FROM
    (SELECT array_agg(k ORDER BY k) FROM singleton_reference
     WHERE k BETWEEN -3 AND 3) AS wider_results_match;
SELECT count(*) FROM singleton_int4 WHERE k BETWEEN NULL::integer AND 2;
SELECT count(*) FROM singleton_int4 WHERE k BETWEEN 2 AND NULL::integer;
SELECT count(*) FROM singleton_int4 WHERE k > 2 AND k <= 2;
SELECT count(*) FROM singleton_int4 WHERE k BETWEEN 3 AND 2;

-- Keep comparisons outside the supported scope on their existing path.
SELECT public.explain_has_single_task(
    'EXPLAIN (COSTS OFF) SELECT * FROM singleton_int4
     WHERE k BETWEEN 2::bigint AND 2::bigint') AS mixed_type_one_task;
SELECT array_agg(k ORDER BY k) FROM singleton_int4
WHERE k BETWEEN 2::bigint AND 2::bigint;

SELECT public.explain_has_single_task(
    'EXPLAIN (COSTS OFF) SELECT * FROM singleton_int4
     WHERE k > 2 AND k <= 2') AS exclusive_bound_one_task;

CREATE TABLE singleton_numeric (k numeric);
SELECT create_distributed_table('singleton_numeric', 'k');
INSERT INTO singleton_numeric VALUES (2), (2.5), (3);
SELECT public.explain_has_single_task(
    'EXPLAIN (COSTS OFF) SELECT * FROM singleton_numeric
     WHERE k BETWEEN 2::numeric AND 2::numeric') AS numeric_one_task;
SELECT array_agg(k ORDER BY k) FROM singleton_numeric
WHERE k BETWEEN 2::numeric AND 2::numeric;
DROP TABLE singleton_numeric;

-- Mixed operand directions must preserve both routing and affected rows.
CREATE FUNCTION check_mixed_direction_pruning()
RETURNS TABLE(column_type text, test_case text, one_task boolean, matching_rows bigint[])
LANGUAGE plpgsql AS $$
DECLARE
    table_name text;
    bound text;
    command text;
BEGIN
    FOR column_type, table_name IN
        SELECT * FROM (VALUES
            ('smallint', 'singleton_int2'),
            ('integer', 'singleton_int4'),
            ('bigint', 'singleton_int8')
        ) AS tables
    LOOP
        bound := format('2::%s', column_type);
        FOR test_case, command IN
            SELECT * FROM (VALUES
                ('lower bound', format('SELECT k FROM %I WHERE k >= %s AND %s <= k',
                                       table_name, bound, bound)),
                ('upper bound', format('SELECT k FROM %I WHERE k <= %s AND %s >= k',
                                       table_name, bound, bound)),
                ('singleton 1', format('SELECT k FROM %I WHERE k >= %s AND %s >= k',
                                       table_name, bound, bound)),
                ('singleton 2', format('SELECT k FROM %I WHERE %s <= k AND k <= %s',
                                       table_name, bound, bound)),
                ('update', format('UPDATE %I SET touched = true WHERE k >= %s AND %s <= k RETURNING k',
                                  table_name, bound, bound)),
                ('delete', format('DELETE FROM %I WHERE k <= %s AND %s >= k RETURNING k',
                                  table_name, bound, bound))
            ) AS queries
        LOOP
            one_task := public.explain_has_single_task('EXPLAIN (COSTS OFF) ' || command);
            EXECUTE format('WITH matched AS (%s) SELECT array_agg(k::bigint ORDER BY k) FROM matched',
                           command) INTO matching_rows;
            RETURN NEXT;
        END LOOP;
    END LOOP;
END;
$$;

BEGIN;
SET LOCAL citus.enable_fast_path_router_planner TO on;
SELECT * FROM check_mixed_direction_pruning() ORDER BY column_type, test_case;
ROLLBACK;
BEGIN;
SET LOCAL citus.enable_fast_path_router_planner TO off;
SELECT * FROM check_mixed_direction_pruning() ORDER BY column_type, test_case;
ROLLBACK;
DROP FUNCTION check_mixed_direction_pruning();

DROP TABLE singleton_reference;
DROP TABLE singleton_int2, singleton_int4, singleton_int8;
RESET citus.enable_fast_path_router_planner;
RESET citus.shard_count;
RESET citus.shard_replication_factor;

SET search_path TO public;
DROP SCHEMA singleton_range_pruning;
