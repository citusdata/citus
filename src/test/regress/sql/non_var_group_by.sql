--
-- NON_VAR_GROUP_BY
--

CREATE SCHEMA non_var_group_by;
SET search_path TO non_var_group_by;

SET citus.next_shard_id TO 83674000;
CREATE TABLE aggdata (id int, key int, val int, valf float8);
SELECT create_distributed_table('aggdata', 'id');
INSERT INTO aggdata (id, key, val, valf) VALUES
	(1, 1, 2, 11.2),
	(2, 1, NULL, 2.1),
	(3, 2, 2, 3.22),
	(4, 2, 3, 4.23),
	(5, 2, 5, 5.25),
	(6, 3, 4, 63.4),
	(7, 5, NULL, 75),
	(8, 6, NULL, NULL),
	(9, 6, NULL, 96),
	(10, 7, 8, 1078),
	(11, 9, 0, 1.19);

-- Non-Var GROUP BY mixed with aggregate in the same target entry

-- GROUP BY expression combined with aggregate via concatenation
SELECT abs(val) + sum(id) AS combined
FROM aggdata
GROUP BY abs(val)
ORDER BY combined;

-- GROUP BY expression used as argument alongside aggregate in a function
SELECT coalesce(abs(val)::text, 'null') || ':' || sum(id)::text AS label
FROM aggdata
GROUP BY abs(val)
ORDER BY label;

-- GROUP BY expression in arithmetic with multiple aggregates
SELECT abs(val) * count(*) + sum(id) AS calc
FROM aggdata
GROUP BY abs(val)
ORDER BY calc;

-- Nested function GROUP BY mixed with aggregate
SELECT floor(abs(val)) + min(id) AS nested_mix
FROM aggdata
GROUP BY floor(abs(val))
ORDER BY nested_mix;

-- CASE expression in GROUP BY mixed with aggregate in same target
SELECT case when val > 3 then 'high' else 'low' end || ':' || sum(id)::text AS bucket_total
FROM aggdata
GROUP BY case when val > 3 then 'high' else 'low' end
ORDER BY bucket_total;

-- DISTINCT on mixed GROUP BY expression + aggregate
SELECT DISTINCT abs(val) + sum(id) AS combined
FROM aggdata
GROUP BY abs(val)
ORDER BY combined;

-- Subquery wrapping a mixed GROUP BY expression + aggregate
SELECT combined, combined * 2 AS doubled
FROM (
	SELECT abs(val) + sum(id) AS combined
	FROM aggdata
	GROUP BY abs(val)
) sub
ORDER BY combined;

-- GROUP BY expr appears both standalone AND mixed with aggregate in same query
SELECT abs(val) AS av, abs(val) + sum(id) AS mixed
FROM aggdata
GROUP BY abs(val)
ORDER BY av;

-- Multiple non-Var GROUP BY expressions mixed with aggregate in same target
SELECT abs(val) + key * 2 + sum(id) AS multi_group
FROM aggdata
GROUP BY abs(val), key * 2
ORDER BY multi_group;

-- HAVING that references a mixed GROUP BY expression + aggregate
SELECT abs(val) + sum(id) AS combined
FROM aggdata
GROUP BY abs(val)
HAVING abs(val) + sum(id) > 10
ORDER BY combined;

-- Constant in expression
SELECT abs(val) + 1 + sum(id) - floor(valf)
FROM aggdata
GROUP BY abs(val), floor(valf)
ORDER BY 1;

-- Count(DISTINCT) (decomposed differently from the basic aggregates)
SELECT abs(val) + count(DISTINCT id) AS combined
FROM aggdata
GROUP BY abs(val)
ORDER BY combined;

-- Type-casts
SELECT val::text || ':' || sum(id)::text AS combined
FROM aggdata
GROUP BY val::text
ORDER BY combined;

-- ORDER BY with LIMIT
SELECT abs(val) + sum(id) AS combined
FROM aggdata
GROUP BY abs(val)
ORDER BY abs(val) DESC, sum(id) ASC
LIMIT 5;

-- EXPLAIN output showing worker queries preserve GROUP BY expressions intact
EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF)
SELECT abs(val) + sum(id) AS combined
FROM aggdata
GROUP BY abs(val)
ORDER BY combined;

EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF)
SELECT abs(val) + sum(id) AS combined
FROM aggdata
GROUP BY abs(val)
HAVING abs(val) + sum(id) > 10
ORDER BY combined;

EXPLAIN (ANALYZE ON, VERBOSE ON, COSTS OFF, TIMING OFF, BUFFERS OFF, SUMMARY OFF)
SELECT abs(val) + key * 2 + sum(id) AS multi_group
FROM aggdata
GROUP BY abs(val), key * 2
ORDER BY multi_group;

SET client_min_messages TO ERROR;
DROP SCHEMA non_var_group_by CASCADE;
RESET client_min_messages;
