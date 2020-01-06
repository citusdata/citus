SET citus.log_multi_join_order to TRUE;
SET client_min_messages to DEBUG1;
SET citus.enable_repartition_joins to on;
SELECT
    l_partkey, l_suppkey, count(*)
FROM
    lineitem, part_append, supplier
WHERE
    l_partkey = p_partkey
    AND l_suppkey < s_suppkey
GROUP BY
    l_partkey, l_suppkey
ORDER BY
    l_partkey, l_suppkey
LIMIT 10;

SELECT
    l_partkey, l_suppkey, count(*)
FROM
    lineitem, part_append, supplier
WHERE
    l_partkey = p_partkey
    AND int4eq(l_suppkey, s_suppkey)
GROUP BY
    l_partkey, l_suppkey
ORDER BY
    l_partkey, l_suppkey
LIMIT 10;

SELECT
    l_partkey, l_suppkey, count(*)
FROM
    lineitem, part_append, supplier
WHERE
    l_partkey = p_partkey
    AND NOT int4ne(l_suppkey, s_suppkey)
GROUP BY
    l_partkey, l_suppkey
ORDER BY
    l_partkey, l_suppkey
LIMIT 10;

SELECT
    l_partkey, l_suppkey, count(*)
FROM
    lineitem, part_append, supplier
WHERE
    l_partkey = p_partkey
GROUP BY
    l_partkey, l_suppkey
ORDER BY
    l_partkey, l_suppkey
LIMIT 10;

SELECT
    l_partkey, l_suppkey, count(*)
FROM
    lineitem, part_append, supplier
WHERE
    l_partkey = p_partkey
    AND (int4eq(l_suppkey, s_suppkey) OR l_suppkey = s_suppkey)
GROUP BY
    l_partkey, l_suppkey
ORDER BY
    l_partkey, l_suppkey
LIMIT 10;

SELECT
    l_partkey, l_suppkey, count(*)
FROM
    lineitem, part_append, supplier
WHERE
    l_partkey = p_partkey
    AND (int4eq(l_suppkey, s_suppkey) OR random() > 2)
GROUP BY
    l_partkey, l_suppkey
ORDER BY
    l_partkey, l_suppkey
LIMIT 10;

SELECT
    l_partkey, l_suppkey, count(*)
FROM
    lineitem, part_append, supplier
WHERE
    l_partkey = p_partkey
    AND (l_suppkey = 1 OR s_suppkey = 1)
GROUP BY
    l_partkey, l_suppkey
ORDER BY
    l_partkey, l_suppkey
LIMIT 10;

SELECT
    l_partkey, l_suppkey, count(*)
FROM
    lineitem, part_append, supplier
WHERE
    l_partkey = p_partkey
    AND l_partkey + p_partkey = s_suppkey
GROUP BY
    l_partkey, l_suppkey
ORDER BY
    l_partkey, l_suppkey
LIMIT 10;
