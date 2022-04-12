\set VERBOSITY terse
SET search_path TO function_create;

SELECT
    warning (id, message)
FROM
    warnings
WHERE
    id = 1;

SELECT warning (1, 'Push down to worker that holds the partition value of 1');
SELECT warning (2, 'Push down to worker that holds the partition value of 2');
SELECT warning (3, 'Push down to worker that holds the partition value of 3');
SELECT warning (4, 'Push down to worker that holds the partition value of 4');
SELECT warning (5, 'Push down to worker that holds the partition value of 5');
SELECT warning (6, 'Push down to worker that holds the partition value of 6');
SELECT warning (7, 'Push down to worker that holds the partition value of 7');

SELECT
    key,
    sum2 (val),
    sum2_strict (val),
    stddev(valf)::numeric(10, 5),
    psum (val, valf::int),
    psum_strict (val, valf::int)
FROM
    aggdata
GROUP BY
    key
ORDER BY
    key;

SELECT add_new_item_to_series();
SELECT add_new_item_to_series();
SELECT add_new_item_to_series();
SELECT add_new_item_to_series();
SELECT add_new_item_to_series();
SELECT add_new_item_to_series();
