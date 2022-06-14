\set VERBOSITY terse
SET search_path TO function_create;

-- test user defined function with a distribution column argument
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


-- insert some data to test user defined aggregates
INSERT INTO aggdata (id, key, val, valf)
    VALUES (1, 1, 2, 11.2),
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

-- test user defined aggregates
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

-- test function that writes to a reference table
SELECT add_new_item_to_series();
SELECT add_new_item_to_series();
SELECT add_new_item_to_series();
SELECT add_new_item_to_series();
SELECT add_new_item_to_series();
SELECT add_new_item_to_series();
