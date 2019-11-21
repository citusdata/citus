CREATE SCHEMA cte_nested_modifications;
SET search_path TO cte_nested_modifications, public;
CREATE TABLE tt1(id int, value_1 int);
INSERT INTO tt1 VALUES(1,2),(2,3),(3,4);
SELECT create_distributed_table('tt1','id');
CREATE TABLE tt2(id int, value_1 int);
INSERT INTO tt2 VALUES(3,3),(4,4),(5,5);
SELECT create_distributed_table('tt2','id');
CREATE TABLE tt3(id int, json_val json);
INSERT INTO tt3 VALUES(1, '{"prod_name":"name_1", "qty":"6"}'),
                      (2, '{"prod_name":"name_2", "qty":"4"}'),
                      (3, '{"prod_name":"name_3", "qty":"2"}');
-- DELETE within CTE and use it from UPDATE
BEGIN;
WITH cte_1 AS (
    WITH cte_2 AS (
        SELECT id as cte2_id
        FROM tt1
        WHERE value_1 >= 2
    )

    DELETE FROM tt2
    USING cte_2
    WHERE tt2.id = cte_2.cte2_id
    RETURNING cte2_id - 1 as id
)
UPDATE tt1
SET value_1 = abs(2 + 3.5)
FROM cte_1
WHERE cte_1.id = tt1.id;
SELECT * FROM tt1 ORDER BY id;
ROLLBACK;
-- Similar to test above, now use the CTE in the SET part of the UPDATE
BEGIN;
WITH cte_1 AS (
    WITH cte_2 AS (
        SELECT id as cte2_id
        FROM tt1
        WHERE value_1 >= 2
    )

    DELETE FROM tt2
    USING cte_2
    WHERE tt2.id = cte_2.cte2_id
    RETURNING cte2_id as id
)
UPDATE tt1
SET value_1 = (SELECT max(id) + abs(2 + 3.5) FROM cte_1);
SELECT * FROM tt1 ORDER BY id;
ROLLBACK;
-- Use alias in the definition of CTE, instead of in the RETURNING
BEGIN;
WITH cte_1(id) AS (
    WITH cte_2 AS (
        SELECT id as cte2_id
        FROM tt1
        WHERE value_1 >= 2
    )

    DELETE FROM tt2
    USING cte_2
    WHERE tt2.id = cte_2.cte2_id
    RETURNING cte2_id
)
UPDATE tt1
SET value_1 = (SELECT max(id) + abs(2 + 3.5) FROM cte_1);
SELECT * FROM tt1 ORDER BY id;
ROLLBACK;
-- Update within CTE and use it from Delete
BEGIN;
WITH cte_1 AS (
    WITH cte_2 AS (
        SELECT id as cte2_id
        FROM tt1
        WHERE value_1 >= 2
    )

    UPDATE tt2
    SET value_1 = 10
    FROM cte_2
    WHERE id = cte2_id
    RETURNING id, value_1
)
DELETE FROM tt1
USING cte_1
WHERE tt1.id < cte_1.id;
SELECT * FROM tt1 ORDER BY id;
ROLLBACK;
-- Similar to test above, but use json column
BEGIN;
WITH cte_1 AS (
    WITH cte_2 AS (
        SELECT * FROM tt3
    )

    UPDATE tt2
    SET value_1 = (SELECT max((json_val->>'qty')::int) FROM cte_2)
    RETURNING id, value_1
)
DELETE FROM tt1
USING cte_1
WHERE tt1.id < cte_1.id;
SELECT * FROM tt1 ORDER BY id;
ROLLBACK;
DROP SCHEMA cte_nested_modifications CASCADE;
