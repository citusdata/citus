CREATE SCHEMA cte_prepared_modify;
SET search_path TO cte_prepared_modify, public;
CREATE TABLE tt1(id int, value_1 int);
INSERT INTO tt1 VALUES(1,2),(2,3),(3,4);
SELECT create_distributed_table('tt1','id');
CREATE TABLE tt2(id int, value_1 int);
INSERT INTO tt2 VALUES(3,3),(4,4),(5,5);
SELECT create_distributed_table('tt2','id');
-- Test with prepared statements (parameter used by SET)
PREPARE prepared_test(integer) AS
WITH cte_1 AS(
  SELECT * FROM tt1 WHERE id >= 2
)
UPDATE tt2
SET value_1 = $1
FROM cte_1
WHERE tt2.id = cte_1.id;
-- Test with prepared statements (parameter used by WHERE on partition column)
PREPARE prepared_test_2(integer) AS
WITH cte_1 AS(
  SELECT * FROM tt1 WHERE id >= 2
)
UPDATE tt2
SET value_1 = (SELECT max(id) FROM cte_1)
WHERE tt2.id = $1;
-- Test with prepared statements (parameter used by WHERE on non-partition column)
PREPARE prepared_test_3(integer) AS
WITH cte_1 AS(
  SELECT * FROM tt1 WHERE id >= 2
)
UPDATE tt2
SET value_1 = (SELECT max(id) FROM cte_1)
WHERE tt2.value_1 = $1;
EXECUTE prepared_test(1);
EXECUTE prepared_test(2);
EXECUTE prepared_test(3);
EXECUTE prepared_test(4);
EXECUTE prepared_test(5);
EXECUTE prepared_test(6);
EXECUTE prepared_test(1);
EXECUTE prepared_test(2);
EXECUTE prepared_test(3);
EXECUTE prepared_test(4);
EXECUTE prepared_test(5);
EXECUTE prepared_test(6);
EXECUTE prepared_test_3(1);
EXECUTE prepared_test_3(2);
EXECUTE prepared_test_3(3);
EXECUTE prepared_test_3(4);
EXECUTE prepared_test_3(5);
EXECUTE prepared_test_3(6);
DROP SCHEMA cte_prepared_modify CASCADE;
