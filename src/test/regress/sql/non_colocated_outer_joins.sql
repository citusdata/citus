CREATE TABLE t1(col1 INT, col2 INT);
SELECT create_distributed_table('t1', 'col1');
INSERT INTO t1 SELECT i, i FROM generate_series(1,10) i;

CREATE TABLE t2(col1 INT, col2 INT);
SELECT create_distributed_table('t2', 'col2');
INSERT INTO t2 SELECT i, i FROM generate_series(6,15) i;

CREATE TABLE t3(col1 INT, col2 INT);
SELECT create_distributed_table('t3', 'col1');
INSERT INTO t3 SELECT i, i FROM generate_series(11,20) i;

SET citus.enable_repartition_joins TO ON;
SET citus.log_multi_join_order TO ON;
SET client_min_messages TO LOG;


-- join order planner can handle left outer join between tables with simple join clause

-- outer table restricted on partition column whereas inner one is not restricted on the partition column
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
-- inner table restricted on partition column whereas outer one is not restricted on the partition column
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON (t1.col2 = t2.col2) ORDER BY 1,2,3,4;
-- both tables are not restricted on partition column
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON (t1.col2 = t2.col1) ORDER BY 1,2,3,4;


-- join order planner can handle right outer join between tables with simple join clause

-- outer table restricted on partition column whereas inner one is not restricted on the partition column
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
-- inner table restricted on partition column whereas outer one is not restricted on the partition column
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON (t1.col2 = t2.col2) ORDER BY 1,2,3,4;
-- both tables are not restricted on partition column
SELECT t1.*, t2.* FROM t1 RIGHT JOIN t2 ON (t1.col2 = t2.col1) ORDER BY 1,2,3,4;


-- join order planner can handle full outer join between tables with simple join clause

-- left outer table restricted on partition column whereas right outer one is not restricted on the partition column
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON (t1.col1 = t2.col1) ORDER BY 1,2,3,4;
-- right outer table restricted on partition column whereas left outer one is not restricted on the partition column
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON (t1.col2 = t2.col2) ORDER BY 1,2,3,4;
-- both tables are not restricted on partition column
SELECT t1.*, t2.* FROM t1 FULL JOIN t2 ON (t1.col2 = t2.col1) ORDER BY 1,2,3,4;


-- join order planner can handle queries with multi joins consisting of outer joins with simple join clause

SELECT t1.*, t2.*, t3.* FROM t1 RIGHT JOIN t2 ON (t1.col1 = t2.col1) INNER JOIN t3 ON (t3.col1 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t1 RIGHT JOIN t2 ON (t1.col1 = t2.col1) INNER JOIN t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t1 RIGHT JOIN t2 ON (t1.col2 = t2.col2) INNER JOIN t3 ON (t3.col2 = t1.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t1 RIGHT JOIN t2 ON (t1.col2 = t2.col2) INNER JOIN t3 ON (t3.col2 = t2.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t1 RIGHT JOIN t2 ON (t1.col2 = t2.col1) INNER JOIN t3 ON (t3.col2 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t1 RIGHT JOIN t2 ON (t1.col2 = t2.col1) INNER JOIN t3 ON (t3.col2 = t2.col1) ORDER BY 1,2,3,4,5,6;

SELECT t1.*, t2.*, t3.* FROM t2 LEFT JOIN t1 ON (t1.col1 = t2.col1) INNER JOIN t3 ON (t3.col1 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t2 LEFT JOIN t1 ON (t1.col1 = t2.col1) INNER JOIN t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t2 LEFT JOIN t1 ON (t1.col2 = t2.col2) INNER JOIN t3 ON (t3.col2 = t1.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t2 LEFT JOIN t1 ON (t1.col2 = t2.col2) INNER JOIN t3 ON (t3.col2 = t2.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t2 LEFT JOIN t1 ON (t1.col2 = t2.col1) INNER JOIN t3 ON (t3.col2 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t2 LEFT JOIN t1 ON (t1.col2 = t2.col1) INNER JOIN t3 ON (t3.col2 = t2.col1) ORDER BY 1,2,3,4,5,6;

SELECT t1.*, t2.*, t3.* FROM t1 FULL JOIN t2 ON (t1.col1 = t2.col1) INNER JOIN t3 ON (t3.col1 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t1 FULL JOIN t2 ON (t1.col1 = t2.col1) INNER JOIN t3 ON (t3.col1 = t2.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t1 FULL JOIN t2 ON (t1.col2 = t2.col2) INNER JOIN t3 ON (t3.col2 = t1.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t1 FULL JOIN t2 ON (t1.col2 = t2.col2) INNER JOIN t3 ON (t3.col2 = t2.col2) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t1 FULL JOIN t2 ON (t1.col2 = t2.col1) INNER JOIN t3 ON (t3.col2 = t1.col1) ORDER BY 1,2,3,4,5,6;
SELECT t1.*, t2.*, t3.* FROM t1 FULL JOIN t2 ON (t1.col2 = t2.col1) INNER JOIN t3 ON (t3.col2 = t2.col1) ORDER BY 1,2,3,4,5,6;

-- join order planner cannot handle semi joins

SELECT t1.* FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t1.col1 = t2.col1) ORDER BY 1,2;
SELECT t1.* FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t1.col2 = t2.col2) ORDER BY 1,2;
SELECT t2.* FROM t2 WHERE EXISTS (SELECT * FROM t1 WHERE t1.col1 = t2.col1) ORDER BY 1,2;
SELECT t2.* FROM t2 WHERE EXISTS (SELECT * FROM t1 WHERE t1.col2 = t2.col2) ORDER BY 1,2;


------------------------- wrong results below, should not be supported if there exists nonsimple join clause

-- join order planner cannot handle anti join between tables with simple join clause

SELECT t1.* FROM t1 WHERE NOT EXISTS (SELECT * FROM t2 WHERE t1.col1 = t2.col1) ORDER BY 1,2;
SELECT t1.* FROM t1 WHERE NOT EXISTS (SELECT * FROM t2 WHERE t1.col2 = t2.col2) ORDER BY 1,2;
SELECT t2.* FROM t2 WHERE NOT EXISTS (SELECT * FROM t1 WHERE t1.col1 = t2.col1) ORDER BY 1,2;
SELECT t2.* FROM t2 WHERE NOT EXISTS (SELECT * FROM t1 WHERE t1.col2 = t2.col2) ORDER BY 1,2;

-- join order planner cannot handle left outer join between tables with nonsimple join clause

-- where constraint(t1.col1 IS NULL or t2.col2 IS NULL) is considered as join constraint
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON (t1.col1 = t2.col1) WHERE (t1.col1 IS NULL or t2.col2 IS NULL) ORDER BY 1,2,3,4;
-- join constraint(t1.col1 < 0) is considered as where constraint
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON (t1.col1 = t2.col1 and t1.col1 < 0) ORDER BY 1,2,3,4;
