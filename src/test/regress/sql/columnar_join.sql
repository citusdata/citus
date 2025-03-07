CREATE SCHEMA am_columnar_join;
SET search_path TO am_columnar_join;

CREATE TABLE users (id int, name text) USING columnar;
INSERT INTO users SELECT a, 'name' || a FROM generate_series(0,30-1) AS a;

CREATE TABLE things (id int, user_id int, name text) USING columnar;
INSERT INTO things SELECT a, a % 30, 'thing' || a FROM generate_series(1,300) AS a;

-- force the nested loop to rescan the table
SET enable_material TO off;
SET enable_hashjoin TO off;
SET enable_mergejoin TO off;

SELECT count(*)
FROM users
JOIN things ON (users.id = things.user_id)
WHERE things.id > 290;

-- verify the join uses a nested loop to trigger the rescan behaviour
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM users
JOIN things ON (users.id = things.user_id)
WHERE things.id > 299990;

EXPLAIN (COSTS OFF)
SELECT u1.id, u2.id, COUNT(u2.*)
FROM users u1
JOIN users u2 ON (u1.id::text = u2.name)
WHERE u2.id > 299990
GROUP BY u1.id, u2.id;

-- ================================
-- join COLUMNAR with HEAP
-- ================================

-- Left Join with Mixed Table Types
CREATE TABLE tbl_left_heap1 (id integer);
CREATE TABLE tbl_left_heap2 (id integer);
CREATE TABLE tbl_left_columnar (id integer) USING columnar;

INSERT INTO tbl_left_heap1 VALUES (1), (2), (3), (4);
INSERT INTO tbl_left_heap2 VALUES (2), (3), (5), (6);
INSERT INTO tbl_left_columnar VALUES (3), (5), (7);

SELECT *
FROM tbl_left_heap1 h1
LEFT JOIN tbl_left_heap2 h2 ON h1.id = h2.id
LEFT JOIN tbl_left_columnar c ON h2.id = c.id
ORDER BY 1;

-- Left Join with Filter
CREATE TABLE tbl_left_filter_heap1 (id integer);
CREATE TABLE tbl_left_filter_heap2 (id integer);
CREATE TABLE tbl_left_filter_columnar (id integer) USING columnar;

INSERT INTO tbl_left_filter_heap1 VALUES (1), (2), (3), (4);
INSERT INTO tbl_left_filter_heap2 VALUES (2), (3), (5), (6);
INSERT INTO tbl_left_filter_columnar VALUES (3), (5), (7);

SELECT *
FROM tbl_left_filter_heap1 h1
LEFT JOIN tbl_left_filter_heap2 h2 ON h1.id = h2.id
LEFT JOIN tbl_left_filter_columnar c ON h2.id = c.id
WHERE h1.id > 2
ORDER BY 1;


-- Right Join with Mixed Table Types
CREATE TABLE tbl_right_heap1 (id integer);
CREATE TABLE tbl_right_heap2 (id integer);
CREATE TABLE tbl_right_columnar (id integer) USING columnar;

INSERT INTO tbl_right_heap1 VALUES (1), (2), (3), (4);
INSERT INTO tbl_right_heap2 VALUES (2), (3), (5), (6);
INSERT INTO tbl_right_columnar VALUES (3), (5), (7);

SELECT *
FROM tbl_right_heap1 h1
RIGHT JOIN tbl_right_heap2 h2 ON h1.id = h2.id
RIGHT JOIN tbl_right_columnar c ON h2.id = c.id
ORDER BY 3;

-- Right Join with Filters
CREATE TABLE tbl_right_filter_heap1 (id integer);
CREATE TABLE tbl_right_filter_heap2 (id integer);
CREATE TABLE tbl_right_filter_columnar (id integer) USING columnar;

INSERT INTO tbl_right_filter_heap1 VALUES (1), (2), (3), (4);
INSERT INTO tbl_right_filter_heap2 VALUES (2), (3), (5), (6);
INSERT INTO tbl_right_filter_columnar VALUES (3), (5), (7);

SELECT *
FROM tbl_right_filter_heap1 h1
RIGHT JOIN tbl_right_filter_heap2 h2 ON h1.id = h2.id
RIGHT JOIN tbl_right_filter_columnar c ON h2.id = c.id
WHERE c.id < 6
ORDER BY 3;


-- Inner Join with Mixed Table Types
CREATE TABLE tbl_heap1 (id serial primary key, val integer);
CREATE TABLE tbl_heap2 (id serial primary key, val integer);
CREATE TABLE tbl_columnar (id integer, val integer) USING columnar;
INSERT INTO tbl_heap1 (val) SELECT generate_series(1, 100);
INSERT INTO tbl_heap2 (val) SELECT generate_series(50, 150);
INSERT INTO tbl_columnar SELECT generate_series(75, 125), generate_series(200, 250);

SELECT h1.id, h1.val, h2.val, c.val
FROM tbl_heap1 h1
JOIN tbl_heap2 h2 ON h1.val = h2.val
JOIN tbl_columnar c ON h1.val = c.id
ORDER BY 1;

-- Outer Join with NULLs
CREATE TABLE tbl_null_heap (id integer, val integer);
CREATE TABLE tbl_null_columnar (id integer, val integer) USING columnar;

INSERT INTO tbl_null_heap VALUES (1, NULL), (2, 20), (3, 30);
INSERT INTO tbl_null_columnar VALUES (1, 100), (NULL, 200), (3, 300);

SELECT nh.id, nh.val, nc.val
FROM tbl_null_heap nh
FULL OUTER JOIN tbl_null_columnar nc ON nh.id = nc.id
ORDER BY 1;

-- Join with Aggregates
CREATE TABLE tbl_agg_heap (id serial primary key, val integer);
CREATE TABLE tbl_agg_columnar (id integer, val integer) USING columnar;

INSERT INTO tbl_agg_heap (val) SELECT generate_series(1, 100);
INSERT INTO tbl_agg_columnar SELECT generate_series(50, 150), generate_series(200, 300);

SELECT ah.val AS heap_val, COUNT(ac.val) AS columnar_count
FROM tbl_agg_heap ah
LEFT JOIN tbl_agg_columnar ac ON ah.val = ac.id
GROUP BY ah.val
ORDER BY ah.val;

-- Join with Filters
CREATE TABLE tbl_filter_heap (id integer, val integer);
CREATE TABLE tbl_filter_columnar (id integer, val integer) USING columnar;

INSERT INTO tbl_filter_heap SELECT generate_series(1, 100), generate_series(1001, 1100);
INSERT INTO tbl_filter_columnar SELECT generate_series(90, 120), generate_series(2001, 2031);

SELECT fh.id, fh.val, fc.val
FROM tbl_filter_heap fh
INNER JOIN tbl_filter_columnar fc ON fh.id = fc.id
WHERE fh.val > 1050 AND fc.val < 2025
ORDER BY 1;

-- Cross Join
CREATE TABLE tbl_cross_heap (id integer, val integer);
CREATE TABLE tbl_cross_columnar (id integer, val integer) USING columnar;

INSERT INTO tbl_cross_heap VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO tbl_cross_columnar VALUES (4, 40), (5, 50), (6, 60);

SELECT h.id AS heap_id, h.val AS heap_val, c.id AS columnar_id, c.val AS columnar_val
FROM tbl_cross_heap h
CROSS JOIN tbl_cross_columnar c
ORDER BY 3,4,1,2;

-- Left Join with Mixed Table Types and columnar in the middle
CREATE TABLE tbl_middle_left_heap1 (id integer);
CREATE TABLE tbl_middle_left_heap2 (id integer);
CREATE TABLE tbl_middle_left_columnar (id integer) USING columnar;

INSERT INTO tbl_middle_left_heap1 VALUES (1), (2), (3), (4);
INSERT INTO tbl_middle_left_heap2 VALUES (2), (3), (5), (6);
INSERT INTO tbl_middle_left_columnar VALUES (3), (5), (7);

EXPLAIN (COSTS OFF)
SELECT h1.*, h2.*, c.*
FROM tbl_middle_left_heap1 h1
LEFT JOIN tbl_middle_left_columnar c ON h1.id = c.id
LEFT JOIN tbl_middle_left_heap2 h2 ON c.id = h2.id
ORDER BY 1;

-- End test case
SET client_min_messages TO warning;
DROP SCHEMA am_columnar_join CASCADE;
