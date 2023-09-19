CREATE SCHEMA sqlancer_failures;
SET search_path TO sqlancer_failures;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 92862400;

CREATE TABLE t0 (c0 int, c1 MONEY);
SELECT create_distributed_table('t0', 'c0');
UPDATE t0 SET c1 = ((0.43107963)::MONEY) WHERE ((upper('-14295774') COLLATE "de_CH
.utf8") SIMILAR TO '');
UPDATE t0 SET c1 = 1 WHERE '' COLLATE "C" = '';

CREATE TABLE t1 (c0 text);
SELECT create_distributed_table('t1', 'c0');
INSERT INTO t1 VALUES ('' COLLATE "C");

CREATE TABLE t2 (c0 text, c1 bool, c2 timestamptz default now());
SELECT create_distributed_table('t2', 'c0');
INSERT INTO t2 VALUES ('key', '' COLLATE "C" = '');

CREATE TABLE t3 (c0 text, c1 text, c2 timestamptz default now());
SELECT create_distributed_table('t3', 'c0');
INSERT INTO t3 VALUES ('key', '' COLLATE "C");

CREATE TABLE t4(c0 real, c1 boolean);
SELECT create_distributed_table('t4', 'c1');
INSERT INTO t4 VALUES (1.0, 2 BETWEEN 1 AND 3);
-- NOTE: For some reason shard pruning doesn't happen correctly here. It does
-- work for non boolean const expressions. See explain plans for t5 below that
-- show that. The query still works though. So doesn't seem important enough to
-- fix, since boolean partition columns should not happen much/at all for
-- actual users.
EXPLAIN (COSTS FALSE) SELECT FROM t4 WHERE c1 = 2 BETWEEN 1 AND 3;
EXPLAIN (COSTS FALSE) SELECT FROM t4 WHERE c1 = true;

CREATE TABLE t5(c0 int);
SELECT create_distributed_table('t5', 'c0');
INSERT INTO t5 VALUES (CASE WHEN 2 BETWEEN 1 AND 3 THEN 2 ELSE 1 END);
EXPLAIN (COSTS FALSE) SELECT FROM t5 WHERE c0 = 2;
EXPLAIN (COSTS FALSE) SELECT FROM t5 WHERE c0 = CASE WHEN 2 BETWEEN 1 AND 3 THEN 2 ELSE 1 END;

CREATE TABLE IF NOT EXISTS t6(c0 TEXT  CHECK (TRUE), c1 money ) WITH (autovacuum_vacuum_threshold=1180014707, autovacuum_freeze_table_age=13771154, autovacuum_vacuum_cost_delay=23, autovacuum_analyze_threshold=1935153914, autovacuum_freeze_min_age=721733768, autovacuum_enabled=0, autovacuum_vacuum_cost_limit=9983);
CREATE UNLOGGED TABLE IF NOT EXISTS t7(LIKE t6);
CREATE TABLE t8(LIKE t6 INCLUDING INDEXES);
CREATE UNLOGGED TABLE t9(LIKE t6 EXCLUDING STATISTICS);
CREATE TABLE t10(LIKE t7);

SELECT create_distributed_table('t6', 'c0');
ALTER TABLE t6 ALTER COLUMN c0 SET NOT NULL;
SELECT create_reference_table('t7');
SELECT create_distributed_table('t8', 'c0');
ALTER TABLE t8 ALTER COLUMN c0 SET NOT NULL;
SELECT create_distributed_table('t9', 'c0');
ALTER TABLE t9 ALTER COLUMN c0 SET NOT NULL;
SELECT create_reference_table('t10');

SELECT count(*) FROM (
SELECT ALL t7.c1, t7.c0, t8.c1, t10.c1, t8.c0 FROM t7 CROSS JOIN t10 FULL OUTER JOIN t8 ON (((((((('[832125354,1134163512)'::int4range)*('(0,2106623281)'::int4range)))-('(-600267905,509840582]'::int4range)))*('(-365203965,1662828182)'::int4range)))&<((((((('(-1286467417,697584012]'::int4range)*('[-1691485781,1341103963)'::int4range)))*((('(-1768368435,1719707648)'::int4range)*('(139536997,1275813540]'::int4range)))))*((((('[-2103910157,-1961746758)'::int4range)*('[-834534078,533073939)'::int4range)))*((('[-1030552151,552856781]'::int4range)*('[-1109419376,1205173697]'::int4range))))))))
) AS foo;

CREATE TABLE reference_table(id int, it_name varchar(25), k_no int);
SELECT create_reference_table('reference_table');

CREATE TABLE distributed_table(user_id int, item_id int, buy_count int);
SELECT create_distributed_table('distributed_table', 'user_id');

INSERT INTO distributed_table VALUES
(1, 10),
(2, 22),
(3, 34),
(7, 40);

INSERT INTO reference_table VALUES
(1, '100'),
(null, '202'),
(4, '300'),
(null, '401'),
(null, '402');

-- postgres plans below queries by evaluating joins as below:
--     L
--   /   \
-- ref     L
--       /   \
--     dist  ref

SELECT count(*) FROM distributed_table a
LEFT JOIN reference_table b ON (true)
RIGHT JOIN reference_table c ON (true);

SELECT count(*) FROM distributed_table a
LEFT JOIN (SELECT * FROM reference_table OFFSET 0) b ON (true)
RIGHT JOIN (SELECT * FROM reference_table OFFSET 0) c ON (true);

SELECT count(*) FROM distributed_table a
LEFT JOIN reference_table b ON (true)
RIGHT JOIN reference_table c ON (c.id > 0);

SELECT count(*) FROM distributed_table a
LEFT JOIN (SELECT * FROM reference_table OFFSET 0) b ON (true)
RIGHT JOIN (SELECT * FROM reference_table OFFSET 0) c ON (c.id > 0);

-- drop existing sqlancer tables before next tests
DROP TABLE t0, t1, t2, t3, t4 CASCADE;

CREATE TABLE tbl1(a REAL, b FLOAT, c money);
CREATE TABLE tbl2(a REAL, b FLOAT, c money);

SELECT create_distributed_table('tbl1', 'a');
SELECT create_distributed_table('tbl2', 'b');

INSERT INTO tbl1 VALUES(1, 1, 1);

SET citus.enable_repartition_joins to ON;

SELECT * FROM tbl1, tbl2 WHERE tbl2.c=tbl1.c;

DROP TABLE tbl1, tbl2 CASCADE;

CREATE TABLE IF NOT EXISTS t0(c0 TEXT CHECK (TRUE), c1 money ) WITH (autovacuum_vacuum_threshold=1180014707, autovacuum_freeze_table_age=13771154, autovacuum_vacuum_cost_delay=23, autovacuum_analyze_threshold=1935153914, autovacuum_freeze_min_age=721733768, autovacuum_enabled=0, autovacuum_vacuum_cost_limit=9983);
CREATE UNLOGGED TABLE IF NOT EXISTS t1(LIKE t0);
CREATE TABLE t2(LIKE t0 INCLUDING INDEXES);
CREATE UNLOGGED TABLE t3(LIKE t0 EXCLUDING STATISTICS);
CREATE TABLE t4(LIKE t1);

SELECT create_distributed_table('t0', 'c0');
SELECT create_reference_table('t1');
SELECT create_distributed_table('t2', 'c0');
SELECT create_distributed_table('t3', 'c0');
SELECT create_reference_table('t4');

-- whole join tree for below query is:
--           L
--        /     \
--   t1(ref)     L
--            /     \
--       t0(dist)  t4(ref)
SELECT count(*) FROM (
SELECT ALL t4.c1, t0.c0, t0.c1 FROM ONLY t0
   LEFT OUTER JOIN t4 ON CAST(masklen('142.158.96.44') AS BOOLEAN)
   RIGHT OUTER JOIN t1 ON ((0.024767844)::MONEY) BETWEEN (t1.c1) AND (CAST(0.0602135 AS MONEY))
) AS foo;

-- first subquery has the same join tree as above, so we should error out
SELECT count(*) FROM (
SELECT ALL t4.c1, t0.c0, t0.c1 FROM ONLY t0
   LEFT OUTER JOIN t4 ON CAST(masklen('142.158.96.44') AS BOOLEAN)
   RIGHT OUTER JOIN t1 ON (CAST(0.024767844 AS MONEY)) BETWEEN (t1.c1) AND (CAST(0.0602135 AS MONEY))
   WHERE NOT (((t0.c0)LIKE((t4.c0))))
UNION ALL SELECT t4.c1, t0.c0, t0.c1 FROM ONLY t0
   LEFT OUTER JOIN t4 ON CAST(masklen('142.158.96.44') AS BOOLEAN)
   RIGHT OUTER JOIN t1 ON ((0.024767844)::MONEY) BETWEEN (t1.c1) AND (CAST(0.0602135 AS MONEY))
   WHERE NOT (NOT (((t0.c0)LIKE((t4.c0)))))
UNION ALL SELECT ALL t4.c1, t0.c0, t0.c1 FROM ONLY t0
   LEFT OUTER JOIN t4 ON (masklen('142.158.96.44'))::BOOLEAN
   RIGHT OUTER JOIN t1 ON ((0.024767844)::MONEY) BETWEEN (t1.c1) AND ((0.0602135)::MONEY)
   WHERE (NOT (((t0.c0)LIKE((t4.c0))))) ISNULL
) AS foo;

-- unsupported outer JOIN inside a subquery in WHERE clause
SELECT * FROM distributed_table WHERE buy_count > (
SELECT count(*) FROM distributed_table a
LEFT JOIN reference_table b ON (true)
RIGHT JOIN reference_table c ON (false));

-- unsupported outer JOIN via subqueries
SELECT count(*) FROM (SELECT *, random() FROM distributed_table) AS a
LEFT JOIN (SELECT *, random() FROM reference_table) AS b ON (true)
RIGHT JOIN (SELECT *, random() FROM reference_table) AS c ON (false);

-- unsupported outer JOIN in a sublevel subquery
SELECT
   count(*)
FROM
   (
    SELECT a.* FROM distributed_table a JOIN distributed_table b USING (user_id)
   ) AS bar
JOIN
   (
      SELECT a.* FROM distributed_table a
      LEFT JOIN reference_table b ON (true)
      RIGHT JOIN reference_table c ON (true)
   ) AS unsupported_join
ON (true);

SELECT
   count(*)
FROM
   (
    SELECT a.* FROM distributed_table a JOIN distributed_table b USING (user_id)
   ) AS bar
JOIN
   (
      SELECT a.* FROM distributed_table a
      LEFT JOIN (SELECT * FROM reference_table OFFSET 0) b ON (true)
      RIGHT JOIN (SELECT * FROM reference_table OFFSET 0) c ON (true)
   ) AS unsupported_join
ON (true);


-- unsupported outer JOIN in a sublevel INNER JOIN
SELECT
 COUNT(unsupported_join.*)
FROM
   (distributed_table a
   LEFT JOIN reference_table b ON (true)
   RIGHT JOIN reference_table c ON (true)) as unsupported_join (x,y,z,t,e,f,q)
JOIN
   (reference_table d JOIN reference_table e ON(true)) ON (true);

-- unsupported outer JOIN in a sublevel LEFT JOIN
SELECT
 COUNT(unsupported_join.*)
FROM
   (distributed_table a
   LEFT JOIN reference_table b ON (true)
   RIGHT JOIN reference_table c ON (true)) as unsupported_join
LEFT JOIN
   (reference_table d JOIN reference_table e ON(true)) ON (true);

SELECT
 COUNT(unsupported_join.*)
FROM
   (distributed_table a
   LEFT JOIN (SELECT * FROM reference_table OFFSET 0) b ON (true)
   RIGHT JOIN (SELECT * FROM reference_table OFFSET 0) c ON (true)) as unsupported_join
LEFT JOIN
   (
    (SELECT * FROM reference_table OFFSET 0) d
    JOIN
    (SELECT * FROM reference_table OFFSET 0) e
    ON(true)
   )
ON (true);

-- unsupported outer JOIN in a sublevel RIGHT JOIN
SELECT
 COUNT(unsupported_join.*)
FROM
   ((distributed_table a
   LEFT JOIN reference_table b ON (true)
   RIGHT JOIN reference_table c ON (false))
RIGHT JOIN
   (reference_table d JOIN reference_table e ON(true)) ON (true)) as unsupported_join;

SELECT
 COUNT(unsupported_join.*)
FROM
   ((distributed_table a
   LEFT JOIN (SELECT * FROM reference_table OFFSET 0) b ON (true)
   RIGHT JOIN (SELECT * FROM reference_table OFFSET 0) c ON (false))
RIGHT JOIN
   (
    (SELECT * FROM reference_table OFFSET 0) d
    JOIN
    (SELECT * FROM reference_table OFFSET 0) e
    ON(true)
   )
ON (true)) as unsupported_join;

EXPLAIN (COSTS OFF) SELECT
  unsupported_join.*
FROM
   (distributed_table a
   LEFT JOIN reference_table b ON (true)
   RIGHT JOIN reference_table c ON (true)) as unsupported_join (x,y,z,t,e,f,q)
JOIN
   (reference_table d JOIN reference_table e ON(true)) ON (d.id > 0);

SET client_min_messages TO WARNING;
DROP SCHEMA sqlancer_failures CASCADE;
