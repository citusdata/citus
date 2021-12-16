--
-- Test querying columnar tables.
--

CREATE SCHEMA columnar_join;
SET search_path to columnar_join, public;

-- Settings to make the result deterministic
SET datestyle = "ISO, YMD";

-- Query uncompressed data
SELECT count(*) FROM contestant;
SELECT avg(rating), stddev_samp(rating) FROM contestant;
SELECT country, avg(rating) FROM contestant WHERE rating > 2200
	GROUP BY country ORDER BY country;
SELECT * FROM contestant ORDER BY handle;

-- all special column accesses should fail
SELECT ctid FROM contestant;
SELECT cmin FROM contestant;
SELECT cmax FROM contestant;
SELECT xmin FROM contestant;
SELECT xmax FROM contestant;
SELECT tableid FROM contestant;

-- sample scans should fail
SELECT * FROM contestant TABLESAMPLE SYSTEM(0.1);

-- Query compressed data
SELECT count(*) FROM contestant_compressed;
SELECT avg(rating), stddev_samp(rating) FROM contestant_compressed;
SELECT country, avg(rating) FROM contestant_compressed WHERE rating > 2200
	GROUP BY country ORDER BY country;
SELECT * FROM contestant_compressed ORDER BY handle;

-- Verify that we handle whole-row references correctly
SELECT to_json(v) FROM contestant v ORDER BY rating LIMIT 1;

-- Test variables used in expressions
CREATE TABLE union_first (a int, b int) USING columnar;
CREATE TABLE union_second (a int, b int) USING columnar;

INSERT INTO union_first SELECT a, a FROM generate_series(1, 5) a;
INSERT INTO union_second SELECT a, a FROM generate_series(11, 15) a;

(SELECT a*1, b FROM union_first) union all (SELECT a*1, b FROM union_second);

DROP TABLE union_first, union_second;

-- https://github.com/citusdata/citus/issues/4600
CREATE TABLE INT8_TBL_columnar(q1 int8, q2 int8) using columnar;

INSERT INTO INT8_TBL_columnar VALUES('  123   ','  456');
INSERT INTO INT8_TBL_columnar VALUES('123   ','4567890123456789');
INSERT INTO INT8_TBL_columnar VALUES('4567890123456789','123');
INSERT INTO INT8_TBL_columnar VALUES(+4567890123456789,'4567890123456789');
INSERT INTO INT8_TBL_columnar VALUES('+4567890123456789','-4567890123456789');

explain (costs off, summary off) select * from
  INT8_TBL_columnar a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   INT8_TBL_columnar b cross join INT8_TBL_columnar c) ss
  on a.q2 = ss.bq1;

explain (costs off, summary off)
  SELECT COUNT(*) FROM INT8_TBL_columnar t1 JOIN
  LATERAL (SELECT * FROM INT8_TBL_columnar t2 WHERE t1.q1 = t2.q1)
  as foo ON (true);

CREATE TABLE INT8_TBL_heap (LIKE INT8_TBL_columnar) USING heap;
INSERT INTO INT8_TBL_heap SELECT * FROM INT8_TBL_columnar;

CREATE TABLE result_columnar AS
select * from
  INT8_TBL_columnar a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   INT8_TBL_columnar b cross join INT8_TBL_columnar c) ss
  on a.q2 = ss.bq1;

CREATE TABLE result_regular AS
select * from
  INT8_TBL_heap a left join lateral
  (select b.q1 as bq1, c.q1 as cq1, least(a.q1,b.q1,c.q1) from
   INT8_TBL_heap b cross join INT8_TBL_heap c) ss
  on a.q2 = ss.bq1;

-- 2 results should be identical, so the following should be empty
(table result_columnar EXCEPT table result_regular)
UNION
(table result_regular EXCEPT table result_columnar);

SET client_min_messages TO WARNING;
DROP SCHEMA columnar_join CASCADE;

--
-- https://github.com/citusdata/citus/issues/5258
--

set default_table_access_method to columnar;
CREATE TABLE atest1 ( a int, b text );
CREATE TABLE atest2 (col1 varchar(10), col2 boolean);

INSERT INTO atest1 VALUES (1, 'one');
SELECT * FROM atest1; -- ok
SELECT * FROM atest2; -- ok
INSERT INTO atest1 VALUES (2, 'two'); -- ok
INSERT INTO atest1 SELECT 1, b FROM atest1; -- ok

SELECT * FROM atest2 WHERE ( col1 IN ( SELECT b FROM atest1 ) );

DROP TABLE atest1;
DROP TABLE atest2;
set default_table_access_method to default;

create temp table t1 (f1 numeric(14,0), f2 varchar(30)) USING columnar;
select * from
  (select distinct f1, f2, (select f2 from t1 x where x.f1 = up.f1) as fs
   from t1 up) ss
group by f1,f2,fs;
drop table t1;

CREATE TABLE tbl1(c0 int4range) USING COLUMNAR;
CREATE TABLE tbl2(c0 int4range);

INSERT INTO tbl1(c0) VALUES('[0,1]'::int4range);
INSERT INTO tbl1(c0) VALUES('[0,1]'::int4range);

SELECT tbl1.c0 FROM tbl1 JOIN tbl2 ON tbl1.c0=tbl2.c0 WHERE tbl2.c0<=tbl2.c0 ISNULL;
DROP TABLE tbl1;
DROP TABLE tbl2;
