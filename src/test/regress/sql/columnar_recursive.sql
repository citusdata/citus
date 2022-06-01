
CREATE TABLE t1(a int, b int) USING columnar;
CREATE TABLE t2(a int, b int) USING columnar;

CREATE FUNCTION f(x INT) RETURNS INT AS $$
  INSERT INTO t1 VALUES(x, x * 2) RETURNING b - 1;
$$ LANGUAGE SQL;

--
-- Following query will start a write to t1 before finishing
-- write to t1, so it tests that we handle recursive writes
-- correctly.
--
INSERT INTO t2 SELECT i, f(i) FROM generate_series(1, 5) i;

-- there are no subtransactions, so above statement should batch
-- INSERTs inside the UDF and create on stripe per table.
SELECT relname, count(*) FROM columnar.stripe a, pg_class b
WHERE columnar.get_storage_id(b.oid)=a.storage_id AND relname IN ('t1', 't2')
GROUP BY relname
ORDER BY relname;

SELECT * FROM t1 ORDER BY a;
SELECT * FROM t2 ORDER BY a;

TRUNCATE t1;
TRUNCATE t2;
DROP FUNCTION f(INT);

--
-- Test the case when 2 writes are going on concurrently in the
-- same executor, and those 2 writes are dependent.
--
WITH t AS (
	INSERT INTO t1 SELECT i, 2*i FROM generate_series(1, 5) i RETURNING *
)
INSERT INTO t2 SELECT t.a, t.a+1 FROM t;

SELECT * FROM t1;
SELECT * FROM t2;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

TRUNCATE t1;
TRUNCATE t2;

--
-- Test the case when there are 2 independent inserts in a CTE.
-- Also tests the case where some of the tuple_inserts happen in
-- ExecutorFinish() instead of ExecutorRun().
--
WITH t AS (
	INSERT INTO t1 SELECT i, 2*i FROM generate_series(1, 5) i RETURNING *
)
INSERT INTO t2 SELECT i, (select count(*) from t1) FROM generate_series(1, 3) i;

SELECT * FROM t1;
SELECT * FROM t2;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

TRUNCATE t1;
TRUNCATE t2;

--
-- double insert on the same relation
--
WITH t AS (
	INSERT INTO t1 SELECT i, 2*i FROM generate_series(1, 5) i RETURNING *
)
INSERT INTO t1 SELECT t.a, t.a+1 FROM t;

SELECT * FROM t1 ORDER BY a, b;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

TRUNCATE t1;
TRUNCATE t2;

--
-- A test where result of a UDF call will depend on execution
-- of previous UDF calls.
--

CREATE FUNCTION g(x INT) RETURNS INT AS $$
  INSERT INTO t1 VALUES(x, x * 2);
  SELECT count(*)::int FROM t1;
$$ LANGUAGE SQL;

-- t3 and t4 are heap tables to help with cross-checking results
CREATE TABLE t3(a int, b int);
CREATE TABLE t4(a int, b int);

CREATE FUNCTION g2(x INT) RETURNS INT AS $$
  INSERT INTO t3 VALUES(x, x * 2);
  SELECT count(*)::int FROM t3;
$$ LANGUAGE SQL;

INSERT INTO t2 SELECT i, g(i) FROM generate_series(1, 5) i;
INSERT INTO t4 SELECT i, g2(i) FROM generate_series(1, 5) i;

-- check that t1==t3 and t2==t4.
((table t1) except (table t3)) union ((table t3) except (table t1));
((table t2) except (table t4)) union ((table t4) except (table t2));

SELECT * FROM t2 ORDER BY a, b;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

TRUNCATE t1, t2, t3, t4;

--
-- INSERT into the same relation that was INSERTed into in the UDF
--
INSERT INTO t1 SELECT i, g(i) FROM generate_series(1, 3) i;
INSERT INTO t3 SELECT i, g2(i) FROM generate_series(1, 3) i;
SELECT * FROM t1 ORDER BY a, b;
SELECT * FROM t3 ORDER BY a, b;

-- check that t1==t3 and t2==t4.
((table t1) except (table t3)) union ((table t3) except (table t1));
((table t2) except (table t4)) union ((table t4) except (table t2));

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

DROP FUNCTION g(int), g2(int);
TRUNCATE t1, t2, t3, t4;

--
-- EXCEPTION in plpgsql, which is implemented internally using
-- subtransactions. plgpsql uses SPI to execute INSERT statements.
--

CREATE FUNCTION f(a int) RETURNS VOID AS $$
DECLARE
 x int;
BEGIN
  INSERT INTO t1 SELECT i, i + 1 FROM generate_series(a, a + 1) i;
  x := 10 / a;
  INSERT INTO t1 SELECT i, i * 2 FROM generate_series(a + 2, a + 3) i;
EXCEPTION WHEN division_by_zero THEN
  INSERT INTO t1 SELECT i, i + 1 FROM generate_series(a + 2, a + 3) i;
END;
$$ LANGUAGE plpgsql;

SELECT f(10);
SELECT f(0), f(20);

SELECT * FROM t1 ORDER BY a, b;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

DROP FUNCTION f(int);
DROP TABLE t1, t2, t3, t4;

