
-- Issue #7698: An incorrect query result, where the distributed query plan seems wrong
-- https://github.com/citusdata/citus/issues/7698

CREATE TABLE t1 (vkey int4 ,c10 int4);
CREATE TABLE t3 (vkey int4);
INSERT INTO t3 (vkey) values (1);
INSERT INTO t1 (vkey,c10) values (4, -70);

SELECT  t3.vkey
    FROM  (t1  RIGHT OUTER JOIN t3
    ON (t1.c10 = t3.vkey ))
    WHERE EXISTS (SELECT * FROM t3);

-- Make t1 a distributed table
SELECT create_distributed_table('t1', 'vkey');

-- Result should remain the same after making t1 a distributed table

SELECT  t3.vkey
    FROM  (t1  RIGHT OUTER JOIN t3
    ON (t1.c10 = t3.vkey ))
    WHERE EXISTS (SELECT * FROM t3);

--- cleanup
DROP TABLE t1;
DROP TABLE t3;

-- Issue #7697: Incorrect result from a distributed table full outer join an undistributed table.
-- https://github.com/citusdata/citus/issues/7697

CREATE TABLE t0 (vkey int4 ,c3 timestamp);
CREATE TABLE t3 (vkey int4 ,c26 timestamp);
CREATE TABLE t4 (vkey int4);


INSERT INTO t0 (vkey, c3) VALUES
    (13,make_timestamp(2019, 10, 23, 15, 34, 50));

INSERT INTO t3 (vkey,c26) VALUES
    (1, make_timestamp(2024, 3, 26, 17, 36, 53));

INSERT INTO t4 (vkey) VALUES
    (1);

SELECT * FROM
  (t0 FULL OUTER JOIN t3 ON (t0.c3 = t3.c26 ))
  WHERE (
    EXISTS (SELECT * FROM t4)
  );

-- change t0 to distributed table
SELECT create_distributed_table('t0', 'vkey');

-- Result should remain the same after making t0 a distributed table

SELECT * FROM
  (t0 FULL OUTER JOIN t3 ON (t0.c3 = t3.c26 ))
  WHERE (
    EXISTS (SELECT * FROM t4)
  );

--- cleanup
DROP TABLE t0;
DROP TABLE t3;
DROP TABLE t4;
