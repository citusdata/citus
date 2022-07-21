CREATE TABLE with_ties_table (a INT, b INT);
SELECT create_distributed_table('with_ties_table', 'a');
INSERT INTO with_ties_table VALUES (10, 20), (11, 21), (12, 22), (12, 22), (12, 22), (12, 23), (14, 24);

-- test ordering by distribution column
SELECT a FROM with_ties_table ORDER BY a OFFSET 1 FETCH FIRST 2 ROWS WITH TIES;

-- test ordering by non-distribution column
SELECT b FROM with_ties_table ORDER BY b OFFSET 1 FETCH FIRST 2 ROWS WITH TIES;

-- test ordering by distribution column filtering one shard
SELECT a FROM with_ties_table WHERE a=12 ORDER BY a OFFSET 1 FETCH FIRST 1 ROWS WITH TIES;

-- test ordering by non-distribution column filtering one shard
SELECT b FROM with_ties_table WHERE a=12 ORDER BY b OFFSET 1 FETCH FIRST 1 ROWS WITH TIES;


-- test INSERT SELECTs into local table
CREATE TABLE with_ties_table_2 (a INT, b INT);

-- test ordering by distribution column
INSERT INTO with_ties_table_2 SELECT a, b FROM with_ties_table ORDER BY a OFFSET 1 FETCH FIRST 2 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;

-- test ordering by non-distribution column
INSERT INTO with_ties_table_2 SELECT a, b FROM with_ties_table ORDER BY b OFFSET 1 FETCH FIRST 2 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;

-- test ordering by distribution column filtering one shard
INSERT INTO with_ties_table_2 SELECT a FROM with_ties_table WHERE a=12 ORDER BY a OFFSET 1 FETCH FIRST 1 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;

-- test ordering by non-distribution column filtering one shard
INSERT INTO with_ties_table_2 SELECT a, b FROM with_ties_table WHERE a=12 ORDER BY b OFFSET 1 FETCH FIRST 1 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;


-- test INSERT SELECTs into distributed table
SELECT create_distributed_table('with_ties_table_2', 'a');

-- test ordering by distribution column
INSERT INTO with_ties_table_2 SELECT a, b FROM with_ties_table ORDER BY a OFFSET 1 FETCH FIRST 2 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;

-- test ordering by non-distribution column
INSERT INTO with_ties_table_2 SELECT a, b FROM with_ties_table ORDER BY b OFFSET 1 FETCH FIRST 2 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;

-- test ordering by distribution column filtering one shard
INSERT INTO with_ties_table_2 SELECT a FROM with_ties_table WHERE a=12 ORDER BY a OFFSET 1 FETCH FIRST 1 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;

-- test ordering by non-distribution column filtering one shard
INSERT INTO with_ties_table_2 SELECT a, b FROM with_ties_table WHERE a=12 ORDER BY b OFFSET 1 FETCH FIRST 1 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;


-- test INSERT SELECTs into distributed table with a different distribution column
SELECT undistribute_table('with_ties_table_2');
SELECT create_distributed_table('with_ties_table_2', 'b');

-- test ordering by distribution column
INSERT INTO with_ties_table_2 SELECT a, b FROM with_ties_table ORDER BY a OFFSET 1 FETCH FIRST 2 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;

-- test ordering by non-distribution column
INSERT INTO with_ties_table_2 SELECT a, b FROM with_ties_table ORDER BY b OFFSET 1 FETCH FIRST 2 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;

-- test ordering by distribution column filtering one shard
-- selecting actual b column makes this test flaky but we have tp select something for dist column
INSERT INTO with_ties_table_2 SELECT a, 1 FROM with_ties_table WHERE a=12 ORDER BY a OFFSET 1 FETCH FIRST 1 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;

-- test ordering by non-distribution column filtering one shard
INSERT INTO with_ties_table_2 SELECT a, b FROM with_ties_table WHERE a=12 ORDER BY b OFFSET 1 FETCH FIRST 1 ROWS WITH TIES;
SELECT * FROM with_ties_table_2 ORDER BY a, b;
TRUNCATE with_ties_table_2;


-- test ordering by multiple columns
SELECT a, b FROM with_ties_table ORDER BY a, b OFFSET 1 FETCH FIRST 2 ROWS WITH TIES;

-- test ordering by multiple columns filtering one shard
SELECT a, b FROM with_ties_table WHERE a=12 ORDER BY a, b OFFSET 1 FETCH FIRST 1 ROWS WITH TIES;


-- test without ties
-- test ordering by distribution column
SELECT a FROM with_ties_table ORDER BY a OFFSET 1 FETCH FIRST 2 ROWS ONLY;

-- test ordering by non-distribution column
SELECT b FROM with_ties_table ORDER BY b OFFSET 1 FETCH FIRST 2 ROWS ONLY;

-- test ordering by distribution column filtering one shard
SELECT a FROM with_ties_table WHERE a=12 ORDER BY a OFFSET 1 FETCH FIRST 1 ROWS ONLY;

-- test ordering by non-distribution column filtering one shard
SELECT b FROM with_ties_table WHERE a=12 ORDER BY b OFFSET 1 FETCH FIRST 1 ROWS ONLY;


--test other syntaxes
SELECT a FROM with_ties_table ORDER BY a OFFSET 1 FETCH NEXT 2 ROW WITH TIES;
SELECT a FROM with_ties_table ORDER BY a OFFSET 2 FETCH NEXT ROW WITH TIES;
