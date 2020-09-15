--
-- Test querying cstore_fdw tables.
--

-- Settings to make the result deterministic
SET datestyle = "ISO, YMD";

-- Query uncompressed data
SELECT count(*) FROM contestant;
SELECT avg(rating), stddev_samp(rating) FROM contestant;
SELECT country, avg(rating) FROM contestant WHERE rating > 2200
	GROUP BY country ORDER BY country;
SELECT * FROM contestant ORDER BY handle;

-- Query compressed data
SELECT count(*) FROM contestant_compressed;
SELECT avg(rating), stddev_samp(rating) FROM contestant_compressed;
SELECT country, avg(rating) FROM contestant_compressed WHERE rating > 2200
	GROUP BY country ORDER BY country;
SELECT * FROM contestant_compressed ORDER BY handle;

-- Verify that we handle whole-row references correctly
SELECT to_json(v) FROM contestant v ORDER BY rating LIMIT 1;

-- Test variables used in expressions
CREATE FOREIGN TABLE union_first (a int, b int) SERVER cstore_server;
CREATE FOREIGN TABLE union_second (a int, b int) SERVER cstore_server;

INSERT INTO union_first SELECT a, a FROM generate_series(1, 5) a;
INSERT INTO union_second SELECT a, a FROM generate_series(11, 15) a;

(SELECT a*1, b FROM union_first) union all (SELECT a*1, b FROM union_second);

DROP FOREIGN TABLE union_first, union_second;
