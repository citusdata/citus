--
-- MULTI_WORKING_COLUMNS
--

-- Columns that are used in sorting and grouping but that do not appear in the
-- projection order are called working (resjunk) columns. We check in here that
-- these columns are pulled to the master, and are correctly used in sorting and
-- grouping.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1040000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1040000;


SELECT l_quantity FROM lineitem ORDER BY l_shipdate, l_quantity LIMIT 20;

SELECT l_quantity, count(*) as count FROM lineitem
	GROUP BY l_quantity, l_shipdate ORDER BY l_quantity, count
	LIMIT 20;

SELECT l_quantity, l_shipdate, count(*) as count FROM lineitem
	GROUP BY l_quantity, l_shipdate ORDER BY l_quantity, count, l_shipdate
	LIMIT 20;
