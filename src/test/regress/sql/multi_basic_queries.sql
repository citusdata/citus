--
-- MULTI_BASIC_QUERIES
--

-- Execute simple sum, average, and count queries on data recently uploaded to
-- our partitioned table.


SELECT count(*) FROM lineitem;

SELECT sum(l_extendedprice) FROM lineitem;

SELECT avg(l_extendedprice) FROM lineitem;

-- Verify that we can do queries in read-only mode
BEGIN;
SET TRANSACTION READ ONLY;
SELECT count(*) FROM lineitem;
COMMIT;

-- Verify temp tables which are used for final result aggregation don't persist.
SELECT count(*) FROM pg_class WHERE relname LIKE 'pg_merge_job_%' AND relkind = 'r';
