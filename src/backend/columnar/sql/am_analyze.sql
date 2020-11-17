--
-- Test the ANALYZE command for cstore_fdw tables.
--

-- ANALYZE uncompressed table
ANALYZE contestant;
SELECT count(*) FROM pg_stats WHERE tablename='contestant';

-- ANALYZE compressed table
ANALYZE contestant_compressed;
SELECT count(*) FROM pg_stats WHERE tablename='contestant_compressed';
