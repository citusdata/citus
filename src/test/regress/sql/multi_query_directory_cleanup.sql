--
-- MULTI_QUERY_DIRECTORY_CLEANUP
--

-- We execute sub-queries on worker nodes, and copy query results to a directory
-- on the master node for final processing. When the query completes or fails,
-- the resource owner should automatically clean up these intermediate query
-- result files.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 810000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 810000;


BEGIN;

-- pg_ls_dir() displays jobids. We explicitly set the jobId sequence
-- here so that the regression output becomes independent of the
-- number of jobs executed prior to running this test.
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1250;

SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;

SELECT pg_ls_dir('base/pgsql_job_cache');

COMMIT;

SELECT pg_ls_dir('base/pgsql_job_cache');

BEGIN;

SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;

SELECT pg_ls_dir('base/pgsql_job_cache');

ROLLBACK;

SELECT pg_ls_dir('base/pgsql_job_cache');

-- Test that multiple job directories are all cleaned up correctly,
-- both individually (by closing a cursor) and in bulk when ending a
-- transaction.
BEGIN;
DECLARE c_00 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_01 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_02 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_03 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_04 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_05 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_06 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_07 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_08 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_09 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_10 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_11 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_12 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_13 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_14 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_15 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_16 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_17 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_18 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
DECLARE c_19 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
SELECT * FROM pg_ls_dir('base/pgsql_job_cache') f ORDER BY f;
-- close first, 17th (first after re-alloc) and last cursor.
CLOSE c_00;
CLOSE c_16;
CLOSE c_19;
SELECT * FROM pg_ls_dir('base/pgsql_job_cache') f ORDER BY f;
ROLLBACK;
SELECT pg_ls_dir('base/pgsql_job_cache');
