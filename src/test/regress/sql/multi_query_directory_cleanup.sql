--
-- MULTI_QUERY_DIRECTORY_CLEANUP
--

-- We execute sub-queries on worker nodes, and copy query results to a directory
-- on the master node for final processing. When the query completes or fails,
-- the resource owner should automatically clean up these intermediate query
-- result files.


SET citus.next_shard_id TO 810000;
SET citus.enable_unique_job_ids TO off;

BEGIN;

-- pg_ls_dir() displays jobids. We explicitly set the jobId sequence
-- here so that the regression output becomes independent of the
-- number of jobs executed prior to running this test.

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
FETCH 1 FROM c_00;
DECLARE c_01 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_01;
DECLARE c_02 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_02;
DECLARE c_03 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_03;
DECLARE c_04 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_04;
DECLARE c_05 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_05;
DECLARE c_06 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_06;
DECLARE c_07 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_07;
DECLARE c_08 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_08;
DECLARE c_09 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_09;
DECLARE c_10 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_10;
DECLARE c_11 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_11;
DECLARE c_12 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_12;
DECLARE c_13 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_13;
DECLARE c_14 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_14;
DECLARE c_15 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_15;
DECLARE c_16 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_16;
DECLARE c_17 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_17;
DECLARE c_18 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_18;
DECLARE c_19 CURSOR FOR SELECT sum(l_extendedprice * l_discount) as revenue FROM lineitem;
FETCH 1 FROM c_19;
SELECT * FROM pg_ls_dir('base/pgsql_job_cache') f ORDER BY f;
-- close first, 17th (first after re-alloc) and last cursor.
CLOSE c_00;
CLOSE c_16;
CLOSE c_19;
SELECT * FROM pg_ls_dir('base/pgsql_job_cache') f ORDER BY f;
ROLLBACK;
SELECT pg_ls_dir('base/pgsql_job_cache');
