CREATE SCHEMA clock;
SET search_path TO clock;

SHOW citus.enable_cluster_clock;
SET citus.enable_cluster_clock to ON;
SHOW citus.enable_cluster_clock;

CREATE TABLE clock_test (id int, nonid int);
SELECT create_distributed_table('clock_test', 'id', colocate_with := 'none');

--
-- Compare <logical, counter> pairs
--
-- Returns true
SELECT citus_is_clock_after(ROW(5,1), ROW(3,6));

-- Returns false
SELECT citus_is_clock_after(ROW(2,9), ROW(3,0));

-- Returns true
SELECT citus_is_clock_after(ROW(5,6), ROW(5,1));

-- Returns false
SELECT citus_is_clock_after(ROW(5,6), ROW(5,6));

--
-- Check the clock is *monotonically increasing*
--
SELECT citus_get_cluster_clock() \gset t1
SELECT citus_get_cluster_clock() \gset t2
SELECT citus_get_cluster_clock() \gset t3

-- Both should return true
SELECT citus_is_clock_after(:t2citus_get_cluster_clock, :t1citus_get_cluster_clock);
SELECT citus_is_clock_after(:'t3citus_get_cluster_clock', :'t2citus_get_cluster_clock');

-- Returns false
SELECT citus_is_clock_after(:'t1citus_get_cluster_clock', :'t3citus_get_cluster_clock');

--
-- Check the value returned by citus_get_cluster_clock is close to Epoch in ms
--
SELECT (extract(epoch from now()) * 1000)::bigint AS epoch \gset
SELECT logical FROM citus_get_cluster_clock() \gset
-- Returns false
SELECT (:logical - :epoch) > 100 as epoch_drift_in_ms;

-- Transaction that accesses multiple nodes
BEGIN;
INSERT INTO clock_test SELECT generate_series(1, 10000, 1), 0;
SELECT get_current_transaction_id() \gset tid
SET client_min_messages TO DEBUG1;
COMMIT;

--
-- Check to see if the transaction is indeed persisted in the catalog
--
SELECT count(*)
FROM pg_dist_commit_transaction commit_clock
WHERE transaction_id = :'tidget_current_transaction_id';

BEGIN;
INSERT INTO clock_test SELECT generate_series(1, 10000, 1), 0;
SELECT get_current_transaction_id() \gset tid
SET client_min_messages TO DEBUG1;
ROLLBACK;
RESET client_min_messages;

--
-- Check that the transaction is not persisted
--
SELECT count(*)
FROM pg_dist_commit_transaction commit_clock
WHERE transaction_id = :'tidget_current_transaction_id';

RESET client_min_messages;
RESET citus.enable_cluster_clock;
DROP SCHEMA clock CASCADE;
