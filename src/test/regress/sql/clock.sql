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
SELECT citus_is_clock_after('(5,1)', '(3,6)');

-- Returns false
SELECT citus_is_clock_after('(2,9)', '(3,0)');

-- Returns true
SELECT citus_is_clock_after('(5,6)', '(5,1)');

-- Returns false
SELECT citus_is_clock_after('(5,6)', '(5,6)');

--
-- Check the clock is *monotonically increasing*
--
SELECT citus_get_cluster_clock() \gset t1
SELECT citus_get_cluster_clock() \gset t2
SELECT citus_get_cluster_clock() \gset t3

-- Both should return true
SELECT citus_is_clock_after(:'t2citus_get_cluster_clock', :'t1citus_get_cluster_clock');
SELECT citus_is_clock_after(:'t3citus_get_cluster_clock', :'t2citus_get_cluster_clock');

-- Returns false
SELECT citus_is_clock_after(:'t1citus_get_cluster_clock', :'t3citus_get_cluster_clock');

CREATE TABLE cluster_clock_type(cc cluster_clock);
INSERT INTO cluster_clock_type values('(0, 100)');
INSERT INTO cluster_clock_type values('(100, 0)');
INSERT INTO cluster_clock_type values('(100, 1)');
INSERT INTO cluster_clock_type values('(100, 2)');
INSERT INTO cluster_clock_type values('(100, 200)');
INSERT INTO cluster_clock_type values('(100, 100)');
INSERT INTO cluster_clock_type values('(200, 20)');
INSERT INTO cluster_clock_type values('(200, 3)');
INSERT INTO cluster_clock_type values('(200, 400)');
INSERT INTO cluster_clock_type values('(500, 600)');
INSERT INTO cluster_clock_type values('(500, 0)');

SELECT cc FROM cluster_clock_type ORDER BY 1 ASC;
SELECT cc FROM cluster_clock_type where cc = '(200, 400)';
SELECT cc FROM cluster_clock_type where cc <> '(500, 600)';
SELECT cc FROM cluster_clock_type where cc != '(500, 600)';
SELECT cc FROM cluster_clock_type where cc < '(200, 20)' ORDER BY 1 ASC;
SELECT cc FROM cluster_clock_type where cc <= '(200, 20)' ORDER BY 1 ASC;
SELECT cc FROM cluster_clock_type where cc > '(200, 20)' ORDER BY 1 ASC;
SELECT cc FROM cluster_clock_type where cc >= '(200, 20)' ORDER BY 1 ASC;

CREATE INDEX cc_idx on cluster_clock_type(cc);

-- Multiply rows to check index usage
INSERT INTO cluster_clock_type SELECT a.cc FROM cluster_clock_type a, cluster_clock_type b;
INSERT INTO cluster_clock_type SELECT a.cc FROM cluster_clock_type a, cluster_clock_type b;

EXPLAIN SELECT cc FROM cluster_clock_type ORDER BY 1 ASC LIMIT 1;
SELECT cc FROM cluster_clock_type ORDER BY 1 ASC LIMIT 1;

EXPLAIN SELECT cc FROM cluster_clock_type where cc = '(200, 20)' LIMIT 5;
SELECT cc FROM cluster_clock_type where cc = '(200, 20)' LIMIT 5;

-- Max limits
INSERT INTO cluster_clock_type values('(4398046511103, 0)');
INSERT INTO cluster_clock_type values('(0, 4194303)');
INSERT INTO cluster_clock_type values('(4398046511103, 4194303)');

-- Bad input
INSERT INTO cluster_clock_type values('(-1, 100)');
INSERT INTO cluster_clock_type values('(100, -1)');

INSERT INTO cluster_clock_type values('(4398046511104, 100)'); -- too big to into 42 bits
INSERT INTO cluster_clock_type values('(0, 4194304)'); -- too big to into 22 bits

DROP TABLE cluster_clock_type;
CREATE TABLE cluster_clock_type(cc cluster_clock UNIQUE);
INSERT INTO cluster_clock_type values('(100, 1)');
INSERT INTO cluster_clock_type values('(100, 1)');
INSERT INTO cluster_clock_type values('(100, 200)');
INSERT INTO cluster_clock_type values('(100, 200)');
INSERT INTO cluster_clock_type values('(100, 100)');
INSERT INTO cluster_clock_type values('(100, 100)');

-- Get wall clock epoch in milliseconds
SELECT (extract(epoch from now()) * 1000)::bigint AS epoch \gset

-- This should return the epoch value as there is no difference from (0,0)
SELECT cluster_clock_diff_in_ms('(0,0)') epoch_diff \gset

-- Returns false
SELECT (:epoch - :epoch_diff) > 10 as epoch_diff;

--
-- Check the value returned by citus_get_cluster_clock is close to epoch in ms
--
SELECT citus_get_cluster_clock() AS latest_clock \gset

-- Returns the clock difference from epoch in ms
SELECT cluster_clock_diff_in_ms(:'latest_clock') as epoch_diff_in_ms \gset

-- Returns false
SELECT :epoch_diff_in_ms > 10 as epoch_diff_in_ms;

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
