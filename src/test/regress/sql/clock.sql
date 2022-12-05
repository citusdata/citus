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
SELECT citus_get_node_clock() \gset t1
SELECT citus_get_node_clock() \gset t2
SELECT citus_get_node_clock() \gset t3

-- Both should return true
SELECT citus_is_clock_after(:'t2citus_get_node_clock', :'t1citus_get_node_clock');
SELECT citus_is_clock_after(:'t3citus_get_node_clock', :'t2citus_get_node_clock');

-- Returns false
SELECT citus_is_clock_after(:'t1citus_get_node_clock', :'t3citus_get_node_clock');

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

INSERT INTO cluster_clock_type values('(4398046511104, 100)'); -- too big to fit into 42 bits
INSERT INTO cluster_clock_type values('(0, 4194304)'); -- too big to fit into 22 bits

DROP TABLE cluster_clock_type;
CREATE TABLE cluster_clock_type(cc cluster_clock UNIQUE);
INSERT INTO cluster_clock_type values('(100, 1)');
INSERT INTO cluster_clock_type values('(100, 1)');
INSERT INTO cluster_clock_type values('(100, 200)');
INSERT INTO cluster_clock_type values('(100, 200)');
INSERT INTO cluster_clock_type values('(100, 100)');
INSERT INTO cluster_clock_type values('(100, 100)');

--
-- Check the value returned by citus_get_node_clock is close to epoch in ms
--
SELECT (extract(epoch from now()) * 1000)::bigint AS epoch,
	citus_get_node_clock() AS latest_clock \gset

-- Returns difference in epoch-milliseconds
SELECT CASE WHEN msdiff BETWEEN 0 AND 25 THEN 0 ELSE msdiff END
FROM ABS(:epoch - cluster_clock_logical(:'latest_clock')) msdiff;

BEGIN;
SELECT citus_get_transaction_clock();
END;

-- Transaction that accesses multiple nodes
BEGIN;
INSERT INTO clock_test SELECT generate_series(1, 10000, 1), 0;
SELECT get_current_transaction_id() \gset tid
SET client_min_messages TO DEBUG1;
-- Capture the transaction timestamp
SELECT citus_get_transaction_clock() as txnclock \gset
COMMIT;

--
-- Check to see if the clock is persisted in the sequence.
--
SELECT result as logseq from run_command_on_workers($$SELECT last_value FROM pg_dist_clock_logical_seq$$) limit 1 \gset
SELECT cluster_clock_logical(:'txnclock') as txnlog \gset
SELECT :logseq = :txnlog;

BEGIN;
INSERT INTO clock_test SELECT generate_series(1, 10000, 1), 0;
SELECT get_current_transaction_id() \gset tid
SET client_min_messages TO DEBUG1;

-- Capture the transaction timestamp
SELECT citus_get_transaction_clock() as txnclock \gset
ROLLBACK;

SELECT result as logseq from run_command_on_workers($$SELECT last_value FROM pg_dist_clock_logical_seq$$) limit 1 \gset
SELECT cluster_clock_logical(:'txnclock') as txnlog \gset
SELECT :logseq = :txnlog;

SELECT run_command_on_workers($$SELECT citus_get_node_clock()$$);

SET citus.enable_cluster_clock to OFF;
BEGIN;
SELECT citus_get_transaction_clock();
END;

SET citus.enable_cluster_clock to ON;

-- Test if the clock UDFs are volatile, result should never be the same
SELECT citus_get_node_clock() = citus_get_node_clock();
select citus_get_transaction_clock() = citus_get_transaction_clock();

-- Test if the clock UDFs are usable by non-superusers
CREATE ROLE non_super_user_clock;
SET ROLE non_super_user_clock;
SELECT citus_get_node_clock();
BEGIN;
SELECT citus_get_transaction_clock();
COMMIT;

-- Test setting the persisted clock value (it must fail)
SELECT setval('pg_dist_clock_logical_seq', 100, true);

\c
RESET client_min_messages;
RESET citus.enable_cluster_clock;
DROP ROLE non_super_user_clock;
DROP SCHEMA clock CASCADE;
