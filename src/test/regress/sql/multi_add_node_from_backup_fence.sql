--
-- Test that citus_promote_clone_and_rebalance() keeps its write fence while
-- the clone catches up, and that it does not promote the clone after a node
-- lost the fence. The metadata nodes hold the fence in transactions that stay
-- idle meanwhile. The primary has no shards, so the fence locks no shards on
-- them.
--
-- Pausing the replay on the clone keeps the promotion waiting for the clone to
-- catch up. The promotion runs over dblink. Both promotions fail before
-- pg_promote(), so the clone stays a replica.
--
-- The test also checks that the promotion makes the primary flush its WAL up to
-- the catch-up target, so that the clone does not wait for an unrelated flush
-- to receive it.
--
CREATE SCHEMA promotion_fence;
SET search_path TO promotion_fence;
SET statement_timeout TO '120s';

SET client_min_messages TO WARNING;
SELECT 1 FROM master_add_node('localhost', :worker_7_port);
SELECT citus_add_clone_node('localhost', :follower_worker_7_port, 'localhost', :worker_7_port) AS clone_node_id \gset
RESET client_min_messages;

SELECT count(*) AS primary_placements FROM pg_dist_placement
WHERE groupid = (SELECT groupid FROM pg_dist_node WHERE nodeport = :worker_7_port);

SET citus.enable_ddl_propagation TO off;
CREATE EXTENSION dblink;
RESET citus.enable_ddl_propagation;

SELECT format('host=localhost port=%s dbname=%s user=%s', :master_port, current_database(), current_user) AS coordinator_conn,
       format('host=localhost port=%s dbname=%s user=%s', :worker_7_port, current_database(), current_user) AS primary_conn,
       format('host=localhost port=%s dbname=%s user=%s', :follower_worker_7_port, current_database(), current_user) AS clone_conn \gset
SELECT set_config('promotion_fence.primary_conn', :'primary_conn', false) IS NOT NULL AS set,
       set_config('promotion_fence.clone_conn', :'clone_conn', false) IS NOT NULL AS set;

-- pause the replay on the clone, then write WAL on the primary that it has to catch up with
SELECT r AS paused FROM dblink(:'clone_conn', $$SELECT pg_wal_replay_pause()$$) AS t(r text);
DO $$
BEGIN
    WHILE (SELECT state FROM dblink(current_setting('promotion_fence.clone_conn'),
                                    $q$SELECT pg_get_wal_replay_pause_state()$q$) AS t(state text)) <> 'paused' LOOP
        PERFORM pg_sleep(0.01);
    END LOOP;
END $$;
SELECT dblink_exec(:'primary_conn', 'CREATE TABLE promotion_fence_wal (id int); DROP TABLE promotion_fence_wal');

-- idle_in_transaction_session_timeout on the primary does not end the fence
SELECT dblink_exec(:'primary_conn', 'ALTER SYSTEM SET idle_in_transaction_session_timeout TO ''1s''');
SELECT r AS reloaded FROM dblink(:'primary_conn', 'SELECT pg_catalog.pg_reload_conf()') AS t(r bool);
DO $$
BEGIN
    WHILE (SELECT timeout FROM dblink(current_setting('promotion_fence.primary_conn'),
                                      $q$SELECT current_setting('idle_in_transaction_session_timeout')$q$) AS t(timeout text)) <> '1s' LOOP
        PERFORM pg_sleep(0.01);
    END LOOP;
END $$;

-- a transaction that is still open on the primary wrote WAL, which the primary
-- does not flush on its own right away
SELECT dblink_connect('unflushed', :'primary_conn');
SELECT dblink_exec('unflushed', 'SET idle_in_transaction_session_timeout TO 0');
SELECT dblink_exec('unflushed', 'CREATE TABLE promotion_fence_unflushed (id int)');
SELECT dblink_exec('unflushed', 'BEGIN');
SELECT dblink_exec('unflushed', 'INSERT INTO promotion_fence_unflushed VALUES (1)');
SELECT lsn AS unflushed_lsn FROM dblink('unflushed', 'SELECT pg_current_wal_insert_lsn()') AS t(lsn pg_lsn) \gset

-- the promotions are found waiting for the clone by their connection to it,
-- so wait for the ones of earlier promotions to go away first
DO $$
BEGIN
    WHILE EXISTS (
        SELECT FROM dblink(current_setting('promotion_fence.clone_conn'),
                           $q$SELECT 1 FROM pg_stat_activity
                              WHERE query LIKE 'SELECT pg_last_wal_replay_lsn()%'
                                AND pid <> pg_backend_pid()$q$) AS t(found int)) LOOP
        PERFORM pg_sleep(0.01);
    END LOOP;
END $$;
SELECT dblink_connect('idle_promotion', :'coordinator_conn');
SELECT dblink_send_query('idle_promotion', format('SELECT citus_promote_clone_and_rebalance(%s)', :clone_node_id));
DO $$
BEGIN
    WHILE dblink_is_busy('idle_promotion') = 1 AND NOT EXISTS (
        SELECT FROM dblink(current_setting('promotion_fence.clone_conn'),
                           $q$SELECT 1 FROM pg_stat_activity
                              WHERE query LIKE 'SELECT pg_last_wal_replay_lsn()%'
                                AND pid <> pg_backend_pid()$q$) AS t(found int)) LOOP
        PERFORM pg_sleep(0.01);
    END LOOP;
END $$;
-- the promotion made the primary flush the WAL of the open transaction
SELECT r AS open_transaction_wal_flushed
FROM dblink(:'primary_conn', format('SELECT pg_current_wal_flush_lsn() >= %L', :'unflushed_lsn')) AS t(r bool);
SELECT pg_sleep(2);
SELECT relation AS fence_on_primary
FROM dblink(:'primary_conn', $$SELECT relation::regclass::text FROM pg_locks
                               WHERE relation IN ('pg_catalog.pg_dist_node'::regclass,
                                                  'pg_catalog.pg_dist_transaction'::regclass)
                                 AND mode = 'ExclusiveLock' AND granted
                               ORDER BY 1$$) AS t(relation text);

SELECT pg_cancel_backend(pid) AS cancelled FROM pg_stat_activity
WHERE query LIKE 'SELECT citus_promote_clone_and_rebalance%' AND state = 'active';
SELECT count(*) AS promotion_rows FROM dblink_get_result('idle_promotion', false) AS t(r text);
SELECT dblink_disconnect('idle_promotion');
SELECT dblink_exec('unflushed', 'ROLLBACK');
SELECT dblink_disconnect('unflushed');

SELECT dblink_exec(:'primary_conn', 'ALTER SYSTEM RESET idle_in_transaction_session_timeout');
SELECT r AS reloaded FROM dblink(:'primary_conn', 'SELECT pg_catalog.pg_reload_conf()') AS t(r bool);

-- the clone is not promoted after the primary lost the fence
DO $$
BEGIN
    WHILE EXISTS (
        SELECT FROM dblink(current_setting('promotion_fence.clone_conn'),
                           $q$SELECT 1 FROM pg_stat_activity
                              WHERE query LIKE 'SELECT pg_last_wal_replay_lsn()%'
                                AND pid <> pg_backend_pid()$q$) AS t(found int)) LOOP
        PERFORM pg_sleep(0.01);
    END LOOP;
END $$;
SELECT dblink_connect('lost_promotion', :'coordinator_conn');
SELECT dblink_send_query('lost_promotion', format('SELECT citus_promote_clone_and_rebalance(%s)', :clone_node_id));
DO $$
BEGIN
    WHILE dblink_is_busy('lost_promotion') = 1 AND NOT EXISTS (
        SELECT FROM dblink(current_setting('promotion_fence.clone_conn'),
                           $q$SELECT 1 FROM pg_stat_activity
                              WHERE query LIKE 'SELECT pg_last_wal_replay_lsn()%'
                                AND pid <> pg_backend_pid()$q$) AS t(found int)) LOOP
        PERFORM pg_sleep(0.01);
    END LOOP;
END $$;
SELECT count(*) AS terminated
FROM dblink(:'primary_conn', $$SELECT pg_terminate_backend(pid) FROM pg_locks
                               WHERE relation = 'pg_catalog.pg_dist_transaction'::regclass
                                 AND mode = 'ExclusiveLock' AND granted$$) AS t(terminated bool);
SELECT r AS resumed FROM dblink(:'clone_conn', $$SELECT pg_wal_replay_resume()$$) AS t(r text);

-- the error depends on when the coordinator notices the closed connection
SET client_min_messages TO WARNING;
SELECT count(*) AS promotion_rows FROM dblink_get_result('lost_promotion', false) AS t(r text);
RESET client_min_messages;
SELECT dblink_error_message('lost_promotion') <> 'OK' AS promotion_failed;
SELECT dblink_disconnect('lost_promotion');

SELECT r AS clone_in_recovery FROM dblink(:'clone_conn', $$SELECT pg_is_in_recovery()$$) AS t(r bool);
SELECT isactive, nodeisclone FROM pg_dist_node WHERE nodeid = :clone_node_id;

-- cleanup
SELECT dblink_exec(:'primary_conn', 'DROP TABLE promotion_fence_unflushed');
SET client_min_messages TO WARNING;
DROP SCHEMA promotion_fence CASCADE;
RESET client_min_messages;
RESET statement_timeout;
