--
-- SINGLE_SHARD_PROCEDURE_OPTIMIZATION
--
-- Tests for citus.enable_single_shard_procedure_optimization GUC.
-- This optimization allows single-statement procedures that execute
-- a single task on a single shard to skip coordinated (2PC) transactions.
--

SET citus.next_shard_id TO 109000;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA single_shard_proc_opt;
SET search_path TO single_shard_proc_opt;

CREATE TABLE test_dist (shard_key int PRIMARY KEY, other_key int);
SELECT create_distributed_table('test_dist', 'shard_key');

-- Enable remote command logging and disable local execution so that
-- all commands go through the distributed path and we can observe
-- BEGIN / PREPARE TRANSACTION / COMMIT PREPARED (or lack thereof).
SET citus.log_remote_commands TO on;
SET citus.enable_local_execution TO off;

------------------------------------------------------------
-- TEST 1: Single-statement INSERT with optimization ON
-- Should succeed without error
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE single_insert(val int)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO test_dist VALUES (val, val * 10);
END;
$$;

SET citus.enable_single_shard_procedure_optimization TO on;
CALL single_insert(1);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 2: Single-statement INSERT with optimization OFF
-- Should also succeed (uses coordinated transaction)
------------------------------------------------------------
SET citus.enable_single_shard_procedure_optimization TO off;
CALL single_insert(2);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 3: Two single-shard inserts in procedure
-- Should ERROR on second statement with optimization ON
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE two_single_inserts(val1 int, val2 int)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO test_dist VALUES (val1, val1 * 10);
    INSERT INTO test_dist VALUES (val2, val2 * 10);
END;
$$;

SET citus.enable_single_shard_procedure_optimization TO on;
CALL two_single_inserts(10, 20);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 4: Multi-row insert + single insert in procedure
-- First statement is multi-task, so coordination kicks in
-- for both statements. No error expected.
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE multi_then_single(input int)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO test_dist VALUES (input + 1, (input + 1) * 10), (input + 2, (input + 2) * 10);
    INSERT INTO test_dist VALUES (input + 3, (input + 3) * 10);
END;
$$;

TRUNCATE test_dist;
SET citus.enable_single_shard_procedure_optimization TO on;
CALL multi_then_single(100);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 5: Explicit BEGIN block should NOT use optimization
-- (IsTransactionBlock() returns true, so optimization is skipped)
------------------------------------------------------------
SET citus.enable_single_shard_procedure_optimization TO on;
BEGIN;
CALL single_insert(50);
COMMIT;
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 6: Nested CALL should NOT use optimization
-- (StoredProcedureLevel > 1, so optimization is skipped)
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE outer_proc(val int)
LANGUAGE plpgsql AS $$
BEGIN
    CALL single_insert(val);
END;
$$;

SET citus.enable_single_shard_procedure_optimization TO on;
CALL outer_proc(60);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 7: Multi-row insert + single insert with optimization OFF
-- Should work fine (normal coordinated transaction)
------------------------------------------------------------
SET citus.enable_single_shard_procedure_optimization TO off;
CALL multi_then_single(200);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 8: Single UPDATE by shard_key with optimization ON
-- WHERE clause targets a single shard, should skip 2PC
------------------------------------------------------------
-- Seed some data
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);

CREATE OR REPLACE PROCEDURE update_by_shard_key(key_val int, new_other int)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE test_dist SET other_key = new_other WHERE shard_key = key_val;
END;
$$;

SET citus.enable_single_shard_procedure_optimization TO on;
CALL update_by_shard_key(2, 999);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 9: Single UPDATE by other_key with optimization ON
-- WHERE clause on non-distribution column => multi-shard query
-- Coordination kicks in normally (optimization does not apply
-- because the planner generates multiple tasks).
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);

CREATE OR REPLACE PROCEDURE update_by_other_key(other_val int, new_other int)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE test_dist SET other_key = new_other WHERE other_key = other_val;
END;
$$;

SET citus.enable_single_shard_procedure_optimization TO on;
CALL update_by_other_key(20, 888);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 10: Two UPDATEs by shard_key in one procedure
-- Should ERROR on second statement with optimization ON
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);

CREATE OR REPLACE PROCEDURE two_updates_by_shard_key(k1 int, v1 int, k2 int, v2 int)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE test_dist SET other_key = v1 WHERE shard_key = k1;
    UPDATE test_dist SET other_key = v2 WHERE shard_key = k2;
END;
$$;

SET citus.enable_single_shard_procedure_optimization TO on;
CALL two_updates_by_shard_key(1, 111, 2, 222);
-- Second UPDATE triggers ERROR; first UPDATE is also rolled back
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 11: Single UPDATE by shard_key with optimization OFF
-- Normal coordinated transaction path
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);

SET citus.enable_single_shard_procedure_optimization TO off;
CALL update_by_shard_key(3, 777);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 12: INSERT + UPDATE in same procedure with optimization ON
-- Should ERROR on the second statement (UPDATE)
------------------------------------------------------------
CREATE OR REPLACE PROCEDURE insert_then_update(val int, upd_key int, new_other int)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO test_dist VALUES (val, val * 10);
    UPDATE test_dist SET other_key = new_other WHERE shard_key = upd_key;
END;
$$;

SET citus.enable_single_shard_procedure_optimization TO on;
CALL insert_then_update(5, 5, 555);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 13: UPDATE modifying the shard_key itself with optimization ON
-- Citus does not allow modifying the distribution column, so this
-- should ERROR regardless of the optimization setting.
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);

CREATE OR REPLACE PROCEDURE update_shard_key_value(old_key int, new_key int)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE test_dist SET shard_key = new_key WHERE shard_key = old_key;
END;
$$;

SET citus.enable_single_shard_procedure_optimization TO on;
CALL update_shard_key_value(2, 200);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

-- Cleanup
SET citus.log_remote_commands TO off;
DROP TABLE test_dist;
DROP PROCEDURE single_insert;
DROP PROCEDURE two_single_inserts;
DROP PROCEDURE multi_then_single;
DROP PROCEDURE outer_proc;
DROP PROCEDURE update_by_shard_key;
DROP PROCEDURE update_by_other_key;
DROP PROCEDURE two_updates_by_shard_key;
DROP PROCEDURE insert_then_update;
DROP PROCEDURE update_shard_key_value;
DROP SCHEMA single_shard_proc_opt;

RESET citus.enable_single_shard_procedure_optimization;
RESET citus.enable_local_execution;
RESET citus.log_remote_commands;
RESET citus.shard_count;
RESET citus.shard_replication_factor;
