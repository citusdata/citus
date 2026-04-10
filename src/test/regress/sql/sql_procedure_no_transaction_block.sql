--
-- SQL_PROCEDURE_NO_TRANSACTION_BLOCK
--
-- Tests for citus.enable_procedure_transaction_skip GUC.
-- This optimization allows single-statement procedures that execute
-- a single task on a single shard to skip coordinated (2PC) transactions.
--

SET citus.next_shard_id TO 109000;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA sql_proc_no_txn_block;
SET search_path TO sql_proc_no_txn_block;

CREATE TABLE test_dist (shard_key int PRIMARY KEY, other_key int);
SELECT create_distributed_table('test_dist', 'shard_key');

-- Disable local execution so all commands go through the distributed path.
SET citus.enable_local_execution TO off;

-- Create all procedures upfront (before enabling log_remote_commands)
-- to avoid non-deterministic DDL propagation output.
CREATE OR REPLACE PROCEDURE single_insert(val int)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO test_dist VALUES (val, val * 10);
END;
$$;

CREATE OR REPLACE PROCEDURE two_single_inserts(val1 int, val2 int)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO test_dist VALUES (val1, val1 * 10);
    INSERT INTO test_dist VALUES (val2, val2 * 10);
END;
$$;

CREATE OR REPLACE PROCEDURE multi_then_single(input int)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO test_dist VALUES (input + 1, (input + 1) * 10), (input + 2, (input + 2) * 10);
    INSERT INTO test_dist VALUES (input + 3, (input + 3) * 10);
END;
$$;

CREATE OR REPLACE PROCEDURE outer_proc(val int)
LANGUAGE plpgsql AS $$
BEGIN
    CALL single_insert(val);
END;
$$;

CREATE OR REPLACE PROCEDURE update_by_shard_key(key_val int, new_other int)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE test_dist SET other_key = new_other WHERE shard_key = key_val;
END;
$$;

CREATE OR REPLACE PROCEDURE update_by_other_key(other_val int, new_other int)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE test_dist SET other_key = new_other WHERE other_key = other_val;
END;
$$;

CREATE OR REPLACE PROCEDURE two_updates_by_shard_key(k1 int, v1 int, k2 int, v2 int)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE test_dist SET other_key = v1 WHERE shard_key = k1;
    UPDATE test_dist SET other_key = v2 WHERE shard_key = k2;
END;
$$;

CREATE OR REPLACE PROCEDURE insert_then_update(val int, upd_key int, new_other int)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO test_dist VALUES (val, val * 10);
    UPDATE test_dist SET other_key = new_other WHERE shard_key = upd_key;
END;
$$;

CREATE OR REPLACE PROCEDURE update_shard_key_value(old_key int, new_key int)
LANGUAGE plpgsql AS $$
BEGIN
    UPDATE test_dist SET shard_key = new_key WHERE shard_key = old_key;
END;
$$;

CREATE OR REPLACE PROCEDURE delete_by_shard_key(key_val int)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM test_dist WHERE shard_key = key_val;
END;
$$;

CREATE OR REPLACE PROCEDURE two_deletes(k1 int, k2 int)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM test_dist WHERE shard_key = k1;
    DELETE FROM test_dist WHERE shard_key = k2;
END;
$$;

CREATE OR REPLACE PROCEDURE delete_by_other_key(other_val int)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM test_dist WHERE other_key = other_val;
END;
$$;

CREATE OR REPLACE PROCEDURE reads_then_write(val int)
LANGUAGE plpgsql AS $$
DECLARE cnt int;
BEGIN
    SELECT count(*) INTO cnt FROM test_dist WHERE shard_key = val;
    INSERT INTO test_dist VALUES (val + 100, val * 10);
END;
$$;

CREATE OR REPLACE PROCEDURE loop_insert(start_val int, end_val int)
LANGUAGE plpgsql AS $$
BEGIN
    FOR i IN start_val..end_val LOOP
        INSERT INTO test_dist VALUES (i, i * 10);
    END LOOP;
END;
$$;

-- log_remote_commands is toggled ON only around single-shard CALL statements
-- whose NOTICE output is deterministic (single connection, single shard).
-- It is kept OFF for multi-shard operations (TRUNCATE, multi-shard CALL,
-- SELECT ORDER BY, INSERT seeds) whose parallel NOTICE ordering is
-- non-deterministic across connections.

------------------------------------------------------------
-- TEST 1: Single-statement INSERT with optimization ON
-- Should succeed without error — note: no BEGIN/PREPARE/COMMIT
------------------------------------------------------------
SET citus.enable_procedure_transaction_skip TO on;
SET citus.log_remote_commands TO on;
CALL single_insert(1);
SET citus.log_remote_commands TO off;
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 2: Single-statement INSERT with optimization OFF
-- Should use coordinated transaction (BEGIN + COMMIT)
------------------------------------------------------------
SET citus.enable_procedure_transaction_skip TO off;
SET citus.log_remote_commands TO on;
CALL single_insert(2);
SET citus.log_remote_commands TO off;
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 3: Two single-shard inserts in procedure
-- With static body analysis, this is detected as multi-statement
-- before execution, so it falls back to coordinated transactions.
-- No ERROR, both inserts succeed.
-- logging off: multi-statement procedures use coordinated
-- transactions with non-deterministic output.
------------------------------------------------------------
SET citus.enable_procedure_transaction_skip TO on;
CALL two_single_inserts(10, 20);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 4: Multi-row insert + single insert in procedure
-- First statement is multi-task, so coordination kicks in
-- for both statements. No error expected.
-- logging off: multi-shard output is non-deterministic
------------------------------------------------------------
SET citus.enable_procedure_transaction_skip TO on;
CALL multi_then_single(100);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 5: Explicit BEGIN block should NOT use optimization
-- (IsTransactionBlock() returns true, so optimization is skipped)
------------------------------------------------------------
SET citus.enable_procedure_transaction_skip TO on;
SET citus.log_remote_commands TO on;
BEGIN;
CALL single_insert(50);
COMMIT;
SET citus.log_remote_commands TO off;
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 6: Nested CALL should NOT use optimization
-- (StoredProcedureLevel > 1, so optimization is skipped)
------------------------------------------------------------
SET citus.enable_procedure_transaction_skip TO on;
SET citus.log_remote_commands TO on;
CALL outer_proc(60);
SET citus.log_remote_commands TO off;
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 7: Multi-row insert + single insert with optimization OFF
-- Should work fine (normal coordinated transaction)
-- logging off: multi-shard output is non-deterministic
------------------------------------------------------------
SET citus.enable_procedure_transaction_skip TO off;
CALL multi_then_single(200);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 8: Single UPDATE by shard_key with optimization ON
-- WHERE clause targets a single shard, should skip 2PC
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);
SET citus.enable_procedure_transaction_skip TO on;
SET citus.log_remote_commands TO on;
CALL update_by_shard_key(2, 999);
SET citus.log_remote_commands TO off;
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 9: Single UPDATE by other_key with optimization ON
-- WHERE clause on non-distribution column => multi-shard query
-- Coordination kicks in normally (optimization does not apply
-- because the planner generates multiple tasks).
-- logging off: multi-shard output is non-deterministic
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);
SET citus.enable_procedure_transaction_skip TO on;
CALL update_by_other_key(20, 888);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 10: Two UPDATEs by shard_key in one procedure
-- Detected as multi-statement, uses coordinated transaction.
-- Both updates succeed, no error.
-- logging off: multi-shard coordinated output is non-deterministic
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);
SET citus.enable_procedure_transaction_skip TO on;
CALL two_updates_by_shard_key(1, 111, 2, 222);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 11: Single UPDATE by shard_key with optimization OFF
-- Normal coordinated transaction path
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);
SET citus.enable_procedure_transaction_skip TO off;
SET citus.log_remote_commands TO on;
CALL update_by_shard_key(3, 777);
SET citus.log_remote_commands TO off;
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 12: INSERT + UPDATE in same procedure with optimization ON
-- Detected as multi-statement, uses coordinated transaction.
-- Both operations succeed, no error.
-- logging off: coordinated output is non-deterministic
------------------------------------------------------------
SET citus.enable_procedure_transaction_skip TO on;
CALL insert_then_update(5, 5, 555);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 13: UPDATE modifying the shard_key itself with optimization ON
-- Citus does not allow modifying the distribution column, so this
-- should ERROR regardless of the optimization setting.
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);
SET citus.enable_procedure_transaction_skip TO on;
SET citus.log_remote_commands TO on;
CALL update_shard_key_value(2, 200);
SET citus.log_remote_commands TO off;
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 14: Single-shard DELETE with optimization ON
-- Should succeed without error (skip 2PC)
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);
SET citus.enable_procedure_transaction_skip TO on;
SET citus.log_remote_commands TO on;
CALL delete_by_shard_key(2);
SET citus.log_remote_commands TO off;
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 15: Two single-shard DELETEs in one procedure
-- Detected as multi-statement, uses coordinated transaction.
-- Both deletes succeed, no error.
-- logging off: multi-shard coordinated output is non-deterministic
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);
SET citus.enable_procedure_transaction_skip TO on;
CALL two_deletes(1, 2);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 16: Multi-shard DELETE with optimization ON
-- WHERE clause on non-distribution column => multi-shard query
-- Coordination kicks in normally (optimization does not apply)
-- logging off: multi-shard output is non-deterministic
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);
SET citus.enable_procedure_transaction_skip TO on;
CALL delete_by_other_key(20);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 17: Read(s) then write
-- Static analysis detects 2 SQL statements (SELECT + INSERT),
-- so the procedure falls back to coordinated transactions.
-- logging off: coordinated output is non-deterministic
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);
SET citus.enable_procedure_transaction_skip TO on;
CALL reads_then_write(1);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- Now switch to local execution for Tests 18-21
-- (log_remote_commands is not relevant for local execution)
------------------------------------------------------------
SET citus.enable_local_execution TO on;

------------------------------------------------------------
-- TEST 18: Single-statement INSERT with local execution ON
-- Verify the optimization works correctly with local execution
------------------------------------------------------------
SET citus.enable_procedure_transaction_skip TO on;
CALL single_insert(1);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 19: Two single-shard inserts with local execution ON
-- Detected as multi-statement, uses coordinated transaction.
-- Both inserts succeed, no error.
------------------------------------------------------------
SET citus.enable_procedure_transaction_skip TO on;
CALL two_single_inserts(10, 20);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 20: Single-shard DELETE with local execution ON
-- Should succeed without error
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);

SET citus.enable_procedure_transaction_skip TO on;
CALL delete_by_shard_key(2);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 21: Reads then write with local execution ON
-- Should succeed without error
------------------------------------------------------------
INSERT INTO test_dist VALUES (1, 10), (2, 20), (3, 30);

SET citus.enable_procedure_transaction_skip TO on;
CALL reads_then_write(1);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

------------------------------------------------------------
-- TEST 22: Loop with single INSERT inside
-- Static analysis detects the FOR loop and disqualifies the
-- procedure to prevent partial commits if a mid-loop iteration
-- fails. Uses coordinated transaction.
------------------------------------------------------------
SET citus.enable_procedure_transaction_skip TO on;
SET citus.enable_local_execution TO off;
CALL loop_insert(1, 3);
SELECT * FROM test_dist ORDER BY shard_key;
TRUNCATE test_dist;

-- Cleanup
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
DROP PROCEDURE delete_by_shard_key;
DROP PROCEDURE two_deletes;
DROP PROCEDURE delete_by_other_key;
DROP PROCEDURE reads_then_write;
DROP PROCEDURE loop_insert;
DROP SCHEMA sql_proc_no_txn_block;

RESET citus.enable_procedure_transaction_skip;
RESET citus.enable_local_execution;
RESET citus.log_remote_commands;
RESET citus.shard_count;
RESET citus.shard_replication_factor;
