CREATE SCHEMA propagate_set_commands;
SET search_path TO propagate_set_commands;

CREATE TABLE test (id int, value int);
SELECT create_distributed_table('test', 'id');
INSERT INTO test VALUES (1,1), (3,3);

-- test set local propagation
SET citus.propagate_set_commands TO 'local';

-- make sure we send BEGIN before a SELECT
SET citus.task_executor_type TO 'adaptive';
SET citus.select_opens_transaction_block TO on;

BEGIN;
SELECT current_setting('enable_hashagg') FROM test WHERE id = 1;
-- should not be propagated, error should be coming from coordinator
SET LOCAL TRANSACTION ISOLATION LEVEL REPEATABLE READ;
END;

BEGIN;
-- set session commands are not propagated
SET enable_hashagg TO false;
SELECT current_setting('enable_hashagg') FROM test WHERE id = 1;
ABORT;

BEGIN;
-- should not propagate exit_on_error
SET LOCAL exit_on_error TO on;
SELECT current_setting('exit_on_error') FROM test WHERE id = 1;
END;

BEGIN;
-- should be off on worker
SET LOCAL enable_hashagg TO false;
SELECT current_setting('enable_hashagg') FROM test WHERE id = 1;

-- expand to new node, set should still apply
SELECT current_setting('enable_hashagg') FROM test WHERE id = 3;
END;

BEGIN;
-- should be off on worker
SET LOCAL enable_hashagg TO false;
SELECT current_setting('enable_hashagg') FROM test WHERE id = 1;

-- should be back on after set to default
SET LOCAL enable_hashagg TO DEFAULT;
SELECT current_setting('enable_hashagg') FROM test WHERE id = 1;

-- expand to new node, set to default should still apply
SELECT current_setting('enable_hashagg') FROM test WHERE id = 3;
END;

BEGIN;
-- should be off on worker
SET LOCAL enable_hashagg TO false;
SELECT current_setting('enable_hashagg') FROM test WHERE id = 1;

-- does not have the LOCAL keyword, not propagated
SET enable_hashagg TO DEFAULT;
SELECT current_setting('enable_hashagg') FROM test WHERE id = 3;
END;

BEGIN;
-- should be off on worker
SET LOCAL enable_hashagg TO false;
SELECT current_setting('enable_hashagg') FROM test WHERE id = 1;

-- should be back on after reset
RESET enable_hashagg;
SELECT current_setting('enable_hashagg') FROM test WHERE id = 1;

-- expand to new node, reset should still apply
SELECT current_setting('enable_hashagg') FROM test WHERE id = 3;
END;

BEGIN;
-- should be off on worker
SET LOCAL enable_hashagg TO false;
SELECT current_setting('enable_hashagg') FROM test WHERE id = 1;

-- should be back on after reset all
RESET ALL;
SET search_path = 'propagate_set_commands';
SELECT current_setting('enable_hashagg') FROM test WHERE id = 1;

-- funky case, we reset citus.propagate_set_commands, so not set again
SET LOCAL enable_hashagg TO false;
SELECT current_setting('enable_hashagg') FROM test WHERE id = 3;
ABORT;

DROP SCHEMA propagate_set_commands CASCADE;
