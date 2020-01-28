CREATE SCHEMA long_select;
SET search_path TO 'long_select';

SET citus.shard_count TO 200;
SET citus.max_adaptive_executor_pool_size TO 32;
CREATE TABLE t(a int, b int);
SELECT create_distributed_table('t', 'a');

BEGIN;
UPDATE t SET b = 1;
SELECT pg_sleep(10);
END;

DROP SCHEMA long_select CASCADE;
