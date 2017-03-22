--
-- MULTI_MULTIUSERS
--
-- Test user permissions.
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1420000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1420000;

SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 2;

CREATE TABLE test (id integer);
SELECT create_distributed_table('test', 'id');

CREATE USER full_access;
CREATE USER read_access;
CREATE USER no_access;

GRANT ALL ON TABLE test TO full_access;
GRANT SELECT ON TABLE test TO read_access;

\c - - - :worker_1_port
CREATE USER full_access;
CREATE USER read_access;
CREATE USER no_access;

GRANT ALL ON TABLE test_1420000 TO full_access;
GRANT SELECT ON TABLE test_1420000 TO read_access;

\c - - - :worker_2_port
CREATE USER full_access;
CREATE USER read_access;
CREATE USER no_access;

GRANT ALL ON TABLE test_1420001 TO full_access;
GRANT SELECT ON TABLE test_1420001 TO read_access;

\c - - - :master_port

-- create prepare tests
PREPARE prepare_insert AS INSERT INTO test VALUES ($1);
PREPARE prepare_select AS SELECT count(*) FROM test;

-- check full permission
SET ROLE full_access;

EXECUTE prepare_insert(1);
EXECUTE prepare_select;

INSERT INTO test VALUES (2);
SELECT count(*) FROM test;
SELECT count(*) FROM test WHERE id = 1;

SET citus.task_executor_type TO 'task-tracker';
SELECT count(*) FROM test;
SET citus.task_executor_type TO 'real-time';

-- check read permission
SET ROLE read_access;

EXECUTE prepare_insert(1);
EXECUTE prepare_select;

INSERT INTO test VALUES (2);
SELECT count(*) FROM test;
SELECT count(*) FROM test WHERE id = 1;

SET citus.task_executor_type TO 'task-tracker';
SELECT count(*) FROM test;
SET citus.task_executor_type TO 'real-time';

-- check no permission
SET ROLE no_access;

EXECUTE prepare_insert(1);
EXECUTE prepare_select;

INSERT INTO test VALUES (2);
SELECT count(*) FROM test;
SELECT count(*) FROM test WHERE id = 1;

SET citus.task_executor_type TO 'task-tracker';
SELECT count(*) FROM test;
SET citus.task_executor_type TO 'real-time';

RESET ROLE;

DROP TABLE test;
DROP USER full_access;
DROP USER read_access;
DROP USER no_access;
