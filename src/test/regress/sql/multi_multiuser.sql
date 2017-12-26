--
-- MULTI_MULTIUSERS
--
-- Test user permissions.
--

SET citus.next_shard_id TO 1420000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1420000;

SET citus.shard_replication_factor TO 1;

CREATE TABLE test (id integer, val integer);
SELECT create_distributed_table('test', 'id');

-- turn off propagation to avoid Enterprise processing the following section
SET citus.enable_ddl_propagation TO off;

CREATE USER full_access;
CREATE USER read_access;
CREATE USER no_access;

GRANT ALL ON TABLE test TO full_access;
GRANT SELECT ON TABLE test TO read_access;

SET citus.enable_ddl_propagation TO DEFAULT;

\c - - - :worker_1_port
CREATE USER full_access;
CREATE USER read_access;
CREATE USER no_access;

GRANT ALL ON TABLE test_1420000 TO full_access;
GRANT SELECT ON TABLE test_1420000 TO read_access;

GRANT ALL ON TABLE test_1420002 TO full_access;
GRANT SELECT ON TABLE test_1420002 TO read_access;

\c - - - :worker_2_port
CREATE USER full_access;
CREATE USER read_access;
CREATE USER no_access;

GRANT ALL ON TABLE test_1420001 TO full_access;
GRANT SELECT ON TABLE test_1420001 TO read_access;

GRANT ALL ON TABLE test_1420003 TO full_access;
GRANT SELECT ON TABLE test_1420003 TO read_access;

\c - - - :master_port

-- create prepare tests
PREPARE prepare_insert AS INSERT INTO test VALUES ($1);
PREPARE prepare_select AS SELECT count(*) FROM test;

-- not allowed to read absolute paths, even as superuser
COPY "/etc/passwd" TO STDOUT WITH (format transmit);

-- check full permission
SET ROLE full_access;

EXECUTE prepare_insert(1);
EXECUTE prepare_select;

INSERT INTO test VALUES (2);
SELECT count(*) FROM test;
SELECT count(*) FROM test WHERE id = 1;

SET citus.task_executor_type TO 'task-tracker';
SELECT count(*) FROM test;

-- test re-partition query (needs to transmit intermediate results)
SELECT count(*) FROM test a JOIN test b ON (a.val = b.val) WHERE a.id = 1 AND b.id = 2;

-- should not be able to transmit directly
COPY "postgresql.conf" TO STDOUT WITH (format transmit);

SET citus.task_executor_type TO 'real-time';

-- should not be able to transmit directly
COPY "postgresql.conf" TO STDOUT WITH (format transmit);

-- check read permission
SET ROLE read_access;

EXECUTE prepare_insert(1);
EXECUTE prepare_select;

INSERT INTO test VALUES (2);
SELECT count(*) FROM test;
SELECT count(*) FROM test WHERE id = 1;

SET citus.task_executor_type TO 'task-tracker';
SELECT count(*) FROM test;

-- test re-partition query (needs to transmit intermediate results)
SELECT count(*) FROM test a JOIN test b ON (a.val = b.val) WHERE a.id = 1 AND b.id = 2;

-- should not be able to transmit directly
COPY "postgresql.conf" TO STDOUT WITH (format transmit);

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

-- test re-partition query
SELECT count(*) FROM test a JOIN test b ON (a.val = b.val) WHERE a.id = 1 AND b.id = 2;

-- should not be able to transmit directly
COPY "postgresql.conf" TO STDOUT WITH (format transmit);

SET citus.task_executor_type TO 'real-time';

-- should be able to use intermediate results as any user
BEGIN;
SELECT create_intermediate_result('topten', 'SELECT s FROM generate_series(1,10) s');
SELECT * FROM read_intermediate_result('topten', 'binary'::citus_copy_format) AS res (s int) ORDER BY s;
END;

-- as long as we don't read from a table
BEGIN;
SELECT create_intermediate_result('topten', 'SELECT count(*) FROM test');
ABORT;

RESET ROLE;

DROP TABLE test;
DROP USER full_access;
DROP USER read_access;
DROP USER no_access;
