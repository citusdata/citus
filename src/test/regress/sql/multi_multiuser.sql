--
-- MULTI_MULTIUSERS
--
-- Test user permissions.
--

-- print whether we're using version > 10 to make version-specific tests clear
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 10 AS version_above_ten;

SET citus.next_shard_id TO 1420000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1420000;

SET citus.shard_replication_factor TO 1;

CREATE TABLE test (id integer, val integer);
SELECT create_distributed_table('test', 'id');

CREATE TABLE test_coloc (id integer, val integer);
SELECT create_distributed_table('test_coloc', 'id', colocate_with := 'none');

SET citus.shard_count TO 1;
CREATE TABLE singleshard (id integer, val integer);
SELECT create_distributed_table('singleshard', 'id');

-- turn off propagation to avoid Enterprise processing the following section
SET citus.enable_ddl_propagation TO off;

CREATE USER full_access;
CREATE USER read_access;
CREATE USER no_access;
CREATE ROLE some_role;
GRANT some_role TO full_access;
GRANT some_role TO read_access;

GRANT ALL ON TABLE test TO full_access;
GRANT SELECT ON TABLE test TO read_access;

CREATE SCHEMA full_access_user_schema;
REVOKE ALL ON SCHEMA full_access_user_schema FROM PUBLIC;
GRANT USAGE ON SCHEMA full_access_user_schema TO full_access;

SET citus.enable_ddl_propagation TO DEFAULT;

\c - - - :worker_1_port
CREATE USER full_access;
CREATE USER read_access;
CREATE USER no_access;
CREATE ROLE some_role;
GRANT some_role TO full_access;
GRANT some_role TO read_access;

GRANT ALL ON TABLE test_1420000 TO full_access;
GRANT SELECT ON TABLE test_1420000 TO read_access;

GRANT ALL ON TABLE test_1420002 TO full_access;
GRANT SELECT ON TABLE test_1420002 TO read_access;

CREATE SCHEMA full_access_user_schema;
REVOKE ALL ON SCHEMA full_access_user_schema FROM PUBLIC;
GRANT USAGE ON SCHEMA full_access_user_schema TO full_access;

\c - - - :worker_2_port
CREATE USER full_access;
CREATE USER read_access;
CREATE USER no_access;
CREATE ROLE some_role;
GRANT some_role TO full_access;
GRANT some_role TO read_access;

GRANT ALL ON TABLE test_1420001 TO full_access;
GRANT SELECT ON TABLE test_1420001 TO read_access;

GRANT ALL ON TABLE test_1420003 TO full_access;
GRANT SELECT ON TABLE test_1420003 TO read_access;

CREATE SCHEMA full_access_user_schema;
REVOKE ALL ON SCHEMA full_access_user_schema FROM PUBLIC;
GRANT USAGE ON SCHEMA full_access_user_schema TO full_access;

\c - - - :master_port

-- create prepare tests
PREPARE prepare_insert AS INSERT INTO test VALUES ($1);
PREPARE prepare_select AS SELECT count(*) FROM test;

-- not allowed to read absolute paths, even as superuser
COPY "/etc/passwd" TO STDOUT WITH (format transmit);

-- not allowed to read paths outside pgsql_job_cache, even as superuser
COPY "postgresql.conf" TO STDOUT WITH (format transmit);

-- check full permission
SET ROLE full_access;

EXECUTE prepare_insert(1);
EXECUTE prepare_select;

INSERT INTO test VALUES (2);
SELECT count(*) FROM test;
SELECT count(*) FROM test WHERE id = 1;

SET citus.task_executor_type TO 'task-tracker';
SELECT count(*), min(current_user) FROM test;

-- test re-partition query (needs to transmit intermediate results)
SELECT count(*) FROM test a JOIN test b ON (a.val = b.val) WHERE a.id = 1 AND b.id = 2;

-- should not be able to transmit directly
COPY "postgresql.conf" TO STDOUT WITH (format transmit);

SET citus.task_executor_type TO 'real-time';

-- should not be able to transmit directly
COPY "postgresql.conf" TO STDOUT WITH (format transmit);

-- create a task that other users should not be able to inspect
SELECT task_tracker_assign_task(1, 1, 'SELECT 1');

-- check read permission
SET ROLE read_access;

EXECUTE prepare_insert(1);
EXECUTE prepare_select;

INSERT INTO test VALUES (2);
SELECT count(*) FROM test;
SELECT count(*) FROM test WHERE id = 1;

SET citus.task_executor_type TO 'task-tracker';
SELECT count(*), min(current_user) FROM test;

-- test re-partition query (needs to transmit intermediate results)
SELECT count(*) FROM test a JOIN test b ON (a.val = b.val) WHERE a.id = 1 AND b.id = 2;

-- should not be able to transmit directly
COPY "postgresql.conf" TO STDOUT WITH (format transmit);

-- should not be able to access tasks or jobs belonging to a different user
SELECT task_tracker_task_status(1, 1);
SELECT task_tracker_assign_task(1, 2, 'SELECT 1');
SELECT task_tracker_cleanup_job(1);

-- should not be allowed to take aggressive locks on table
BEGIN;
SELECT lock_relation_if_exists('test', 'ACCESS SHARE');
SELECT lock_relation_if_exists('test', 'EXCLUSIVE');
ABORT;

SET citus.task_executor_type TO 'real-time';

-- check no permission
SET ROLE no_access;

EXECUTE prepare_insert(1);
EXECUTE prepare_select;

INSERT INTO test VALUES (2);
SELECT count(*) FROM test;
SELECT count(*) FROM test WHERE id = 1;

SET citus.task_executor_type TO 'task-tracker';
SELECT count(*), min(current_user) FROM test;

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

SELECT * FROM citus_stat_statements_reset();

-- should not be allowed to upgrade to reference table
SELECT upgrade_to_reference_table('singleshard');

-- should not be allowed to co-located tables
SELECT mark_tables_colocated('test', ARRAY['test_coloc'::regclass]);

-- table owner should be the same on the shards, even when distributing the table as superuser
SET ROLE full_access;
CREATE TABLE my_table (id integer, val integer);
RESET ROLE;
SELECT create_distributed_table('my_table', 'id');
SELECT result FROM run_command_on_workers($$SELECT tableowner FROM pg_tables WHERE tablename LIKE 'my_table_%' LIMIT 1$$);

SELECT task_tracker_cleanup_job(1);

-- table should be distributable by super user when it has data in there
SET ROLE full_access;
CREATE TABLE my_table_with_data (id integer, val integer);
INSERT INTO my_table_with_data VALUES (1,2);
RESET ROLE;
SELECT create_distributed_table('my_table_with_data', 'id');
SELECT count(*) FROM my_table_with_data;

-- table that is owned by a role should be distributable by a user that has that role granted
-- while it should not be if the user has the role not granted
SET ROLE full_access;
CREATE TABLE my_role_table_with_data (id integer, val integer);
ALTER TABLE my_role_table_with_data OWNER TO some_role;
INSERT INTO my_role_table_with_data VALUES (1,2);
RESET ROLE;

-- we first try to distribute it with a user that does not have the role so we can reuse the table
SET ROLE no_access;
SELECT create_distributed_table('my_role_table_with_data', 'id');
RESET ROLE;

-- then we try to distribute it with a user that has the role but different then the one creating
SET ROLE read_access;
SELECT create_distributed_table('my_role_table_with_data', 'id');
RESET ROLE;

-- lastly we want to verify the table owner is set to the role, not the user that distributed
SELECT result FROM run_command_on_workers($cmd$
  SELECT tableowner FROM pg_tables WHERE tablename LIKE 'my_role_table_with_data%' LIMIT 1;
$cmd$);

-- we want to verify a user without CREATE access cannot distribute its table, but can get
-- its table distributed by the super user

-- we want to make sure the schema and user are setup in such a way they can't create a
-- table
SET ROLE full_access;
CREATE TABLE full_access_user_schema.t1 (id int);
RESET ROLE;

-- now we create the table for the user
CREATE TABLE full_access_user_schema.t1 (id int);
ALTER TABLE full_access_user_schema.t1 OWNER TO full_access;

-- make sure we can insert data
SET ROLE full_access;
INSERT INTO full_access_user_schema.t1 VALUES (1),(2),(3);

-- creating the table should fail with a failure on the worker machine since the user is
-- not allowed to create a table
SELECT create_distributed_table('full_access_user_schema.t1', 'id');
RESET ROLE;

-- now we distribute the table as super user
SELECT create_distributed_table('full_access_user_schema.t1', 'id');

-- verify the owner of the shards for the distributed tables
SELECT result FROM run_command_on_workers($cmd$
  SELECT tableowner FROM pg_tables WHERE
    true
    AND schemaname = 'full_access_user_schema'
    AND tablename LIKE 't1_%'
  LIMIT 1;
$cmd$);

DROP SCHEMA full_access_user_schema CASCADE;
DROP TABLE
    my_table,
    my_table_with_data,
    my_role_table_with_data,
    singleshard,
    test,
    test_coloc;
DROP USER full_access;
DROP USER read_access;
DROP USER no_access;
DROP ROLE some_role;
