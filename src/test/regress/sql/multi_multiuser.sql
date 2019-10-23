--
-- MULTI_MULTIUSERS
--
-- Test user permissions.
--

-- print whether we're using version > 10 to make version-specific tests clear
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 10 AS version_above_ten;

SET citus.next_shard_id TO 1420000;

SET citus.shard_replication_factor TO 1;

ALTER SYSTEM SET citus.metadata_sync_interval TO 3000;
ALTER SYSTEM SET citus.metadata_sync_retry_interval TO 500;
SELECT pg_reload_conf();
CREATE FUNCTION wait_until_metadata_sync(timeout INTEGER DEFAULT 15000)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';


CREATE TABLE test (id integer, val integer);
SELECT create_distributed_table('test', 'id');

CREATE TABLE test_coloc (id integer, val integer);
SELECT create_distributed_table('test_coloc', 'id', colocate_with := 'test');

SET citus.shard_count TO 1;
CREATE TABLE singleshard (id integer, val integer);
SELECT create_distributed_table('singleshard', 'id');

-- turn off propagation to avoid Enterprise processing the following section
SET citus.enable_ddl_propagation TO off;

CREATE USER full_access;
CREATE USER usage_access;
CREATE USER read_access;
CREATE USER no_access;
CREATE ROLE some_role;
GRANT some_role TO full_access;
GRANT some_role TO read_access;

GRANT ALL ON TABLE test TO full_access;
GRANT SELECT ON TABLE test TO read_access;

CREATE SCHEMA full_access_user_schema;
REVOKE ALL ON SCHEMA full_access_user_schema FROM PUBLIC;
GRANT ALL ON SCHEMA full_access_user_schema TO full_access;
GRANT USAGE ON SCHEMA full_access_user_schema TO usage_access;

SET citus.enable_ddl_propagation TO DEFAULT;

\c - - - :worker_1_port
CREATE USER full_access;
CREATE USER usage_access;
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
GRANT ALL ON SCHEMA full_access_user_schema TO full_access;
GRANT USAGE ON SCHEMA full_access_user_schema TO usage_access;

\c - - - :worker_2_port
CREATE USER full_access;
CREATE USER usage_access;
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
GRANT ALL ON SCHEMA full_access_user_schema TO full_access;
GRANT USAGE ON SCHEMA full_access_user_schema TO usage_access;

\c - - - :master_port

SET citus.replication_model TO 'streaming';
SET citus.shard_replication_factor TO 1;

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

RESET citus.task_executor_type;

-- should not be able to transmit directly
COPY "postgresql.conf" TO STDOUT WITH (format transmit);

-- create a task that other users should not be able to inspect
SELECT task_tracker_assign_task(1, 1, 'SELECT 1');

-- check read permission
SET ROLE read_access;

-- should be allowed to run commands, as the current user
SELECT result FROM run_command_on_workers($$SELECT current_user$$);
SELECT result FROM run_command_on_placements('test', $$SELECT current_user$$);
SELECT result FROM run_command_on_colocated_placements('test', 'test_coloc', $$SELECT current_user$$);

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

RESET citus.task_executor_type;

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

RESET citus.task_executor_type;

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

-- should not be allowed to take any locks
BEGIN;
SELECT lock_relation_if_exists('test', 'ACCESS SHARE');
ABORT;
BEGIN;
SELECT lock_relation_if_exists('test', 'EXCLUSIVE');
ABORT;

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
SET ROLE usage_access;
CREATE TABLE full_access_user_schema.t1 (id int);
RESET ROLE;

-- now we create the table for the user
CREATE TABLE full_access_user_schema.t1 (id int);
ALTER TABLE full_access_user_schema.t1 OWNER TO usage_access;

-- make sure we can insert data
SET ROLE usage_access;
INSERT INTO full_access_user_schema.t1 VALUES (1),(2),(3);

-- creating the table should fail with a failure on the worker machine since the user is
-- not allowed to create a table
SELECT create_distributed_table('full_access_user_schema.t1', 'id');
RESET ROLE;

SET ROLE usage_access;

CREATE TYPE usage_access_type AS ENUM ('a', 'b');
CREATE FUNCTION usage_access_func(x usage_access_type, variadic v int[]) RETURNS int[]
    LANGUAGE plpgsql AS 'begin return v; end;';

SET ROLE no_access;
SELECT create_distributed_function('usage_access_func(usage_access_type,int[])');


SET ROLE usage_access;
SELECT create_distributed_function('usage_access_func(usage_access_type,int[])');

SELECT typowner::regrole FROM pg_type WHERE typname = 'usage_access_type';
SELECT proowner::regrole FROM pg_proc WHERE proname = 'usage_access_func';
SELECT run_command_on_workers($$SELECT typowner::regrole FROM pg_type WHERE typname = 'usage_access_type'$$);
SELECT run_command_on_workers($$SELECT proowner::regrole FROM pg_proc WHERE proname = 'usage_access_func'$$);

SELECT wait_until_metadata_sync();

CREATE TABLE colocation_table(id text);
SELECT create_distributed_table('colocation_table','id');

-- now, make sure that the user can use the function
-- created in the transaction
BEGIN;
CREATE FUNCTION usage_access_func_second(key int, variadic v int[]) RETURNS text
    LANGUAGE plpgsql AS 'begin return current_user; end;';
SELECT create_distributed_function('usage_access_func_second(int,int[])', '$1', colocate_with := 'colocation_table');

SELECT usage_access_func_second(1, 2,3,4,5) FROM full_access_user_schema.t1 LIMIT 1; 

ROLLBACK;

CREATE FUNCTION usage_access_func_third(key int, variadic v int[]) RETURNS text
    LANGUAGE plpgsql AS 'begin return current_user; end;';

-- connect back as super user
\c - - - :master_port

-- show that the current user is a super user
SELECT usesuper FROM pg_user where usename IN (SELECT current_user);

-- superuser creates the distributed function that is owned by a regular user
SELECT create_distributed_function('usage_access_func_third(int,int[])', '$1', colocate_with := 'colocation_table');

SELECT proowner::regrole FROM pg_proc WHERE proname = 'usage_access_func_third';
SELECT run_command_on_workers($$SELECT proowner::regrole FROM pg_proc WHERE proname = 'usage_access_func_third'$$);

-- we don't want other tests to have metadata synced
-- that might change the test outputs, so we're just trying to be careful
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);

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

-- a user with all privileges on a schema should be able to distribute tables
SET ROLE full_access;
CREATE TABLE full_access_user_schema.t2(id int);
SELECT create_distributed_table('full_access_user_schema.t2', 'id');
RESET ROLE;

-- a user with all privileges on a schema should be able to upgrade a distributed table to
-- a reference table
SET ROLE full_access;
BEGIN;
CREATE TABLE full_access_user_schema.r1(id int);
SET LOCAL citus.shard_count TO 1;
SELECT create_distributed_table('full_access_user_schema.r1', 'id');
SELECT upgrade_to_reference_table('full_access_user_schema.r1');
COMMIT;
RESET ROLE;

-- the super user should be able to upgrade a distributed table to a reference table, even
-- if it is owned by another user
SET ROLE full_access;
BEGIN;
CREATE TABLE full_access_user_schema.r2(id int);
SET LOCAL citus.shard_count TO 1;
SELECT create_distributed_table('full_access_user_schema.r2', 'id');
COMMIT;
RESET ROLE;

-- the usage_access should not be able to upgrade the table
SET ROLE usage_access;
SELECT upgrade_to_reference_table('full_access_user_schema.r2');
RESET ROLE;

-- the super user should be able
SELECT upgrade_to_reference_table('full_access_user_schema.r2');

-- verify the owner of the shards for the reference table
SELECT result FROM run_command_on_workers($cmd$
  SELECT tableowner FROM pg_tables WHERE
    true
    AND schemaname = 'full_access_user_schema'
    AND tablename LIKE 'r2_%'
  LIMIT 1;
$cmd$);

-- super user should be the only one being able to call worker_cleanup_job_schema_cache
SELECT worker_cleanup_job_schema_cache();
SET ROLE full_access;
SELECT worker_cleanup_job_schema_cache();
SET ROLE usage_access;
SELECT worker_cleanup_job_schema_cache();
SET ROLE read_access;
SELECT worker_cleanup_job_schema_cache();
SET ROLE no_access;
SELECT worker_cleanup_job_schema_cache();
RESET ROLE;

-- to test access to files created during repartition we will create some on worker 1
\c - - - :worker_1_port
SET ROLE full_access;
SELECT worker_hash_partition_table(42,1,'SELECT a FROM generate_series(1,100) AS a', 'a', 23, ARRAY[-2147483648, -1073741824, 0, 1073741824]::int4[]);
RESET ROLE;

-- all attempts for transfer are initiated from other workers

\c - - - :worker_2_port
-- super user should not be able to copy files created by a user
SELECT worker_fetch_partition_file(42, 1, 1, 1, 'localhost', :worker_1_port);

-- different user should not be able to fetch partition file
SET ROLE usage_access;
SELECT worker_fetch_partition_file(42, 1, 1, 1, 'localhost', :worker_1_port);

-- only the user whom created the files should be able to fetch
SET ROLE full_access;
SELECT worker_fetch_partition_file(42, 1, 1, 1, 'localhost', :worker_1_port);
RESET ROLE;

-- now we will test that only the user who owns the fetched file is able to merge it into
-- a table
-- test that no other user can merge the downloaded file before the task is being tracked
SET ROLE usage_access;
SELECT worker_merge_files_into_table(42, 1, ARRAY['a'], ARRAY['integer']);
RESET ROLE;

SET ROLE full_access;
-- use the side effect of this function to have a schema to use, otherwise only the super
-- user could call worker_merge_files_into_table and store the results in public, which is
-- not what we want
SELECT task_tracker_assign_task(42, 1, 'SELECT 1');
RESET ROLE;

-- test that no other user can merge the downloaded file after the task is being tracked
SET ROLE usage_access;
SELECT worker_merge_files_into_table(42, 1, ARRAY['a'], ARRAY['integer']);
RESET ROLE;

-- test that the super user is unable to read the contents of the intermediate file,
-- although it does create the table
SELECT worker_merge_files_into_table(42, 1, ARRAY['a'], ARRAY['integer']);
SELECT count(*) FROM pg_merge_job_0042.task_000001;
DROP TABLE pg_merge_job_0042.task_000001; -- drop table so we can reuse the same files for more tests

SET ROLE full_access;
SELECT worker_merge_files_into_table(42, 1, ARRAY['a'], ARRAY['integer']);
SELECT count(*) FROM pg_merge_job_0042.task_000001;
DROP TABLE pg_merge_job_0042.task_000001; -- drop table so we can reuse the same files for more tests
RESET ROLE;

-- test that no other user can merge files and run query on the already fetched files
SET ROLE usage_access;
SELECT worker_merge_files_and_run_query(42, 1,
    'CREATE TABLE task_000001_merge(merge_column_0 int)',
    'CREATE TABLE task_000001 (a) AS SELECT sum(merge_column_0) FROM task_000001_merge'
);
RESET ROLE;

-- test that the super user is unable to read the contents of the partitioned files after
-- trying to merge with run query
SELECT worker_merge_files_and_run_query(42, 1,
    'CREATE TABLE task_000001_merge(merge_column_0 int)',
    'CREATE TABLE task_000001 (a) AS SELECT sum(merge_column_0) FROM task_000001_merge'
);
SELECT count(*) FROM pg_merge_job_0042.task_000001_merge;
SELECT count(*) FROM pg_merge_job_0042.task_000001;
DROP TABLE pg_merge_job_0042.task_000001, pg_merge_job_0042.task_000001_merge; -- drop table so we can reuse the same files for more tests

-- test that the owner of the task can merge files and run query correctly
SET ROLE full_access;
SELECT worker_merge_files_and_run_query(42, 1,
    'CREATE TABLE task_000001_merge(merge_column_0 int)',
    'CREATE TABLE task_000001 (a) AS SELECT sum(merge_column_0) FROM task_000001_merge'
);

-- test that owner of task cannot execute arbitrary sql
SELECT worker_merge_files_and_run_query(42, 1,
    'CREATE TABLE task_000002_merge(merge_column_0 int)',
    'DROP USER usage_access'
);

SELECT worker_merge_files_and_run_query(42, 1,
    'DROP USER usage_access',
    'CREATE TABLE task_000002 (a) AS SELECT sum(merge_column_0) FROM task_000002_merge'
);

SELECT count(*) FROM pg_merge_job_0042.task_000001_merge;
SELECT count(*) FROM pg_merge_job_0042.task_000001;
DROP TABLE pg_merge_job_0042.task_000001, pg_merge_job_0042.task_000001_merge; -- drop table so we can reuse the same files for more tests
RESET ROLE;

\c - - - :master_port


DROP SCHEMA full_access_user_schema CASCADE;
DROP TABLE
    my_table,
    my_table_with_data,
    my_role_table_with_data,
    singleshard,
    test,
    test_coloc,
    colocation_table;
DROP USER full_access;
DROP USER read_access;
DROP USER no_access;
DROP ROLE some_role;
