--
-- MULTI_MULTIUSERS
--
-- Test user permissions.
--

SET citus.next_shard_id TO 1420000;

SET citus.shard_replication_factor TO 1;

CREATE TABLE test (id integer, val integer);
SELECT create_distributed_table('test', 'id');

CREATE TABLE test_coloc (id integer, val integer);
SELECT create_distributed_table('test_coloc', 'id', colocate_with := 'test');

SET citus.shard_count TO 1;
CREATE TABLE singleshard (id integer, val integer);
SELECT create_distributed_table('singleshard', 'id');

-- turn off propagation to avoid Enterprise processing the following section
CREATE USER full_access;
CREATE USER usage_access;
CREATE USER read_access;
CREATE USER no_access;
CREATE ROLE some_role;
GRANT some_role TO full_access;
GRANT some_role TO read_access;

SET citus.enable_ddl_propagation TO off;

GRANT ALL ON TABLE test TO full_access;
GRANT SELECT ON TABLE test TO read_access;

CREATE SCHEMA full_access_user_schema;
REVOKE ALL ON SCHEMA full_access_user_schema FROM PUBLIC;
GRANT ALL ON SCHEMA full_access_user_schema TO full_access;
GRANT USAGE ON SCHEMA full_access_user_schema TO usage_access;

SET citus.enable_ddl_propagation TO DEFAULT;

\c - - - :worker_1_port
GRANT ALL ON TABLE test_1420000 TO full_access;
GRANT SELECT ON TABLE test_1420000 TO read_access;

GRANT ALL ON TABLE test_1420002 TO full_access;
GRANT SELECT ON TABLE test_1420002 TO read_access;

\c - - - :worker_2_port
GRANT ALL ON TABLE test_1420001 TO full_access;
GRANT SELECT ON TABLE test_1420001 TO read_access;

GRANT ALL ON TABLE test_1420003 TO full_access;
GRANT SELECT ON TABLE test_1420003 TO read_access;

\c - - - :master_port

SET citus.shard_replication_factor TO 1;

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

SET citus.enable_repartition_joins to ON;
SELECT count(*), min(current_user) FROM test;

-- test re-partition query (needs to transmit intermediate results)
SELECT count(*) FROM test a JOIN test b ON (a.val = b.val) WHERE a.id = 1 AND b.id = 2;

SET citus.enable_repartition_joins TO true;
SELECT count(*) FROM test a JOIN test b ON (a.val = b.val) WHERE a.id = 1 AND b.id = 2;

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

SET citus.enable_repartition_joins to ON;
SELECT count(*), min(current_user) FROM test;

-- test re-partition query (needs to transmit intermediate results)
SELECT count(*) FROM test a JOIN test b ON (a.val = b.val) WHERE a.id = 1 AND b.id = 2;

SET citus.enable_repartition_joins TO true;
SELECT count(*) FROM test a JOIN test b ON (a.val = b.val) WHERE a.id = 1 AND b.id = 2;

-- should not be allowed to take aggressive locks on table
BEGIN;
SELECT lock_relation_if_exists('test', 'ACCESS SHARE');
SELECT lock_relation_if_exists('test', 'EXCLUSIVE');
ABORT;

-- test creating columnar tables and accessing to columnar metadata tables via unprivileged user

-- all below 5 commands should throw no permission errors
-- read columnar metadata table
SELECT * FROM columnar.stripe;
-- alter a columnar setting
SET columnar.chunk_group_row_limit = 1050;

-- create columnar table
CREATE TABLE columnar_table (a int) USING columnar;
-- alter a columnar table that is created by that unprivileged user
ALTER TABLE columnar_table SET (columnar.chunk_group_row_limit = 2000);
-- insert some data and read
INSERT INTO columnar_table VALUES (1), (1);
SELECT * FROM columnar_table;
-- Fail to alter a columnar table that is created by a different user
SET ROLE full_access;
ALTER TABLE columnar_table SET (columnar.chunk_group_row_limit = 2000);
-- Fail to reset a columnar table value created by a different user
ALTER TABLE columnar_table RESET (columnar.chunk_group_row_limit);
SET ROLE read_access;
-- and drop it
DROP TABLE columnar_table;

-- cannot modify columnar metadata table as unprivileged user
INSERT INTO columnar_internal.stripe VALUES(99);
-- Cannot drop columnar metadata table as unprivileged user.
-- Privileged user also cannot drop but with a different error message.
-- (since citus extension has a dependency to it)
DROP TABLE columnar_internal.chunk;

SELECT * FROM columnar.chunk;

-- test whether a read-only user can read from citus_tables view
SELECT distribution_column FROM citus_tables WHERE table_name = 'test'::regclass;


-- check no permission
SET ROLE no_access;

EXECUTE prepare_insert(1);
EXECUTE prepare_select;

INSERT INTO test VALUES (2);
SELECT count(*) FROM test;
SELECT count(*) FROM test WHERE id = 1;

SET citus.enable_repartition_joins to ON;
SELECT count(*), min(current_user) FROM test;

-- test re-partition query
SELECT count(*) FROM test a JOIN test b ON (a.val = b.val) WHERE a.id = 1 AND b.id = 2;

SET citus.enable_repartition_joins TO true;
SELECT count(*) FROM test a JOIN test b ON (a.val = b.val) WHERE a.id = 1 AND b.id = 2;

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

-- should not be allowed to co-located tables
SELECT update_distributed_table_colocation('test', colocate_with => 'test_coloc');

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

SELECT wait_until_metadata_sync(30000);

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
