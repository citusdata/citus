SET citus.next_shard_id TO 1610000;

-- enforce 1 connection per placement since
-- the tests are prepared for that
SET citus.force_max_query_parallelization TO ON;

CREATE SCHEMA multi_real_time_transaction;
SET search_path = 'multi_real_time_transaction';
SET citus.shard_replication_factor to 1;

CREATE TABLE test_table(id int, col_1 int, col_2 text);
SELECT create_distributed_table('test_table','id');
\COPY test_table FROM stdin delimiter ',';
1,2,'aa'
2,3,'bb'
3,4,'cc'
4,5,'dd'
5,6,'ee'
6,7,'ff'
\.

CREATE TABLE co_test_table(id int, col_1 int, col_2 text);
SELECT create_distributed_table('co_test_table','id');
\COPY co_test_table FROM stdin delimiter ',';
1,20,'aa10'
2,30,'bb10'
3,40,'cc10'
3,4,'cc1'
3,5,'cc2'
1,2,'cc2'
\.


CREATE TABLE ref_test_table(id int, col_1 int, col_2 text);
SELECT create_reference_table('ref_test_table');
\COPY ref_test_table FROM stdin delimiter ',';
1,2,'rr1'
2,3,'rr2'
3,4,'rr3'
4,5,'rr4'
\.

-- Test two reference table joins, will both run in parallel
BEGIN;
SELECT COUNT(*) FROM test_table JOIN ref_test_table USING (id);
SELECT COUNT(*) FROM test_table JOIN ref_test_table USING (id);
ROLLBACK;

-- Test two reference table joins, second one will be serialized
BEGIN;
SELECT COUNT(*) FROM test_table JOIN ref_test_table USING (id);
INSERT INTO ref_test_table VALUES(1,2,'da');
SELECT COUNT(*) FROM test_table JOIN ref_test_table USING (id);
ROLLBACK;

-- this does not work because the inserts into shards go over different connections
-- and the insert into the reference table goes over a single connection, and the
-- final SELECT cannot see both
BEGIN;
SELECT COUNT(*) FROM test_table JOIN ref_test_table USING (id);
INSERT INTO test_table VALUES(1,2,'da');
INSERT INTO test_table VALUES(2,2,'da');
INSERT INTO test_table VALUES(3,3,'da');
INSERT INTO ref_test_table VALUES(1,2,'da');
SELECT COUNT(*) FROM test_table JOIN ref_test_table USING (id);
ROLLBACK;

-- Test with select and router insert
BEGIN;
SELECT COUNT(*) FROM test_table;
INSERT INTO test_table VALUES(7,8,'gg');
SELECT COUNT(*) FROM test_table;
ROLLBACK;

-- Test with select and multi-row insert
BEGIN;
SELECT COUNT(*) FROM test_table;
INSERT INTO test_table VALUES (7,8,'gg'),(8,9,'hh'),(9,10,'ii');
SELECT COUNT(*) FROM test_table;
ROLLBACK;

-- Test with INSERT .. SELECT
BEGIN;
SELECT COUNT(*) FROM test_table;
INSERT INTO test_table SELECT * FROM co_test_table;
SELECT COUNT(*) FROM test_table;
ROLLBACK;

-- Test with COPY
BEGIN;
SELECT COUNT(*) FROM test_table;
\COPY test_table FROM stdin delimiter ',';
8,9,'gg'
9,10,'hh'
10,11,'ii'
\.
SELECT COUNT(*) FROM test_table;
ROLLBACK;

-- Test with router update
BEGIN;
SELECT SUM(col_1) FROM test_table;
UPDATE test_table SET col_1 = 0 WHERE id = 2;
DELETE FROM test_table WHERE id = 3;
SELECT SUM(col_1) FROM test_table;
ROLLBACK;

-- Test with multi-shard update
BEGIN;
SELECT SUM(col_1) FROM test_table;
UPDATE test_table SET col_1 = 5;
SELECT SUM(col_1) FROM test_table;
ROLLBACK;

-- Test with subqueries
BEGIN;
SELECT SUM(col_1) FROM test_table;
UPDATE
	test_table
SET
	col_1 = 4
WHERE
	test_table.col_1 IN (SELECT co_test_table.col_1 FROM co_test_table WHERE co_test_table.id = 1)
	AND test_table.id = 1;
SELECT SUM(col_1) FROM test_table;
ROLLBACK;

-- Test with partitioned table
CREATE TABLE partitioning_test(id int, time date) PARTITION BY RANGE (time);
SET citus.shard_replication_factor TO 1;

-- create its partitions
CREATE TABLE partitioning_test_2009 PARTITION OF partitioning_test FOR VALUES FROM ('2009-01-01') TO ('2010-01-01');
CREATE TABLE partitioning_test_2010 PARTITION OF partitioning_test FOR VALUES FROM ('2010-01-01') TO ('2011-01-01');

-- load some data and distribute tables
INSERT INTO partitioning_test VALUES (1, '2009-06-06');
INSERT INTO partitioning_test VALUES (2, '2010-07-07');
SELECT create_distributed_table('partitioning_test', 'id');

BEGIN;
SELECT COUNT(*) FROM partitioning_test;
INSERT INTO partitioning_test_2009 VALUES (3, '2009-09-09');
INSERT INTO partitioning_test_2010 VALUES (4, '2010-03-03');
SELECT COUNT(*) FROM partitioning_test;
COMMIT;

DROP TABLE partitioning_test;

-- Test with create-drop table
BEGIN;
CREATE TABLE test_table_inn(id int, num_1 int);
SELECT create_distributed_table('test_table_inn','id');
INSERT INTO test_table_inn VALUES(1,3),(4,5),(6,7);
SELECT COUNT(*) FROM test_table_inn;
DROP TABLE test_table_inn;
COMMIT;

-- Test with utility functions
BEGIN;
SELECT COUNT(*) FROM test_table;
CREATE INDEX tt_ind_1 ON test_table(col_1);
ALTER TABLE test_table ADD CONSTRAINT num_check CHECK (col_1 < 50);
SELECT COUNT(*) FROM test_table;
ROLLBACK;

-- We don't get a distributed transaction id outside a transaction block
SELECT (get_current_transaction_id()).transaction_number > 0 FROM test_table LIMIT 1;

-- We should get a distributed transaction id inside a transaction block
BEGIN;
SELECT (get_current_transaction_id()).transaction_number > 0 FROM test_table LIMIT 1;
END;

-- Add a function to insert a row into a table
SELECT public.run_command_on_master_and_workers($$
CREATE FUNCTION multi_real_time_transaction.insert_row_test(table_name name)
RETURNS bool
AS $BODY$
BEGIN
  EXECUTE format('INSERT INTO %s VALUES(100,100,''function'')', table_name);
  RETURN true;
END;
$BODY$ LANGUAGE plpgsql;
$$);

-- SELECT should be rolled back because we send BEGIN
BEGIN;
SELECT count(*) FROM test_table;

-- Sneakily insert directly into shards
SELECT insert_row_test(pg_typeof(test_table)::name) FROM test_table;
SELECT count(*) FROM test_table;
ABORT;

SELECT count(*) FROM test_table;


-- Test with foreign key
ALTER TABLE test_table ADD CONSTRAINT p_key_tt PRIMARY KEY (id);
ALTER TABLE co_test_table ADD CONSTRAINT f_key_ctt FOREIGN KEY (id) REFERENCES test_table(id) ON DELETE CASCADE;

BEGIN;
DELETE FROM test_table where id = 1 or id = 3;
SELECT * FROM co_test_table;
ROLLBACK;

-- Test cancelling behaviour. See https://github.com/citusdata/citus/pull/1905.
-- Repeating it multiple times to increase the chance of failure before PR #1905.
SET client_min_messages TO ERROR;
alter system set deadlock_timeout TO '250ms';
SELECT pg_reload_conf();

BEGIN;
SELECT id, pg_advisory_lock(15) FROM test_table;
ROLLBACK;
BEGIN;
SELECT id, pg_advisory_lock(15) FROM test_table;
ROLLBACK;
BEGIN;
SELECT id, pg_advisory_lock(15) FROM test_table;
ROLLBACK;
BEGIN;
SELECT id, pg_advisory_lock(15) FROM test_table;
ROLLBACK;
BEGIN;
SELECT id, pg_advisory_lock(15) FROM test_table;
ROLLBACK;
BEGIN;
SELECT id, pg_advisory_lock(15) FROM test_table;
ROLLBACK;
BEGIN;
SELECT id, pg_advisory_lock(15) FROM test_table;
ROLLBACK;
BEGIN;
SELECT id, pg_advisory_lock(15) FROM test_table;
ROLLBACK;
BEGIN;
SELECT id, pg_advisory_lock(15) FROM test_table;
ROLLBACK;
BEGIN;
SELECT id, pg_advisory_lock(15) FROM test_table;
ROLLBACK;

-- test propagation of SET LOCAL
-- gonna need a non-superuser as we'll use RLS to test GUC propagation
CREATE USER rls_user;
SELECT run_command_on_workers('CREATE USER rls_user');

GRANT ALL ON SCHEMA multi_real_time_transaction TO rls_user;
GRANT ALL ON ALL TABLES IN SCHEMA multi_real_time_transaction TO rls_user;

SELECT run_command_on_workers('GRANT ALL ON SCHEMA multi_real_time_transaction TO rls_user');
SELECT run_command_on_workers('GRANT ALL ON ALL TABLES IN SCHEMA multi_real_time_transaction TO rls_user');

-- create trigger on one worker to reject access if GUC not
\c - - :public_worker_1_host :worker_1_port
SET search_path = 'multi_real_time_transaction';

ALTER TABLE test_table_1610000 ENABLE ROW LEVEL SECURITY;

CREATE POLICY hide_by_default ON test_table_1610000 TO PUBLIC
    USING (COALESCE(current_setting('app.show_rows', TRUE)::bool, FALSE));

\c - - :master_host :master_port
SET ROLE rls_user;
SET search_path = 'multi_real_time_transaction';

-- shouldn't see all rows because of RLS
SELECT COUNT(*) FROM test_table;

BEGIN;
-- without enabling SET LOCAL prop, still won't work
SET LOCAL app.show_rows TO TRUE;
SELECT COUNT(*) FROM test_table;

SET LOCAL citus.propagate_set_commands TO 'local';

-- now we should be good to go
SET LOCAL app.show_rows TO TRUE;
SELECT COUNT(*) FROM test_table;

SAVEPOINT disable_rls;
SET LOCAL app.show_rows TO FALSE;
SELECT COUNT(*) FROM test_table;

ROLLBACK TO SAVEPOINT disable_rls;
SELECT COUNT(*) FROM test_table;

SAVEPOINT disable_rls_for_real;
SET LOCAL app.show_rows TO FALSE;
RELEASE SAVEPOINT disable_rls_for_real;

SELECT COUNT(*) FROM test_table;
COMMIT;

RESET ROLE;

-- Test GUC propagation of SET LOCAL in combination with a RLS policy
-- that uses a GUC to filter tenants. Tenant data is spread across nodes.
-- First, as a non-superuser, we'll see all rows because RLS is not in place yet.
SET ROLE rls_user;
SET search_path = 'multi_real_time_transaction';
SELECT * FROM co_test_table ORDER BY id, col_1;

\c - - :public_worker_1_host :worker_1_port
SET search_path = 'multi_real_time_transaction';

-- shard 1610004 contains data from tenant id 1
SELECT * FROM co_test_table_1610004 ORDER BY id, col_1;
SELECT * FROM co_test_table_1610006 ORDER BY id, col_1;

\c - - :public_worker_2_host :worker_2_port
SET search_path = 'multi_real_time_transaction';

-- shard 1610005 contains data from tenant id 3
SELECT * FROM co_test_table_1610005 ORDER BY id, col_1;
-- shard 1610007 contains data from tenant id 2
SELECT * FROM co_test_table_1610007 ORDER BY id, col_1;

\c - - :master_host :master_port
SET search_path = 'multi_real_time_transaction';

-- Let's set up a policy on the coordinator and workers which filters the tenants.
SET citus.enable_ddl_propagation to off;
CREATE POLICY filter_by_tenant_id ON co_test_table TO PUBLIC
    USING (id = ANY(string_to_array(current_setting('app.tenant_id'), ',')::int[]));
SET citus.enable_ddl_propagation to on;
SELECT run_command_on_shards('co_test_table', $cmd$CREATE POLICY filter_by_tenant_id ON %s TO PUBLIC
        USING (id = ANY(string_to_array(current_setting('app.tenant_id'), ',')::int[]));$cmd$);

-- Let's activate RLS on the coordinator and workers.
SET citus.enable_ddl_propagation to off;
ALTER TABLE co_test_table ENABLE ROW LEVEL SECURITY;
SET citus.enable_ddl_propagation to on;
SELECT run_command_on_shards('co_test_table','ALTER TABLE %s ENABLE ROW LEVEL SECURITY;');

-- Switch to non-superuser to make sure RLS takes effect.
SET ROLE rls_user;

BEGIN;
-- Make sure, from now on, GUCs will be propagated to workers.
SET LOCAL citus.propagate_set_commands TO 'local';

-- Only tenant id 1 will be fetched, and so on.
SET LOCAL app.tenant_id TO 1;
SELECT * FROM co_test_table ORDER BY id, col_1;

SAVEPOINT disable_rls;
SET LOCAL app.tenant_id TO 3;
SELECT * FROM co_test_table ORDER BY id, col_1;

ROLLBACK TO SAVEPOINT disable_rls;
SELECT * FROM co_test_table ORDER BY id, col_1;

SAVEPOINT disable_rls_for_real;
SET LOCAL app.tenant_id TO 3;
RELEASE SAVEPOINT disable_rls_for_real;

SELECT * FROM co_test_table ORDER BY id, col_1;

RELEASE SAVEPOINT disable_rls;

-- Make sure it's possible to fetch multiple tenants located on separate nodes
-- via RLS policies that use GUCs.
SET LOCAL app.tenant_id TO '1,3';
SELECT * FROM co_test_table ORDER BY id, col_1;

COMMIT;

RESET ROLE;

-- Cleanup RLS
SET citus.enable_ddl_propagation to off;
ALTER TABLE co_test_table DISABLE ROW LEVEL SECURITY;
SET citus.enable_ddl_propagation to on;
SELECT run_command_on_shards('co_test_table','ALTER TABLE %s DISABLE ROW LEVEL SECURITY;');
SET citus.enable_ddl_propagation to off;
DROP POLICY filter_by_tenant_id ON co_test_table;
SET citus.enable_ddl_propagation to on;
SELECT run_command_on_shards('co_test_table', 'DROP POLICY filter_by_tenant_id ON %s;');

-- sequential real-time queries should be successfully executed
-- since the queries are sent over the same connection
BEGIN;
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
SELECT id, pg_advisory_lock(15) FROM test_table ORDER BY 1 DESC;
ROLLBACK;


SET client_min_messages TO DEFAULT;
alter system set deadlock_timeout TO DEFAULT;
SELECT pg_reload_conf();

BEGIN;
SET citus.select_opens_transaction_block TO off;
-- This query would self-deadlock if it ran in a distributed transaction
-- we use a different advisory lock because previous tests
-- still holds the advisory locks since the sessions are still active
SELECT id, pg_advisory_xact_lock(16) FROM test_table ORDER BY id;
END;

DROP SCHEMA multi_real_time_transaction CASCADE;
