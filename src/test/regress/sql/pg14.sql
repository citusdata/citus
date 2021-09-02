SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 13 AS server_version_above_thirteen
\gset
\if :server_version_above_thirteen
\else
\q
\endif

create schema pg14;
set search_path to pg14;

SET citus.next_shard_id TO 980000;
SET citus.shard_count TO 2;

-- test the new vacuum option, process_toast
CREATE TABLE t1 (a int);
SELECT create_distributed_table('t1','a');
SET citus.log_remote_commands TO ON;
VACUUM (FULL) t1;
VACUUM (FULL, PROCESS_TOAST) t1;
VACUUM (FULL, PROCESS_TOAST true) t1;
VACUUM (FULL, PROCESS_TOAST false) t1;
VACUUM (PROCESS_TOAST false) t1;
SET citus.log_remote_commands TO OFF;

create table dist(a int, b int);
select create_distributed_table('dist','a');
create index idx on dist(a);

set citus.log_remote_commands to on;
-- make sure that we send the tablespace option
SET citus.multi_shard_commit_protocol TO '1pc';
SET citus.multi_shard_modify_mode TO 'sequential';
reindex(TABLESPACE test_tablespace) index idx;
reindex(TABLESPACE test_tablespace, verbose) index idx;
reindex(TABLESPACE test_tablespace, verbose false) index idx ;
reindex(verbose, TABLESPACE test_tablespace) index idx ;
-- should error saying table space doesn't exist
reindex(TABLESPACE test_tablespace1) index idx;
reset citus.log_remote_commands;
-- CREATE STATISTICS only allow simple column references
CREATE TABLE tbl1(a timestamp, b int);
SELECT create_distributed_table('tbl1','a');
-- the last one should error out
CREATE STATISTICS s1 (dependencies) ON a, b FROM tbl1;
CREATE STATISTICS s2 (mcv) ON a, b FROM tbl1;
CREATE STATISTICS s3 (ndistinct) ON date_trunc('month', a), date_trunc('day', a) FROM tbl1;
set citus.log_remote_commands to off;

-- error out in case of ALTER TABLE .. DETACH PARTITION .. CONCURRENTLY/FINALIZE
-- only if it's a distributed partitioned table
CREATE TABLE par (a INT UNIQUE) PARTITION BY RANGE(a);
CREATE TABLE par_1 PARTITION OF par FOR VALUES FROM (1) TO (4);
CREATE TABLE par_2 PARTITION OF par FOR VALUES FROM (5) TO (8);
-- works as it's not distributed
ALTER TABLE par DETACH PARTITION par_1 CONCURRENTLY;
-- errors out
SELECT create_distributed_table('par','a');
ALTER TABLE par DETACH PARTITION par_2 CONCURRENTLY;
ALTER TABLE par DETACH PARTITION par_2 FINALIZE;


-- test column compression propagation in distribution
SET citus.shard_replication_factor TO 1;
CREATE TABLE col_compression (a TEXT COMPRESSION pglz, b TEXT);
SELECT create_distributed_table('col_compression', 'a', shard_count:=4);

SELECT attname || ' ' || attcompression AS column_compression FROM pg_attribute WHERE attrelid::regclass::text LIKE 'col\_compression%' AND attnum > 0 ORDER BY 1;
SELECT result AS column_compression FROM run_command_on_workers($$SELECT ARRAY(
SELECT attname || ' ' || attcompression FROM pg_attribute WHERE attrelid::regclass::text LIKE 'pg14.col\_compression%' AND attnum > 0 ORDER BY 1
)$$);

-- test column compression propagation in rebalance
SELECT shardid INTO moving_shard FROM citus_shards WHERE table_name='col_compression'::regclass AND nodeport=:worker_1_port LIMIT 1;
SELECT citus_move_shard_placement((SELECT * FROM moving_shard), :'public_worker_1_host', :worker_1_port, :'public_worker_2_host', :worker_2_port);
SELECT rebalance_table_shards('col_compression', rebalance_strategy := 'by_shard_count');
CALL citus_cleanup_orphaned_shards();
SELECT result AS column_compression FROM run_command_on_workers($$SELECT ARRAY(
SELECT attname || ' ' || attcompression FROM pg_attribute WHERE attrelid::regclass::text LIKE 'pg14.col\_compression%' AND attnum > 0 ORDER BY 1
)$$) ORDER BY length(result);

RESET citus.multi_shard_modify_mode;

-- test procedure OUT parameters with procedure pushdown
CREATE TABLE test_proc_table (a int);

create or replace procedure proc_pushdown(dist_key integer, OUT created int4[], OUT res_out text)
language plpgsql
as $$
DECLARE
    res INT := 0;
begin
    INSERT INTO pg14.test_proc_table VALUES (dist_key);
    SELECT count(*) INTO res FROM pg14.test_proc_table;
    created := created || res;
    PERFORM array_prepend(res, created);
    res_out := res::text;
    commit;
end;$$;

-- show the behaviour before distributing
CALL proc_pushdown(1, NULL, NULL);
CALL proc_pushdown(1, ARRAY[2000,1], 'AAAA');

SELECT create_distributed_table('test_proc_table', 'a');
SELECT create_distributed_function('proc_pushdown(integer)', 'dist_key', 'test_proc_table' );

-- make sure that metadata is synced, it may take few seconds
CREATE OR REPLACE FUNCTION wait_until_metadata_sync(timeout INTEGER DEFAULT 15000)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';
SELECT wait_until_metadata_sync(30000);
SELECT bool_and(hasmetadata) FROM pg_dist_node WHERE nodeport IN (:worker_1_port, :worker_2_port);

-- still, we do not pushdown procedures with OUT parameters
SET client_min_messages TO DEBUG1;
CALL proc_pushdown(1, NULL, NULL);
CALL proc_pushdown(1, ARRAY[2000,1], 'AAAA');
RESET client_min_messages;

-- we don't need metadata syncing anymore
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SELECT stop_metadata_sync_to_node('localhost', :worker_2_port);

-- ALTER STATISTICS .. OWNER TO CURRENT_ROLE
CREATE TABLE st1 (a int, b int);
CREATE STATISTICS role_s1 ON a, b FROM st1;
SELECT create_distributed_table('st1','a');
ALTER STATISTICS role_s1 OWNER TO CURRENT_ROLE;
SET citus.enable_ddl_propagation TO off; -- for enterprise
CREATE ROLE role_1 WITH LOGIN SUPERUSER;
SET citus.enable_ddl_propagation TO on;
SELECT run_command_on_workers($$CREATE ROLE role_1 WITH LOGIN SUPERUSER;$$);
ALTER STATISTICS role_s1 OWNER TO CURRENT_ROLE;
SELECT run_command_on_workers($$SELECT rolname FROM pg_roles WHERE oid IN (SELECT stxowner FROM pg_statistic_ext WHERE stxname LIKE 'role\_s1%');$$);
SET ROLE role_1;
ALTER STATISTICS role_s1 OWNER TO CURRENT_ROLE;
SELECT run_command_on_workers($$SELECT rolname FROM pg_roles WHERE oid IN (SELECT stxowner FROM pg_statistic_ext WHERE stxname LIKE 'role\_s1%');$$);
SET ROLE postgres;
ALTER STATISTICS role_s1 OWNER TO CURRENT_USER;
SELECT run_command_on_workers($$SELECT rolname FROM pg_roles WHERE oid IN (SELECT stxowner FROM pg_statistic_ext WHERE stxname LIKE 'role\_s1%');$$);
SET ROLE to NONE;
ALTER STATISTICS role_s1 OWNER TO CURRENT_ROLE;
SELECT run_command_on_workers($$SELECT rolname FROM pg_roles WHERE oid IN (SELECT stxowner FROM pg_statistic_ext WHERE stxname LIKE 'role\_s1%');$$);
set client_min_messages to error;
drop schema pg14 cascade;
