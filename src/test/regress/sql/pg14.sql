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

set client_min_messages to error;
drop schema pg14 cascade;
