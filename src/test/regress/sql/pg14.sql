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
)$$);

-- test propagation of ALTER TABLE .. ALTER COLUMN .. SET COMPRESSION ..
ALTER TABLE col_compression ALTER COLUMN b SET COMPRESSION pglz;
ALTER TABLE col_compression ALTER COLUMN a SET COMPRESSION default;
SELECT result AS column_compression FROM run_command_on_workers($$SELECT ARRAY(
SELECT attname || ' ' || attcompression FROM pg_attribute WHERE attrelid::regclass::text LIKE 'pg14.col\_compression%' AND attnum > 0 ORDER BY 1
)$$);

-- test propagation of ALTER TABLE .. ADD COLUMN .. COMPRESSION ..
ALTER TABLE col_compression ADD COLUMN c TEXT COMPRESSION pglz;
SELECT result AS column_compression FROM run_command_on_workers($$SELECT ARRAY(
SELECT attname || ' ' || attcompression FROM pg_attribute WHERE attrelid::regclass::text LIKE 'pg14.col\_compression%' AND attnum > 0 ORDER BY 1
)$$);

-- test attaching to a partitioned table with column compression
CREATE TABLE col_comp_par (a TEXT COMPRESSION pglz, b TEXT) PARTITION BY RANGE (a);
SELECT create_distributed_table('col_comp_par', 'a');

CREATE TABLE col_comp_par_1 PARTITION OF col_comp_par FOR VALUES FROM ('abc') TO ('def');

SELECT result AS column_compression FROM run_command_on_workers($$SELECT ARRAY(
SELECT attname || ' ' || attcompression FROM pg_attribute WHERE attrelid::regclass::text LIKE 'pg14.col\_comp\_par\_1\_%' AND attnum > 0 ORDER BY 1
)$$);

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
create TABLE test_jsonb_subscript (
       id int,
       test_json jsonb
);

SELECT create_distributed_table('test_jsonb_subscript', 'id');

insert into test_jsonb_subscript values
(1, '{}'), -- empty jsonb
(2, '{"key": "value"}'); -- jsonb with data

-- update empty jsonb
update test_jsonb_subscript set test_json['a'] = '1' where id = 1;
select * from test_jsonb_subscript ORDER BY 1,2;

-- update jsonb with some data
update test_jsonb_subscript set test_json['a'] = '1' where id = 2;
select * from test_jsonb_subscript ORDER BY 1,2;

-- replace jsonb
update test_jsonb_subscript set test_json['a'] = '"test"';
select * from test_jsonb_subscript ORDER BY 1,2;

-- replace by object
update test_jsonb_subscript set test_json['a'] = '{"b": 1}'::jsonb;
select * from test_jsonb_subscript ORDER BY 1,2;

-- replace by array
update test_jsonb_subscript set test_json['a'] = '[1, 2, 3]'::jsonb;
select * from test_jsonb_subscript ORDER BY 1,2;

-- use jsonb subscription in where clause
select * from test_jsonb_subscript where test_json['key'] = '"value"' ORDER BY 1,2;
select * from test_jsonb_subscript where test_json['key_doesnt_exists'] = '"value"' ORDER BY 1,2;
select * from test_jsonb_subscript where test_json['key'] = '"wrong_value"' ORDER BY 1,2;

-- NULL
update test_jsonb_subscript set test_json[NULL] = '1';
update test_jsonb_subscript set test_json['another_key'] = NULL;
select * from test_jsonb_subscript ORDER BY 1,2;

-- NULL as jsonb source
insert into test_jsonb_subscript values (3, NULL);
update test_jsonb_subscript set test_json['a'] = '1' where id = 3;
select * from test_jsonb_subscript ORDER BY 1,2;

update test_jsonb_subscript set test_json = NULL where id = 3;
update test_jsonb_subscript set test_json[0] = '1';
select * from test_jsonb_subscript ORDER BY 1,2;

-- JOIN ALIAS
CREATE TABLE J1_TBL (
  i integer,
  j integer,
  t text
);
CREATE TABLE J2_TBL (
  i integer,
  k integer
);
INSERT INTO J1_TBL VALUES (1, 4, 'one');
INSERT INTO J1_TBL VALUES (2, 3, 'two');
INSERT INTO J1_TBL VALUES (3, 2, 'three');
INSERT INTO J1_TBL VALUES (4, 1, 'four');
INSERT INTO J1_TBL VALUES (5, 0, 'five');
INSERT INTO J1_TBL VALUES (6, 6, 'six');
INSERT INTO J1_TBL VALUES (7, 7, 'seven');
INSERT INTO J1_TBL VALUES (8, 8, 'eight');
INSERT INTO J1_TBL VALUES (0, NULL, 'zero');
INSERT INTO J2_TBL VALUES (1, -1);
INSERT INTO J2_TBL VALUES (2, 2);
INSERT INTO J2_TBL VALUES (3, -3);
INSERT INTO J2_TBL VALUES (2, 4);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (5, -5);
INSERT INTO J2_TBL VALUES (0, NULL);

SELECT create_distributed_table('J1_TBL','i');
SELECT create_distributed_table('J2_TBL','i');

-- test join using aliases
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) WHERE J1_TBL.t = 'one' ORDER BY 1,2,3,4;  -- ok
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one' ORDER BY 1,2,3,4;  -- ok
SELECT * FROM (J1_TBL JOIN J2_TBL USING (i)) AS x WHERE J1_TBL.t = 'one' ORDER BY 1,2,3,4;  -- error
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE x.i = 1 ORDER BY 1,2,3,4;  -- ok
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE x.t = 'one' ORDER BY 1,2,3,4;  -- error
SELECT * FROM (J1_TBL JOIN J2_TBL USING (i) AS x) AS xx WHERE x.i = 1 ORDER BY 1,2,3,4;  -- error (XXX could use better hint)
SELECT * FROM J1_TBL a1 JOIN J2_TBL a2 USING (i) AS a1 ORDER BY 1,2,3,4;  -- error
SELECT x.* FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one' ORDER BY 1;
SELECT ROW(x.*) FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one' ORDER BY 1;
SELECT * FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE x.i > 1 ORDER BY 1,2,3,4;
-- ORDER BY is not supported for json and this returns 1 row, so it is okay.
SELECT row_to_json(x.*) FROM J1_TBL JOIN J2_TBL USING (i) AS x WHERE J1_TBL.t = 'one';

-- we don't support REINDEX TABLE queries on distributed partitioned tables
CREATE TABLE dist_part_table (a int) PARTITION BY RANGE (a);
CREATE TABLE dist_part_table_1 PARTITION OF dist_part_table FOR VALUES FROM (1) TO (5);
CREATE TABLE dist_part_table_2 PARTITION OF dist_part_table FOR VALUES FROM (5) TO (10);
SELECT create_distributed_table('dist_part_table', 'a');
CREATE INDEX dist_part_idx ON dist_part_table(a);

REINDEX TABLE dist_part_table;

-- but we support REINDEXing partitions
REINDEX TABLE dist_part_table_1;

-- test if we error with CTEs with search clauses
CREATE TABLE graph0(f INT, t INT, label TEXT);
SELECT create_distributed_table('graph0', 'f');

INSERT INTO graph0 VALUES (1, 2, 'arc 1 -> 2'),
    (1, 3, 'arc 1 -> 3'), (2, 3, 'arc 2 -> 3'),
    (1, 4, 'arc 1 -> 4'), (4, 5, 'arc 4 -> 5');

WITH RECURSIVE search_graph(f, t, label) AS (
    SELECT * FROM graph0 g WHERE f = 1
    UNION ALL
    SELECT g.*
        FROM graph0 g, search_graph sg
        WHERE g.f = sg.t and  g.f = 1
) SEARCH DEPTH FIRST BY f, t SET seq
SELECT * FROM search_graph ORDER BY seq;

WITH RECURSIVE search_graph(f, t, label) AS (
    SELECT * FROM graph0 g WHERE f = 1
    UNION ALL
    SELECT g.*
        FROM graph0 g, search_graph sg
        WHERE g.f = sg.t and  g.f = 1
) SEARCH DEPTH FIRST BY f, t SET seq
DELETE FROM graph0 WHERE t IN (SELECT t FROM search_graph ORDER BY seq);

CREATE TABLE graph1(f INT, t INT, label TEXT);
SELECT create_reference_table('graph1');

INSERT INTO graph1 VALUES (1, 2, 'arc 1 -> 2'),
    (1, 3, 'arc 1 -> 3'), (2, 3, 'arc 2 -> 3'),
    (1, 4, 'arc 1 -> 4'), (4, 5, 'arc 4 -> 5');

WITH RECURSIVE search_graph(f, t, label) AS (
    SELECT * FROM graph1 g WHERE f = 1
    UNION ALL
    SELECT g.*
        FROM graph1 g, search_graph sg
        WHERE g.f = sg.t and  g.f = 1
) SEARCH DEPTH FIRST BY f, t SET seq
SELECT * FROM search_graph ORDER BY seq;

WITH RECURSIVE search_graph(f, t, label) AS (
    SELECT * FROM graph1 g WHERE f = 1
    UNION ALL
    SELECT g.*
        FROM graph1 g, search_graph sg
        WHERE g.f = sg.t and  g.f = 1
) SEARCH DEPTH FIRST BY f, t SET seq
DELETE FROM graph1 WHERE t IN (SELECT t FROM search_graph ORDER BY seq);


SELECT * FROM (
    WITH RECURSIVE search_graph(f, t, label) AS (
        SELECT *
        FROM graph0 g
        WHERE f = 1
        UNION ALL SELECT g.*
        FROM graph0 g, search_graph sg
        WHERE g.f = sg.t AND g.f = 1
    ) SEARCH DEPTH FIRST BY f, t SET seq
    SELECT * FROM search_graph ORDER BY seq
) as foo;

--
-- https://github.com/citusdata/citus/issues/5258
--
CREATE TABLE nummultirange_test (nmr NUMMULTIRANGE) USING columnar;
INSERT INTO nummultirange_test VALUES('{}');
INSERT INTO nummultirange_test VALUES('{[,)}');
INSERT INTO nummultirange_test VALUES('{[3,]}');
INSERT INTO nummultirange_test VALUES('{[, 5)}');
INSERT INTO nummultirange_test VALUES(nummultirange());
INSERT INTO nummultirange_test VALUES(nummultirange(variadic '{}'::numrange[]));
INSERT INTO nummultirange_test VALUES(nummultirange(numrange(1.1, 2.2)));
INSERT INTO nummultirange_test VALUES('{empty}');
INSERT INTO nummultirange_test VALUES(nummultirange(numrange(1.7, 1.7, '[]'), numrange(1.7, 1.9)));
INSERT INTO nummultirange_test VALUES(nummultirange(numrange(1.7, 1.7, '[]'), numrange(1.9, 2.1)));

create table nummultirange_test2(nmr nummultirange) USING columnar;
INSERT INTO nummultirange_test2 VALUES('{[, 5)}');
INSERT INTO nummultirange_test2 VALUES(nummultirange(numrange(1.1, 2.2)));
INSERT INTO nummultirange_test2 VALUES(nummultirange(numrange(1.1, 2.2)));
INSERT INTO nummultirange_test2 VALUES(nummultirange(numrange(1.1, 2.2,'()')));
INSERT INTO nummultirange_test2 VALUES('{}');
select * from nummultirange_test2 where nmr = '{}';
select * from nummultirange_test2 where nmr = nummultirange(numrange(1.1, 2.2));
select * from nummultirange_test2 where nmr = nummultirange(numrange(1.1, 2.3));

set enable_nestloop=t;
set enable_hashjoin=f;
set enable_mergejoin=f;
select * from nummultirange_test natural join nummultirange_test2 order by nmr;

-- verify that recreating distributed procedures with OUT param gets propagated to workers
CREATE OR REPLACE PROCEDURE proc_with_out_param(IN parm1 date, OUT parm2 int)
  LANGUAGE SQL
AS $$
    SELECT 1;
$$;

-- this should error out
SELECT create_distributed_function('proc_with_out_param(date,int)');
-- this should work
SELECT create_distributed_function('proc_with_out_param(date)');

SET client_min_messages TO ERROR;
CREATE ROLE r1;
SELECT 1 FROM run_command_on_workers($$CREATE ROLE r1;$$);
GRANT EXECUTE ON PROCEDURE proc_with_out_param TO r1;
SELECT 1 FROM run_command_on_workers($$GRANT EXECUTE ON PROCEDURE proc_with_out_param TO r1;$$);
RESET client_min_messages;

CREATE OR REPLACE PROCEDURE proc_with_out_param(IN parm1 date, OUT parm2 int)
  LANGUAGE SQL
AS $$
    SELECT 2;
$$;

SELECT count(*) FROM
  (SELECT result FROM
    run_command_on_workers($$select row(pg_proc.pronargs, pg_proc.proargtypes, pg_proc.prosrc, pg_proc.proowner) from pg_proc where proname = 'proc_with_out_param';$$)
    UNION  select row(pg_proc.pronargs, pg_proc.proargtypes, pg_proc.prosrc, pg_proc.proowner)::text from pg_proc where proname = 'proc_with_out_param')
  as test;

set client_min_messages to error;
drop schema pg14 cascade;
