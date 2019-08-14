SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 11 AS server_version_above_eleven;

create schema test_pg12;
set search_path to test_pg12;

CREATE FUNCTION blackhole_am_handler(internal)
RETURNS table_am_handler
AS 'citus'
LANGUAGE C;
CREATE ACCESS METHOD blackhole_am TYPE TABLE HANDLER blackhole_am_handler;

create table test_am(id int, val int) using blackhole_am;
insert into test_am values (1, 1);
-- Custom table access methods should be rejected
select create_distributed_table('test_am','id');

-- Test generated columns
create table gen1 (
	id int,
	val1 int,
	val2 int GENERATED ALWAYS AS (val1 + 2) STORED
);
create table gen2 (
	id int,
	val1 int,
	val2 int GENERATED ALWAYS AS (val1 + 2) STORED
);

insert into gen1 (id, val1) values (1,4),(3,6),(5,2),(7,2);
insert into gen2 (id, val1) values (1,4),(3,6),(5,2),(7,2);

select * from create_distributed_table('gen1', 'id');
select * from create_distributed_table('gen2', 'val2');

insert into gen1 (id, val1) values (2,4),(4,6),(6,2),(8,2);
insert into gen2 (id, val1) values (2,4),(4,6),(6,2),(8,2);

select * from gen1;
select * from gen2;

-- Test new VACUUM/ANALYZE options
analyze (skip_locked) gen1;
vacuum (skip_locked) gen1;
vacuum (truncate 0) gen1;
vacuum (index_cleanup 1) gen1;

-- COPY FROM
create table cptest (id int, val int);
select create_distributed_table('cptest', 'id');
copy cptest from STDIN with csv where val < 4;
1,6
2,3
3,2
4,9
5,4
\.
select sum(id), sum(val) from cptest;

\set VERBOSITY terse
drop schema test_pg12 cascade;
\set VERBOSITY default
