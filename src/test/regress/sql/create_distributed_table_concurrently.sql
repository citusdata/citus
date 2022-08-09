create schema create_distributed_table_concurrently;
set search_path to create_distributed_table_concurrently;
set citus.shard_replication_factor to 1;

-- make sure we have the coordinator in the metadata
SELECT 1 FROM citus_set_coordinator_host('localhost', :master_port);

create table ref (id int primary key);
select create_reference_table('ref');
insert into ref select s from generate_series(0,9) s;

create table test (key text, id int references ref (id) on delete cascade, t timestamptz default now()) partition by range (t);
create table test_1 partition of test for values from ('2022-01-01') to ('2022-12-31');
create table test_2 partition of test for values from ('2023-01-01') to ('2023-12-31');
insert into test (key,id,t) select s,s%10, '2022-01-01'::date + interval '1 year' * (s%2) from generate_series(1,100) s;

create table nocolo (x int, y int);

-- test error conditions

select create_distributed_table_concurrently('test','key', 'append');
select create_distributed_table_concurrently('test','key', 'range');
select create_distributed_table_concurrently('test','noexists', 'hash');
select create_distributed_table_concurrently(0,'key');
select create_distributed_table_concurrently('ref','id');

set citus.shard_replication_factor to 2;
select create_distributed_table_concurrently('test','key', 'hash');
set citus.shard_replication_factor to 1;

begin;
select create_distributed_table_concurrently('test','key');
rollback;

select create_distributed_table_concurrently('nocolo','x');
select create_distributed_table_concurrently('test','key', colocate_with := 'nocolo');
select create_distributed_table_concurrently('test','key', colocate_with := 'noexists');

-- use colocate_with "default"
select create_distributed_table_concurrently('test','key', shard_count := 11);

select shardcount from pg_dist_partition p join pg_dist_colocation c using (colocationid) where logicalrelid = 'test'::regclass;
select count(*) from pg_dist_shard where logicalrelid = 'test'::regclass;

-- verify queries still work
select count(*) from test;
select key, id from test where key = '1';
select count(*) from test_1;

-- verify that the foreign key to reference table was created
begin;
delete from ref;
select count(*) from test;
rollback;

-- verify that we can undistribute the table
begin;
select undistribute_table('test', cascade_via_foreign_keys := true);
rollback;

-- verify that we can co-locate with create_distributed_table_concurrently
create table test2 (x text primary key, y text);
insert into test2 (x,y) select s,s from generate_series(1,100) s;
select create_distributed_table_concurrently('test2','x', colocate_with := 'test');

-- verify co-located joins work
select count(*) from test join test2 on (key = x);
select id, y from test join test2 on (key = x) where key = '1';

-- verify co-locaed foreign keys work
alter table test add constraint fk foreign key (key) references test2 (x);

set client_min_messages to warning;
drop schema create_distributed_table_concurrently cascade;
