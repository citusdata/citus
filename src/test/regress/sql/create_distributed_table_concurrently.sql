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

set citus.shard_replication_factor to 2;
create table dist_1(a int);
select create_distributed_table('dist_1', 'a');
set citus.shard_replication_factor to 1;

create table dist_2(a int);
select create_distributed_table_concurrently('dist_2', 'a', colocate_with=>'dist_1');

begin;
select create_distributed_table_concurrently('test','key');
rollback;

select create_distributed_table_concurrently('test','key'), create_distributed_table_concurrently('test','key');

select create_distributed_table_concurrently('nocolo','x');
select create_distributed_table_concurrently('test','key', colocate_with := 'nocolo');
select create_distributed_table_concurrently('test','key', colocate_with := 'noexists');

select citus_set_node_property('localhost', :worker_1_port, 'shouldhaveshards', false);
select citus_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', false);
select create_distributed_table_concurrently('test','key');
select citus_set_node_property('localhost', :worker_1_port, 'shouldhaveshards', true);
select citus_set_node_property('localhost', :worker_2_port, 'shouldhaveshards', true);

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
set local client_min_messages to warning;
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

-------foreign key tests among different table types--------
-- verify we do not allow foreign keys from reference table to distributed table concurrently
create table ref_table1(id int);
create table dist_table1(id int primary key);
select create_reference_table('ref_table1');
alter table ref_table1 add constraint fkey foreign key (id) references dist_table1(id);
select create_distributed_table_concurrently('dist_table1', 'id');

-- verify we do not allow foreign keys from citus local table to distributed table concurrently
create table citus_local_table1(id int);
select citus_add_local_table_to_metadata('citus_local_table1');
create table dist_table2(id int primary key);
alter table citus_local_table1 add constraint fkey foreign key (id) references dist_table2(id);
select create_distributed_table_concurrently('dist_table2', 'id');

-- verify we do not allow foreign keys from regular table to distributed table concurrently
create table local_table1(id int);
create table dist_table3(id int primary key);
alter table local_table1 add constraint fkey foreign key (id) references dist_table3(id);
select create_distributed_table_concurrently('dist_table3', 'id');

-- verify we allow foreign keys from distributed table to reference table concurrently
create table ref_table2(id int primary key);
select create_reference_table('ref_table2');
create table dist_table4(id int references ref_table2(id));
select create_distributed_table_concurrently('dist_table4', 'id');

insert into ref_table2 select s from generate_series(1,100) s;
insert into dist_table4 select s from generate_series(1,100) s;
select count(*) as total from dist_table4;

-- verify we do not allow foreign keys from distributed table to citus local table concurrently
create table citus_local_table2(id int primary key);
select citus_add_local_table_to_metadata('citus_local_table2');
create table dist_table5(id int references citus_local_table2(id));
select create_distributed_table_concurrently('dist_table5', 'id');

-- verify we do not allow foreign keys from distributed table to regular table concurrently
create table local_table2(id int primary key);
create table dist_table6(id int references local_table2(id));
select create_distributed_table_concurrently('dist_table6', 'id');
-------foreign key tests among different table types--------

-- columnar tests --

-- create table with partitions
create table test_columnar (id int) partition by range (id);
create table test_columnar_1 partition of test_columnar for values from (1) to (51);
create table test_columnar_2 partition of test_columnar for values from (51) to (101) using columnar;

-- load some data
insert into test_columnar (id) select s from generate_series(1,100) s;

-- distribute table
select create_distributed_table_concurrently('test_columnar','id');

-- verify queries still work
select count(*) from test_columnar;
select id from test_columnar where id = 1;
select id from test_columnar where id = 51;
select count(*) from test_columnar_1;
select count(*) from test_columnar_2;

-- columnar tests --

set client_min_messages to warning;
drop schema create_distributed_table_concurrently cascade;
