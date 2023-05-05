
create table no_access (i int) using columnar;

insert into no_access values(1);
insert into no_access values(2);
insert into no_access values(3);

select current_user \gset

create user columnar_user;

GRANT CREATE ON SCHEMA public TO columnar_user;

\c - columnar_user

-- owned by columnar_user
create table columnar_permissions(i int) using columnar;
insert into columnar_permissions values(1);
insert into columnar_permissions values(2);
alter table columnar_permissions add column j int;
alter table columnar_permissions reset (columnar.compression);
alter table columnar_permissions set (columnar.compression = none);
select alter_columnar_table_reset('columnar_permissions', stripe_row_limit => true);
select alter_columnar_table_set('columnar_permissions', stripe_row_limit => 2222);

select 1 from columnar.get_storage_id('columnar_permissions'::regclass);

-- error
select 1 from columnar.get_storage_id('no_access'::regclass);

-- only tuples related to columnar_permissions should be visible
select relation, chunk_group_row_limit, stripe_row_limit, compression, compression_level
  from columnar.options
  where relation in ('no_access'::regclass, 'columnar_permissions'::regclass)
  order by relation;
select relation, stripe_num, row_count, first_row_number
  from columnar.stripe
  where relation in ('no_access'::regclass, 'columnar_permissions'::regclass)
  order by relation, stripe_num;
select relation, stripe_num, attr_num, chunk_group_num, value_count
  from columnar.chunk
  where relation in ('no_access'::regclass, 'columnar_permissions'::regclass)
  order by relation, stripe_num;
select relation, stripe_num, chunk_group_num, row_count
  from columnar.chunk_group
  where relation in ('no_access'::regclass, 'columnar_permissions'::regclass)
  order by relation, stripe_num;

truncate columnar_permissions;

insert into columnar_permissions values(2,20);
insert into columnar_permissions values(2,30);
insert into columnar_permissions values(4,40);
insert into columnar_permissions values(5,50);

vacuum columnar_permissions;

-- error: columnar_user can't alter no_access
alter table no_access reset (columnar.stripe_row_limit);
alter table no_access set (columnar.stripe_row_limit = 12000);
select alter_columnar_table_reset('no_access', chunk_group_row_limit => true);
select alter_columnar_table_set('no_access', chunk_group_row_limit => 1111);

\c - :current_user

-- should see tuples from both columnar_permissions and no_access
select relation, chunk_group_row_limit, stripe_row_limit, compression, compression_level
  from columnar.options
  where relation in ('no_access'::regclass, 'columnar_permissions'::regclass)
  order by relation;
select relation, stripe_num, row_count, first_row_number
  from columnar.stripe
  where relation in ('no_access'::regclass, 'columnar_permissions'::regclass)
  order by relation, stripe_num;
select relation, stripe_num, attr_num, chunk_group_num, value_count
  from columnar.chunk
  where relation in ('no_access'::regclass, 'columnar_permissions'::regclass)
  order by relation, stripe_num, attr_num;
select relation, stripe_num, chunk_group_num, row_count
  from columnar.chunk_group
  where relation in ('no_access'::regclass, 'columnar_permissions'::regclass)
  order by relation, stripe_num;

drop table columnar_permissions;
drop table no_access;
