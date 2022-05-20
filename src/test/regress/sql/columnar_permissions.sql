
create table no_access (i int) using columnar;

select current_user \gset

create user columnar_user;

\c - columnar_user

create table columnar_permissions(i int) using columnar;
insert into columnar_permissions values(1);
alter table columnar_permissions add column j int;
alter table columnar_permissions reset (columnar.compression);
alter table columnar_permissions set (columnar.compression = none);
select alter_columnar_table_reset('columnar_permissions', stripe_row_limit => true);
select alter_columnar_table_set('columnar_permissions', stripe_row_limit => 2222);
select * from columnar.options where regclass = 'columnar_permissions'::regclass;
insert into columnar_permissions values(2,20);
vacuum columnar_permissions;
truncate columnar_permissions;
drop table columnar_permissions;

-- should error
alter table no_access reset (columnar.stripe_row_limit);
alter table no_access set (columnar.stripe_row_limit = 12000);
select alter_columnar_table_reset('no_access', chunk_group_row_limit => true);
select alter_columnar_table_set('no_access', chunk_group_row_limit => 1111);

\c - :current_user
