
select current_user \gset

create user columnar_user;

\c - columnar_user

create table columnar_permissions(i int) using columnar;
insert into columnar_permissions values(1);
alter table columnar_permissions add column j int;
insert into columnar_permissions values(2,20);
vacuum columnar_permissions;
truncate columnar_permissions;
drop table columnar_permissions;

\c - :current_user

