create user myuser;

select current_user;
select current_database();

grant create on database regression to myuser;

set role myuser;
select current_user;
select current_database();
create schema myschema;

drop schema myschema;

set role postgres;
drop user myuser;
--



