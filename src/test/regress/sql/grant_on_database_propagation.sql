create user myuser;

grant create on database regression to myuser;

set role myuser;
create schema myschema;

drop schema myschema;

set role postgres;
revoke create on database regression from myuser;
drop user myuser;
--



