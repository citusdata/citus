
create user myuser;

-- test grant/revoke create on database
grant create on database regression to myuser;
set role myuser;

select has_database_privilege('gurkanindibay', 'CREATE');
\c - - - :worker_1_port
select has_database_privilege('gurkanindibay', 'CREATE');
\c - - - :master_port

RESET ROLE;

revoke create on database regression from myuser;

select has_database_privilege('gurkanindibay', 'CREATE');
\c - - - :worker_1_port
select has_database_privilege('gurkanindibay', 'CREATE');
\c - - - :master_port

drop user myuser;

-- test grant/revoke connect on database

create user myuser;

-- test grant/revoke CONNECT on database
grant connect on database regression to myuser;
set role myuser;

select has_database_privilege('gurkanindibay', 'CONNECT');
\c - - - :worker_1_port
select has_database_privilege('gurkanindibay', 'CONNECT');
\c - - - :master_port

RESET ROLE;

revoke CONNECT on database regression from myuser;

select has_database_privilege('gurkanindibay', 'CONNECT');
\c - - - :worker_1_port
select has_database_privilege('gurkanindibay', 'CONNECT');
\c - - - :master_port

drop user myuser;









