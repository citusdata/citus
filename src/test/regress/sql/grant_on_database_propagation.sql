
create user myuser;

-- test grant/revoke create on database
grant create on database regression to myuser;
set role myuser;

select has_database_privilege('regression', 'CREATE');
\c - - - :worker_1_port
select has_database_privilege('regression', 'CREATE');
\c - - - :master_port

RESET ROLE;

revoke create on database regression from myuser;
set role myuser;

select has_database_privilege('regression', 'CREATE');
\c - - - :worker_1_port
select has_database_privilege('regression', 'CREATE');
\c - - - :master_port

drop user myuser;










