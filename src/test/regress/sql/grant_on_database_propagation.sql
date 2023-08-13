-- Public role has connect,temp,temporary privileges on database
-- To test these scenarios, we need to revoke these privileges from public role
-- since public role privileges are inherited by new roles/users
revoke connect,temp,temporary  on database regression from public;

-- test grant/revoke CREATE privilege propagation on database
create user myuser;

grant create on database regression to myuser;
set role myuser;

select has_database_privilege('regression', 'CREATE');
\c - - - :worker_1_port;
select has_database_privilege('regression', 'CREATE');
\c - - - :master_port

RESET ROLE;

revoke create on database regression from myuser;
set role myuser;

select has_database_privilege('regression', 'CREATE');
\c - - - :worker_1_port

set role myuser;
select has_database_privilege('regression', 'CREATE');
\c - - - :master_port

drop user myuser;
------------------------------------------------------------------------------------------------------------------------

-- test grant/revoke CONNECT privilege propagation on database
create user myuser;

grant CONNECT on database regression to myuser;
set role myuser;

select has_database_privilege('regression', 'CONNECT');
\c - - - :worker_1_port;
select has_database_privilege('regression', 'CONNECT');
\c - - - :master_port

RESET ROLE;

revoke connect on database regression from myuser;
set role myuser;

select has_database_privilege('regression', 'CONNECT');
\c - - - :worker_1_port

set role myuser;
select has_database_privilege('regression', 'CONNECT');
\c - - - :master_port

drop user myuser;

-- test grant/revoke TEMP privilege propagation on database
create user myuser;

-- test grant/revoke temp on database
grant TEMP on database regression to myuser;
set role myuser;

select has_database_privilege('regression', 'TEMP');
\c - - - :worker_1_port;
select has_database_privilege('regression', 'TEMP');
\c - - - :master_port

RESET ROLE;

revoke TEMP on database regression from myuser;
set role myuser;

select has_database_privilege('regression', 'TEMP');
\c - - - :worker_1_port

set role myuser;
select has_database_privilege('regression', 'TEMP');
\c - - - :master_port

drop user myuser;

-- test temporary privilege on database
create user myuser;

-- test grant/revoke temporary on database
grant TEMPORARY on database regression to myuser;
set role myuser;

select has_database_privilege('regression', 'TEMPORARY');
\c - - - :worker_1_port;
select has_database_privilege('regression', 'TEMPORARY');
\c - - - :master_port

RESET ROLE;

revoke TEMPORARY on database regression from myuser;
set role myuser;

select has_database_privilege('regression', 'TEMPORARY');
\c - - - :worker_1_port

set role myuser;
select has_database_privilege('regression', 'TEMPORARY');
\c - - - :master_port

drop user myuser;


-- test ALL privileges with ALL statement on database
create user myuser;

grant ALL on database regression to myuser;
set role myuser;

select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');
\c - - - :worker_1_port;
select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');
\c - - - :master_port

RESET ROLE;

revoke ALL on database regression from myuser;
set role myuser;

select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');
\c - - - :worker_1_port

set role myuser;
select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');
\c - - - :master_port

drop user myuser;


-- test CREATE,CONNECT,TEMP,TEMPORARY privileges one by one on database
create user myuser;

grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser;
set role myuser;

select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');
\c - - - :worker_1_port;
select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');
\c - - - :master_port

RESET ROLE;

revoke CREATE,CONNECT,TEMP,TEMPORARY on database regression from myuser;
set role myuser;

select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');
\c - - - :worker_1_port

set role myuser;
select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');
\c - - - :master_port

drop user myuser;















