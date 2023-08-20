-- Public role has connect,temp,temporary privileges on database
-- To test these scenarios, we need to revoke these privileges from public role
-- since public role privileges are inherited by new roles/users
revoke connect,temp,temporary  on database regression from public;

CREATE SCHEMA grant_on_database_propagation;
SET search_path TO grant_on_database_propagation;

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
-----------------------------------------------------------------------

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

-----------------------------------------------------------------------

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

-----------------------------------------------------------------------

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
-----------------------------------------------------------------------

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
-----------------------------------------------------------------------

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
-----------------------------------------------------------------------

-- test CREATE,CONNECT,TEMP,TEMPORARY privileges one by one on database with grant option
create user myuser;
create user myuser_1;

grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser;

set role myuser;

grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser_1;

set role myuser_1;
select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');


RESET ROLE;

grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser with grant option;
set role myuser;

grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser_1 granted by myuser;

set role myuser_1;
select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');

RESET ROLE;

revoke CREATE,CONNECT,TEMP,TEMPORARY on database regression from myuser_1;
revoke grant option for CREATE,CONNECT,TEMP,TEMPORARY on database regression from myuser cascade;

grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser;

set role myuser;

grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser_1;

set role myuser_1;

select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');

reset role;

revoke  CREATE,CONNECT,TEMP,TEMPORARY on database regression from myuser_1;
revoke  CREATE,CONNECT,TEMP,TEMPORARY on database regression from myuser cascade;

drop user myuser_1;
drop user myuser;

-----------------------------------------------------------------------

-- test CREATE,CONNECT,TEMP,TEMPORARY privileges one by one on database multi database
-- and multi user

create user myuser;
create user myuser_1;

create database test_db;
SELECT result FROM run_command_on_workers($$create database test_db$$);
revoke connect,temp,temporary  on database test_db from public;

grant CREATE,CONNECT,TEMP,TEMPORARY on database regression,test_db to myuser,myuser_1;
set role myuser;

select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');

select has_database_privilege('test_db', 'CREATE');
select has_database_privilege('test_db', 'CONNECT');
select has_database_privilege('test_db', 'TEMP');
select has_database_privilege('test_db', 'TEMPORARY');

set role myuser_1;
select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');

select has_database_privilege('test_db', 'CREATE');
select has_database_privilege('test_db', 'CONNECT');
select has_database_privilege('test_db', 'TEMP');
select has_database_privilege('test_db', 'TEMPORARY');

RESET ROLE;


revoke  CREATE,CONNECT,TEMP,TEMPORARY on database regression,test_db from myuser_1;
revoke  CREATE,CONNECT,TEMP,TEMPORARY on database regression,test_db from myuser cascade;

set role myuser;

select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');

select has_database_privilege('test_db', 'CREATE');
select has_database_privilege('test_db', 'CONNECT');
select has_database_privilege('test_db', 'TEMP');
select has_database_privilege('test_db', 'TEMPORARY');

set role myuser_1;
select has_database_privilege('regression', 'CREATE');
select has_database_privilege('regression', 'CONNECT');
select has_database_privilege('regression', 'TEMP');
select has_database_privilege('regression', 'TEMPORARY');

select has_database_privilege('test_db', 'CREATE');
select has_database_privilege('test_db', 'CONNECT');
select has_database_privilege('test_db', 'TEMP');
select has_database_privilege('test_db', 'TEMPORARY');

reset role;

drop user myuser_1;
drop user myuser;

drop database test_db;
SELECT result FROM run_command_on_workers($$drop database test_db$$);
---------------------------------------------------------------------------
-- rollbacks public role database privileges to original state
grant connect,temp,temporary  on database regression to public;


SET client_min_messages TO ERROR;
DROP SCHEMA grant_on_database_propagation CASCADE;

---------------------------------------------------------------------------
