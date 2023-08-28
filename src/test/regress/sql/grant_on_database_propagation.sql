-- Public role has connect,temp,temporary privileges on database
-- To test these scenarios, we need to revoke these privileges from public role
-- since public role privileges are inherited by new roles/users
revoke connect,temp,temporary  on database regression from public;

CREATE SCHEMA grant_on_database_propagation;
SET search_path TO grant_on_database_propagation;

-- test grant/revoke CREATE privilege propagation on database
create user myuser;

grant create on database regression to myuser;

select has_database_privilege('myuser','regression', 'CREATE');
\c - - - :worker_1_port;
select has_database_privilege('myuser','regression', 'CREATE');
\c - - - :master_port

revoke create on database regression from myuser;


select has_database_privilege('myuser','regression', 'CREATE');
\c - - - :worker_1_port

select has_database_privilege('myuser','regression', 'CREATE');
\c - - - :master_port

drop user myuser;
-----------------------------------------------------------------------

-- test grant/revoke CONNECT privilege propagation on database
create user myuser;

grant CONNECT on database regression to myuser;

select has_database_privilege('myuser','regression', 'CONNECT');
\c - - - :worker_1_port;
select has_database_privilege('myuser','regression', 'CONNECT');
\c - - - :master_port

revoke connect on database regression from myuser;

select has_database_privilege('myuser','regression', 'CONNECT');
\c - - - :worker_1_port

select has_database_privilege('myuser','regression', 'CONNECT');
\c - - - :master_port

drop user myuser;

-----------------------------------------------------------------------

-- test grant/revoke TEMP privilege propagation on database
create user myuser;

-- test grant/revoke temp on database
grant TEMP on database regression to myuser;

select has_database_privilege('myuser','regression', 'TEMP');
\c - - - :worker_1_port;
select has_database_privilege('myuser','regression', 'TEMP');
\c - - - :master_port

revoke TEMP on database regression from myuser;

select has_database_privilege('myuser','regression', 'TEMP');
\c - - - :worker_1_port

select has_database_privilege('myuser','regression', 'TEMP');
\c - - - :master_port

drop user myuser;

-----------------------------------------------------------------------

-- test temporary privilege on database
create user myuser;

-- test grant/revoke temporary on database
grant TEMPORARY on database regression to myuser;

select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :worker_1_port;
select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :master_port

revoke TEMPORARY on database regression from myuser;

select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :worker_1_port

select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :master_port

drop user myuser;
-----------------------------------------------------------------------

-- test ALL privileges with ALL statement on database
create user myuser;

grant ALL on database regression to myuser;

select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :worker_1_port;
select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :master_port


revoke ALL on database regression from myuser;

select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :worker_1_port

select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :master_port

drop user myuser;
-----------------------------------------------------------------------

-- test CREATE,CONNECT,TEMP,TEMPORARY privileges one by one on database
create user myuser;

grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser;

select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :worker_1_port;
select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :master_port

RESET ROLE;

revoke CREATE,CONNECT,TEMP,TEMPORARY on database regression from myuser;

select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :worker_1_port

select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');
\c - - - :master_port

drop user myuser;
-----------------------------------------------------------------------

-- test CREATE,CONNECT,TEMP,TEMPORARY privileges one by one on database with grant option
create user myuser;
create user myuser_1;

grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser;

set role myuser;
--here since myuser does not have grant option, it should fail
grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser_1;

select has_database_privilege('myuser_1','regression', 'CREATE');
select has_database_privilege('myuser_1','regression', 'CONNECT');
select has_database_privilege('myuser_1','regression', 'TEMP');
select has_database_privilege('myuser_1','regression', 'TEMPORARY');

\c - - - :worker_1_port

select has_database_privilege('myuser_1','regression', 'CREATE');
select has_database_privilege('myuser_1','regression', 'CONNECT');
select has_database_privilege('myuser_1','regression', 'TEMP');
select has_database_privilege('myuser_1','regression', 'TEMPORARY');

\c - - - :master_port

RESET ROLE;

grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser with grant option;
set role myuser;

--here since myuser have grant option, it should succeed
grant CREATE,CONNECT,TEMP,TEMPORARY on database regression to myuser_1 granted by myuser;

select has_database_privilege('myuser_1','regression', 'CREATE');
select has_database_privilege('myuser_1','regression', 'CONNECT');
select has_database_privilege('myuser_1','regression', 'TEMP');
select has_database_privilege('myuser_1','regression', 'TEMPORARY');

\c - - - :worker_1_port

select has_database_privilege('myuser_1','regression', 'CREATE');
select has_database_privilege('myuser_1','regression', 'CONNECT');
select has_database_privilege('myuser_1','regression', 'TEMP');
select has_database_privilege('myuser_1','regression', 'TEMPORARY');

\c - - - :master_port


RESET ROLE;

--below test should fail and should throw an error since myuser_1 still have the dependent privileges
revoke  CREATE,CONNECT,TEMP,TEMPORARY on database regression from myuser restrict;
--below test should fail and should throw an error since myuser_1 still have the dependent privileges
revoke grant option for CREATE,CONNECT,TEMP,TEMPORARY on database regression from myuser restrict ;

--below test should succeed and should not throw any error since myuser_1 privileges are revoked with cascade
revoke grant option for CREATE,CONNECT,TEMP,TEMPORARY on database regression from myuser cascade ;

--here we test if myuser still have the privileges after revoke grant option for

select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');

\c - - - :worker_1_port

select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');

\c - - - :master_port

reset role;



revoke  CREATE,CONNECT,TEMP,TEMPORARY on database regression from myuser;
revoke CREATE,CONNECT,TEMP,TEMPORARY on database regression from myuser_1;

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

select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');

select has_database_privilege('myuser','test_db', 'CREATE');
select has_database_privilege('myuser','test_db', 'CONNECT');
select has_database_privilege('myuser','test_db', 'TEMP');
select has_database_privilege('myuser','test_db', 'TEMPORARY');

select has_database_privilege('myuser_1','regression', 'CREATE');
select has_database_privilege('myuser_1','regression', 'CONNECT');
select has_database_privilege('myuser_1','regression', 'TEMP');
select has_database_privilege('myuser_1','regression', 'TEMPORARY');

select has_database_privilege('myuser_1','test_db', 'CREATE');
select has_database_privilege('myuser_1','test_db', 'CONNECT');
select has_database_privilege('myuser_1','test_db', 'TEMP');
select has_database_privilege('myuser_1','test_db', 'TEMPORARY');

\c - - - :worker_1_port

select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');

select has_database_privilege('myuser','test_db', 'CREATE');
select has_database_privilege('myuser','test_db', 'CONNECT');
select has_database_privilege('myuser','test_db', 'TEMP');
select has_database_privilege('myuser','test_db', 'TEMPORARY');

select has_database_privilege('myuser_1','regression', 'CREATE');
select has_database_privilege('myuser_1','regression', 'CONNECT');
select has_database_privilege('myuser_1','regression', 'TEMP');
select has_database_privilege('myuser_1','regression', 'TEMPORARY');

select has_database_privilege('myuser_1','test_db', 'CREATE');
select has_database_privilege('myuser_1','test_db', 'CONNECT');
select has_database_privilege('myuser_1','test_db', 'TEMP');
select has_database_privilege('myuser_1','test_db', 'TEMPORARY');

\c - - - :master_port

RESET ROLE;
--below test should fail and should throw an error
revoke  CREATE,CONNECT,TEMP,TEMPORARY on database regression,test_db from myuser ;

--below test should succeed and should not throw any error
revoke  CREATE,CONNECT,TEMP,TEMPORARY on database regression,test_db from myuser_1;

--below test should succeed and should not throw any error
revoke  CREATE,CONNECT,TEMP,TEMPORARY on database regression,test_db from myuser cascade;


select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');

select has_database_privilege('myuser','test_db', 'CREATE');
select has_database_privilege('myuser','test_db', 'CONNECT');
select has_database_privilege('myuser','test_db', 'TEMP');
select has_database_privilege('myuser','test_db', 'TEMPORARY');

select has_database_privilege('myuser_1','regression', 'CREATE');
select has_database_privilege('myuser_1','regression', 'CONNECT');
select has_database_privilege('myuser_1','regression', 'TEMP');
select has_database_privilege('myuser_1','regression', 'TEMPORARY');

select has_database_privilege('myuser_1','test_db', 'CREATE');
select has_database_privilege('myuser_1','test_db', 'CONNECT');
select has_database_privilege('myuser_1','test_db', 'TEMP');
select has_database_privilege('myuser_1','test_db', 'TEMPORARY');

\c - - - :worker_1_port

select has_database_privilege('myuser','regression', 'CREATE');
select has_database_privilege('myuser','regression', 'CONNECT');
select has_database_privilege('myuser','regression', 'TEMP');
select has_database_privilege('myuser','regression', 'TEMPORARY');

select has_database_privilege('myuser','test_db', 'CREATE');
select has_database_privilege('myuser','test_db', 'CONNECT');
select has_database_privilege('myuser','test_db', 'TEMP');
select has_database_privilege('myuser','test_db', 'TEMPORARY');

select has_database_privilege('myuser_1','regression', 'CREATE');
select has_database_privilege('myuser_1','regression', 'CONNECT');
select has_database_privilege('myuser_1','regression', 'TEMP');
select has_database_privilege('myuser_1','regression', 'TEMPORARY');

select has_database_privilege('myuser_1','test_db', 'CREATE');
select has_database_privilege('myuser_1','test_db', 'CONNECT');
select has_database_privilege('myuser_1','test_db', 'TEMP');
select has_database_privilege('myuser_1','test_db', 'TEMPORARY');

\c - - - :master_port

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
