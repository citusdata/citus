CREATE SCHEMA alter_role;

-- test if the passowrd of the extension owner can be upgraded
ALTER ROLE CURRENT_USER PASSWORD 'password123';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = current_user$$);

-- test if the password and some connection settings are propagated when a node gets added
ALTER ROLE CURRENT_USER WITH CONNECTION LIMIT 66 VALID UNTIL '2032-05-05' PASSWORD 'password456';
SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = current_user$$);
SELECT master_remove_node('localhost', :worker_1_port);
ALTER ROLE CURRENT_USER WITH CONNECTION LIMIT 0 VALID UNTIL '2052-05-05' PASSWORD 'password789';
SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = current_user$$);
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = current_user$$);

DROP SCHEMA alter_role CASCADE;
