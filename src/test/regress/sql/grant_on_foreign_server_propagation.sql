--
-- GRANT_ON_FOREIGN_SERVER_PROPAGATION
-- 'password_required' option for USER MAPPING is introduced in PG13.
--
CREATE SCHEMA "grant on server";
SET search_path TO "grant on server";

-- remove one of the worker nodes to test adding a new node later
SELECT 1 FROM citus_remove_node('localhost', :master_port);
SELECT 1 FROM citus_remove_node('localhost', :worker_2_port);
select 1 from citus_add_node('localhost',:master_port,groupId=>0);

SET citus.use_citus_managed_tables TO ON;

-- create target table and insert some data
CREATE TABLE foreign_table_test (id integer NOT NULL, data text);
INSERT INTO foreign_table_test VALUES (1, 'text_test');

CREATE EXTENSION postgres_fdw;

CREATE ROLE ownerrole WITH LOGIN;
GRANT ALL ON FOREIGN DATA WRAPPER postgres_fdw TO ownerrole WITH GRANT OPTION;
SET ROLE ownerrole;
-- verify we can create server using the privilege on FDW granted to non-superuser role
CREATE SERVER "Foreign Server"
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'localhost', port :'master_port', dbname 'regression');
RESET ROLE;

CREATE USER MAPPING FOR CURRENT_USER
        SERVER "Foreign Server"
        OPTIONS (user 'postgres');

-- foreign table owned by superuser
CREATE FOREIGN TABLE foreign_table_owned_by_superuser (
        id integer NOT NULL,
        data text
)
        SERVER "Foreign Server"
        OPTIONS (schema_name 'grant on server', table_name 'foreign_table_test');

-- create a non-superuser role
CREATE ROLE role_test_servers;
ALTER ROLE role_test_servers WITH LOGIN;
CREATE ROLE role_test_servers_2 WITH LOGIN;

SET ROLE ownerrole;
-- verify that non-superuser role can GRANT on other non-superuser roles, on FDWs
-- if WITH GRANT OPTION is provided
GRANT USAGE ON FOREIGN DATA WRAPPER postgres_fdw TO role_test_servers_2;
RESET ROLE;

-- grant privileges
GRANT ALL ON SCHEMA "grant on server" TO role_test_servers WITH GRANT OPTION;
GRANT ALL ON foreign_table_owned_by_superuser TO role_test_servers WITH GRANT OPTION;
GRANT ALL ON foreign_table_test TO role_test_servers WITH GRANT OPTION;
GRANT ALL ON foreign server "Foreign Server" TO role_test_servers, postgres WITH GRANT OPTION;

SET ROLE role_test_servers;

GRANT ALL ON SCHEMA "grant on server" TO role_test_servers_2 WITH GRANT OPTION;
GRANT ALL ON foreign_table_owned_by_superuser TO role_test_servers_2 WITH GRANT OPTION;
GRANT ALL ON foreign_table_test TO role_test_servers_2 WITH GRANT OPTION;
GRANT ALL ON foreign server "Foreign Server" TO role_test_servers_2, postgres WITH GRANT OPTION;

RESET ROLE;

-- add user mapping for the role
CREATE USER MAPPING FOR role_test_servers
        SERVER "Foreign Server"
        OPTIONS (user 'role_test_servers', password_required 'false');
CREATE USER MAPPING FOR role_test_servers_2
        SERVER "Foreign Server"
        OPTIONS (user 'role_test_servers_2', password_required 'false');

SET ROLE role_test_servers_2;

-- foreign table owned by non-superuser
CREATE FOREIGN TABLE foreign_table_owned_by_regular_user (
        id integer NOT NULL,
        data text
)
        SERVER "Foreign Server"
        OPTIONS (schema_name 'grant on server', table_name 'foreign_table_test');

RESET ROLE;

-- now add the node and verify that all propagated correctly
select 1 from citus_add_node('localhost', :'worker_2_port');

\c - - - :worker_2_port
SET search_path TO "grant on server";
SET ROLE role_test_servers_2;
SELECT * from foreign_table_owned_by_superuser;
SELECT * from foreign_table_owned_by_regular_user;

SET ROLE postgres;
SELECT * from foreign_table_owned_by_superuser;
SELECT * from foreign_table_owned_by_regular_user;

\c - - - :master_port

-- verify that the non-superuser privileges has been propagated to the existing worker,
-- and also granted on the newly added worker as well
SELECT run_command_on_workers($$
        SELECT fdwacl FROM pg_foreign_data_wrapper WHERE fdwname = 'postgres_fdw';$$);

SELECT run_command_on_workers($$
        SELECT rolname FROM pg_roles WHERE oid IN (SELECT srvowner FROM pg_foreign_server WHERE srvname = 'Foreign Server');$$);

REVOKE GRANT OPTION FOR ALL ON FOREIGN SERVER "Foreign Server" FROM role_test_servers CASCADE;
REVOKE GRANT OPTION FOR ALL ON FOREIGN DATA WRAPPER postgres_fdw FROM ownerrole CASCADE;

SELECT run_command_on_workers($$
        SELECT fdwacl FROM pg_foreign_data_wrapper WHERE fdwname = 'postgres_fdw';$$);

SELECT run_command_on_workers($$
        SELECT srvacl FROM pg_foreign_server WHERE srvname = 'Foreign Server';$$);

REVOKE ALL ON FOREIGN DATA WRAPPER postgres_fdw FROM role_test_servers_2, ownerrole CASCADE;

SELECT run_command_on_workers($$
        SELECT fdwacl FROM pg_foreign_data_wrapper WHERE fdwname = 'postgres_fdw';$$);

REVOKE ALL ON FOREIGN SERVER "Foreign Server" FROM role_test_servers, postgres CASCADE;

SELECT run_command_on_workers($$
        SELECT srvacl FROM pg_foreign_server WHERE srvname = 'Foreign Server';$$);

REVOKE ALL ON SCHEMA "grant on server" FROM role_test_servers CASCADE;

-- cleanup
SET client_min_messages TO ERROR;
DROP SERVER "Foreign Server" CASCADE;
DROP SCHEMA "grant on server" CASCADE;
DROP ROLE role_test_servers, role_test_servers_2, ownerrole;
