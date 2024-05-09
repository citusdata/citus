-- Create a new database
ALTER SYSTEM SET citus.max_maintenance_shared_pool_size = 1;
SELECT pg_reload_conf();
set citus.enable_create_database_propagation to on;
CREATE DATABASE role_operations_test_db;
SET citus.superuser TO 'postgres';
-- Connect to the new database
\c role_operations_test_db

CREATE ROLE test_role1;

\c regression - - :master_port
-- Clean up: drop the database
set citus.enable_create_database_propagation to on;
DROP DATABASE role_operations_test_db;
reset citus.enable_create_database_propagation;
ALTER SYSTEM SET citus.max_maintenance_shared_pool_size = 1;
SELECT pg_reload_conf();
