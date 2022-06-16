-- Show that dependent user/role objects can be created safely
CREATE USER non_super_user_test_user;
CREATE SCHEMA role_dependency_schema;

CREATE TYPE role_dependency_schema.superusers_type AS (a int, b int);
GRANT CREATE ON SCHEMA role_dependency_schema to non_super_user_test_user;
GRANT USAGE ON SCHEMA role_dependency_schema to non_super_user_test_user;
GRANT USAGE ON TYPE role_dependency_schema.superusers_type TO non_super_user_test_user;

SET ROLE non_super_user_test_user;
CREATE TABLE role_dependency_schema.non_super_user_table(a int, b role_dependency_schema.superusers_type);
SELECT create_distributed_table('role_dependency_schema.non_super_user_table','a');

-- Show that table and superuser's type is marked as distributed
RESET ROLE;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where pg_identify_object_as_address(classid, objid, objsubid)::text like '%non_super_user_table%';
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where pg_identify_object_as_address(classid, objid, objsubid)::text like '%non_super_user_table%';$$) ORDER BY 1,2;

SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where pg_identify_object_as_address(classid, objid, objsubid)::text like '%superusers_type%';
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where pg_identify_object_as_address(classid, objid, objsubid)::text like '%superusers_type%';$$) ORDER BY 1,2;

DROP SCHEMA role_dependency_schema CASCADE;
