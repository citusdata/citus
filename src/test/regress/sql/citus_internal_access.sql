--- Create a non-superuser role and check if it can access citus_internal schema functions
CREATE USER nonsuperuser CREATEROLE;

SET ROLE nonsuperuser;
--- The non-superuser role should not be able to access citus_internal functions
SELECT citus_internal.replace_isolation_tester_func();

RESET ROLE;
DROP USER nonsuperuser;
