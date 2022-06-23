--
-- MULTI_MULTIUSER_GRANT
--
-- check that after-the-fact REVOKE/GRANTs are taken into account
--

-- check that permissions are checked on the master and workers
REVOKE ALL ON TABLE customer FROM full_access;
SET ROLE full_access;
SELECT count(*) FROM customer; -- should fail
SELECT has_table_privilege('customer', 'SELECT');
RESET ROLE;
\c - - - :worker_1_port
SET ROLE full_access;
SELECT oid, relname, relacl FROM pg_class WHERE relkind = 'r' AND relname LIKE 'customer_%' AND has_table_privilege(oid, 'SELECT');
SELECT count(*) FROM pg_class WHERE relkind = 'r' AND relname LIKE 'customer_%' AND NOT has_table_privilege(oid, 'SELECT');
RESET ROLE;

-- check that GRANT command obeys citus.enable_ddl_propagation setting
\c - - - :master_port
SET citus.enable_ddl_propagation TO 'off';
GRANT ALL ON TABLE customer TO full_access;
SET ROLE full_access;
SELECT has_table_privilege('customer', 'SELECT'); -- should be true

\c - - - :worker_1_port
SET ROLE full_access;
SELECT oid, relname, relacl FROM pg_class WHERE relkind = 'r' AND relname LIKE 'customer_%' AND has_table_privilege(oid, 'SELECT');
SELECT count(*) FROM pg_class WHERE relkind = 'r' AND relname LIKE 'customer_%' AND NOT has_table_privilege(oid, 'SELECT');

\c - - - :master_port
SET citus.enable_ddl_propagation TO 'on';
GRANT ALL ON TABLE customer TO full_access;
SET ROLE full_access;
SELECT count(*) FROM customer; -- should work again
RESET ROLE;
