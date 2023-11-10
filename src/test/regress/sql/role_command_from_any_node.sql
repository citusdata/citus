-- idempotently remove the coordinator from metadata
SELECT COUNT(citus_remove_node(nodename, nodeport)) >= 0 FROM pg_dist_node WHERE nodename = 'localhost' AND nodeport = :master_port;

-- make sure that CREATE ROLE from workers is not supported when coordinator is not added to metadata
SELECT result FROM run_command_on_workers('CREATE ROLE test_role');

\c - - - :master_port

CREATE SCHEMA role_command_from_any_node;
SET search_path TO role_command_from_any_node;

SET client_min_messages TO WARNING;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);

CREATE OR REPLACE FUNCTION check_role_on_all_nodes(p_role_name text)
RETURNS TABLE (node_type text, result text)
AS $func$
DECLARE
  v_worker_query text;
BEGIN
  v_worker_query := format(
    $$
      SELECT to_jsonb(q1.*) FROM (
        SELECT
          (
            SELECT COUNT(*) = 1 FROM pg_roles WHERE rolname = '%s'
          ) AS role_exists,
          (
            SELECT to_jsonb(q.*) FROM (SELECT * FROM pg_roles WHERE rolname = '%s') q
          ) AS role_properties,
          (
            SELECT COUNT(*) = 1
            FROM pg_dist_object
            WHERE objid = (SELECT oid FROM pg_roles WHERE rolname = '%s')
          ) AS pg_dist_object_record_for_role_exists,
          (
            SELECT COUNT(*) > 0
            FROM pg_dist_object
            WHERE classid = 1260 AND objid NOT IN (SELECT oid FROM pg_roles)
          ) AS stale_pg_dist_object_record_for_a_role_exists
      ) q1
    $$,
    p_role_name, p_role_name, p_role_name
  );

  RETURN QUERY
    SELECT
      CASE WHEN (groupid = 0 AND groupid = (SELECT groupid FROM pg_dist_local_group)) THEN 'coordinator (local)'
           WHEN (groupid = 0) THEN 'coordinator (remote)'
           WHEN (groupid = (SELECT groupid FROM pg_dist_local_group)) THEN 'worker node (local)'
           ELSE 'worker node (remote)'
      END AS node_type,
      q2.result
    FROM run_command_on_all_nodes(v_worker_query) q2
  JOIN pg_dist_node USING (nodeid);
END;
$func$ LANGUAGE plpgsql;

\c - - - :worker_1_port

SET search_path TO role_command_from_any_node;
SET client_min_messages TO NOTICE;

SET citus.enable_create_role_propagation TO OFF;

CREATE ROLE test_role;
SELECT node_type, (result::jsonb - 'role_properties') as result FROM check_role_on_all_nodes('test_role') ORDER BY node_type;

DROP ROLE test_role;
SELECT node_type, (result::jsonb - 'role_properties') as result FROM check_role_on_all_nodes('test_role') ORDER BY node_type;

CREATE ROLE test_role;
SELECT node_type, (result::jsonb - 'role_properties') as result FROM check_role_on_all_nodes('test_role') ORDER BY node_type;

SET citus.enable_create_role_propagation TO ON;

-- doesn't fail even if the role doesn't exist on other nodes
DROP ROLE test_role;

SELECT node_type, (result::jsonb - 'role_properties') as result FROM check_role_on_all_nodes('test_role') ORDER BY node_type;

CREATE ROLE test_role;
SELECT node_type, (result::jsonb - 'role_properties') as result FROM check_role_on_all_nodes('test_role') ORDER BY node_type;

DROP ROLE test_role;
SELECT node_type, (result::jsonb - 'role_properties') as result FROM check_role_on_all_nodes('test_role') ORDER BY node_type;

CREATE ROLE test_role;

SET citus.enable_alter_role_propagation TO OFF;

ALTER ROLE test_role RENAME TO test_role_renamed;
SELECT node_type, (result::jsonb - 'role_properties') as result FROM check_role_on_all_nodes('test_role_renamed') ORDER BY node_type;

ALTER ROLE test_role_renamed RENAME TO test_role;

SET citus.enable_alter_role_propagation TO ON;

ALTER ROLE test_role RENAME TO test_role_renamed;
SELECT node_type, (result::jsonb - 'role_properties') as result FROM check_role_on_all_nodes('test_role_renamed') ORDER BY node_type;

SET citus.enable_alter_role_propagation TO OFF;
ALTER ROLE test_role_renamed CREATEDB;
SET citus.enable_alter_role_propagation TO ON;

SELECT node_type, (result::jsonb)->'role_properties'->'rolcreatedb' as result FROM check_role_on_all_nodes('test_role_renamed') ORDER BY node_type;

ALTER ROLE test_role_renamed CREATEDB;
SELECT node_type, (result::jsonb)->'role_properties'->'rolcreatedb' as result FROM check_role_on_all_nodes('test_role_renamed') ORDER BY node_type;

SET citus.enable_alter_role_set_propagation TO ON;

ALTER ROLE current_user IN DATABASE "regression" SET enable_hashjoin TO OFF;

SELECT result FROM run_command_on_all_nodes('SHOW enable_hashjoin') ORDER BY result;

SET citus.enable_alter_role_set_propagation TO OFF;

ALTER ROLE current_user IN DATABASE "regression" SET enable_hashjoin TO ON;

SELECT result FROM run_command_on_all_nodes('SHOW enable_hashjoin') ORDER BY result;

SET citus.enable_alter_role_set_propagation TO ON;

ALTER ROLE current_user IN DATABASE "regression" RESET enable_hashjoin;

CREATE ROLE another_user;

SET citus.enable_create_role_propagation TO OFF;

GRANT another_user TO test_role_renamed;

SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=1 FROM pg_auth_members WHERE roleid = 'another_user'::regrole AND member = 'test_role_renamed'::regrole
$$) ORDER BY result;

SET citus.enable_create_role_propagation TO ON;

SET client_min_messages TO ERROR;
GRANT another_user TO test_role_renamed;
SET client_min_messages TO NOTICE;

SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=1 FROM pg_auth_members WHERE roleid = 'another_user'::regrole AND member = 'test_role_renamed'::regrole
$$) ORDER BY result;

\c - - - :master_port

SET search_path TO role_command_from_any_node;
SET client_min_messages TO NOTICE;

SELECT citus_remove_node('localhost', :worker_1_port);
SELECT 1 FROM citus_add_node('localhost', :worker_1_port);

-- make sure that citus_add_node() propagates the roles created via a worker
SELECT node_type, (result::jsonb - 'role_properties') as result FROM check_role_on_all_nodes('test_role_renamed') ORDER BY node_type;

SELECT citus_remove_node('localhost', :master_port);

\c - - - :worker_1_port

-- they fail because the coordinator is not added to metadata
DROP ROLE test_role_renamed;
ALTER ROLE test_role_renamed RENAME TO test_role;
ALTER ROLE test_role_renamed CREATEDB;
ALTER ROLE current_user IN DATABASE "regression" SET enable_hashjoin TO OFF;
GRANT another_user TO test_role_renamed;

\c - - - :master_port

DROP ROLE test_role_renamed, another_user;

SET client_min_messages TO WARNING;
DROP SCHEMA role_command_from_any_node CASCADE;
