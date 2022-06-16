CREATE SCHEMA alter_database_owner;
SET search_path TO alter_database_owner, public;

CREATE USER database_owner_1;
CREATE USER database_owner_2;

-- make sure the propagation of ALTER DATABASE ... OWNER TO ... is on
-- in enterprise we default to on, to verify this we don't set it explicitly to on and run
-- the rest of the tests exactly as on community
-- SET citus.enable_alter_database_owner TO on;

-- list the owners of the current database on all nodes
SELECT run_command_on_workers($$
    SELECT u.rolname
      FROM pg_database d
      JOIN pg_roles u
        ON (d.datdba = u.oid)
     WHERE d.datname = current_database();
$$);

-- remove a node to verify addition later
SELECT master_remove_node('localhost', :worker_2_port);

-- verify we can change the owner of a database
ALTER DATABASE regression OWNER TO database_owner_1;

-- list the owner of the current database on the coordinator
SELECT u.rolname
  FROM pg_database d
  JOIN pg_roles u
    ON (d.datdba = u.oid)
 WHERE d.datname = current_database();

-- list the owners of the current database on all nodes
SELECT run_command_on_workers($$
    SELECT u.rolname
      FROM pg_database d
      JOIN pg_roles u
        ON (d.datdba = u.oid)
     WHERE d.datname = current_database();
$$);

-- turn off propagation to verify it does _not_ propagate to new nodes when turned off
SET citus.enable_alter_database_owner TO off;

-- add back second node to verify the owner of the database was set accordingly
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- list the owners of the current database on all nodes, should reflect on newly added node
SELECT run_command_on_workers($$
    SELECT u.rolname
      FROM pg_database d
      JOIN pg_roles u
        ON (d.datdba = u.oid)
     WHERE d.datname = current_database();
$$);

-- turn on propagation to verify it does propagate to new nodes when enabled
SET citus.enable_alter_database_owner TO on;
SELECT master_remove_node('localhost', :worker_2_port); -- remove so we can re add with propagation on

-- add back second node to verify the owner of the database was set accordingly
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- list the owners of the current database on all nodes, should reflect on newly added node
SELECT run_command_on_workers($$
    SELECT u.rolname
      FROM pg_database d
      JOIN pg_roles u
        ON (d.datdba = u.oid)
     WHERE d.datname = current_database();
$$);

-- test changing the owner in a transaction and rollback to cancel
BEGIN;
ALTER DATABASE regression OWNER TO database_owner_2;
ROLLBACK;
-- list the owners of the current database on all nodes
SELECT u.rolname
  FROM pg_database d
  JOIN pg_roles u
    ON (d.datdba = u.oid)
 WHERE d.datname = current_database();
SELECT run_command_on_workers($$
    SELECT u.rolname
      FROM pg_database d
      JOIN pg_roles u
        ON (d.datdba = u.oid)
     WHERE d.datname = current_database();
$$);

-- verify that a user not present on the newly added node will automatically be propagated
SELECT master_remove_node('localhost', :worker_2_port);
CREATE USER database_owner_3;
ALTER DATABASE regression OWNER TO database_owner_3;
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- list the owners of the current database on all nodes
SELECT u.rolname
  FROM pg_database d
  JOIN pg_roles u
    ON (d.datdba = u.oid)
 WHERE d.datname = current_database();
SELECT run_command_on_workers($$
    SELECT u.rolname
      FROM pg_database d
      JOIN pg_roles u
        ON (d.datdba = u.oid)
     WHERE d.datname = current_database();
$$);

-- reset back to original owner to not break subsequent tests and remove enterprise test user
ALTER DATABASE regression OWNER TO database_owner_1;
DROP USER database_owner_3;

CREATE TABLE t (a int PRIMARY KEY);
SELECT create_distributed_table('t', 'a');
-- test changing the owner in a xact that already had parallel execution
BEGIN;
SELECT count(*) FROM t; -- parallel execution;
ALTER DATABASE regression OWNER TO database_owner_2; -- should ERROR
ROLLBACK;

-- list the owners of the current database on all nodes
SELECT u.rolname
  FROM pg_database d
  JOIN pg_roles u
    ON (d.datdba = u.oid)
 WHERE d.datname = current_database();
SELECT run_command_on_workers($$
    SELECT u.rolname
      FROM pg_database d
      JOIN pg_roles u
        ON (d.datdba = u.oid)
     WHERE d.datname = current_database();
$$);

BEGIN;
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
SELECT count(*) FROM t; -- parallel execution;
ALTER DATABASE regression OWNER TO database_owner_2;
COMMIT;

-- list the owners of the current database on all nodes
SELECT u.rolname
  FROM pg_database d
  JOIN pg_roles u
    ON (d.datdba = u.oid)
 WHERE d.datname = current_database();
SELECT run_command_on_workers($$
    SELECT u.rolname
      FROM pg_database d
      JOIN pg_roles u
        ON (d.datdba = u.oid)
     WHERE d.datname = current_database();
$$);

-- turn propagation off and verify it does not propagate interactively when turned off
SET citus.enable_alter_database_owner TO off;

ALTER DATABASE regression OWNER TO database_owner_1;
-- list the owners of the current database on all nodes
SELECT u.rolname
  FROM pg_database d
  JOIN pg_roles u
    ON (d.datdba = u.oid)
 WHERE d.datname = current_database();
SELECT run_command_on_workers($$
    SELECT u.rolname
      FROM pg_database d
      JOIN pg_roles u
        ON (d.datdba = u.oid)
     WHERE d.datname = current_database();
$$);

-- reset state of cluster
SET citus.enable_alter_database_owner TO on;
ALTER DATABASE regression OWNER TO current_user;
-- list the owners of the current database on all nodes
SELECT u.rolname
  FROM pg_database d
  JOIN pg_roles u
    ON (d.datdba = u.oid)
 WHERE d.datname = current_database();
SELECT run_command_on_workers($$
    SELECT u.rolname
      FROM pg_database d
      JOIN pg_roles u
        ON (d.datdba = u.oid)
     WHERE d.datname = current_database();
$$);

DROP USER database_owner_1;
DROP USER database_owner_2;
SET client_min_messages TO warning;
DROP SCHEMA alter_database_owner CASCADE;
