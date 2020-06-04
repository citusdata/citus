-- Regression test for this issue:
-- https://github.com/citusdata/citus/issues/3867
SET citus.shard_count = 4;
SET citus.next_shard_id TO 1959300;
CREATE SCHEMA set_role_in_transaction;
SET search_path TO set_role_in_transaction;
CREATE TABLE t(a int);
SELECT create_distributed_table('t', 'a');

CREATE USER user1 SUPERUSER;
CREATE USER user2 SUPERUSER;
-- Not needed on Citus Enterprise
SELECT run_command_on_workers('CREATE USER user1');
SELECT run_command_on_workers('CREATE USER user2');
GRANT ALL ON SCHEMA set_role_in_transaction TO user1;
GRANT ALL ON SCHEMA set_role_in_transaction TO user2;
GRANT ALL ON ALL TABLES IN SCHEMA set_role_in_transaction TO user1;
GRANT ALL ON ALL TABLES IN SCHEMA set_role_in_transaction TO user2;
-- Not needed on Citus Enterprise
SELECT run_command_on_workers('GRANT ALL ON ALL TABLES IN SCHEMA set_role_in_transaction TO user1');
SELECT run_command_on_workers('GRANT ALL ON ALL TABLES IN SCHEMA set_role_in_transaction TO user2');

-- Visibility bugs
SET ROLE user1;
SET search_path TO set_role_in_transaction;
BEGIN;
INSERT INTO t values (1);
SELECT * FROM t; -- 1 is visible as expected
SET ROLE user2;
SET search_path TO set_role_in_transaction;
SELECT * FROM t; -- 1 is not visible
INSERT INTO t values (1); -- assert failure
SELECT * from t;
ROLLBACK;

-- Self deadlock
SET ROLE user1;
SET search_path TO set_role_in_transaction;
BEGIN;
TRUNCATE t;
SET ROLE user2;
SET search_path TO set_role_in_transaction;
-- assert failure or self deadlock (it is detected by deadlock detector)
INSERT INTO t values (2);
ROLLBACK;

RESET ROLE;

REVOKE ALL ON SCHEMA set_role_in_transaction FROM user1;
REVOKE ALL ON SCHEMA set_role_in_transaction FROM user2;
REVOKE ALL ON ALL TABLES IN SCHEMA set_role_in_transaction FROM user1;
REVOKE ALL ON ALL TABLES IN SCHEMA set_role_in_transaction FROM user2;
SELECT run_command_on_workers('REVOKE ALL ON ALL TABLES IN SCHEMA set_role_in_transaction FROM user1');
SELECT run_command_on_workers('REVOKE ALL ON ALL TABLES IN SCHEMA set_role_in_transaction FROM user2');

DROP USER user1;
DROP USER user2;
SELECT run_command_on_workers('DROP USER user1');
SELECT run_command_on_workers('DROP USER user2');

DROP SCHEMA set_role_in_transaction CASCADE;
