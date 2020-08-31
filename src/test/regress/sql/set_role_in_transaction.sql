-- Regression test for this issue:
-- https://github.com/citusdata/citus/issues/3867
SET citus.shard_count = 4;
SET citus.next_shard_id TO 1959300;
CREATE SCHEMA set_role_in_transaction;
SET search_path TO set_role_in_transaction;
CREATE TABLE t(a int);
SELECT create_distributed_table('t', 'a');

-- hide messages only visible in Citus Opensource
SET client_min_messages TO WARNING;
CREATE USER user1;
CREATE USER user2;
-- Not needed on Citus Enterprise, so using count(*) for same output
SELECT count(*) FROM run_command_on_workers('CREATE USER user1');
SELECT count(*) FROM run_command_on_workers('CREATE USER user2');

GRANT ALL ON SCHEMA set_role_in_transaction TO user1;
GRANT ALL ON SCHEMA set_role_in_transaction TO user2;
GRANT ALL ON ALL TABLES IN SCHEMA set_role_in_transaction TO user1;
GRANT ALL ON ALL TABLES IN SCHEMA set_role_in_transaction TO user2;
-- Not needed on Citus Enterprise, so using count(*) for same output
SELECT count(*) FROM run_command_on_workers('GRANT ALL ON ALL TABLES IN SCHEMA set_role_in_transaction TO user1');
SELECT count(*) FROM run_command_on_workers('GRANT ALL ON ALL TABLES IN SCHEMA set_role_in_transaction TO user2');
RESET client_min_messages;

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

-- we cannot change role in between COPY commands as well
SET ROLE user1;
SET search_path TO set_role_in_transaction;
BEGIN;
    COPY t FROM STDIN;
1
2
3
\.
    SET ROLE user2;
    SET search_path TO set_role_in_transaction;
    COPY t FROM STDIN;
1
2
3
\.
ROLLBACK;

RESET ROLE;

REVOKE ALL ON SCHEMA set_role_in_transaction FROM user1;
REVOKE ALL ON SCHEMA set_role_in_transaction FROM user2;
REVOKE ALL ON ALL TABLES IN SCHEMA set_role_in_transaction FROM user1;
REVOKE ALL ON ALL TABLES IN SCHEMA set_role_in_transaction FROM user2;
-- Not needed on Citus Enterprise, so using count(*) for same output
SELECT count(*) FROM run_command_on_workers('REVOKE ALL ON ALL TABLES IN SCHEMA set_role_in_transaction FROM user1');
SELECT count(*) FROM run_command_on_workers('REVOKE ALL ON ALL TABLES IN SCHEMA set_role_in_transaction FROM user2');

DROP USER user1;
DROP USER user2;
-- Not needed on Citus Enterprise, so using count(*) for same output
SELECT count(*) FROM run_command_on_workers('DROP USER user1');
SELECT count(*) FROM run_command_on_workers('DROP USER user2');

DROP SCHEMA set_role_in_transaction CASCADE;
