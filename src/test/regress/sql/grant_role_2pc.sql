

CREATE SCHEMA grant_role2pc;

SET search_path TO grant_role2pc;

set citus.enable_create_database_propagation to on;


CREATE DATABASE grant_role2pc_db;

revoke connect,temp,temporary,create  on database grant_role2pc_db from public;

\c grant_role2pc_db
SHOW citus.main_db;

-- check that empty citus.superuser gives error
SET citus.superuser TO '';
SET citus.superuser TO 'postgres';

CREATE USER grant_role2pc_user1;
CREATE USER grant_role2pc_user2;
CREATE USER grant_role2pc_user3;
CREATE USER grant_role2pc_user4;
CREATE USER grant_role2pc_user5;
CREATE USER grant_role2pc_user6;
CREATE USER grant_role2pc_user7;

\c grant_role2pc_db

--test with empty superuser
SET citus.superuser TO '';
grant grant_role2pc_user1 to grant_role2pc_user2;

SET citus.superuser TO 'postgres';
grant grant_role2pc_user1 to grant_role2pc_user2 with admin option granted by CURRENT_USER;

\c regression

select result FROM run_command_on_all_nodes(
    $$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
    SELECT r.rolname AS role, g.rolname AS group, a.rolname AS grantor, m.admin_option
    FROM pg_auth_members m
    JOIN pg_roles r ON r.oid = m.roleid
    JOIN pg_roles g ON g.oid = m.member
    JOIN pg_roles a ON a.oid = m.grantor
    WHERE g.rolname = 'grant_role2pc_user2'
    order by g.rolname
    ) t
    $$
);

\c grant_role2pc_db
--test grant under transactional context with multiple operations
BEGIN;
grant grant_role2pc_user1 to grant_role2pc_user3;
grant grant_role2pc_user1 to grant_role2pc_user4;
COMMIT;

BEGIN;
grant grant_role2pc_user1 to grant_role2pc_user5;
grant grant_role2pc_user1 to grant_role2pc_user6;
ROLLBACK;



BEGIN;
grant grant_role2pc_user1 to grant_role2pc_user7;
SELECT 1/0;
commit;


\c regression

select result FROM run_command_on_all_nodes($$
SELECT array_to_json(array_agg(row_to_json(t)))
FROM (
  SELECT r.rolname AS role, g.rolname AS group, a.rolname AS grantor, m.admin_option
  FROM pg_auth_members m
  JOIN pg_roles r ON r.oid = m.roleid
  JOIN pg_roles g ON g.oid = m.member
  JOIN pg_roles a ON a.oid = m.grantor
  WHERE g.rolname in ('grant_role2pc_user3','grant_role2pc_user4','grant_role2pc_user5','grant_role2pc_user6','grant_role2pc_user7')
  order by g.rolname
) t
$$);


\c grant_role2pc_db

grant grant_role2pc_user1,grant_role2pc_user2 to grant_role2pc_user5,grant_role2pc_user6,grant_role2pc_user7;

\c regression

select result FROM run_command_on_all_nodes($$
SELECT array_to_json(array_agg(row_to_json(t)))
FROM (
  SELECT r.rolname AS role, g.rolname AS group, a.rolname AS grantor, m.admin_option
  FROM pg_auth_members m
  JOIN pg_roles r ON r.oid = m.roleid
  JOIN pg_roles g ON g.oid = m.member
  JOIN pg_roles a ON a.oid = m.grantor
  WHERE g.rolname in ('grant_role2pc_user5','grant_role2pc_user6','grant_role2pc_user7')
  order by g.rolname
) t
$$);

\c grant_role2pc_db
revoke admin option for grant_role2pc_user1 from grant_role2pc_user2 granted by CURRENT_USER;

--test revoke under transactional context with multiple operations
BEGIN;
revoke grant_role2pc_user1 from grant_role2pc_user3;
revoke grant_role2pc_user1 from grant_role2pc_user4;
COMMIT;

BEGIN;
revoke grant_role2pc_user1 from grant_role2pc_user5,grant_role2pc_user6;
revoke grant_role2pc_user1 from grant_role2pc_user7;
COMMIT;

\c regression

select result FROM run_command_on_all_nodes($$
SELECT array_to_json(array_agg(row_to_json(t)))
FROM (
  SELECT r.rolname AS role, g.rolname AS group, a.rolname AS grantor, m.admin_option
  FROM pg_auth_members m
  JOIN pg_roles r ON r.oid = m.roleid
  JOIN pg_roles g ON g.oid = m.member
  JOIN pg_roles a ON a.oid = m.grantor
  WHERE g.rolname in ('grant_role2pc_user2','grant_role2pc_user3','grant_role2pc_user4','grant_role2pc_user5','grant_role2pc_user6','grant_role2pc_user7')
  order by g.rolname
) t
$$);

DROP SCHEMA grant_role2pc;



set citus.enable_create_database_propagation to on;
DROP DATABASE grant_role2pc_db;

drop user grant_role2pc_user2,grant_role2pc_user3,grant_role2pc_user4,grant_role2pc_user5,grant_role2pc_user6,grant_role2pc_user7;


drop user grant_role2pc_user1;

grant connect,temp,temporary  on database regression to public;

reset citus.enable_create_database_propagation;
