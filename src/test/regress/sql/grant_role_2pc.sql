

CREATE SCHEMA grant_role2pc;

SET search_path TO grant_role2pc;

set citus.enable_create_database_propagation to on;


set citus.log_remote_commands to on;


SET citus.next_shard_id TO 10231023;

CREATE DATABASE grant_role2pc_db;

revoke connect,temp,temporary,create  on database grant_role2pc_db from public;

\c grant_role2pc_db
SHOW citus.main_db;

-- check that empty citus.superuser gives error
SET citus.superuser TO '';
CREATE USER empty_superuser;
SET citus.superuser TO 'postgres';

CREATE USER grant_role2pc_user1;
CREATE USER grant_role2pc_user2;
CREATE USER grant_role2pc_user3;
CREATE USER grant_role2pc_user4;
CREATE USER grant_role2pc_user5;
CREATE USER grant_role2pc_user6;
CREATE USER grant_role2pc_user7;


\c regression

SELECT * FROM public.check_database_privileges('grant_role2pc_user2', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);

grant create,connect,temporary,temp on database grant_role2pc_db to grant_role2pc_user1;

\c grant_role2pc_db

grant grant_role2pc_user1 to grant_role2pc_user2;

\c regression


SELECT * FROM public.check_database_privileges('grant_role2pc_user2', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);


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

SELECT * FROM public.check_database_privileges('grant_role2pc_user3', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);
SELECT * FROM public.check_database_privileges('grant_role2pc_user4', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);
SELECT * FROM public.check_database_privileges('grant_role2pc_user5', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);
SELECT * FROM public.check_database_privileges('grant_role2pc_user6', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);
SELECT * FROM public.check_database_privileges('grant_role2pc_user7', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);

\c grant_role2pc_db

grant grant_role2pc_user1 to grant_role2pc_user5,grant_role2pc_user6,grant_role2pc_user7;

\c regression
SELECT * FROM public.check_database_privileges('grant_role2pc_user5', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);
SELECT * FROM public.check_database_privileges('grant_role2pc_user6', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);
SELECT * FROM public.check_database_privileges('grant_role2pc_user7', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);

\c grant_role2pc_db
revoke grant_role2pc_user1 from grant_role2pc_user2;

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

SELECT * FROM public.check_database_privileges('grant_role2pc_user2', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);
SELECT * FROM public.check_database_privileges('grant_role2pc_user3', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);
SELECT * FROM public.check_database_privileges('grant_role2pc_user4', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);
SELECT * FROM public.check_database_privileges('grant_role2pc_user5', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);
SELECT * FROM public.check_database_privileges('grant_role2pc_user6', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);
SELECT * FROM public.check_database_privileges('grant_role2pc_user7', 'grant_role2pc_db', ARRAY['CREATE', 'CONNECT', 'TEMP', 'TEMPORARY']);

DROP SCHEMA grant_role2pc;

REVOKE ALL PRIVILEGES ON DATABASE grant_role2pc_db FROM grant_role2pc_user1;

set citus.enable_create_database_propagation to on;
DROP DATABASE grant_role2pc_db;

drop user grant_role2pc_user2,grant_role2pc_user3,grant_role2pc_user4,grant_role2pc_user5,grant_role2pc_user6,grant_role2pc_user7;


drop user grant_role2pc_user1;

grant connect,temp,temporary  on database regression to public;

reset citus.enable_create_database_propagation;
