--
-- GRANT_ON_TABLE_PROPAGATION
--
SET citus.shard_replication_factor TO 1;
CREATE SCHEMA grant_on_table;
SET search_path TO grant_on_table;

-- create some simple tables: 1 local and 2 managed by citus
-- d ACL must not be updated in anyway.
CREATE TABLE dist_table (id int primary key, a int, b text, c int, d text);
SELECT create_distributed_table('grant_on_table.dist_table', 'id');
CREATE TABLE ref_table (id int primary key, a int, b text, c int, d text);
SELECT create_reference_table('grant_on_table.ref_table');

CREATE TABLE local_table (id int primary key, a int, b text, c int, d text);
\c - - - :worker_1_port
SET search_path TO grant_on_table;
CREATE TABLE local_table (id int primary key, a int, b text, c int, d text);
\c - - - :worker_2_port
SET search_path TO grant_on_table;
CREATE TABLE local_table (id int primary key, a int, b text, c int, d text);
\c - - - :master_port
SET search_path TO grant_on_table;

-- create some users
CREATE USER grant_user_0;
CREATE USER grant_user_1;
-- this one should not be granted anything:
CREATE USER nogrant_user;

--
-- tests related to columns ACL
--
-- test single table, single priv, single col, single user
GRANT SELECT (a) ON ref_table TO grant_user_0;
-- test several tables, several distinct priv, several cols, severla users
GRANT SELECT (c, b), INSERT (a) ON ref_table, dist_table TO grant_user_0, grant_user_1;
-- test several tables, several distinct priv, several cols and non cols, severla users
GRANT UPDATE (c, b), DELETE ON ref_table TO grant_user_0, grant_user_1;
-- test special case: with system columns
GRANT INSERT (ctid, xmin, b) ON ref_table TO grant_user_0;
-- test special case: ALL
GRANT ALL (id) ON ref_table TO grant_user_0;
GRANT ALL (id, c) ON ref_table TO grant_user_1;
-- ALL cannot be mixed with other privs
GRANT SELECT (d), ALL (d) ON ref_table TO nogrant_user;
GRANT ALL (d), SELECT (d) ON ref_table TO nogrant_user;
-- test special case: ALL TABLES IN SCHEMA is not supposed to be correct
-- but is accepted by PostgreSQL - non documented feature
GRANT SELECT (id) ON ALL TABLES IN SCHEMA grant_on_table TO grant_user_1;
-- test special case: TRUNCATE and some others are not correct, here mixed with correct
GRANT TRUNCATE (b, a), SELECT (d) ON ref_table TO nogrant_user;

-- check non propagation for local table
GRANT UPDATE (id) ON local_table TO grant_user_0;
-- special case: mixed with distributed table:
GRANT INSERT (b, c) ON local_table, dist_table TO grant_user_1;

-- check on coordinator and workers
SELECT relname, relacl FROM pg_class
WHERE relname IN ('dist_table', 'ref_table', 'local_table') ORDER BY 1;
SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid  IN ('dist_table'::regclass, 'ref_table'::regclass, 'local_table'::regclass)
  AND attacl IS NOT NULL
ORDER BY 1, 2;
\c - - - :worker_1_port
SET search_path TO grant_on_table;
SELECT relname, relacl FROM pg_class
WHERE relname IN ('dist_table', 'ref_table', 'local_table') ORDER BY 1;
SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid  IN ('dist_table'::regclass, 'ref_table'::regclass, 'local_table'::regclass)
  AND attacl IS NOT NULL
ORDER BY 1, 2;
\c - - - :worker_2_port
SET search_path TO grant_on_table;
SELECT relname, relacl FROM pg_class
WHERE relname IN ('dist_table', 'ref_table', 'local_table') ORDER BY 1;
SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid  IN ('dist_table'::regclass, 'ref_table'::regclass, 'local_table'::regclass)
  AND attacl IS NOT NULL
ORDER BY 1, 2;
\c - - - :master_port
SET search_path TO grant_on_table;

-- revoke, for columns it's the same logic so we don't bother testing combinations
REVOKE ALL ON ref_table, dist_table, local_table from grant_user_0, grant_user_1;

-- check on coordinator and workers
SELECT relname, relacl FROM pg_class
WHERE relname IN ('dist_table', 'ref_table', 'local_table') ORDER BY 1;
SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid  IN ('dist_table'::regclass, 'ref_table'::regclass, 'local_table'::regclass)
  AND attacl IS NOT NULL
ORDER BY 1, 2;
\c - - - :worker_1_port
SET search_path TO grant_on_table;
SELECT relname, relacl FROM pg_class
WHERE relname IN ('dist_table', 'ref_table', 'local_table') ORDER BY 1;
SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid  IN ('dist_table'::regclass, 'ref_table'::regclass, 'local_table'::regclass)
  AND attacl IS NOT NULL
ORDER BY 1, 2;
\c - - - :worker_2_port
SET search_path TO grant_on_table;
SELECT relname, relacl FROM pg_class
WHERE relname IN ('dist_table', 'ref_table', 'local_table') ORDER BY 1;
SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid  IN ('dist_table'::regclass, 'ref_table'::regclass, 'local_table'::regclass)
  AND attacl IS NOT NULL
ORDER BY 1, 2;

-- and test from a worker
\c - - - :worker_1_port
SET search_path TO grant_on_table;
GRANT SELECT (a, b) ON ref_table TO grant_user_0;

-- check on coordinator and workers
\c - - - :master_port
SET search_path TO grant_on_table;
SELECT relname, relacl FROM pg_class
WHERE relname IN ('dist_table', 'ref_table', 'local_table') ORDER BY 1;
SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid  IN ('dist_table'::regclass, 'ref_table'::regclass, 'local_table'::regclass)
  AND attacl IS NOT NULL
ORDER BY 1, 2;
\c - - - :worker_1_port
SET search_path TO grant_on_table;
SELECT relname, relacl FROM pg_class
WHERE relname IN ('dist_table', 'ref_table', 'local_table') ORDER BY 1;
SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid  IN ('dist_table'::regclass, 'ref_table'::regclass, 'local_table'::regclass)
  AND attacl IS NOT NULL
ORDER BY 1, 2;
\c - - - :worker_2_port
SET search_path TO grant_on_table;
SELECT relname, relacl FROM pg_class
WHERE relname IN ('dist_table', 'ref_table', 'local_table') ORDER BY 1;
SELECT attrelid::regclass, attname, attacl FROM pg_attribute
WHERE attrelid  IN ('dist_table'::regclass, 'ref_table'::regclass, 'local_table'::regclass)
  AND attacl IS NOT NULL
ORDER BY 1, 2;
\c - - - :master_port
SET search_path TO grant_on_table;

-- cleanup
-- prevent useless messages on DROP objects.
SET client_min_messages TO ERROR;
DROP SCHEMA grant_on_table CASCADE;
DROP ROLE grant_user_0, grant_user_1, nogrant_user;
RESET client_min_messages;
RESET search_path;
