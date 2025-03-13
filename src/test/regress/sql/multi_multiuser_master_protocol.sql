--
-- MULTI_MULTIUSER_MASTER_PROTOCOL
--

-- Test multi_multiuser_master_protocol has an alternative output file because
-- PG17's support for the MAINTAIN privilege:
-- https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=ecb0fd337
-- means that calls of master_get_table_ddl_events() can show MAINTAIN and the
-- pg_class.relacl column may have 'm' for MAINTAIN

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 109079;

-- Tests that check the metadata returned by the master node. At the
-- same time ensure that any user, not just a superuser, can call
-- these. Note that, for now at least, any user can call these. That's
-- OK-ish, since the schema is visible from the catalogs anyway, and
-- exhausting shardids doesn't seem like a super viable attack path.
SET ROLE no_access;

SELECT * FROM master_get_table_ddl_events('lineitem') order by 1;

SELECT * FROM master_get_new_shardid();

SELECT * FROM master_get_active_worker_nodes();

RESET ROLE;

-- ensure GRANT/REVOKE's do something sane for creating shards of
CREATE TABLE checkperm(key int);
SELECT create_distributed_table('checkperm', 'key', 'append');
SELECT * FROM master_get_table_ddl_events('checkperm');

REVOKE ALL ON checkperm FROM PUBLIC;
SELECT * FROM master_get_table_ddl_events('checkperm');

GRANT SELECT ON checkperm TO read_access;
GRANT ALL ON checkperm TO full_access;
SELECT * FROM master_get_table_ddl_events('checkperm');

REVOKE ALL ON checkperm FROM read_access;
GRANT SELECT ON checkperm TO PUBLIC;
SELECT * FROM master_get_table_ddl_events('checkperm');

GRANT ALL ON checkperm TO full_access WITH GRANT OPTION;
SELECT * FROM master_get_table_ddl_events('checkperm');

-- create table as superuser/postgres
CREATE TABLE trivial_postgres (id int);
SELECT create_distributed_table('trivial_postgres', 'id', 'append');
GRANT ALL ON trivial_postgres TO full_access;

GRANT CREATE ON SCHEMA public TO full_access;

SET ROLE full_access;
CREATE TABLE trivial_full_access (id int);
SELECT create_distributed_table('trivial_full_access', 'id', 'append');
RESET ROLE;

SELECT relname, rolname, relacl FROM pg_class JOIN pg_roles ON (pg_roles.oid = pg_class.relowner) WHERE relname LIKE 'trivial%' ORDER BY relname;

SET citus.shard_replication_factor = 2; -- on all workers...

-- create shards as each user, verify ownership

SELECT master_create_empty_shard('trivial_postgres');
SELECT master_create_empty_shard('trivial_full_access');
SET ROLE full_access;
SELECT master_create_empty_shard('trivial_postgres');
SELECT master_create_empty_shard('trivial_full_access');
RESET ROLE;

SET ROLE full_access;
SELECT master_create_empty_shard('trivial_postgres');
SELECT master_create_empty_shard('trivial_full_access');
RESET ROLE;

\c - - - :worker_1_port
SELECT relname, rolname, relacl FROM pg_class JOIN pg_roles ON (pg_roles.oid = pg_class.relowner) WHERE relname LIKE 'trivial%' ORDER BY relname;
\c - - - :worker_2_port
SELECT relname, rolname, relacl FROM pg_class JOIN pg_roles ON (pg_roles.oid = pg_class.relowner) WHERE relname LIKE 'trivial%' ORDER BY relname;
\c - - - :master_port

-- ensure COPY into append tables works
CREATE TABLE stage_postgres(id) AS SELECT 2;
GRANT ALL ON stage_postgres TO full_access;
SET ROLE full_access;
CREATE TABLE stage_full_access(id) AS SELECT 1;
RESET ROLE;

SELECT master_create_empty_shard('trivial_postgres') AS shardid \gset
COPY trivial_postgres FROM STDIN WITH (append_to_shard :shardid);
1
2
\.

SELECT master_create_empty_shard('trivial_full_access') AS shardid \gset
COPY trivial_full_access FROM STDIN WITH (append_to_shard :shardid);
1
2
\.

SET ROLE full_access;

SELECT master_create_empty_shard('trivial_postgres') AS shardid \gset
COPY trivial_postgres FROM STDIN WITH (append_to_shard :shardid);
1
2
\.

SELECT master_create_empty_shard('trivial_full_access') AS shardid \gset
COPY trivial_full_access FROM STDIN WITH (append_to_shard :shardid);
1
2
\.
RESET ROLE;

SELECT * FROM trivial_postgres ORDER BY id;
SELECT * FROM trivial_full_access ORDER BY id;
SET ROLE full_access;
SELECT * FROM trivial_postgres ORDER BY id;
SELECT * FROM trivial_full_access ORDER BY id;
RESET ROLE;

-- verify column level grants are not supported
GRANT UPDATE (id) ON trivial_postgres TO read_access;

DROP TABLE trivial_full_access;
DROP TABLE trivial_postgres;
DROP TABLE stage_full_access;
DROP TABLE stage_postgres;

-- test GRANT/REVOKE on all tables in schema
CREATE SCHEMA multiuser_schema;
CREATE TABLE multiuser_schema.hash_table(a int, b int);
CREATE TABLE multiuser_schema.reference_table(a int, b int);

SET citus.shard_replication_factor TO 1;
SELECT create_distributed_table('multiuser_schema.hash_table', 'a', colocate_with => 'none');


-- usage right must be granted to user
GRANT USAGE ON SCHEMA multiuser_schema TO read_access;

-- verify test user (read_access) does not have select privilege on both tables
SELECT * FROM run_command_on_placements('multiuser_schema.hash_table', $$ select has_table_privilege('read_access', '%s', 'select') $$)
ORDER BY nodename, nodeport, shardid;

-- grant select
GRANT SELECT ON ALL TABLES IN SCHEMA multiuser_schema TO read_access;

-- verify select is granted
SELECT * FROM run_command_on_placements('multiuser_schema.hash_table', $$ select has_table_privilege('read_access', '%s', 'select') $$)
ORDER BY nodename, nodeport, shardid;

-- distribute the second table
SELECT create_reference_table('multiuser_schema.reference_table');

-- verify select is also granted
SELECT * FROM run_command_on_placements('multiuser_schema.reference_table', $$ select has_table_privilege('read_access', '%s', 'select') $$)
ORDER BY nodename, nodeport, shardid;

-- create another table in the schema, verify select is not granted
CREATE TABLE multiuser_schema.another_table(a int, b int);
SELECT create_distributed_table('multiuser_schema.another_table', 'a', colocate_with => 'none');

SELECT * FROM run_command_on_placements('multiuser_schema.another_table', $$ select has_table_privilege('read_access', '%s', 'select') $$)
ORDER BY nodename, nodeport, shardid;

-- grant select again, verify it is granted
GRANT SELECT ON ALL TABLES IN SCHEMA multiuser_schema TO read_access;
SELECT * FROM run_command_on_placements('multiuser_schema.another_table', $$ select has_table_privilege('read_access', '%s', 'select') $$)
ORDER BY nodename, nodeport, shardid;

-- verify isolate tenant carries grants
SELECT isolate_tenant_to_new_shard('multiuser_schema.hash_table', 5, shard_transfer_mode => 'block_writes');
SELECT * FROM run_command_on_placements('multiuser_schema.hash_table', $$ select has_table_privilege('read_access', '%s', 'select') $$)
ORDER BY nodename, nodeport, shardid;

-- revoke select
REVOKE SELECT ON ALL TABLES IN SCHEMA multiuser_schema FROM read_access;
SELECT * FROM run_command_on_placements('multiuser_schema.hash_table', $$ select has_table_privilege('read_access', '%s', 'select') $$)
ORDER BY nodename, nodeport, shardid;

-- test multi-schema grants
CREATE SCHEMA multiuser_second_schema;
CREATE TABLE multiuser_second_schema.hash_table(a int, b int);
SELECT create_distributed_table('multiuser_second_schema.hash_table', 'a');

GRANT ALL ON ALL TABLES IN SCHEMA multiuser_schema, multiuser_second_schema TO read_access;

SELECT * FROM run_command_on_placements('multiuser_schema.hash_table', $$ select has_table_privilege('read_access', '%s', 'select') $$)
ORDER BY nodename, nodeport, shardid;
SELECT * FROM run_command_on_placements('multiuser_second_schema.hash_table', $$ select has_table_privilege('read_access', '%s', 'select') $$)
ORDER BY nodename, nodeport, shardid;

-- revoke from multiple schemas, verify result
REVOKE SELECT ON ALL TABLES IN SCHEMA multiuser_schema, multiuser_second_schema FROM read_access;

SELECT * FROM run_command_on_placements('multiuser_schema.hash_table', $$ select has_table_privilege('read_access', '%s', 'select') $$)
ORDER BY nodename, nodeport, shardid;
SELECT * FROM run_command_on_placements('multiuser_second_schema.hash_table', $$ select has_table_privilege('read_access', '%s', 'select') $$)
ORDER BY nodename, nodeport, shardid;

DROP SCHEMA multiuser_schema CASCADE;
DROP SCHEMA multiuser_second_schema CASCADE;

