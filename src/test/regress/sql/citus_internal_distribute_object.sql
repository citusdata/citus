--
-- CITUS_INTERNAL_DISTRIBUTE_OBJECT
--
-- Tests for the superuser-only citus_internal.distribute_object() repair UDF
-- that (re)creates a single object on all worker nodes and records it in
-- pg_dist_object on the coordinator and all worker nodes in an idempotent
-- manner. Dependencies of the object are deliberately not considered.
--
-- For each supported, DDL-generating object type we test:
--   i)   force_recreate := false on a missing object -> created on workers
--   ii-a) force_recreate := true when it already exists -> no error
--   ii-b) force_recreate := true after coordinator-only drift -> drift synced
-- Table (OCLASS_CLASS) and database (OCLASS_DATABASE) are intentionally excluded.
SET client_min_messages TO WARNING;
CREATE SCHEMA distobj;
SET search_path TO distobj, public;
-- ---------------------------------------------------------------------------
-- FUNCTION (OCLASS_PROC)
-- ---------------------------------------------------------------------------
SET citus.enable_metadata_sync TO OFF;
CREATE FUNCTION distobj.fn(int) RETURNS int LANGUAGE sql IMMUTABLE AS $fn$ SELECT $1 $fn$;
RESET citus.enable_metadata_sync;
SELECT bool_and(result::int = 0) AS fn_missing FROM run_command_on_workers($$SELECT count(*) FROM pg_proc WHERE proname = 'fn' AND pronamespace = 'distobj'::regnamespace$$);
SELECT citus_internal.distribute_object('pg_proc'::regclass::oid, 'distobj.fn(int)'::regprocedure::oid);
SELECT bool_and(result::int = 1) AS fn_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_proc WHERE proname = 'fn' AND pronamespace = 'distobj'::regnamespace$$);
SELECT bool_and(result::int = 1) AS fn_distributed FROM run_command_on_workers($$SELECT count(*) FROM pg_dist_object WHERE classid = 'pg_proc'::regclass AND objid = 'distobj.fn(int)'::regprocedure$$);
-- ii-a: force on an object that already exists everywhere is a no-op.
SELECT citus_internal.distribute_object('pg_proc'::regclass::oid, 'distobj.fn(int)'::regprocedure::oid, force_recreate := true);
-- ii-b: drift the function on the coordinator, then force-sync to workers.
SET citus.enable_metadata_sync TO OFF;
ALTER FUNCTION distobj.fn(int) STRICT;
RESET citus.enable_metadata_sync;
SELECT citus_internal.distribute_object('pg_proc'::regclass::oid, 'distobj.fn(int)'::regprocedure::oid, force_recreate := true);
SELECT bool_and(result = 't') AS fn_strict_synced FROM run_command_on_workers($$SELECT proisstrict FROM pg_proc WHERE proname = 'fn' AND pronamespace = 'distobj'::regnamespace$$);
-- ---------------------------------------------------------------------------
-- SCHEMA (OCLASS_SCHEMA)
-- ---------------------------------------------------------------------------
SET citus.enable_metadata_sync TO OFF;
CREATE SCHEMA distobj_s;
RESET citus.enable_metadata_sync;
SELECT citus_internal.distribute_object('pg_namespace'::regclass::oid, 'distobj_s'::regnamespace::oid);
SELECT bool_and(result::int = 1) AS schema_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_namespace WHERE nspname = 'distobj_s'$$);
SELECT citus_internal.distribute_object('pg_namespace'::regclass::oid, 'distobj_s'::regnamespace::oid, force_recreate := true);
-- ---------------------------------------------------------------------------
-- COLLATION (OCLASS_COLLATION)
-- ---------------------------------------------------------------------------
SET citus.enable_metadata_sync TO OFF;
CREATE COLLATION distobj.coll (locale = 'C');
RESET citus.enable_metadata_sync;
SELECT citus_internal.distribute_object('pg_collation'::regclass::oid, 'distobj.coll'::regcollation::oid);
SELECT bool_and(result::int = 1) AS coll_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_collation WHERE collname = 'coll'$$);
SELECT citus_internal.distribute_object('pg_collation'::regclass::oid, 'distobj.coll'::regcollation::oid, force_recreate := true);
-- ---------------------------------------------------------------------------
-- TYPE (OCLASS_TYPE)
-- ---------------------------------------------------------------------------
SET citus.enable_metadata_sync TO OFF;
CREATE TYPE distobj.ty AS (a int, b text);
RESET citus.enable_metadata_sync;
SELECT citus_internal.distribute_object('pg_type'::regclass::oid, 'distobj.ty'::regtype::oid);
SELECT bool_and(result::int = 1) AS type_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_type WHERE typname = 'ty'$$);
SELECT citus_internal.distribute_object('pg_type'::regclass::oid, 'distobj.ty'::regtype::oid, force_recreate := true);
-- ---------------------------------------------------------------------------
-- TEXT SEARCH DICTIONARY / CONFIGURATION (OCLASS_TSDICT / OCLASS_TSCONFIG)
-- ---------------------------------------------------------------------------
SET citus.enable_metadata_sync TO OFF;
CREATE TEXT SEARCH DICTIONARY distobj.dict (template = simple);
CREATE TEXT SEARCH CONFIGURATION distobj.cfg (parser = default);
RESET citus.enable_metadata_sync;
SELECT citus_internal.distribute_object('pg_ts_dict'::regclass::oid, 'distobj.dict'::regdictionary::oid);
SELECT bool_and(result::int = 1) AS dict_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_ts_dict WHERE dictname = 'dict'$$);
SELECT citus_internal.distribute_object('pg_ts_config'::regclass::oid, 'distobj.cfg'::regconfig::oid);
SELECT bool_and(result::int = 1) AS cfg_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_ts_config WHERE cfgname = 'cfg'$$);
SELECT citus_internal.distribute_object('pg_ts_config'::regclass::oid, 'distobj.cfg'::regconfig::oid, force_recreate := true);
-- ---------------------------------------------------------------------------
-- SEQUENCE (OCLASS_CLASS / RELKIND_SEQUENCE)
-- ---------------------------------------------------------------------------
SET citus.enable_metadata_sync TO OFF;
CREATE SEQUENCE distobj.seq;
RESET citus.enable_metadata_sync;
SELECT citus_internal.distribute_object('pg_class'::regclass::oid, 'distobj.seq'::regclass::oid);
SELECT bool_and(result::int = 1) AS seq_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_class WHERE relname = 'seq' AND relkind = 'S'$$);
SELECT citus_internal.distribute_object('pg_class'::regclass::oid, 'distobj.seq'::regclass::oid, force_recreate := true);
-- ---------------------------------------------------------------------------
-- VIEW (OCLASS_CLASS / RELKIND_VIEW)
-- ---------------------------------------------------------------------------
SET citus.enable_metadata_sync TO OFF;
CREATE VIEW distobj.vw AS SELECT 1 AS a;
RESET citus.enable_metadata_sync;
SELECT citus_internal.distribute_object('pg_class'::regclass::oid, 'distobj.vw'::regclass::oid);
SELECT bool_and(result::int = 1) AS view_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_class WHERE relname = 'vw' AND relkind = 'v'$$);
SELECT citus_internal.distribute_object('pg_class'::regclass::oid, 'distobj.vw'::regclass::oid, force_recreate := true);
-- ---------------------------------------------------------------------------
-- PUBLICATION (OCLASS_PUBLICATION)
-- ---------------------------------------------------------------------------
SET citus.enable_metadata_sync TO OFF;
CREATE PUBLICATION distobj_pub;
RESET citus.enable_metadata_sync;
SELECT citus_internal.distribute_object('pg_publication'::regclass::oid, oid) FROM pg_publication WHERE pubname = 'distobj_pub';
SELECT bool_and(result::int = 1) AS pub_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_publication WHERE pubname = 'distobj_pub'$$);
SELECT citus_internal.distribute_object('pg_publication'::regclass::oid, oid, force_recreate := true) FROM pg_publication WHERE pubname = 'distobj_pub';
-- ---------------------------------------------------------------------------
-- ROLE (OCLASS_ROLE) -- the motivating case: a role created before the
-- extension that later received ALTERs that never reached all nodes.
-- ---------------------------------------------------------------------------
SET citus.enable_metadata_sync TO OFF;
CREATE ROLE distobj_role CONNECTION LIMIT 3;
RESET citus.enable_metadata_sync;
SELECT citus_internal.distribute_object('pg_authid'::regclass::oid, 'distobj_role'::regrole::oid);
SELECT bool_and(result::int = 1) AS role_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_roles WHERE rolname = 'distobj_role'$$);
SELECT bool_and(result = '3') AS role_connlimit_synced FROM run_command_on_workers($$SELECT rolconnlimit FROM pg_authid WHERE rolname = 'distobj_role'$$);
-- ii-b: alter the role on the coordinator only, then force-sync to workers.
SET citus.enable_metadata_sync TO OFF;
ALTER ROLE distobj_role CONNECTION LIMIT 7;
RESET citus.enable_metadata_sync;
SELECT citus_internal.distribute_object('pg_authid'::regclass::oid, 'distobj_role'::regrole::oid, force_recreate := true);
SELECT bool_and(result = '7') AS role_connlimit_resynced FROM run_command_on_workers($$SELECT rolconnlimit FROM pg_authid WHERE rolname = 'distobj_role'$$);
-- ---------------------------------------------------------------------------
-- EXTENSION (OCLASS_EXTENSION)
-- ---------------------------------------------------------------------------
SET citus.enable_metadata_sync TO OFF;
CREATE EXTENSION seg;
RESET citus.enable_metadata_sync;
SELECT citus_internal.distribute_object('pg_extension'::regclass::oid, oid) FROM pg_extension WHERE extname = 'seg';
SELECT bool_and(result::int = 1) AS ext_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'seg'$$);
SELECT citus_internal.distribute_object('pg_extension'::regclass::oid, oid, force_recreate := true) FROM pg_extension WHERE extname = 'seg';
DROP EXTENSION seg;
-- ---------------------------------------------------------------------------
-- FOREIGN SERVER (OCLASS_FOREIGN_SERVER)
-- ---------------------------------------------------------------------------
SET citus.enable_metadata_sync TO OFF;
CREATE EXTENSION postgres_fdw;
CREATE SERVER distobj_srv FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost');
RESET citus.enable_metadata_sync;
-- The wrapper must exist on workers for the server DDL to succeed.
SELECT citus_internal.distribute_object('pg_extension'::regclass::oid, oid) FROM pg_extension WHERE extname = 'postgres_fdw';
SELECT citus_internal.distribute_object('pg_foreign_server'::regclass::oid, oid) FROM pg_foreign_server WHERE srvname = 'distobj_srv';
SELECT bool_and(result::int = 1) AS srv_exists FROM run_command_on_workers($$SELECT count(*) FROM pg_foreign_server WHERE srvname = 'distobj_srv'$$);
SELECT citus_internal.distribute_object('pg_foreign_server'::regclass::oid, oid, force_recreate := true) FROM pg_foreign_server WHERE srvname = 'distobj_srv';
DROP SERVER distobj_srv;
DROP EXTENSION postgres_fdw;
DROP ROLE distobj_role;
DROP PUBLICATION distobj_pub;
-- It errors for an object that does not exist on the local node.
SELECT citus_internal.distribute_object('pg_proc'::regclass::oid, 0);
-- Cleanup.
SET client_min_messages TO ERROR;
DROP SCHEMA distobj, distobj_s CASCADE;
