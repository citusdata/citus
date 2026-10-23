--
-- METADATA_REPLAY_SECURITY
--
-- Metadata replay recreates distributed objects through a superuser connection.
-- Commands that can carry tenant-authored expressions are prefixed with the
-- owner of the object, and the receiving node runs that statement as the owner
-- under a restricted security context. These tests use the same marked command
-- shape that replay sends.
--

CREATE SCHEMA metadata_replay_security;
SET search_path TO metadata_replay_security;
SET citus.enable_ddl_propagation TO off;

CREATE ROLE metadata_replay_owner;
ALTER SCHEMA metadata_replay_security OWNER TO metadata_replay_owner;

-- Guards that only pass when the expression is evaluated as the object owner.
CREATE DOMAIN owner_only AS text
CHECK (current_user = 'metadata_replay_owner');

-- A tenant expression must never observe the rights of the replaying superuser.
-- pg_read_file is restricted, so the owner must not be able to execute it.
CREATE DOMAIN no_superuser_rights AS text
CHECK (NOT has_function_privilege(current_user, 'pg_read_file(text)', 'EXECUTE'));

CREATE FUNCTION assert_owner()
RETURNS text
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
	IF current_user <> 'metadata_replay_owner' THEN
		RAISE EXCEPTION 'generated column evaluated as %', current_user;
	END IF;

	RETURN 'ok';
END;
$$;

CREATE FUNCTION set_role_rejected()
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
	EXECUTE format('SET ROLE %I', current_user);
	RETURN false;
EXCEPTION WHEN insufficient_privilege THEN
	RETURN true;
END;
$$;

CREATE DOMAIN restricted_context AS text
CHECK (set_role_rejected());

CREATE FUNCTION raise_replay_error()
RETURNS boolean
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
	RAISE EXCEPTION 'controlled replay error';
END;
$$;

CREATE DOMAIN failing_domain AS text CHECK (raise_replay_error());

ALTER DOMAIN owner_only OWNER TO metadata_replay_owner;
ALTER DOMAIN no_superuser_rights OWNER TO metadata_replay_owner;
ALTER DOMAIN restricted_context OWNER TO metadata_replay_owner;
ALTER DOMAIN failing_domain OWNER TO metadata_replay_owner;
ALTER FUNCTION assert_owner() OWNER TO metadata_replay_owner;
ALTER FUNCTION set_role_rejected() OWNER TO metadata_replay_owner;
ALTER FUNCTION raise_replay_error() OWNER TO metadata_replay_owner;

-- Without the marker the expressions run with the rights of the replaying
-- session, which is what the guards are there to detect.
CREATE TABLE unmarked_owner (a owner_only[] DEFAULT '{x}');
CREATE TABLE unmarked_rights (a no_superuser_rights[] DEFAULT '{x}');
CREATE TABLE unmarked_generated (a int, b text GENERATED ALWAYS AS (assert_owner()) STORED);

-- A marked statement runs as the object owner instead. Replay sends the marker
-- and the command together, so they share a transaction.
BEGIN;
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', 'metadata_replay_owner', true); END$marker$;
CREATE TABLE marked_owner (a owner_only[] DEFAULT '{x}');
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', '', true); END$marker$;
COMMIT;

BEGIN;
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', 'metadata_replay_owner', true); END$marker$;
CREATE TABLE marked_rights (a no_superuser_rights[] DEFAULT '{x}');
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', '', true); END$marker$;
COMMIT;

BEGIN;
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', 'metadata_replay_owner', true); END$marker$;
CREATE TABLE marked_generated (a int, b text GENERATED ALWAYS AS (assert_owner()) STORED);
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', '', true); END$marker$;
COMMIT;

-- The restricted context blocks changing role from inside a replayed expression.
BEGIN;
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', 'metadata_replay_owner', true); END$marker$;
CREATE TABLE marked_restricted (a restricted_context[] DEFAULT '{x}');
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', '', true); END$marker$;
COMMIT;

-- An expression whose plan was already built and cached must still be evaluated
-- as the owner, since the boundary changes identity rather than the cache.
CREATE FUNCTION identity_sql() RETURNS text LANGUAGE sql STABLE
AS $$ SELECT current_user::text $$;
CREATE DOMAIN warm_owner_only AS text
CHECK (identity_sql() = 'metadata_replay_owner');
ALTER FUNCTION identity_sql() OWNER TO metadata_replay_owner;
ALTER DOMAIN warm_owner_only OWNER TO metadata_replay_owner;

-- prime the constraint outside the boundary, which also proves it is evaluated
CREATE TABLE unmarked_warm (a warm_owner_only[] DEFAULT '{x}');

BEGIN;
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', 'metadata_replay_owner', true); END$marker$;
CREATE TABLE marked_warm (a warm_owner_only[] DEFAULT '{x}');
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', '', true); END$marker$;
COMMIT;

-- Identity, security context and the marker are restored after the statement.
SELECT current_user = session_user AS user_restored,
	current_setting('citus.metadata_replay_owner') = '' AS marker_consumed;

-- The marker covers a single statement, so a following command is not protected.
BEGIN;
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', 'metadata_replay_owner', true); END$marker$;
CREATE TABLE marked_first (a owner_only[] DEFAULT '{x}');
CREATE TABLE marked_second (a owner_only[] DEFAULT '{x}');
ROLLBACK;

-- An error inside a replayed expression still restores the replay session.
BEGIN;
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', 'metadata_replay_owner', true); END$marker$;
CREATE TABLE marked_failing (a failing_domain[] DEFAULT '{x}');
ROLLBACK;
SELECT current_user = session_user AS user_restored_after_error,
	current_setting('citus.metadata_replay_owner') = '' AS marker_cleared_after_error;

-- Settings changed while evaluating an expression are restored on success and
-- error, not just when the surrounding transaction eventually ends.
SELECT current_setting('application_name') AS saved_application_name \gset
CREATE FUNCTION change_replay_setting(value text)
RETURNS boolean LANGUAGE plpgsql VOLATILE AS $$
BEGIN
	PERFORM set_config('application_name', 'inside replay', false);
	IF value = 'fail' THEN
		RAISE EXCEPTION 'controlled settings error';
	END IF;
	RETURN current_user = 'metadata_replay_owner';
END;
$$;
ALTER FUNCTION change_replay_setting(text) OWNER TO metadata_replay_owner;
CREATE DOMAIN settings_domain AS text CHECK (change_replay_setting(VALUE));
ALTER DOMAIN settings_domain OWNER TO metadata_replay_owner;
BEGIN;
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', 'metadata_replay_owner', true); END$marker$;
CREATE TABLE marked_settings (a settings_domain[] DEFAULT '{ok}');
SELECT current_setting('application_name') = :'saved_application_name' AS settings_restored;
SAVEPOINT before_replay_error;
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', 'metadata_replay_owner', true); END$marker$;
CREATE TABLE marked_settings_error (a settings_domain[] DEFAULT '{fail}');
ROLLBACK TO before_replay_error;
SELECT current_setting('application_name') = :'saved_application_name' AS settings_restored_after_error,
	current_user = session_user AS user_restored_after_savepoint,
	current_setting('citus.metadata_replay_owner') = '' AS marker_cleared_after_savepoint;
COMMIT;

-- A missing owner fails the replayed statement rather than running it privileged.
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', 'no_such_replay_role', true); END$marker$;

-- Only a privileged replay session may set the marker.
SET SESSION AUTHORIZATION metadata_replay_owner;
SELECT set_config('citus.metadata_replay_owner', 'metadata_replay_owner', true);
SET citus.metadata_replay_protection TO off;
RESET SESSION AUTHORIZATION;

-- A SECURITY DEFINER function owned by the tenant runs with the tenant's rights
-- while the session user is still privileged. It must not be able to raise
-- itself by naming a superuser in the marker.
CREATE FUNCTION metadata_replay_security.lift_marker() RETURNS void
LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  PERFORM set_config('citus.metadata_replay_owner', session_user, true);
END $$;
ALTER FUNCTION metadata_replay_security.lift_marker() OWNER TO metadata_replay_owner;
SELECT metadata_replay_security.lift_marker();

-- Running as the owner means the owner must hold the rights the statement needs.
-- Replay fails rather than falling back to the rights of the replaying session.
REVOKE CREATE ON SCHEMA metadata_replay_security FROM metadata_replay_owner;
BEGIN;
DO $marker$BEGIN PERFORM set_config('citus.metadata_replay_owner', 'metadata_replay_owner', true); END$marker$;
CREATE TABLE metadata_replay_security.marked_without_rights (a owner_only[] DEFAULT '{x}');
ROLLBACK;
GRANT CREATE ON SCHEMA metadata_replay_security TO metadata_replay_owner;

-- A node that predates this protection does not know the marker. The emitted
-- command reports that instead of evaluating with the rights of the replaying
-- superuser, and the mode decides whether that is fatal.
SHOW citus.metadata_replay_protection;

DO $marker$BEGIN IF current_setting('citus.metadata_replay_owner_from_a_newer_release', true) IS NULL THEN RAISE WARNING 'this node evaluates replayed expressions with the rights of the replaying superuser' USING HINT = 'Upgrade Citus on this node to restore metadata replay protection, or set citus.metadata_replay_protection to off to accept it.'; ELSE PERFORM set_config('citus.metadata_replay_owner_from_a_newer_release', 'metadata_replay_owner', true); END IF; END$marker$;

DO $marker$BEGIN IF current_setting('citus.metadata_replay_owner_from_a_newer_release', true) IS NULL THEN RAISE EXCEPTION 'this node evaluates replayed expressions with the rights of the replaying superuser' USING HINT = 'Upgrade Citus on this node to restore metadata replay protection, or set citus.metadata_replay_protection to off to accept it.'; ELSE PERFORM set_config('citus.metadata_replay_owner_from_a_newer_release', 'metadata_replay_owner', true); END IF; END$marker$;

SET citus.metadata_replay_protection TO 'require';
SET citus.metadata_replay_protection TO 'off';
RESET citus.metadata_replay_protection;

-- The defaults that were attached remain usable afterwards.
SET ROLE metadata_replay_owner;
INSERT INTO marked_owner DEFAULT VALUES;
SELECT a::text FROM marked_owner;
RESET ROLE;

-- A tenant distributing its own object sends the dependencies it needs through
-- a connection Citus opens as the extension owner, so those creates are marked
-- too. This domain refuses to be created with the rights of that connection.
SET citus.enable_ddl_propagation TO on;
SET client_min_messages TO WARNING;
CREATE ROLE replay_dependency_owner LOGIN;
CREATE SCHEMA replay_dependency AUTHORIZATION replay_dependency_owner;
RESET client_min_messages;

SET ROLE replay_dependency_owner;
SET citus.enable_ddl_propagation TO off;
CREATE DOMAIN replay_dependency.superuser_free AS text
CHECK (NOT pg_has_role(current_user, 'pg_execute_server_program', 'MEMBER'));
-- creating this domain evaluates its default, which runs the check above
CREATE DOMAIN replay_dependency.guarded AS replay_dependency.superuser_free[]
DEFAULT '{x}';
SET citus.enable_ddl_propagation TO on;

-- A worker that already holds a different definition has to resolve the
-- collision before applying the create, which must not cost the create its
-- owner.
SELECT bool_and(success)
FROM run_command_on_workers($$
	SET citus.enable_ddl_propagation TO off;
	CREATE DOMAIN replay_dependency.superuser_free AS text
	CHECK (NOT pg_has_role(current_user, 'pg_execute_server_program', 'MEMBER'));
	CREATE DOMAIN replay_dependency.guarded AS replay_dependency.superuser_free[]
	DEFAULT '{y}';
$$);

CREATE TABLE replay_dependency.tenant_table (id bigint, value replay_dependency.guarded);
SELECT create_distributed_table('replay_dependency.tenant_table', 'id');

-- the worker copy was replaced by the coordinator's, as the owner
SELECT bool_and(result LIKE '%{x}%')
FROM run_command_on_workers($$
	SELECT typdefault FROM pg_type
	WHERE typname = 'guarded' AND typnamespace = 'replay_dependency'::regnamespace
$$);
RESET ROLE;

-- The repair entry point also replays through an extension-owner connection.
SET ROLE replay_dependency_owner;
SET citus.enable_ddl_propagation TO off;
CREATE DOMAIN replay_dependency.repair_guarded AS replay_dependency.superuser_free[]
DEFAULT '{x}';
SET citus.enable_ddl_propagation TO on;
RESET ROLE;
SELECT citus_internal.distribute_object('pg_type'::regclass,
	'replay_dependency.repair_guarded'::regtype);
SELECT bool_and(success AND result = 'replay_dependency_owner')
FROM run_command_on_workers($$
	SELECT pg_get_userbyid(typowner) FROM pg_type
	WHERE oid = 'replay_dependency.repair_guarded'::regtype
$$);

-- Node activation must recreate the missing definition with the same protection.
SET ROLE replay_dependency_owner;
CREATE TABLE replay_dependency.partition_parent (value replay_dependency.superuser_free[])
PARTITION BY RANGE (value);
CREATE TABLE replay_dependency.partition_child
PARTITION OF replay_dependency.partition_parent FOR VALUES FROM ('{a}') TO ('{z}');
SELECT citus_add_local_table_to_metadata('replay_dependency.partition_parent');
RESET ROLE;
SELECT bool_and(success)
FROM run_command_on_workers($$
	SET citus.enable_ddl_propagation TO off;
	DROP DOMAIN replay_dependency.repair_guarded;
$$);
SET citus.enable_ddl_propagation TO off;
SET client_min_messages TO WARNING;
DROP SCHEMA metadata_replay_security CASCADE;
DROP ROLE metadata_replay_owner;
SET citus.enable_ddl_propagation TO on;
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);
SELECT 1 FROM citus_activate_node('localhost', :worker_2_port);
SELECT bool_and(success AND result = '1')
FROM run_command_on_workers($$
	SELECT count(*) FROM pg_inherits
	WHERE inhparent = 'replay_dependency.partition_parent'::regclass
$$);
SELECT bool_and(success AND result = 'replay_dependency_owner')
FROM run_command_on_workers($$
	SELECT pg_get_userbyid(typowner) FROM pg_type
	WHERE oid = 'replay_dependency.repair_guarded'::regtype
$$);

-- the schema was created with propagation on, so it has to be dropped with
-- propagation on as well, or the worker copies outlive the test
SET citus.enable_ddl_propagation TO on;
SET client_min_messages TO WARNING;
DROP SCHEMA replay_dependency CASCADE;
DROP ROLE replay_dependency_owner;
