--
-- citus_internal_database_command creates a database according to the given command.

CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_database_command(command text)
 RETURNS void
 LANGUAGE C
 VOLATILE
AS 'MODULE_PATHNAME', $$citus_internal_database_command$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_database_command(text) IS
 'run a database command without transaction block restrictions';
