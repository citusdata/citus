--
-- citus_internal.database_command run given database command without transaction block restriction.

CREATE OR REPLACE FUNCTION citus_internal.database_command(command text)
 RETURNS void
 LANGUAGE C
 VOLATILE
AS 'MODULE_PATHNAME', $$citus_internal_database_command$$;
COMMENT ON FUNCTION citus_internal.database_command(text) IS
 'run a database command without transaction block restrictions';
