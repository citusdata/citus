--
-- citus_internal_database_command run given database command without transaction block restriction.

CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_database_command(command text)
 RETURNS void
 LANGUAGE C
 VOLATILE
AS 'MODULE_PATHNAME', $$citus_internal_database_command$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_database_command(text) IS
 'run a database command without transaction block restrictions';

CREATE OR REPLACE FUNCTION pg_catalog.pg_database_size_local(db_name name)
 RETURNS bigint
 LANGUAGE C
 VOLATILE
AS 'MODULE_PATHNAME', $$citus_pg_database_size_by_db_name$$;
COMMENT ON FUNCTION pg_catalog.pg_database_size_local(name) IS
 'calculates the size of a database in bytes by its name in a multi-node cluster';

 CREATE OR REPLACE FUNCTION pg_catalog.pg_database_size_local(db_oid oid)
 RETURNS bigint
 LANGUAGE C
 VOLATILE
AS 'MODULE_PATHNAME', $$citus_pg_database_size_by_db_oid$$;
COMMENT ON FUNCTION pg_catalog.pg_database_size_local(oid) IS
 'calculates the size of a database in bytes by its oid in a multi-node cluster';
