CREATE OR REPLACE FUNCTION citus_internal.pg_database_size_local(db_name name)
 RETURNS bigint
 LANGUAGE C
 VOLATILE
AS 'MODULE_PATHNAME', $$citus_internal_pg_database_size_by_db_name$$;
COMMENT ON FUNCTION citus_internal.pg_database_size_local(name) IS
 'calculates the size of a database in bytes by its name in a multi-node cluster';

CREATE OR REPLACE FUNCTION citus_internal.pg_database_size_local(db_oid oid)
 RETURNS bigint
 LANGUAGE C
 VOLATILE
AS 'MODULE_PATHNAME', $$citus_internal_pg_database_size_by_db_oid$$;
COMMENT ON FUNCTION citus_internal.pg_database_size_local(oid) IS
 'calculates the size of a database in bytes by its oid in a multi-node cluster';
