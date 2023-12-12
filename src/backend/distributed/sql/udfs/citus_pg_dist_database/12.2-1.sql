 CREATE OR REPLACE FUNCTION pg_catalog.pg_dist_database_size(db_name name)
 RETURNS bigint
 LANGUAGE C
 VOLATILE
AS 'MODULE_PATHNAME', $$citus_internal_database_size$$;
COMMENT ON FUNCTION pg_catalog.pg_dist_database_size(name) IS
 'calculates the size of a database in bytes by its name in a multi-node cluster';
