CREATE OR REPLACE FUNCTION pg_catalog.citus_database_size(oid)
 RETURNS bigint
 LANGUAGE C
 PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$citus_database_size_oid$function$;
COMMENT ON FUNCTION pg_catalog.citus_database_size(oid)
 IS 'get total disk space used by the specified database';

CREATE OR REPLACE FUNCTION pg_catalog.citus_database_size(name)
 RETURNS bigint
 LANGUAGE C
 PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$citus_database_size_name$function$;
COMMENT ON FUNCTION pg_catalog.citus_database_size(name)
 IS 'get total disk space used by the specified database';
