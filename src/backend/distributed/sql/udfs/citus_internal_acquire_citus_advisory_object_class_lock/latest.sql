CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_acquire_citus_advisory_object_class_lock(objectClass int, qualifiedObjectName cstring)
 RETURNS void
 LANGUAGE C
 VOLATILE
AS 'MODULE_PATHNAME', $$citus_internal_acquire_citus_advisory_object_class_lock$$;
