CREATE OR REPLACE FUNCTION citus_internal.acquire_citus_advisory_object_class_lock(objectClass int, qualifiedObjectName cstring)
 RETURNS void
 LANGUAGE C
 VOLATILE
AS 'MODULE_PATHNAME', $$citus_internal_acquire_citus_advisory_object_class_lock$$;
