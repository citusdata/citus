CREATE OR REPLACE FUNCTION citus_internal.mark_object_distributed(classId Oid, objectName text, objectId Oid, connectionUser text)
    RETURNS VOID
    LANGUAGE C
AS 'MODULE_PATHNAME', $$mark_object_distributed$$;

COMMENT ON FUNCTION citus_internal.mark_object_distributed(classId Oid, objectName text, objectId Oid, connectionUser text)
    IS 'adds an object to pg_dist_object on all nodes';
