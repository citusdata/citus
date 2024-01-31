CREATE FUNCTION pg_catalog.citus_unmark_object_distributed(classid oid, objid oid, objsubid int,checkobjectexistence boolean)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_unmark_object_distributed$$;
COMMENT ON FUNCTION pg_catalog.citus_unmark_object_distributed(classid oid, objid oid, objsubid int,checkobjectexistence boolean)
    IS 'remove an object address from citus.pg_dist_object once the object has been deleted.
    This method is overload of citus_unmark_object_distributed with additionl parameter checkobjectexistence
    In regular scenario, if object exists on database, pg_dist_object record is not being deleted
    However, when we send a command which deletes pg_dist_object,we need to get oid of the object,
    so we need to delete the record before actual drop operation .';
