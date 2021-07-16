CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_add_object_metadata(
							classid oid, objid oid, objsubid oid,
							distribution_argument_index int, colocationid int)
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_add_object_metadata(oid,oid,oid,int,int) IS
    'Inserts into pg_dist_object with user checks';
