-- citus--9.3-1--9.2-4
-- this is an unusual upgrade path, we are doing it because
-- we have accidentally tagged master branch with v9.2-3
-- however master branch was already bumped to v9.3-1
-- with this file, we are undoing the catalog changes that
-- have happened between 9.2-2 to 9.3-1, and making 9.2-4
-- as the release that we can

-- undo the changes for citus_extradata_container that happened on citus 9.3
DROP FUNCTION IF EXISTS pg_catalog.citus_extradata_container(INTERNAL);
CREATE FUNCTION pg_catalog.citus_extradata_container(INTERNAL)
    RETURNS void
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$citus_extradata_container$$;
COMMENT ON FUNCTION pg_catalog.citus_extradata_container(INTERNAL)
    IS 'placeholder function to store additional data in postgres node trees';

DROP FUNCTION IF EXISTS pg_catalog.update_distributed_table_colocation(regclass, text);
