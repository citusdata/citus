CREATE OR REPLACE FUNCTION citus_delete_marked_shards()
    RETURNS int
    LANGUAGE C STRICT
    AS 'citus', $$citus_delete_marked_shards$$;
COMMENT ON FUNCTION citus_delete_marked_shards()
    IS 'remove orphaned shards';