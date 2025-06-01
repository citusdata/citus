-- skip_qualify_public is set to true by default just for backward compatibility
DROP FUNCTION pg_catalog.shard_name(object_name regclass, shard_id bigint);
CREATE FUNCTION pg_catalog.shard_name(object_name regclass, shard_id bigint, skip_qualify_public boolean DEFAULT true)
    RETURNS text
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$shard_name$$;
COMMENT ON FUNCTION pg_catalog.shard_name(object_name regclass, shard_id bigint, skip_qualify_public boolean)
    IS 'returns schema-qualified, shard-extended identifier of object name';
