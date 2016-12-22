/* citus--6.1-4--6.1-5.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION lock_shard_resources(lock_mode int, shard_id bigint[])
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$lock_shard_resources$$;
COMMENT ON FUNCTION lock_shard_resources(lock_mode int, shard_id bigint[])
    IS 'lock shard resource to serialise non-commutative writes';

CREATE FUNCTION lock_shard_metadata(lock_mode int, shard_id bigint[])
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$lock_shard_metadata$$;
COMMENT ON FUNCTION lock_shard_metadata(lock_mode int, shard_id bigint[])
    IS 'lock shard metadata to prevent writes during metadata changes';

RESET search_path;
