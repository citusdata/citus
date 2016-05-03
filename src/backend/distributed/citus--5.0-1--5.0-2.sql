/* citus--5.0-1--5.0-2.sql */

CREATE FUNCTION master_update_shard_statistics(shard_id bigint)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_update_shard_statistics$$;
COMMENT ON FUNCTION master_update_shard_statistics(bigint)
    IS 'updates shard statistics and returns the updated shard size';
