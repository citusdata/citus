CREATE FUNCTION pg_catalog.master_modify_multiple_shards(text)
    RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_modify_multiple_shards$$;
COMMENT ON FUNCTION master_modify_multiple_shards(text)
    IS 'push delete and update queries to shards';