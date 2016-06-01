CREATE AGGREGATE pg_catalog.jsonb_concat_agg(jsonb) (
    SFUNC = jsonb_concat,
    STYPE = jsonb 
);
COMMENT ON AGGREGATE pg_catalog.jsonb_concat_agg(jsonb)
    IS 'concatenate JSONB objects';
