/* citus--5.0--5.0-1.sql */

CREATE FUNCTION pg_catalog.master_stage_shard_row(logicalrelid oid,
                                                  shardid bigint,
                                                  shardstorage "char",
                                                  shardminvalue text,
                                                  shardmaxvalue text)
    RETURNS VOID
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$master_stage_shard_row$$;
COMMENT ON FUNCTION pg_catalog.master_stage_shard_row(oid, bigint, "char", text, text)
    IS 'deprecated function to insert a row into pg_dist_shard';

CREATE FUNCTION pg_catalog.master_stage_shard_placement_row(shardid int8,
                                                            shardstate int4,
                                                            shardlength int8,
                                                            nodename text,
                                                            nodeport int4)
    RETURNS VOID
    STRICT
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$master_stage_shard_placement_row$$;
COMMENT ON FUNCTION pg_catalog.master_stage_shard_placement_row(int8, int4, int8, text, int4)
    IS 'deprecated function to insert a row into pg_dist_shard_placement';
