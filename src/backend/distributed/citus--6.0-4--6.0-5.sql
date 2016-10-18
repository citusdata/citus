/*
 * Replace oid column in pg_dist_shard_placement with a sequence column.
 */
CREATE SEQUENCE citus.pg_dist_shard_placement_placementid_seq
    NO CYCLE;
ALTER SEQUENCE citus.pg_dist_shard_placement_placementid_seq
    SET SCHEMA pg_catalog;
ALTER TABLE pg_catalog.pg_dist_shard_placement
    ADD COLUMN placementid bigint;
-- keep existing oids, and update sequence to match max.
UPDATE pg_catalog.pg_dist_shard_placement SET placementid = oid;
ALTER TABLE pg_catalog.pg_dist_shard_placement
    ALTER COLUMN placementid SET DEFAULT nextval('pg_catalog.pg_dist_shard_placement_placementid_seq'),
    ALTER COLUMN placementid SET NOT NULL,
    SET WITHOUT OIDS;
CREATE UNIQUE INDEX pg_dist_shard_placement_placementid_index
ON pg_catalog.pg_dist_shard_placement using btree(placementid);
SELECT setval('pg_catalog.pg_dist_shard_placement_placementid_seq', max(placementid))
FROM pg_catalog.pg_dist_shard_placement;

CREATE FUNCTION master_get_new_placementid()
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_get_new_placementid$$;
COMMENT ON FUNCTION master_get_new_placementid()
    IS 'fetch unique placementid';

