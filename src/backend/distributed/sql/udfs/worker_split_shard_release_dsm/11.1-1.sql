CREATE OR REPLACE FUNCTION pg_catalog.worker_split_shard_release_dsm()
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_split_shard_release_dsm$$;
COMMENT ON FUNCTION pg_catalog.worker_split_shard_release_dsm()
    IS 'Releases shared memory segment allocated by non-blocking split workflow';

REVOKE ALL ON FUNCTION pg_catalog.worker_split_shard_release_dsm() FROM PUBLIC;
