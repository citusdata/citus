CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_start_replication_origin_tracking()
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_internal_start_replication_origin_tracking$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_start_replication_origin_tracking()
    IS 'To start replication origin tracking for skipping publishing of duplicated events during internal data movements for CDC';

CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_stop_replication_origin_tracking()
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_internal_stop_replication_origin_tracking$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_stop_replication_origin_tracking()
    IS 'To stop replication origin tracking for skipping publishing of duplicated events during internal data movements for CDC';

CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_is_replication_origin_tracking_active()
RETURNS boolean
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_internal_is_replication_origin_tracking_active$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_is_replication_origin_tracking_active()
    IS 'To check if replication origin tracking is active for skipping publishing of duplicated events during internal data movements for CDC';
