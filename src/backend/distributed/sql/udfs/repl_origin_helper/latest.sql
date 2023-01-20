
CREATE OR REPLACE FUNCTION pg_catalog.replication_origin_session_start_no_publish()
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$replication_origin_session_start_no_publish$$;
COMMENT ON FUNCTION pg_catalog.replication_origin_session_start_no_publish()
    IS 'To start Replication origin session for skipping publishing WAL records';

CREATE OR REPLACE FUNCTION pg_catalog.replication_origin_session_end_no_publish()
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$replication_origin_session_end_no_publish$$;
COMMENT ON FUNCTION pg_catalog.replication_origin_session_end_no_publish()
    IS 'To finish Replication origin session for skipping publishing WAL records';

CREATE OR REPLACE FUNCTION pg_catalog.replication_origin_session_is_no_publish()
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$replication_origin_session_is_no_publish$$;
COMMENT ON FUNCTION pg_catalog.replication_origin_session_is_no_publish()
    IS 'To check if Replication origin session is currently active for skipping publishing WAL records';

