-- citus--11.3-1--11.2-1
DROP FUNCTION pg_catalog.citus_internal_start_replication_origin_tracking();
DROP FUNCTION pg_catalog.citus_internal_stop_replication_origin_tracking();
DROP FUNCTION pg_catalog.citus_internal_is_replication_origin_tracking_active();
-- this is an empty downgrade path since citus--11.2-1--11.3-1.sql is empty for now
