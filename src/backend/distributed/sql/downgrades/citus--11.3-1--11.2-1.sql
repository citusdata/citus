-- citus--11.3-1--11.2-1
-- this is an empty downgrade path since citus--11.2-1--11.3-1.sql is empty for now

DROP VIEW pg_catalog.citus_stats_tenants;
DROP FUNCTION pg_catalog.citus_stats_tenants(boolean);
