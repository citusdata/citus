DROP VIEW IF EXISTS pg_catalog.citus_dist_stat_activity;

CREATE OR REPLACE VIEW citus.citus_dist_stat_activity AS
SELECT * FROM citus_stat_activity
WHERE is_worker_query = false;

ALTER VIEW citus.citus_dist_stat_activity SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_dist_stat_activity TO PUBLIC;
