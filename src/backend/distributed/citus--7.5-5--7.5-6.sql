/* citus--7.5-5--7.5-6 */

-- Don't want this to be available to non-superusers.
REVOKE ALL ON FUNCTION pg_catalog.citus_stat_statements_reset() FROM PUBLIC;
