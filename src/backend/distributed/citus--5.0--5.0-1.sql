/* citus--5.0--5.0-1.sql */

ALTER FUNCTION pg_catalog.citus_drop_trigger() SECURITY DEFINER;

GRANT SELECT ON pg_catalog.pg_dist_partition TO public;
GRANT SELECT ON pg_catalog.pg_dist_shard TO public;
GRANT SELECT ON pg_catalog.pg_dist_shard_placement TO public;
