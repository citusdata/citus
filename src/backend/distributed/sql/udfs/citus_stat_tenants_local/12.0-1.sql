CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_tenants_local_internal(
    return_all_tenants BOOLEAN DEFAULT FALSE,
    OUT colocation_id INT,
    OUT tenant_attribute TEXT,
    OUT read_count_in_this_period INT,
    OUT read_count_in_last_period INT,
    OUT query_count_in_this_period INT,
    OUT query_count_in_last_period INT,
    OUT cpu_usage_in_this_period DOUBLE PRECISION,
    OUT cpu_usage_in_last_period DOUBLE PRECISION,
    OUT score BIGINT)
RETURNS SETOF RECORD
LANGUAGE C
AS 'citus', $$citus_stat_tenants_local$$;

CREATE OR REPLACE FUNCTION pg_catalog.citus_stat_tenants_local(
    return_all_tenants BOOLEAN DEFAULT FALSE,
    OUT colocation_id INT,
    OUT tenant_attribute TEXT,
    OUT read_count_in_this_period INT,
    OUT read_count_in_last_period INT,
    OUT query_count_in_this_period INT,
    OUT query_count_in_last_period INT,
    OUT cpu_usage_in_this_period DOUBLE PRECISION,
    OUT cpu_usage_in_last_period DOUBLE PRECISION,
    OUT score BIGINT)
RETURNS SETOF RECORD
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        L.colocation_id,
        CASE WHEN L.tenant_attribute IS NULL THEN N.nspname ELSE L.tenant_attribute END COLLATE "default" as tenant_attribute,
        L.read_count_in_this_period,
        L.read_count_in_last_period,
        L.query_count_in_this_period,
        L.query_count_in_last_period,
        L.cpu_usage_in_this_period,
        L.cpu_usage_in_last_period,
        L.score
    FROM pg_catalog.citus_stat_tenants_local_internal(return_all_tenants) L
    LEFT JOIN pg_dist_schema S ON L.tenant_attribute IS NULL AND L.colocation_id = S.colocationid
    LEFT JOIN pg_namespace N ON N.oid = S.schemaid
    ORDER BY L.score DESC;
END;
$function$;

CREATE OR REPLACE VIEW pg_catalog.citus_stat_tenants_local AS
SELECT
    colocation_id,
    tenant_attribute,
    read_count_in_this_period,
    read_count_in_last_period,
    query_count_in_this_period,
    query_count_in_last_period,
    cpu_usage_in_this_period,
    cpu_usage_in_last_period
FROM pg_catalog.citus_stat_tenants_local()
ORDER BY score DESC;

REVOKE ALL ON FUNCTION pg_catalog.citus_stat_tenants_local_internal(BOOLEAN) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.citus_stat_tenants_local_internal(BOOLEAN) TO pg_monitor;

REVOKE ALL ON FUNCTION pg_catalog.citus_stat_tenants_local(BOOLEAN) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.citus_stat_tenants_local(BOOLEAN) TO pg_monitor;

REVOKE ALL ON pg_catalog.citus_stat_tenants_local FROM PUBLIC;
GRANT SELECT ON pg_catalog.citus_stat_tenants_local TO pg_monitor;
