CREATE OR REPLACE FUNCTION pg_catalog.citus_stats_tenants_storage (
    OUT colocation_id INT,
    OUT tenant_attribute TEXT,
    OUT storage_estimate INT
)
RETURNS SETOF record
LANGUAGE plpgsql
AS $function$
DECLARE
tn TEXT;
dc TEXT;
ci INT;
BEGIN
    FOR ci, tn, dc IN SELECT cts.colocation_id, cts.table_name, cts.distribution_column FROM citus_tables cts
    LOOP
        RETURN QUERY
        EXECUTE 'SELECT ' || ci || '::int, ' || dc || '::text, sum(pg_column_size(' || tn || '.*))::int FROM ' || tn || ' GROUP BY ' || dc;
    END LOOP;
END;
$function$;

CREATE OR REPLACE VIEW citus.citus_stats_tenants_storage AS
SELECT colocation_id, tenant_attribute, sum(storage_estimate) total_storage FROM pg_catalog.citus_stats_tenants_storage()
GROUP BY colocation_id, tenant_attribute
ORDER BY total_storage DESC;

ALTER VIEW citus.citus_stats_tenants_storage SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_stats_tenants_storage TO PUBLIC;
